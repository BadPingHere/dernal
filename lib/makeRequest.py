import requests, os, time, logging
from dotenv import load_dotenv
from threading import Lock
from pathlib import Path
import sqlite3

path = Path(__file__).resolve().parents[1] / '.env'
DBPATH = Path(__file__).resolve().parents[1] / "database" / "api_usage.db"
load_dotenv(path)
logger = logging.getLogger("discord")


rawKeys = os.getenv("KEYS", "") # get the csv-ified shit
keyList = [k.strip() for k in rawKeys.split(",") if k.strip()]
KEYS = {f"KEY_{i+1}": key for i, key in enumerate(keyList)}
KEYS["unauthenticated"] = None  # unauth, so we can get 50 more requests $$$$
#KEYS = {
#    "KEY_1": os.getenv("KEY_1"),
#    "KEY_2": os.getenv("KEY_2"),
#    "KEY_3": os.getenv("KEY_3"),
#    "unauthenticated": None  # unauth, so we can get 50 more requests $$$$
#}

ratelimitDict = {
    key: { # These are the only ones we use, we could add the rest but why would we...
        "/player/": {"remaining": 120 if key != "unauthenticated" else 50, "limit": 120 if key != "unauthenticated" else 50, "reset": 0},
        "/guild/": {"remaining": 120 if key != "unauthenticated" else 50, "limit": 120 if key != "unauthenticated" else 50, "reset": 0},
        "/leaderboard/": {"remaining": 120 if key != "unauthenticated" else 50, "limit": 120 if key != "unauthenticated" else 50, "reset": 0},
    } for key in KEYS
}

rateLock = Lock()

def trackUsage(route):
    try:
        conn = sqlite3.connect(DBPATH)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS api_usage (
                timestamp INTEGER,
                route TEXT,
                count INTEGER,
                PRIMARY KEY (timestamp, route)
            )
        """)
        timestamp = int(time.time() // 60 * 60)  # round to nearest mintue
        cur.execute("""
            INSERT INTO api_usage (timestamp, route, count)
            VALUES (?, ?, 1)
            ON CONFLICT(timestamp, route) DO UPDATE SET count = count + 1
        """, (timestamp, route)) # either updates or puts in the right count
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"Failed to track API usage for {route}: {e}")

def getRoute(url):
    if "/player" in url:
        return "/player/"
    elif "/guild" in url:
        return "/guild/"
    elif "/leaderboard" in url:
        return "/leaderboard/"
    return "/unknown/"  # fallback

def refreshRatelimit(route):
    now = int(time.time())
    with rateLock:
        for key in ratelimitDict:
            if now >= ratelimitDict[key][route]["reset"]:
                ratelimitDict[key][route]["remaining"] = ratelimitDict[key][route]["limit"]
                ratelimitDict[key][route]["reset"] = now + 60  # fallback if unknown

def updateHeaders(key, route, headers): #save what we have
    try:
        remaining = int(headers.get("ratelimit-remaining", ratelimitDict[key][route]["remaining"]))
        limit = int(headers.get("ratelimit-limit", ratelimitDict[key][route]["limit"]))
        reset = int(headers.get("ratelimit-reset", time.time() + 60))
        with rateLock:
            ratelimitDict[key][route] = {
                "remaining": remaining,
                "limit": limit,
                "reset": reset
            }
    except Exception as e:
        logger.error(f"Could not update ratelimit headers for {route}: {e}")

def pickKey(route): # return the first key with ratelimit remaining for that route left
    refreshRatelimit(route)
    with rateLock:
        for key in KEYS:
            if ratelimitDict[key][route]["remaining"] > 0:
                ratelimitDict[key][route]["remaining"] -= 1
                return key
            
    # we are out of requests somehow, now we wait until we arent
    soonest = min(ratelimitDict[k][route]["reset"] for k in KEYS)
    sleepNum = max(soonest - time.time(), 0)
    logger.warning(f"All keys ratelimited for {route}. Sleeping for {sleepNum:.2f}s...")
    time.sleep(sleepNum)
    return pickKey(route)

def getTotalRatelimitRemaining(route): # Check total ratelimit we have left
    with rateLock:
        return sum(ratelimitDict[key][route]["remaining"] for key in KEYS)

def ratelimitCheck(route): # Do our self-ratelimiting
    totalLimit = sum(ratelimitDict[key][route]["limit"] for key in KEYS)
    remaining = getTotalRatelimitRemaining(route)
    percent = remaining / totalLimit if totalLimit else 0

    if percent <= 0.10: # not really looking like sadam hussein hiding spot anymore
        time.sleep(0.75)
    elif percent <= 0.20:
        time.sleep(0.3)
    elif percent <= 0.33:
        time.sleep(0.1)

def makeRequest(url):
    session = requests.Session()
    session.trust_env = False
    retries = 0
    maxRetries = 5
    route = getRoute(url)

    while retries < maxRetries:
        try:
            keyName = pickKey(route)
            keyValue = KEYS[keyName]
            headers = {}
            if keyValue:
                headers["Authorization"] = f"Bearer {keyValue}"
            r = session.get(url, timeout=30, headers=headers)
            #print(r.headers)
            updateHeaders(keyName, route, r.headers)
            trackUsage(route)
            if r.status_code == 300:
                jsonData = r.json()
                if route == "/guild/": # TODO: FIX!
                    objects = jsonData.get("objects", {})

                    firstUUID = next(iter(objects))
                    if not firstUUID:
                        logger.warning(f"No UUID for objects {objects} in 300 response.")
                        return False, None

                    return makeRequest(f"https://api.wynncraft.com/v3/player/{firstUUID}?fullResult")
                elif route == "/player/":
                    objects = jsonData.get("objects", {})

                    firstUUID = next(iter(objects))
                    if not firstUUID:
                        logger.warning(f"No UUID for objects {objects} in 300 response.")
                        return False, None

                    return makeRequest(f"https://api.wynncraft.com/v3/player/{firstUUID}?fullResult")
                
            elif r.status_code >= 400:
                r.raise_for_status() # bad requests send to hell

            ratelimitCheck(route)
            return True, r
        except requests.exceptions.RequestException as err: # hell
            status = getattr(err.response, 'status_code', None)
            retryable = [408, 425, 500, 502, 503, 504, 429]
            if status in retryable:
                if status == 429:  # Specifically handle rate limits
                    retry = 60  # Default fallback
                    if hasattr(err, 'response') and err.response is not None:
                        # Try to get Retry-After header
                        #print(err.response.headers)
                        retry = err.response.headers.get('ratelimit-reset')
                        if retry:
                            try:
                                retry_after = float(retry)
                            except ValueError:
                                pass
                    with rateLock:
                        ratelimitDict[keyName][route]["remaining"] = 0
                        ratelimitDict[keyName][route]["reset"] = time.time() + retry_after
                    
                    #logger.warning(f"Key {keyName} rate limited on {route}. Reset in {retry_after}s")
                else:
                    logger.warning(f"{url} failed with {status}. Retry {retries + 1}/{maxRetries}")
                retries += 1
                time.sleep(1)
                continue
            else:
                logger.error(f"Request error {status}: {err}")
                return False, {}

    logger.error(f"Failed after {maxRetries} retries for {url}")
    return False, {}
