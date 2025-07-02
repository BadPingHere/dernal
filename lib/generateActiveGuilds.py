# This script will on average take 2+ hours! Have fun!

import requests
from alive_progress import alive_bar
import time
import csv

def makeRequest(url): # the world is built on nested if else statements.
    session = requests.Session()
    session.trust_env = False
    apiSwapDict = { # ts pmo icl; also we'd like to use nori as a choice, but with player db using nori, it simply is too straining since we cant know the rate limit.
        "https://api.wynncraft.com/v3/guild/list/territory{}": "https://api.wynncraft.com/v3/guild/list/territory{}", # they call me the goat of hacks.
        "https://api.wynncraft.com/v3/guild/uuid/{}": "https://api.wynncraft.com/v3/guild/uuid/{}", # wait, i got one more in me
        #"https://api.wynncraft.com/v3/guild/prefix/{}": "https://nori.fish/api/guild/{}",
        #"https://api.wynncraft.com/v3/guild/{}": "https://nori.fish/api/guild/{}",
        # "https://api.wynncraft.com/v3/player/{}": "https://nori.fish/api/player/{}", Currently i'd like to save this for player activity sql
    }
        
    usingWynnAPI = True  # Default to official
    for wynn, nori in apiSwapDict.items():
        wynnPrefix = wynn.split("{}")[0] # just the beginning of url before {}
        if url.startswith(wynnPrefix):
            suffix = url[len(wynnPrefix):]  # Extract suffix from official URL
            url = nori.format(suffix)
            usingWynnAPI = False
            break

    retries = 0
    maxRetries = 5

    while retries < maxRetries:
        try:
            r = session.get(url, timeout=30)
            # Nori currently doesnt support multiple responses, itll just go code 500
            if r.status_code == 300: # they say we all got multiple choices in life. be a dog or get pissed on.
                if "/guild/" in url: # In guild endpoint, just select the first option.
                    jsonData = r.json()
                    prefix = jsonData[next(iter(jsonData))]["prefix"]
                    success, r = makeRequest(f"https://api.wynncraft.com/v3/guild/prefix/{prefix}")
                    return success, r
                if "/player/" in url: # In player endpoint, we should select the recently active one, but I dont care! we select the last one.
                    jsonData = r.json()
                    username = jsonData[list(jsonData)[-1]]["storedName"]
                    success, r = makeRequest(f"https://api.wynncraft.com/v3/player/{username}")   
                    return success, r

            elif r.status_code >= 400:
                raise requests.exceptions.HTTPError(request=r) # we send the bad requests to hell
            else:
                if usingWynnAPI:
                    remaining = int(r.headers.get("ratelimit-remaining", 120)) # if we cant get it, act like its 120
                    if remaining < 12: # theyre saying that this lowkey look like saddam hussein hiding spot
                        print("We are under 12. PANIC!!")
                        time.sleep(2)
                    elif remaining < 30:
                        print("We are under 30. PANIC!!")
                        time.sleep(0.75)
                    elif remaining < 60:
                        time.sleep(0.25)
                else: # Nori api doesnt have ratelimit headers yet, but we know ratelimits are usually 3/s
                    if "API rate limit exceeded" in str(r.json()): # Nori doesnt like to tell us when we've hit our limit, so we gotta infer
                        retries += 1
                        time.sleep(2)
                        continue
                    time.sleep(0.6) 
                return True, r
        except requests.exceptions.RequestException as err:
            status = getattr(err.response, 'status_code', None)
            retryable = [408, 425, 429, 500, 502, 503, 504]
            if status in retryable: # if its retryable, retry. idk why i had to make this comment.
                print(f"{url} failed with {status}. Current retry is at {retries}.")
                retries += 1
                time.sleep(2)
                continue
            else:
                print(f"Non-retryable error {status} for {url}: {err}")
                return False, {} 

    print(f"Max retries exceeded for {url}")
    return False, {} 

def main():
    suitableGuilds = []
    success, r = makeRequest("https://api.wynncraft.com/v3/guild/list/guild")
    if not success:
        print("Error getting guild list. Try again later.")
    jsonData = r.json()
    countOfGuilds = len(jsonData)
    with alive_bar(countOfGuilds) as bar:
        for prefix, guild_info in jsonData.items():
            uuid = guild_info["uuid"]
            success, r = makeRequest(f"https://api.wynncraft.com/v3/guild/uuid/{uuid}")
            if not success:
                print(f"Unknown guild uuid, skipping: {uuid}")
                continue
            jsonData = r.json()
            if int(jsonData["level"]) > 30 and int(jsonData["members"]["total"]) > 15: # so we can thin the herd. because storage will be a BITCH, along with api requests.
                suitableGuilds.append(uuid)
            bar()
            time.sleep(4)
    print(countOfGuilds, len(suitableGuilds))
    with open('guildlist.csv', 'w', newline='') as myfile:
        wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
        for guild in suitableGuilds:
            wr.writerow([guild])  # Write each UUID in a separate row
            
main()