import uvicorn
from fastapi import FastAPI, APIRouter, HTTPException, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import sqlite3
import ast
from pathlib import Path
import json
import time
from datetime import timedelta, datetime
from functools import wraps
from cachetools import TTLCache
import hashlib
from collections import Counter, defaultdict
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
from lib.makeRequest import makeRequest, internalMakeRequest
import shelve
import os
import matplotlib.cm as cm
import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt
import io
from matplotlib.dates import HourLocator, DateFormatter, AutoDateLocator
from datetime import timezone
import base64


#TODO: Some bugs I ran into during testing:
# 1. /guild activity graid with no recent data (like on dev) will result in a hanging command, same with /player activty graids (likely the same for all the other database related onces, but i wont worry about that.)


timeframeMap1 = { # Used for heatmap data
    "Season 24": ("04/18/25", "06/01/25"),
    "Season 25": ("06/06/25", "07/20/25"),
    "Season 26": ("07/25/25", "09/14/25"),
    "Season 27": ("09/19/25", "11/02/25"), 
    "Last 7 Days": None, # gotta handle ts outta dict
    "Everything": None
}

timeframeMap2 = { # Used for graid data, note to update it in api
    "Season 25": ("06/06/25", "07/20/25"),
    "Season 26": ("07/25/25", "09/14/25"),
    "Season 27": ("09/19/25", "11/02/25"), 
    "Last 14 Days": None, # gotta handle ts outta dict
    "Last 7 Days": None, # gotta handle ts outta dict
    "Last 24 Hours": None, # gotta handle ts outta dict
    "Everything": None
}

app = FastAPI(title="Dernal API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

route_cache = TTLCache(maxsize=200, ttl=300)  # default 5 min
searchRouter = APIRouter(prefix="/api/search", tags=["Search"])
graidRouter = APIRouter(prefix="/api/graid", tags=["Guild Raids"])
leaderboardRouter = APIRouter(prefix="/api/leaderboard", tags=["Leaderboard"])
activityRouter = APIRouter(prefix="/api/activity", tags=["Activity"])
mapRouter = APIRouter(prefix="/api/map", tags=["Maps"])

GUILDDBPATH = Path(__file__).resolve().parents[1] / "database" / "guild_activity.db"
PLAYERDBPATH = Path(__file__).resolve().parents[1] / "database" / "player_activity.db"
GRAIDDBPATH = Path(__file__).resolve().parents[1] / "database" / "graid.db"
TERRITORIESPATH = Path(__file__).resolve().parents[1] / "lib" /  "documents" / "territories.json"
rootDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
territoryFilePath = os.path.join(rootDir, 'database', 'territory')

sns.set_style("whitegrid")
mpl.use('Agg') # Backend without any gui popping up
blue, = sns.color_palette("muted", 1)

def mapCreator():
    map_img = Image.open("lib/documents/main-map.png").convert("RGBA")
    font = ImageFont.truetype("lib/documents/arial.ttf", 40)
    territoryCounts = defaultdict(int)
    namePrefixMap = {}

    def coordToPixel(x, z):
        return x + 2383, z + 6572 # if only wynntils was ACCURATE!!!

    with open(TERRITORIESPATH, "r") as f:
        local_territories = json.load(f)
    success, r = internalMakeRequest("https://athena.wynntils.com/cache/get/guildList")
    color_map = {
        g["prefix"]: g.get("color", "#FFFFFF")
        for g in r.json()
        if g.get("prefix")}

    # get territory data
    success, r = makeRequest("https://api.wynncraft.com/v3/guild/list/territory")
    territory_data = r.json()

    overlay = Image.new("RGBA", map_img.size)
    overlay_draw = ImageDraw.Draw(overlay)
    draw = ImageDraw.Draw(map_img)

    # Loops through all territories, 
    for name, data in local_territories.items():
        if "Trading Routes" not in data: #shouldnt happen but 
            continue
        try:
            x1 = (data["Location"]["start"][0] + data["Location"]["end"][0]) // 2
            z1 = (data["Location"]["start"][1] + data["Location"]["end"][1]) // 2
            px1, py1 = coordToPixel(x1, z1)
        except KeyError:
            continue

        for destinationName in data["Trading Routes"]:
            destData = local_territories.get(destinationName)
            if not destData: # Shouldnt happen but
                continue
            try:
                x2 = (destData["Location"]["start"][0] + destData["Location"]["end"][0]) // 2
                z2 = (destData["Location"]["start"][1] + destData["Location"]["end"][1]) // 2
                px2, py2 = coordToPixel(x2, z2)
            except KeyError:
                continue

            draw.line([(px1, py1), (px2, py2)], fill=(10, 10, 10), width=5) # lines are not fully black
    for name, info in territory_data.items():
        try:
            startX, startZ = info["location"]["start"]
            endX, endZ = info["location"]["end"]
            prefix = info["guild"]["prefix"]
        except (KeyError, TypeError):
            continue

        color_hex = color_map.get(prefix, "#FFFFFF")
        try:
            color_rgb = tuple(int(color_hex[i:i+2], 16) for i in (1, 3, 5))
        except:
            color_rgb = (255, 255, 255)

        x1, y1 = coordToPixel(startX, startZ)
        x2, y2 = coordToPixel(endX, endZ)
        xMin, xMax = sorted([x1, x2])
        yMin, yMax = sorted([y1, y2])

        overlay_draw.rectangle([xMin, yMin, xMax, yMax], fill=(*color_rgb, 64)) # Draws the inside with the opacity
        draw.rectangle([xMin, yMin, xMax, yMax], outline=color_rgb, width=8) # Draws border of territory

        bbox = font.getbbox(prefix)
        text_w = bbox[2] - bbox[0]
        text_h = bbox[3] - bbox[1]
        text_x = (xMin + xMax) // 2 - text_w // 2
        text_y = (yMin + yMax) // 2 - text_h // 2

        # Adds the black outline to the text
        for dx in (-2, 0, 2):
            for dy in (-2, 0, 2):
                if dx or dy:
                    draw.text((text_x + dx, text_y + dy), prefix, font=font, fill="black")

        draw.text((text_x, text_y), prefix, font=font, fill=color_rgb)

    for info in territory_data.values():
        try: 
            prefix = info["guild"]["prefix"]
            name = info["guild"]["name"]
            territoryCounts[prefix] += 1
            namePrefixMap[prefix] = name
        except (KeyError, TypeError):
            continue
    leaderboardGuilds = sorted(territoryCounts.items(), key=lambda x: x[1], reverse=True)
    legendLines = [
        f"{i+1}. {namePrefixMap[prefix]} ({prefix}) - {count} Territories"
        for i, (prefix, count) in enumerate(leaderboardGuilds)
    ]

    legendPadding = 20
    lineHeight = font.getbbox("Hg")[3] - font.getbbox("Hg")[1] + 10
    # Bottom left
    boxX = 50
    boxY = map_img.height - (lineHeight * len(legendLines) + legendPadding * 2) - 50

    for i, (prefix, count) in enumerate(leaderboardGuilds):
        color_hex = color_map.get(prefix, "#FFFFFF")
        try:
            text_color = tuple(int(color_hex[i:i+2], 16) for i in (1, 3, 5))
        except:
            text_color = (255, 255, 255)
        
        text = f"{i+1}. {namePrefixMap[prefix]} ({prefix}) - {count} Territories"
        draw.text((boxX + legendPadding, boxY + legendPadding + i * lineHeight), text, font=font, fill=text_color)
    
    # Blend the full overlay just once
    mapImg = Image.alpha_composite(map_img, overlay)
    scale_factor = 0.4
    new_size = (int(mapImg.width * scale_factor), int(mapImg.height * scale_factor))
    mapImg = mapImg.resize(new_size, Image.LANCZOS)
    mapBytes = BytesIO()
    mapImg.save(mapBytes, format='PNG', optimize=True, compress_level=5)
    mapBytes.seek(0)
    return Response(content=mapBytes.getvalue(), media_type="image/png")

def heatmapCreator(timeframe):
    if timeframe == "Last 7 Days": # We handle it.
        endDate = datetime.now()
        startDate = endDate - timedelta(days=7)
    elif timeframe != "Everything": # we deal with everything later on
        startDay, endDay = timeframeMap1.get(timeframe, (None, None))
        startDate = datetime.strptime(startDay, "%m/%d/%y")
        endDate = datetime.strptime(endDay, "%m/%d/%y")
    map_img = Image.open("lib/documents/main-map.png").convert("RGBA")
    def coordToPixel(x, z):
        return x + 2383, z + 6572 # if only wynntils was ACCURATE!!!

    with shelve.open(territoryFilePath) as territoryStorage:
        historicalTerritories = territoryStorage.get("historicalTerritories", {})
    success, r = makeRequest("https://api.wynncraft.com/v3/guild/list/territory")
    territory_data = r.json()
    activityCount = defaultdict(int)
    if timeframe == "Everything": # add it all
        for day in historicalTerritories.values():
            for territory, count in day.items():
                activityCount[territory] += count
    else:
        for date, data in historicalTerritories.items():
            fullDate = datetime.strptime(date + f"/{datetime.now().year}", "%m/%d/%Y")
            if startDate <= fullDate <= endDate: # Check if its between our area
                for territory, count in data.items():
                    activityCount[territory] += count
    #logger.info(activityCount)
    maxCount = max(activityCount.values(), default=1)

    def heatToColor(heat): # I'd like to make this better in the future
        r, g, b, _ = [int(255 * c) for c in cm.seismic(heat)]
        return (r, g, b)

    overlay = Image.new("RGBA", map_img.size)
    overlay_draw = ImageDraw.Draw(overlay)
    for name, info in territory_data.items():
        try:
            startX, startZ = info["location"]["start"]
            endX, endZ = info["location"]["end"]
        except (KeyError, TypeError):
            continue

        switchCount = activityCount.get(name, 0)
        heat = switchCount / maxCount if maxCount else 0
        color = heatToColor(heat)
        alpha = int(64 + 191 * heat) if switchCount > 0 else 128 # my genius is frightening

        x1, y1 = coordToPixel(startX, startZ)
        x2, y2 = coordToPixel(endX, endZ)
        xMin, xMax = sorted([x1, x2])
        yMin, yMax = sorted([y1, y2])
        overlay_draw.rectangle([xMin, yMin, xMax, yMax], fill=(*color, alpha))
    mapImg = Image.alpha_composite(map_img, overlay)
    mapBytes = BytesIO()
    mapImg.save(mapBytes, format='PNG', optimize=True, compress_level=5)
    mapBytes.seek(0)

    scale_factor = 0.4
    new_size = (int(mapImg.width * scale_factor), int(mapImg.height * scale_factor))
    mapImg = mapImg.resize(new_size, Image.LANCZOS)
    mapBytes = BytesIO()
    mapImg.save(mapBytes, format='PNG', optimize=True, compress_level=5)
    mapBytes.seek(0)
    return Response(content=mapBytes.getvalue(), media_type="image/png")

async def searchMaster(field, value):
    conn = connectDB("guild")
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM guilds WHERE {field} = ? COLLATE NOCASE", (value,))
    
    guild = cursor.fetchone()
    if not guild:
        conn.close()
        return JSONResponse(status_code=404, content={"error": "Guild not found"})

    data = dict(guild)
    cursor.execute("""
        SELECT level, xp_percent, territories, wars, online_members, total_members, timestamp
        FROM guild_snapshots
        WHERE guild_uuid = ?
        ORDER BY timestamp DESC
        LIMIT 1
    """, (data["uuid"],))
    snapshot = cursor.fetchone()
    if snapshot:
        data["latest_snapshot"] = dict(snapshot)
    else:
        data["latest_snapshot"] = None

    conn.close()
    return data

def connectDB(database):
    if database == "guild":
        conn = sqlite3.connect(GUILDDBPATH)
        conn.row_factory = sqlite3.Row
        return conn
    if database == "player":
        conn = sqlite3.connect(PLAYERDBPATH)
        conn.row_factory = sqlite3.Row
        return conn
    if database == "graid":
        conn = sqlite3.connect(GRAIDDBPATH)
        conn.row_factory = sqlite3.Row
        return conn

def getTimeframe(timeframe, type="normal"):
    if type == "normal":
        # This is for leaderboard commands which use the database
        endDate = datetime.now()
        if timeframe == "Last 14 Days":
            startDate = endDate - timedelta(days=14)
        elif timeframe == "Last 7 Days":
            startDate = endDate - timedelta(days=7)
        elif timeframe == "Last 3 Days":
            startDate = endDate - timedelta(days=3)
        elif timeframe == "Last 24 Hours":
            startDate = endDate - timedelta(hours=24)
        elif timeframe == "Everything":
            startDate = None
            endDate = None
        else: # fallback area
            startDate = None
            endDate = None
        return startDate, endDate
    elif type == "special":
        # guildLeaderboardOnlineButGuildSpecific uses this for the day calculation so we just run it like that
        endDate = datetime.now()
        if timeframe == "Last 14 Days":
            startDate = endDate - timedelta(days=14)
            days = 14.0
        elif timeframe == "Last 7 Days":
            startDate = endDate - timedelta(days=7)
            days = 7.0
        elif timeframe == "Last 3 Days":
            startDate = endDate - timedelta(days=3)
            days = 3.0
        elif timeframe == "Last 24 Hours":
            startDate = endDate - timedelta(hours=24)
            days = 1.0
        elif timeframe == "Everything":
            startDate = endDate - timedelta(days=90) # Moreso so we get evetything
            days = 30.0
        return startDate, endDate, days
    elif type == "graid": # Accounts for seasons
        if timeframe == "Last 14 Days":
            endDate = datetime.now()
            startDate = endDate - timedelta(days=14)
        elif timeframe == "Last 7 Days":
            endDate = datetime.now()
            startDate = endDate - timedelta(days=7)
        elif timeframe == "Last 24 Hours":
            endDate = datetime.now()
            startDate = endDate - timedelta(hours=24)
        elif timeframe == "Everything":
            startDate = None
            endDate = None
        else:  # A season
            startDay, endDay = timeframeMap2.get(timeframe, (None, None))
            if startDay and endDay:
                startDate = datetime.strptime(startDay, "%m/%d/%y")
                endDate = datetime.strptime(endDay, "%m/%d/%y")
            else:
                # fallback
                startDate = None
                endDate = None
        return startDate, endDate
    elif type == "specialGraid": # Shitty ass 1 command uses this and Id rather get this done than rework it rn
        if timeframe == "Last 14 Days":
            cutoff = time.time() - (14*86400)
        elif timeframe == "Last 7 Days":
            cutoff = time.time() - (7*86400)
        elif timeframe == "Last 24 Hours":
            cutoff = time.time() - (1*86400)
        elif timeframe == "Everything":
            cutoff = 0
        else:  # fallback to 14 days
            cutoff = time.time() - (14*86400)
        return cutoff

def cache_route(ttl=None):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            key_data = {
                "func": func.__name__,
                "args": args,
                "kwargs": kwargs
            }
            key_str = json.dumps(key_data, sort_keys=True, default=str)
            key = hashlib.sha256(key_str.encode()).hexdigest()

            cached = route_cache.get(key)
            if cached:
                result, expiry = cached
                if isinstance(result, Response):
                    result.headers["X-Cache"] = "HIT"
                    result.headers["X-Cache-Expires"] = expiry.isoformat() + "Z"
                return result

            result = await func(*args, **kwargs)

            expiry_time = datetime.utcnow() + timedelta(seconds=ttl or route_cache.ttl)
            route_cache[key] = (result, expiry_time)

            if isinstance(result, Response):
                result.headers["X-Cache"] = "MISS"
                result.headers["X-Cache-Expires"] = expiry_time.isoformat() + "Z"

            return result
        return wrapper
    return decorator

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.perf_counter()
    response = await call_next(request)
    process_time = time.perf_counter() - start_time
    response.headers["X-Process-Time"] = f"{process_time:.4f}s"
    print(f"{request.url.path} took {process_time:.4f}s")
    return response

@searchRouter.get("/prefix/{prefix}") 
@cache_route(ttl=600) #10m cache
async def search_prefix(prefix: str):
    if not prefix or len(prefix) > 4:
        return JSONResponse(status_code=400, content={"error": "Please provide a valid prefix."})
    
    return await searchMaster("prefix", prefix)

@searchRouter.get("/uuid/{uuid}") 
@cache_route(ttl=600) #10m cache
async def search_UUID(uuid: str):
    if not uuid:
        return JSONResponse(status_code=400, content={"error": "Please provide a valid UUID."})
    
    return await searchMaster("uuid", uuid)
    
@searchRouter.get("/name/{name}") 
@cache_route(ttl=600) #10m cache
async def search_name(name: str):
    if not name:
        return JSONResponse(status_code=400, content={"error": "Please provide a valid guild name."})
    
    return await searchMaster("name", name)

@searchRouter.get("/username/{username}") 
@cache_route(ttl=600) #10m cache
async def search_username(username: str):
    if not username:
        return JSONResponse(status_code=400, content={"error": "Please provide a valid username."})
    
    conn = connectDB("player")
    cursor = conn.cursor()
    cursor.execute("SELECT username, uuid, online, firstJoin, lastJoin, playtime, guildUUID, timestamp FROM users WHERE username = ? COLLATE NOCASE", (username,))
    
    player = cursor.fetchone()
    if not player:
        conn.close()
        return JSONResponse(status_code=404, content={"error": "Player not found"})

    data = dict(player)

    cursor.execute("""
        SELECT wars, totalLevel, killedMobs, chestsFound, totalDungeons, dungeonsDict, totalRaids, raidsDict, completedQuests
        FROM users_global
        WHERE uuid = ?
        ORDER BY timestamp DESC
        LIMIT 1
    """, (data["uuid"],))
    snapshot = cursor.fetchone()
    if snapshot:
        temp = dict(snapshot)
        for key in ("dungeonsDict", "raidsDict"):
            if temp.get(key):
                try:
                    temp[key] = ast.literal_eval(temp[key])
                except (ValueError, SyntaxError):
                    temp[key] = None
        data["globalData"] = temp
    else:
        data["globalData"] = None

    cursor.execute("""
        SELECT wars, totalLevel, killedMobs, chestsFound, totalDungeons, dungeonsDict, totalRaids, raidsDict, completedQuests
        FROM users_global
        WHERE uuid = ?
        ORDER BY timestamp DESC
        LIMIT 1
    """, (data["uuid"],))
    snapshot = cursor.fetchone()

    conn.close()
    return data

@graidRouter.get("/eligible")
@cache_route(ttl=3600) #1hr cache
async def eligible():
    conn = connectDB("graid")
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM graid_data WHERE key = 'EligibleGuilds'")
    row = cursor.fetchone()
    conn.close()

    if not row:
        return JSONResponse(status_code=404, content={"error": "Eligible guilds not found"})

    guilds = json.loads(row[0])
    return {"guilds": guilds}

@graidRouter.get("/completions")
@cache_route(ttl=300) #5m cache
async def completions():
    conn = connectDB("graid")
    cursor = conn.cursor()
    cursor.execute("SELECT value AS guilds FROM graid_data WHERE key = 'guild_raids'")
    
    row = cursor.fetchone()
    if not row:
        conn.close()
        return JSONResponse(status_code=404, content={"error": "Eligible guilds not found"})

    conn.close()
    data = json.loads(row[0])
    return data
    
@leaderboardRouter.get("/{leaderboardType}")
@cache_route(ttl=600) #10m cache
async def leaderboard(leaderboardType: str, timeframe: str | None = None, uuid: str | None = None):
    if not leaderboardType:
        return JSONResponse(status_code=400, content={"error": "Please provide a valid leaderboard type."})
    guildConn = connectDB("guild")
    playerConn = connectDB("player")
    guildCursor = guildConn.cursor()
    playerCursor = playerConn.cursor()
    if timeframe: # For commands which use it
        startDate, endDate = getTimeframe(timeframe)
    match leaderboardType:
        case "guildLeaderboardOnlineMembers":
            query = """
            WITH avg_online_members AS (
                SELECT 
                    g.name as guild_name,
                    g.prefix as guild_prefix,
                    g.uuid as guild_uuid,
                    ROUND(AVG(gs.online_members), 2) as avg_online_members,
                    COUNT(gs.id) as snapshot_count
                FROM guilds g
                JOIN guild_snapshots gs ON g.uuid = gs.guild_uuid
            """
            params = []
            if startDate and endDate:
                query += " WHERE gs.timestamp BETWEEN ? AND ? "
                params.extend([
                    startDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    endDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                ])

            query += """
                GROUP BY g.uuid, g.name, g.prefix
                HAVING snapshot_count > 0
            )
            SELECT 
                CASE 
                    WHEN guild_prefix IS NOT NULL THEN guild_name || ' (' || guild_prefix || ')'
                    ELSE guild_name 
                END as guild_display_name,
                avg_online_members,
                snapshot_count
            FROM avg_online_members
            ORDER BY avg_online_members DESC
            LIMIT 100;
            """

            guildCursor.execute(query, params)
            data = guildCursor.fetchall()
        
        case "guildLeaderboardTotalMembers":
            guildCursor.execute("""
            WITH recent_guild_stats AS (
                SELECT 
                    g.name as guild_name,
                    g.prefix as guild_prefix,
                    g.uuid as guild_uuid,
                    gs.total_members,
                    gs.online_members,
                    gs.timestamp,
                    ROW_NUMBER() OVER (PARTITION BY g.uuid ORDER BY gs.timestamp DESC) as rn
                FROM guilds g
                JOIN guild_snapshots gs ON g.uuid = gs.guild_uuid
                WHERE gs.timestamp >= datetime('now', '-3 hour')
            )
            SELECT 
                CASE 
                    WHEN guild_prefix IS NOT NULL THEN guild_name || ' (' || guild_prefix || ')'
                    ELSE guild_name 
                END as guild_display_name,
                total_members,
                datetime(timestamp) as last_updated
            FROM recent_guild_stats
            WHERE rn = 1
            AND total_members > 0
            ORDER BY total_members DESC
            LIMIT 100;
            """)
            data = guildCursor.fetchall()

        case "guildLeaderboardWars":
            query = """
                WITH war_gains AS (
                    SELECT 
                        g.name as guild_name,
                        g.prefix as guild_prefix,
                        g.uuid as guild_uuid,
                        MAX(gs.wars) - MIN(gs.wars) as wars_gained,
                        COUNT(gs.id) as snapshot_count
                    FROM guilds g
                    JOIN guild_snapshots gs ON g.uuid = gs.guild_uuid
            """
            params = []
            if startDate and endDate:
                query += " WHERE gs.timestamp BETWEEN ? AND ? "
                params.extend([
                    startDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    endDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                ])

            query += """
                    GROUP BY g.uuid, g.name, g.prefix
                    HAVING snapshot_count > 0
                )
                SELECT 
                    CASE 
                        WHEN guild_prefix IS NOT NULL THEN guild_name || ' (' || guild_prefix || ')'
                        ELSE guild_name 
                    END as guild_display_name,
                    wars_gained
                FROM war_gains
                ORDER BY wars_gained DESC
                LIMIT 100;
            """

            guildCursor.execute(query, params)
            data = guildCursor.fetchall()
        
        case "guildLeaderboardXP":
            if startDate and endDate:
                # For specific timeframes, use the complex difference calculation
                query = """
                    WITH time_bounds AS (
                        SELECT 
                            guild_uuid,
                            member_uuid,
                            MIN(timestamp) as min_time,
                            MAX(timestamp) as max_time
                        FROM member_snapshots
                        WHERE timestamp BETWEEN ? AND ?
                        GROUP BY guild_uuid, member_uuid
                    ),
                    contribution_changes AS (
                        SELECT 
                            t.guild_uuid,
                            t.member_uuid,
                            COALESCE(
                                (SELECT contribution 
                                FROM member_snapshots 
                                WHERE guild_uuid = t.guild_uuid 
                                AND member_uuid = t.member_uuid 
                                AND timestamp = t.max_time
                                ) -
                                (SELECT contribution 
                                FROM member_snapshots 
                                WHERE guild_uuid = t.guild_uuid 
                                AND member_uuid = t.member_uuid 
                                AND timestamp = t.min_time
                                ), 0
                            ) as xp_gained
                        FROM time_bounds t
                    ),
                    guild_totals AS (
                        SELECT 
                            g.uuid as guild_uuid,
                            g.name || ' (' || COALESCE(g.prefix, '') || ')' as guild_name,
                            SUM(c.xp_gained) as xp_gained
                        FROM contribution_changes c
                        JOIN guilds g ON g.uuid = c.guild_uuid
                        GROUP BY g.uuid, g.name, g.prefix
                        HAVING SUM(c.xp_gained) > 0
                    )
                    SELECT
                        guild_name,
                        xp_gained,
                        RANK() OVER (ORDER BY xp_gained DESC) as rank
                    FROM guild_totals
                    ORDER BY xp_gained DESC
                    LIMIT 100;
                """
                params = [
                    startDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    endDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                ]
            else:
                # For "Everything", use a more efficient approach with window functions
                query = """
                    WITH latest_contributions AS (
                        SELECT 
                            guild_uuid,
                            member_uuid,
                            contribution,
                            ROW_NUMBER() OVER (PARTITION BY guild_uuid, member_uuid ORDER BY timestamp DESC) as rn
                        FROM member_snapshots
                    ),
                    filtered_contributions AS (
                        SELECT 
                            guild_uuid,
                            member_uuid,
                            contribution
                        FROM latest_contributions
                        WHERE rn = 1
                    ),
                    guild_totals AS (
                        SELECT 
                            g.uuid as guild_uuid,
                            g.name || ' (' || COALESCE(g.prefix, '') || ')' as guild_name,
                            SUM(fc.contribution) as total_contribution
                        FROM filtered_contributions fc
                        JOIN guilds g ON g.uuid = fc.guild_uuid
                        GROUP BY g.uuid, g.name, g.prefix
                        HAVING SUM(fc.contribution) > 0
                    )
                    SELECT
                        guild_name,
                        total_contribution as xp_gained,
                        RANK() OVER (ORDER BY total_contribution DESC) as rank
                    FROM guild_totals
                    ORDER BY total_contribution DESC
                    LIMIT 100;
                """
                params = []
            guildCursor.execute(query, params)
            data = guildCursor.fetchall()
        
        case "playerLeaderboardRaids":
            if startDate and endDate:
                # For timeframes, we need to calculate the difference in raids
                query = """
                WITH time_bounds AS (
                    SELECT 
                        uuid,
                        MIN(timestamp) as min_time,
                        MAX(timestamp) as max_time
                    FROM users_global
                    WHERE timestamp BETWEEN ? AND ?
                    GROUP BY uuid
                ),
                raid_changes AS (
                    SELECT 
                        t.uuid,
                        COALESCE(
                            (SELECT totalRaids 
                            FROM users_global 
                            WHERE uuid = t.uuid 
                            AND timestamp = t.max_time
                            ) -
                            (SELECT totalRaids 
                            FROM users_global 
                            WHERE uuid = t.uuid 
                            AND timestamp = t.min_time
                            ), 0
                        ) as raids_gained
                    FROM time_bounds t
                ),
                player_totals AS (
                    SELECT 
                        r.uuid,
                        u.username,
                        r.raids_gained
                    FROM raid_changes r
                    JOIN users_global u ON u.uuid = r.uuid
                    WHERE u.timestamp = (SELECT MAX(timestamp) FROM users_global WHERE uuid = r.uuid)
                    AND r.raids_gained > 0
                )
                SELECT 
                    username,
                    raids_gained
                FROM player_totals
                ORDER BY raids_gained DESC
                LIMIT 100;
                """
                params = [
                    startDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    endDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                ]
            else:
                # For "Everything" or fallback, use the original query
                query = """
                SELECT username, totalRaids
                FROM (
                    SELECT *
                    FROM users_global
                    WHERE (uuid, timestamp) IN (
                        SELECT uuid, MAX(timestamp)
                        FROM users_global
                        GROUP BY uuid
                    )
                )
                ORDER BY totalRaids DESC
                LIMIT 100;
                """
                params = []
            
            playerCursor.execute(query, params)
            data = playerCursor.fetchall()
        
        case "playerLeaderboardDungeons":
            if startDate and endDate:
                # For timeframes, we need to calculate the difference in dungeons
                query = """
                WITH time_bounds AS (
                    SELECT 
                        uuid,
                        MIN(timestamp) as min_time,
                        MAX(timestamp) as max_time
                    FROM users_global
                    WHERE timestamp BETWEEN ? AND ?
                    GROUP BY uuid
                ),
                dungeon_changes AS (
                    SELECT 
                        t.uuid,
                        COALESCE(
                            (SELECT totalDungeons 
                            FROM users_global 
                            WHERE uuid = t.uuid 
                            AND timestamp = t.max_time
                            ) -
                            (SELECT totalDungeons 
                            FROM users_global 
                            WHERE uuid = t.uuid 
                            AND timestamp = t.min_time
                            ), 0
                        ) as dungeons_gained
                    FROM time_bounds t
                ),
                player_totals AS (
                    SELECT 
                        d.uuid,
                        u.username,
                        d.dungeons_gained
                    FROM dungeon_changes d
                    JOIN users_global u ON u.uuid = d.uuid
                    WHERE u.timestamp = (SELECT MAX(timestamp) FROM users_global WHERE uuid = d.uuid)
                    AND d.dungeons_gained > 0
                )
                SELECT 
                    username,
                    dungeons_gained
                FROM player_totals
                ORDER BY dungeons_gained DESC
                LIMIT 100;
                """
                params = [
                    startDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    endDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                ]
            else:
                # For "Everything" or fallback, use the original query
                query = """
                SELECT username, totalDungeons
                FROM (
                    SELECT *
                    FROM users_global
                    WHERE (uuid, timestamp) IN (
                        SELECT uuid, MAX(timestamp)
                        FROM users_global
                        GROUP BY uuid
                    )
                )
                ORDER BY totalDungeons DESC
                LIMIT 100;
                """
                params = []
            playerCursor.execute(query, params)
            data = playerCursor.fetchall()
        
        case "playerLeaderboardPVPKills":
            playerCursor.execute("""
            SELECT username, pvpKills
            FROM (
                SELECT *
                FROM users_global
                WHERE (uuid, timestamp) IN (
                    SELECT uuid, MAX(timestamp)
                    FROM users_global
                    GROUP BY uuid
                )
            )
            ORDER BY pvpKills DESC
            LIMIT 100;
            """)
            data = playerCursor.fetchall()
        
        case "playerLeaderboardTotalLevel":
            playerCursor.execute("""
            SELECT username, totalLevel
            FROM (
                SELECT *
                FROM users_global
                WHERE (uuid, timestamp) IN (
                    SELECT uuid, MAX(timestamp)
                    FROM users_global
                    GROUP BY uuid
                )
            )
            ORDER BY totalLevel DESC
            LIMIT 100;
            """)
            data = playerCursor.fetchall()
        
        case "playerLeaderboardPlaytime":
            if startDate is None:
                playerCursor.execute("""
                    SELECT username, playtime
                    FROM (
                        SELECT *
                        FROM users
                        WHERE (uuid, timestamp) IN (
                            SELECT uuid, MAX(timestamp)
                            FROM users
                            GROUP BY uuid
                        )
                    )
                    ORDER BY playtime DESC
                    LIMIT 100;
                """)
            else:
                query = """
                    WITH time_bounds AS (
                        SELECT 
                            uuid,
                            MIN(timestamp) AS min_time,
                            MAX(timestamp) AS max_time
                        FROM users
                        WHERE timestamp BETWEEN ? AND ?
                        GROUP BY uuid
                    ),
                    playtime_diff AS (
                        SELECT
                            tb.uuid,
                            (max_snap.playtime - min_snap.playtime) AS playtime_gained
                        FROM time_bounds tb
                        JOIN users min_snap ON min_snap.uuid = tb.uuid AND min_snap.timestamp = tb.min_time
                        JOIN users max_snap ON max_snap.uuid = tb.uuid AND max_snap.timestamp = tb.max_time
                        WHERE min_snap.playtime != -1
                    ),
                    latest_username AS (
                        SELECT uuid, username
                        FROM users
                        WHERE (uuid, timestamp) IN (
                            SELECT uuid, MAX(timestamp)
                            FROM users
                            GROUP BY uuid
                        )
                    )
                    SELECT 
                        lu.username,
                        pd.playtime_gained,
                        RANK() OVER (ORDER BY pd.playtime_gained DESC) AS rank
                    FROM playtime_diff pd
                    JOIN latest_username lu ON lu.uuid = pd.uuid
                    WHERE pd.playtime_gained > 0
                    ORDER BY pd.playtime_gained DESC
                    LIMIT 100;
                """
                playerCursor.execute(query, [
                    startDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    endDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                ])
            data = playerCursor.fetchall()
        
        case "guildLeaderboardXPButGuildSpecific":
            query = """
                WITH time_bounds AS (
                    SELECT 
                        guild_uuid, 
                        member_uuid, 
                        MIN(timestamp) as min_time, 
                        MAX(timestamp) as max_time
                    FROM member_snapshots 
                    WHERE guild_uuid = ?
            """
            params = [uuid]

            if startDate and endDate:
                query += " AND timestamp BETWEEN ? AND ? "
                params.extend([
                    startDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    endDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                ])

            query += """
                    GROUP BY guild_uuid, member_uuid
                ),
                contribution_changes AS (
                    SELECT 
                        t.guild_uuid,
                        t.member_uuid,
                        COALESCE(
                            (SELECT contribution 
                            FROM member_snapshots 
                            WHERE guild_uuid = t.guild_uuid 
                            AND member_uuid = t.member_uuid 
                            AND timestamp = t.max_time
                            ) - 
                            (SELECT contribution 
                            FROM member_snapshots 
                            WHERE guild_uuid = t.guild_uuid 
                            AND member_uuid = t.member_uuid 
                            AND timestamp = t.min_time
                            ), 0
                        ) as xp_gained
                    FROM time_bounds t
                ),
                player_totals AS (
                    SELECT 
                        c.member_uuid,
                        m.name as player_name,
                        c.xp_gained
                    FROM contribution_changes c
                    JOIN members m ON m.uuid = c.member_uuid
                    WHERE c.xp_gained > 0
                )
                SELECT 
                    player_name,
                    xp_gained,
                    RANK() OVER (ORDER BY xp_gained DESC) as rank
                FROM player_totals
                ORDER BY xp_gained DESC
                LIMIT 100;
            """
            guildCursor.execute(query, params)
            data = guildCursor.fetchall()
        
        case "guildLeaderboardOnlineButGuildSpecific":
            startDate, endDate, days, = getTimeframe(timeframe, "special")
            query = f"""
                WITH recent_users AS (
                    SELECT DISTINCT uuid, username
                    FROM users
                    WHERE guildUUID = ?
            """
            params = [uuid]

            if startDate and endDate:
                query += " AND timestamp BETWEEN ? AND ?"
                params.extend([
                    startDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    endDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                ])
            else:
                query += " AND timestamp >= datetime('now', '-7 day')"

            query += f"""
                ),
                recent_playtime AS (
                    SELECT uuid, timestamp, playtime
                    FROM users
            """

            if startDate and endDate:
                query += " WHERE timestamp BETWEEN ? AND ?"
                params.extend([
                    startDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    endDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                ])
            else:
                query += " WHERE timestamp >= datetime('now', '-7 day')"

            query += """
                ),
                ranked_playtime AS (
                    SELECT
                        rp.uuid,
                        rp.playtime,
                        rp.timestamp,
                        ROW_NUMBER() OVER (PARTITION BY rp.uuid ORDER BY rp.timestamp ASC) AS rn_start,
                        ROW_NUMBER() OVER (PARTITION BY rp.uuid ORDER BY rp.timestamp DESC) AS rn_end
                    FROM recent_playtime rp
                ),
                playtime_start AS (
                    SELECT uuid, playtime AS playtime_start
                    FROM ranked_playtime
                    WHERE rn_start = 1
                ),
                playtime_end AS (
                    SELECT uuid, playtime AS playtime_end
                    FROM ranked_playtime
                    WHERE rn_end = 1
                ),
                playtime_diff AS (
                    SELECT 
                        ru.username,
                        pe.uuid,
                        ROUND((pe.playtime_end - ps.playtime_start) / ?, 2) AS avg_daily_hours
                    FROM playtime_start ps
                    JOIN playtime_end pe ON ps.uuid = pe.uuid
                    JOIN recent_users ru ON ru.uuid = ps.uuid
                    WHERE ps.playtime_start != -1
                )
                SELECT 
                    username,
                    avg_daily_hours,
                    RANK() OVER (ORDER BY avg_daily_hours DESC) AS rank
                FROM playtime_diff
                ORDER BY avg_daily_hours DESC
                LIMIT 100;
            """

            params.append(days)
            playerCursor.execute(query, params)
            data = playerCursor.fetchall()
        
        case "guildLeaderboardWarsButGuildSpecific":
            query = """
                WITH recent_snapshots AS (
                    SELECT *
                    FROM users_global
                    WHERE uuid IN (
                        SELECT DISTINCT uuid 
                        FROM users 
                        WHERE guildUUID = ?
            """
            params = [uuid]

            if startDate and endDate:
                query += " AND timestamp BETWEEN ? AND ? "
                params.extend([
                    startDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    endDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                ])
                query += ") AND timestamp BETWEEN ? AND ? "
                params.extend([
                    startDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    endDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                ])
            else:
                query += ") "

            query += """
                ),
                ranked_snapshots AS (
                    SELECT
                        uuid,
                        username,
                        wars,
                        timestamp,
                        ROW_NUMBER() OVER (PARTITION BY uuid ORDER BY timestamp ASC) AS rn_asc,
                        ROW_NUMBER() OVER (PARTITION BY uuid ORDER BY timestamp DESC) AS rn_desc
                    FROM recent_snapshots
                ),
                min_wars AS (
                    SELECT uuid, wars AS wars_start
                    FROM ranked_snapshots
                    WHERE rn_asc = 1
                ),
                max_wars AS (
                    SELECT uuid, username, wars AS wars_end
                    FROM ranked_snapshots
                    WHERE rn_desc = 1
                ),
                wars_changes AS (
                    SELECT 
                        max.uuid,
                        max.username,
                        max.wars_end - min.wars_start AS wars_gained
                    FROM max_wars max
                    JOIN min_wars min ON max.uuid = min.uuid
                )
                SELECT 
                    username,
                    wars_gained,
                    RANK() OVER (ORDER BY wars_gained DESC) AS rank
                FROM wars_changes
                ORDER BY wars_gained DESC
                LIMIT 100;
            """
            playerCursor.execute(query, params)
            data = playerCursor.fetchall()
        
        case "guildLeaderboardGraids":
            conn = connectDB("graid")
            cursor = conn.cursor()
            cursor.execute("SELECT value AS guilds FROM graid_data WHERE key = 'guild_raids'")
            
            row = cursor.fetchone()
            startDate, endDate = getTimeframe(timeframe, "graid")

            leaderboard = {}
            for prefix, entries in json.loads(row[0]).items():
                count = 0
                for entry in entries:
                    ts = datetime.fromtimestamp(entry["timestamp"])
                    if startDate and endDate:
                        if startDate <= ts <= endDate:
                            count += 1
                    elif not startDate and not endDate:  # all data, everything
                        count += 1
                leaderboard[prefix] = count
                
            sortedLeaderboard = sorted(leaderboard.items(), key=lambda x: x[1], reverse=True)
            
            return sortedLeaderboard
        
        case "guildLeaderboardGraidsButGuildSpecific":
            # So because of bad design, this endpoint uses prefix but requires being called with a uuid from api so we need to convert uuid to prefix.
            guildData = await searchMaster("uuid", uuid)
            prefix = guildData["prefix"]
            
            conn = connectDB("graid")
            cursor = conn.cursor()
            cursor.execute("SELECT value AS guilds FROM graid_data WHERE key = 'guild_raids'")
            
            row = cursor.fetchone()
            startDate, endDate = getTimeframe(timeframe, "graid")
            if prefix not in row[0]: # Guild not valid not being tracked not eligible
                return JSONResponse(status_code=400, content={"error": "Please provide a valid guild."})
            player_counter = Counter()

            for entry in json.loads(row[0])[prefix]:
                ts = datetime.fromtimestamp(entry["timestamp"])
                if startDate and endDate:
                    if startDate <= ts <= endDate:
                        for player in entry["party"]:
                            player_counter[player] += 1
                elif not startDate and not endDate:  # "Everything"
                    for player in entry["party"]:
                        player_counter[player] += 1

            # Sort and display
            sortedPlayers = player_counter.most_common(100)
            return sortedPlayers
        
        case "playerLeaderboardGraids":
            conn = connectDB("graid")
            cursor = conn.cursor()
            cursor.execute("SELECT value AS guilds FROM graid_data WHERE key = 'guild_raids'")
            cutoff = getTimeframe(timeframe, "specialGraid")
            row = cursor.fetchone()
            startDate, endDate = getTimeframe(timeframe, "graid")
            player_counter = Counter()

            for entries in json.loads(row[0]).values():
                for entry in entries:
                    if entry["timestamp"] >= cutoff:
                        for player in entry["party"]:
                            player_counter[player] += 1

            # Sort and display
            sortedPlayers = player_counter.most_common(100) # Limit to 100
            
            return sortedPlayers
        
        case _: # Default case
            return JSONResponse(status_code=400, content={"error": "Please provide a correct leaderboard type."})


            
    guildConn.close()
    playerCursor.close()
    return data

@activityRouter.get("/{activityType}")
@cache_route(ttl=600) #10m cache
async def activity(activityType: str, uuid: str | None = None, name: str | None = None): # Name can be either prefix or gname or player username
    if not activityType:
        return JSONResponse(status_code=400, content={"error": "Please provide a valid leaderboard type."})
    guildConn = connectDB("guild")
    playerConn = connectDB("player")
    guildCursor = guildConn.cursor()
    playerCursor = playerConn.cursor()
    match activityType:
        case "guildActivityXP":
            guildCursor.execute("""
                WITH RECURSIVE dates(date) AS (
                    SELECT date(datetime('now', '-13 days'))
                    UNION ALL
                    SELECT date(datetime(date, '+1 day'))
                    FROM dates
                    WHERE date < date('now')
                )
                SELECT 
                    dates.date,
                    COALESCE(SUM(daily_xp), 0) as total_xp
                FROM dates
                LEFT JOIN (
                    SELECT 
                        date(timestamp) as day,
                        MAX(contribution) - MIN(contribution) as daily_xp
                    FROM member_snapshots
                    WHERE guild_uuid = ?
                    AND timestamp >= datetime('now', '-14 days')
                    GROUP BY date(timestamp), member_uuid
                ) xp_data ON dates.date = xp_data.day
                GROUP BY dates.date
                ORDER BY dates.date
            """, (uuid,))
            
            snapshots = guildCursor.fetchall()
            if not snapshots:
                return None, None

            dates = []
            xp_values = []
            for date_str, xp in snapshots:
                dates.append(datetime.strptime(date_str, '%Y-%m-%d'))
                xp_values.append(xp)

            total_xp = sum(xp_values)
            avg_daily_xp = total_xp / len(dates) if dates else 0
            max_daily_xp = max(xp_values) if xp_values else 0
            min_daily_xp = min(xp_values) if xp_values else 0
            
            plt.figure(figsize=(12, 6))
            plt.bar(dates, xp_values, width=0.8, color=blue)
            plt.axhline(y=avg_daily_xp, color='red', linestyle='-', label=f'Daily Average: {avg_daily_xp:,.0f} XP')
            plt.gca().xaxis.set_major_formatter(DateFormatter('%m/%d'))
            plt.xticks(dates)
            plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
            plt.title(f'Daily Guild XP Contribution - {name}', fontsize=14)
            plt.xlabel('Date (UTC)', fontsize=12)
            plt.ylabel('XP Gained', fontsize=12)
            plt.grid(True, linestyle='-', alpha=0.5)
            plt.legend()
            plt.tight_layout()
            plt.text(1.0, -0.1, f"Generated at {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')} UTC.", 
                    transform=plt.gca().transAxes, 
                    fontsize=9, verticalalignment='bottom', 
                    horizontalalignment='right',color='gray')
            buf = io.BytesIO()
            plt.savefig(buf, format='png')
            plt.close()
            buf.seek(0)
            img = base64.b64encode(buf.getvalue()).decode()
            return JSONResponse({"total_xp": total_xp, "daily_average": avg_daily_xp, "highest_day": max_daily_xp, "lowest_day": min_daily_xp, "image": img})
        
        case "guildActivityTerritories":
            guildCursor.execute("""
                WITH RECURSIVE 
                timepoints AS (
                    SELECT datetime('now', '-7 days') as timepoint
                    UNION ALL
                    SELECT datetime(timepoint, '+15 minutes')
                    FROM timepoints
                    WHERE timepoint < datetime('now')
                ),
                snapshots_with_territories AS (
                    SELECT 
                        strftime('%Y-%m-%d %H:%M:00', timestamp) as snap_time,
                        territories
                    FROM guild_snapshots
                    WHERE guild_uuid = ?
                    AND territories IS NOT NULL
                    AND territories > 0
                )
                SELECT 
                    timepoints.timepoint,
                    COALESCE(
                        (
                            SELECT territories
                            FROM snapshots_with_territories
                            WHERE snap_time <= timepoints.timepoint
                            ORDER BY snap_time DESC
                            LIMIT 1
                        ),
                        0
                    ) as territory_count
                FROM timepoints
                ORDER BY timepoint;
            """, (uuid,))
            snapshots = guildCursor.fetchall()
            if not snapshots or all(count == 0 for _, count in snapshots):
                return None, None
            
            times = []
            territory_counts = []
            for timestamp_str, count in snapshots:
                try:
                    times.append(datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S'))
                    territory_counts.append(float(count) if count is not None else 0.0)
                except (ValueError, TypeError) as e: # shouldnt happen, but after the amount of errors i ran from this, idk
                    continue

            if not times or not territory_counts:
                return None, None
            non_zero_indices = [i for i, count in enumerate(territory_counts) if count > 0]
            if non_zero_indices:
                start_idx = non_zero_indices[0]
                end_idx = non_zero_indices[-1] + 1
                times = times[start_idx:end_idx]
                territory_counts = territory_counts[start_idx:end_idx]

            current_territories = territory_counts[-1] if territory_counts else 0
            max_territories = max(territory_counts) if territory_counts else 0
            min_territories = min(filter(lambda x: x > 0, territory_counts)) if territory_counts else 0
            avg_territories = sum(filter(lambda x: x > 0, territory_counts)) / len(list(filter(lambda x: x > 0, territory_counts))) if territory_counts else 0

            plt.figure(figsize=(12, 6))
            plt.plot(times, territory_counts, '-', label='Territory Count', color=blue, lw=3)
            plt.fill_between(times, 0, territory_counts, alpha=0.3)
            plt.axhline(y=avg_territories, color='red', linestyle='-', label=f'Average: {avg_territories:.1f}')
            time_formatter = DateFormatter('%m/%d %H:%M')
            plt.gca().xaxis.set_major_formatter(time_formatter)
            plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x)}'))
            y_range = max_territories - min_territories
            min_y = max(0, min_territories - (y_range * 0.1))
            max_y = max_territories + (y_range * 0.1)
            plt.ylim(min_y, max_y)
            plt.title(f'Territory Count - {name}', fontsize=14)
            plt.xlabel('Date (UTC)', fontsize=12)
            plt.ylabel('Number of Territories', fontsize=12)
            plt.grid(True, linestyle='-', alpha=0.5)
            plt.legend()
            plt.margins(x=0.01)
            plt.tight_layout()
            plt.text(1.0, -0.1, f"Generated at {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')} UTC.", 
                transform=plt.gca().transAxes, 
                fontsize=9, verticalalignment='bottom', 
                horizontalalignment='right',color='gray')
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=100)
            buf.seek(0)
            plt.close()
            img = base64.b64encode(buf.getvalue()).decode()
            return JSONResponse({"current_territories": current_territories, "maximum_territories": max_territories, "minimum_territories": min_territories, "average_territories": avg_territories, "image": img})
            
        case "guildActivityWars":
            guildCursor.execute("""
                WITH RECURSIVE 
                timepoints AS (
                    SELECT datetime('now', '-7 days') as timepoint
                    UNION ALL
                    SELECT datetime(timepoint, '+15 minutes')
                    FROM timepoints
                    WHERE timepoint < datetime('now')
                ),
                snapshots_with_wars AS (
                    SELECT 
                        strftime('%Y-%m-%d %H:%M:00', timestamp) as snap_time,
                        wars
                    FROM guild_snapshots
                    WHERE guild_uuid = ?
                    AND wars IS NOT NULL
                    AND wars > 0
                )
                SELECT 
                    timepoints.timepoint,
                    COALESCE(
                        (
                            SELECT wars
                            FROM snapshots_with_wars
                            WHERE snap_time <= timepoints.timepoint
                            ORDER BY snap_time DESC
                            LIMIT 1
                        ),
                        0
                    ) as wars_count
                FROM timepoints
                ORDER BY timepoint;
            """, (uuid,))
            snapshots = guildCursor.fetchall()
            if not snapshots or all(count == 0 for _, count in snapshots):
                return None, None
            
            times = []
            war_counts = []
            for timestamp_str, count in snapshots:
                try:
                    times.append(datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S'))
                    war_counts.append(float(count) if count is not None else 0.0)
                except (ValueError, TypeError) as e: # shouldnt happen, but after the amount of errors i ran from this, idk
                    continue

            if not times or not war_counts:
                return None, None
            non_zero_indices = [i for i, count in enumerate(war_counts) if count > 0]
            if non_zero_indices:
                start_idx = non_zero_indices[0]
                end_idx = non_zero_indices[-1] + 1
                times = times[start_idx:end_idx]
                war_counts = war_counts[start_idx:end_idx]

            current_war = war_counts[-1] if war_counts else 0
            max_war = max(war_counts) if war_counts else 0
            min_war = min(filter(lambda x: x > 0, war_counts)) if war_counts else 0

            plt.figure(figsize=(12, 6))
            plt.plot(times, war_counts, '-', label='War Count', color=blue, lw=3)
            time_formatter = DateFormatter('%m/%d %H:%M')
            plt.gca().xaxis.set_major_formatter(time_formatter)
            plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x)}'))
            y_range = max_war - min_war
            min_y = max(0, min_war - (y_range * 0.1))
            max_y = max_war + (y_range * 0.1)
            plt.ylim(min_y, max_y)
            plt.title(f'War History - {name}', fontsize=14)
            plt.xlabel('Date (UTC)', fontsize=12)
            plt.ylabel('Number of Wars', fontsize=12)
            plt.grid(True, linestyle='-', alpha=0.5)
            plt.legend()
            plt.margins(x=0.01)
            plt.tight_layout()
            plt.text(1.0, -0.1, f"Generated at {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')} UTC.", 
                transform=plt.gca().transAxes, 
                fontsize=9, verticalalignment='bottom', 
                horizontalalignment='right',color='gray')
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=100)
            buf.seek(0)
            img = base64.b64encode(buf.getvalue()).decode()
            return JSONResponse({"current_war": current_war,  "image": img})
        
        case "guildActivityOnlineMembers":
            guildCursor.execute("""
                SELECT timestamp, online_members
                FROM guild_snapshots
                WHERE guild_uuid = ?
                AND timestamp >= datetime('now', '-3 day')
                ORDER BY timestamp
            """, (uuid,))
            snapshots = guildCursor.fetchall()
            
            if not snapshots:
                return None, None
            
            times = [datetime.fromisoformat(snapshot[0]) for snapshot in snapshots]
            raw_numbers = [snapshot[1] for snapshot in snapshots]
            
            overall_average = sum(raw_numbers) / len(raw_numbers) if raw_numbers else 0

            plt.figure(figsize=(18, 6))
            plt.plot(times, raw_numbers, '-', label='Average Online Member Count', color=blue, lw=3)
            plt.fill_between(times, 0, raw_numbers, alpha=0.3)
            plt.axhline(y=overall_average, color='red', linestyle='-', label=f'Average: {overall_average:.1f} players')
            time_formatter = DateFormatter('%m/%d %H:%M')
            plt.gca().xaxis.set_major_formatter(time_formatter)
            hour_locator = HourLocator(byhour=[0, 6, 12, 18])
            plt.gca().xaxis.set_major_locator(hour_locator)
            plt.title(f'Online Members - {name}', fontsize=14)
            plt.xlabel('Time (UTC)', fontsize=12)
            plt.ylabel('Players Online', fontsize=12)
            plt.grid(True, linestyle='-', alpha=0.5)
            plt.legend()
            plt.tight_layout()
            plt.text(1.0, -0.1, f"Generated at {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')} UTC.", 
                transform=plt.gca().transAxes, 
                fontsize=9, verticalalignment='bottom', 
                horizontalalignment='right',color='gray')
            buf = io.BytesIO()
            plt.savefig(buf, format='png')
            buf.seek(0)
            img = base64.b64encode(buf.getvalue()).decode()
            return JSONResponse({"max_players": max(raw_numbers), "min_players": min(raw_numbers), "average": overall_average, "image": img})
            
        case "guildActivityTotalMembers":
            guildCursor.execute("""
                SELECT timestamp, total_members
                FROM guild_snapshots
                WHERE guild_uuid = ?
                AND timestamp >= datetime('now', '-7 day')
                ORDER BY timestamp
            """, (uuid,))
            snapshots = guildCursor.fetchall()
            if not snapshots:
                return None, None
            
            times = [datetime.fromisoformat(snapshot[0]) for snapshot in snapshots]
            total_numbers = [snapshot[1] for snapshot in snapshots]
            overall_total = sum(total_numbers) / len(total_numbers) if total_numbers else 0
            plt.figure(figsize=(12, 6))
            plt.plot(times, total_numbers, '-', label='Total Members', color=blue, lw=3)
            plt.fill_between(times, 0, total_numbers, alpha=0.3)
            plt.axhline(y=overall_total, color='r', linestyle='-', label=f'Average: {overall_total:.1f} members')
            time_formatter = DateFormatter('%D')
            plt.gca().xaxis.set_major_formatter(time_formatter)
            plt.gca().xaxis.set_major_locator(AutoDateLocator())
            plt.title(f'Member Count - {name}', fontsize=14)
            plt.xlabel('Time (UTC)', fontsize=12)
            plt.ylabel('Members', fontsize=12)
            plt.grid(True, linestyle='-', alpha=0.5)
            plt.legend()
            plt.tight_layout()
            plt.text(1.0, -0.1, f"Generated at {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')} UTC.", 
            transform=plt.gca().transAxes, 
            fontsize=9, verticalalignment='bottom', 
            horizontalalignment='right',color='gray')
            buf = io.BytesIO()
            plt.savefig(buf, format='png')
            buf.seek(0)
            img = base64.b64encode(buf.getvalue()).decode()
            return JSONResponse({"max_players": max(total_numbers), "min_players": min(total_numbers), "average": overall_total, "image": img})
        
        case "playerActivityPlaytime":
            playerCursor.execute("""
            WITH RECURSIVE dates(day) AS (
                SELECT DATE('now', '-13 days')
                UNION ALL
                SELECT DATE(day, '+1 day')
                FROM dates
                WHERE day < DATE('now')
            ),
            valid_playtime AS (
                SELECT uuid, timestamp, 
                    CASE WHEN playtime < 0 THEN NULL ELSE playtime END AS playtime
                FROM users
            ),
            playtime_per_day AS (
                SELECT DATE(timestamp) AS day,
                    ROUND((MAX(playtime) - MIN(playtime)) * 60.0) AS playtime_minutes
                FROM valid_playtime
                WHERE uuid = ?
                AND DATE(timestamp) >= DATE('now', '-14 days')
                GROUP BY DATE(timestamp)
            )
            SELECT d.day,
                COALESCE(p.playtime_minutes, 0) AS playtime_minutes
            FROM dates d
            LEFT JOIN playtime_per_day p ON d.day = p.day
            ORDER BY d.day;
            """, (uuid,))
            daily_data = playerCursor.fetchall()

            if not daily_data:
                return None, None

            dailyPlaytimes = {
                datetime.strptime(day, '%Y-%m-%d').date(): minutes
                for day, minutes in daily_data
            }
            dates = sorted(dailyPlaytimes.keys())
            playtimeValues = [dailyPlaytimes[date] for date in dates]
            totalPlaytimeinMinutes = sum(playtimeValues)
            averageDailyPlaytime = totalPlaytimeinMinutes / len(dates) if dates else 0

            plt.figure(figsize=(12, 6))
            plt.bar(dates, playtimeValues, width=0.8, color=blue)
            plt.axhline(y=averageDailyPlaytime, color='red', linestyle='-', 
                        label=f'Daily Average: {averageDailyPlaytime:.2f} minutes')
            plt.gca().xaxis.set_major_formatter(DateFormatter('%m/%d'))
            plt.xticks(dates)
            plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:.0f}'))
            plt.title(f'Daily Playtime - {name}', fontsize=14)
            plt.xlabel('Date (UTC)', fontsize=12)
            plt.ylabel('Minutes Played', fontsize=12)
            plt.grid(True, linestyle='-', alpha=0.5)
            plt.legend()
            plt.tight_layout()
            plt.text(1.0, -0.1, f"Generated at {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')} UTC.", 
                    transform=plt.gca().transAxes, 
                    fontsize=9, verticalalignment='bottom', 
                    horizontalalignment='right', color='gray')
            
            buf = io.BytesIO()
            plt.savefig(buf, format='png')
            buf.seek(0)
            img = base64.b64encode(buf.getvalue()).decode()
            return JSONResponse({"daily_average": averageDailyPlaytime, "max_day": max(playtimeValues) if playtimeValues else 0, "min_day": min(playtimeValues) if playtimeValues else 0, "image": img})
            
        case "playerActivityContributions":
            guildCursor.execute("""
            SELECT timestamp, contribution
            FROM member_snapshots
            WHERE member_uuid = ?
            AND timestamp >= datetime('now', '-14 days')
            ORDER BY timestamp
            """, (uuid,))
            snapshots = guildCursor.fetchall()

            if not snapshots:
                return None, None

            parsed_snapshots = [(datetime.strptime(ts, '%Y-%m-%d %H:%M:%S'), xp) for ts, xp in snapshots]

            latest_snapshots_by_day = {}
            for ts, xp in parsed_snapshots:
                day = ts.date()
                if day not in latest_snapshots_by_day or ts > latest_snapshots_by_day[day][0]:
                    latest_snapshots_by_day[day] = (ts, xp)

            filtered_snapshots = sorted([(ts, xp) for ts, xp in latest_snapshots_by_day.values()], key=lambda x: x[0])

            timestamps = [ts for ts, xp in filtered_snapshots]
            xpValues = [xp for ts, xp in filtered_snapshots]
            daily_gains = [xpValues[i] - xpValues[i - 1] for i in range(1, len(xpValues))]


            totalGained = xpValues[-1] - xpValues[0] if len(xpValues) > 1 else 0
            average = totalGained / len(daily_gains) if daily_gains else 0


            plt.figure(figsize=(12, 6))
            plt.bar(timestamps[1:], daily_gains, width=0.8, color=blue)
            plt.axhline(y=average, color='red', linestyle='-', label=f'Daily Average: {average:.2f} XP')
            plt.gca().xaxis.set_major_formatter(DateFormatter('%m/%d'))
            plt.xticks(timestamps[1:])
            plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:.0f}'))
            plt.title(f'Daily XP Gain - {name}', fontsize=14)
            plt.xlabel('Date (UTC)', fontsize=12)
            plt.ylabel('XP Gained', fontsize=12)
            plt.grid(True, linestyle='-', alpha=0.5)
            plt.legend()
            plt.tight_layout()
            plt.text(1.0, -0.1, f"Generated at {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')} UTC.", 
                    transform=plt.gca().transAxes, fontsize=9, verticalalignment='bottom', 
                    horizontalalignment='right', color='gray')

            buf = io.BytesIO()
            plt.savefig(buf, format='png')
            buf.seek(0)
            img = base64.b64encode(buf.getvalue()).decode()
            return JSONResponse({"total_xp": totalGained, "max_xp": max(daily_gains) if daily_gains else 0, "min_xp": min(daily_gains) if daily_gains else 0,  "image": img})
        
        case "playerActivityDungeons":
            playerCursor.execute("""
            SELECT u.timestamp, u.totalDungeons
            FROM users_global u
            WHERE u.uuid = ?
                AND u.timestamp >= DATETIME('now', '-7 days')
            ORDER BY u.timestamp;
            """, (uuid,))
            snapshots = playerCursor.fetchall()

            if not snapshots:
                return None, None

            dates = [datetime.fromisoformat(row[0]) for row in snapshots]
            total_dungeons = [row[1] for row in snapshots]

            # Highest total and daily gain
            highestTotal = total_dungeons[-1] if total_dungeons else 0
            dungeons_by_day = defaultdict(list)
            for dt, count in zip(dates, total_dungeons):
                dungeons_by_day[dt.date()].append(count)
            dailyGain = [max(counts) - min(counts) for counts in dungeons_by_day.values() if len(counts) > 1]
            highestGain = max(dailyGain) if dailyGain else 0
            maxDungeons = max(total_dungeons) if total_dungeons else 0
            minDungeons = min(filter(lambda x: x > 0, total_dungeons)) if total_dungeons else 0

            # Plot
            plt.figure(figsize=(12, 6))
            plt.plot(dates, total_dungeons, '-', label='Dungeon Count', color=blue, lw=3)
            time_formatter = DateFormatter('%m/%d %H:%M')
            plt.gca().xaxis.set_major_formatter(time_formatter)
            plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x)}'))
            y_range = maxDungeons - minDungeons
            min_y = max(0, minDungeons - (y_range * 0.1))
            max_y = maxDungeons + (y_range * 0.1)
            plt.ylim(min_y, max_y)
            plt.title(f'Dungeon History - {name}', fontsize=14)
            plt.xlabel('Date (UTC)', fontsize=12)
            plt.ylabel('Number of Dungeon\'s completed', fontsize=12)
            plt.grid(True, linestyle='-', alpha=0.5)
            plt.legend()
            plt.margins(x=0.01)
            plt.tight_layout()
            plt.text(1.0, -0.1, f"Generated at {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')} UTC.", 
                transform=plt.gca().transAxes, 
                fontsize=9, verticalalignment='bottom', 
                horizontalalignment='right',color='gray')
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=100)
            buf.seek(0)
            img = base64.b64encode(buf.getvalue()).decode()
            return JSONResponse({"total_dungeons": highestTotal, "highest_gain": highestGain, "image": img})
            
        case "playerActivityTotalDungeons":
            playerCursor.execute("""
                SELECT dungeonsDict
                FROM users_global
                WHERE uuid = ?
                ORDER BY timestamp DESC
                LIMIT 1
            """, (uuid,))
            snapshots = playerCursor.fetchall()

            if not snapshots:
                return None, None
            
            dungeons = ast.literal_eval(snapshots[0][0])

            sorted_dungeons = dict(sorted(dungeons.items(), key=lambda item: item[1], reverse=True))

            labels = list(sorted_dungeons.keys())
            sizes = list(sorted_dungeons.values())


            plt.figure(figsize=(10, 8))
            sorted_dungeons = dict(sorted(dungeons.items(), key=lambda item: item[1], reverse=True))
            labels = list(sorted_dungeons.keys())
            sizes = list(sorted_dungeons.values())
            total = sum(sizes)
            percent_labels = [f"{label} — {size} ({(size / total * 100):.1f}%)" for label, size in zip(labels, sizes)]
            wedges, _ = plt.pie(sizes)
            plt.legend(wedges, percent_labels, title="Dungeons", loc="center left", bbox_to_anchor=(1, 0.5))
            plt.title(f"Dungeon Pie Chart - {name}")
            plt.text(1.0, -0.1, f"Generated at {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')} UTC.", 
                transform=plt.gca().transAxes, 
                fontsize=9, verticalalignment='bottom', 
                horizontalalignment='right',color='gray')
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
            buf.seek(0)
            img = base64.b64encode(buf.getvalue()).decode()
            return JSONResponse({"image": img}) # Technically we could just ship this out like how it is on other endpoints, just straight image, but all activity commands should and will b64 images for consistenty
        
        case "playerActivityRaids":
            playerCursor.execute("""
                SELECT u.timestamp, u.totalRaids
                FROM users_global u
                WHERE u.uuid = ?
                    AND u.timestamp >= DATETIME('now', '-7 days')
                ORDER BY u.timestamp;
            """, (uuid,))
            snapshots = playerCursor.fetchall()

            if not snapshots:
                return None, None

            dates = [datetime.fromisoformat(row[0]) for row in snapshots]
            totalRaids = [row[1] for row in snapshots]

            # Highest total and daily gain
            highestTotal = totalRaids[-1] if totalRaids else 0
            raids_by_day = defaultdict(list)
            for dt, count in zip(dates, totalRaids):
                raids_by_day[dt.date()].append(count)
            dailyGain = [max(counts) - min(counts) for counts in raids_by_day.values() if len(counts) > 1]
            highestGain = max(dailyGain) if dailyGain else 0
            maxRaids = max(totalRaids) if totalRaids else 0
            minRaids = min(filter(lambda x: x > 0, totalRaids)) if totalRaids else 0

            # Plot
            plt.figure(figsize=(12, 6))
            plt.plot(dates, totalRaids, '-', label='Raid Count', color=blue, lw=3)
            time_formatter = DateFormatter('%m/%d %H:%M')
            plt.gca().xaxis.set_major_formatter(time_formatter)
            plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x)}'))
            y_range = maxRaids - minRaids
            min_y = max(0, minRaids - (y_range * 0.1))
            max_y = maxRaids + (y_range * 0.1)
            plt.ylim(min_y, max_y)
            plt.title(f'Raid History - {name}', fontsize=14)
            plt.xlabel('Date (UTC)', fontsize=12)
            plt.ylabel('Number of Raid\'s completed', fontsize=12)
            plt.grid(True, linestyle='-', alpha=0.5)
            plt.legend()
            plt.margins(x=0.01)
            plt.tight_layout()
            plt.text(1.0, -0.1, f"Generated at {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')} UTC.", 
                transform=plt.gca().transAxes, 
                fontsize=9, verticalalignment='bottom', 
                horizontalalignment='right',color='gray')
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=100)
            buf.seek(0)
            img = base64.b64encode(buf.getvalue()).decode()
            return JSONResponse({"total": highestTotal, "highest_gain": highestGain, "image": img})
            
        case "playerActivityTotalRaids":
            playerCursor.execute("""
                SELECT raidsDict
                FROM users_global
                WHERE uuid = ?
                ORDER BY timestamp DESC
                LIMIT 1
            """, (uuid,))
            snapshots = playerCursor.fetchall()

            if not snapshots:
                return None, None
            
            raids = ast.literal_eval(snapshots[0][0])

            sortedRaids = dict(sorted(raids.items(), key=lambda item: item[1], reverse=True))

            labels = list(sortedRaids.keys())
            sizes = list(sortedRaids.values())


            plt.figure(figsize=(10, 8))
            sortedRaids = dict(sorted(raids.items(), key=lambda item: item[1], reverse=True))
            labels = list(sortedRaids.keys())
            sizes = list(sortedRaids.values())
            total = sum(sizes)
            percent_labels = [f"{label} — {size} ({(size / total * 100):.1f}%)" for label, size in zip(labels, sizes)]
            wedges, _ = plt.pie(sizes)
            plt.legend(wedges, percent_labels, title="Raids", loc="center left", bbox_to_anchor=(1, 0.5))
            plt.title(f"Raid Pie Chart - {name}")
            plt.text(1.0, -0.1, f"Generated at {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')} UTC.", 
                transform=plt.gca().transAxes, 
                fontsize=9, verticalalignment='bottom', 
                horizontalalignment='right',color='gray')
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
            buf.seek(0)
            img = base64.b64encode(buf.getvalue()).decode()
            return JSONResponse({"image": img})
        
        case "playerActivityMobsKilled":
            playerCursor.execute("""
            SELECT u.timestamp, u.killedMobs
            FROM users_global u
            WHERE u.uuid = ?
                AND u.timestamp >= DATETIME('now', '-7 days')
            ORDER BY u.timestamp;
            """, (uuid,))
            snapshots = playerCursor.fetchall()

            if not snapshots:
                return None, None

            dates = [datetime.fromisoformat(row[0]) for row in snapshots]
            totalKills = [row[1] for row in snapshots]
            highestTotal = totalKills[-1] if totalKills else 0
            kills_by_day = defaultdict(list)
            for dt, count in zip(dates, totalKills):
                kills_by_day[dt.date()].append(count)

            daily_gains = [max(counts) - min(counts) for counts in kills_by_day.values() if len(counts) > 1]
            highestGain = max(daily_gains) if daily_gains else 0
            maxKills = max(totalKills) if totalKills else 0
            minKills = min(filter(lambda x: x > 0, totalKills)) if totalKills else 0

            # Plot
            plt.figure(figsize=(12, 6))
            plt.plot(dates, totalKills, '-', label='Mob Kill Count', color=blue, lw=3)
            time_formatter = DateFormatter('%m/%d %H:%M')
            plt.gca().xaxis.set_major_formatter(time_formatter)
            plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x)}'))
            y_range = maxKills - minKills
            min_y = max(0, minKills - (y_range * 0.1))
            max_y = maxKills + (y_range * 0.1)
            plt.ylim(min_y, max_y)
            plt.title(f'Mob Kill History - {name}', fontsize=14)
            plt.xlabel('Date (UTC)', fontsize=12)
            plt.ylabel('Number of Kill\'s', fontsize=12)
            plt.grid(True, linestyle='-', alpha=0.5)
            plt.legend()
            plt.margins(x=0.01)
            plt.tight_layout()
            plt.text(1.0, -0.1, f"Generated at {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')} UTC.", 
                transform=plt.gca().transAxes, 
                fontsize=9, verticalalignment='bottom', 
                horizontalalignment='right',color='gray')
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=100)
            buf.seek(0)
            img = base64.b64encode(buf.getvalue()).decode()
            return JSONResponse({"total_kills": highestTotal, "highest_gain": highestGain, "image": img})
            
        case "playerActivityWars":
            playerCursor.execute("""
            SELECT u.timestamp, u.wars
            FROM users_global u
            WHERE u.uuid = ?
                AND u.timestamp >= DATETIME('now', '-7 days')
            ORDER BY u.timestamp;
            """, (uuid,))
            snapshots = playerCursor.fetchall()

            if not snapshots:
                return None, None

            dates = [datetime.fromisoformat(row[0]) for row in snapshots]
            totalWars = [row[1] for row in snapshots]
            highestTotal = totalWars[-1] if totalWars else 0
            wars_by_day = defaultdict(list)
            for dt, count in zip(dates, totalWars):
                wars_by_day[dt.date()].append(count)
            daily_gains = [max(counts) - min(counts) for counts in wars_by_day.values() if len(counts) > 1]
            highestGain = max(daily_gains) if daily_gains else 0
            maxWars = max(totalWars) if totalWars else 0
            minWars = min(filter(lambda x: x > 0, totalWars)) if totalWars else 0

            # Plot
            plt.figure(figsize=(12, 6))
            plt.plot(dates, totalWars, '-', label='War Count', color=blue, lw=3)
            time_formatter = DateFormatter('%m/%d %H:%M')
            plt.gca().xaxis.set_major_formatter(time_formatter)
            plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x)}'))
            y_range = maxWars - minWars
            min_y = max(0, minWars - (y_range * 0.1))
            max_y = maxWars + (y_range * 0.1)
            plt.ylim(min_y, max_y)
            plt.title(f'War Count History - {name}', fontsize=14)
            plt.xlabel('Date (UTC)', fontsize=12)
            plt.ylabel('Number of War\'s', fontsize=12)
            plt.grid(True, linestyle='-', alpha=0.5)
            plt.legend()
            plt.margins(x=0.01)
            plt.tight_layout()
            plt.text(1.0, -0.1, f"Generated at {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')} UTC.", 
                transform=plt.gca().transAxes, 
                fontsize=9, verticalalignment='bottom', 
                horizontalalignment='right',color='gray')
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=100)
            buf.seek(0)
            img = base64.b64encode(buf.getvalue()).decode()
            return JSONResponse({"total_wars": highestTotal, "highest_gain": highestGain, "image": img})
        
        case "guildActivityGraids":
            graidConn = connectDB("graid")
            graidCursor = graidConn.cursor()
            graidCursor.execute("SELECT value AS guilds FROM graid_data WHERE key = 'guild_raids'")
            confirmedGRaid = graidCursor.fetchone()
            if not confirmedGRaid:
                graidConn.close()
                return JSONResponse(status_code=404, content={"error": "Eligible guilds not found"})

            graidConn.close()
            guildData = await searchMaster("uuid", uuid)
            prefix = guildData["prefix"]
            
            if prefix not in confirmedGRaid:
                return None, None
            now = datetime.utcnow()
            cutoff = now - timedelta(days=14)

            # Get and filter timestamps
            timestamps = [
                datetime.utcfromtimestamp(entry["timestamp"])
                for entry in confirmedGRaid[prefix]
                if datetime.utcfromtimestamp(entry["timestamp"]) >= cutoff
            ]
            timestamps.sort()

            if not timestamps:
                return None, None

            # Cumulative count
            times = timestamps
            cumulative_counts = list(range(1, len(times) + 1))

            # Raids per day
            day_counts = Counter(t.date() for t in times)
            max_day = max(day_counts.values())
            avg_day = sum(day_counts.values()) / len(day_counts)
            total_raids = len(times)

            plt.figure(figsize=(12, 6))
            plt.plot(times, cumulative_counts, '-', label='Guild Raids', color=blue, lw=3)
            plt.fill_between(times, 0, cumulative_counts, alpha=0.3)
            time_formatter = DateFormatter('%m/%d %H:%M')
            plt.gca().xaxis.set_major_formatter(time_formatter)
            plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x)}'))
            plt.ylim(0, max(cumulative_counts) + 5)
            plt.title(f'Guild Raid Activity - {prefix}', fontsize=14)
            plt.xlabel('Date (UTC)', fontsize=12)
            plt.ylabel('Total Guild Raids', fontsize=12)
            plt.grid(True, linestyle='-', alpha=0.5)
            plt.legend()
            plt.tight_layout()
            plt.margins(x=0.01)
            plt.text(1.0, -0.1, f"Generated at {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')} UTC.", 
                transform=plt.gca().transAxes, 
                fontsize=9, verticalalignment='bottom', 
                horizontalalignment='right',color='gray')
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=100)
            buf.seek(0)
            img = base64.b64encode(buf.getvalue()).decode()
            return JSONResponse({"total_graid": total_raids, "max_graid": max_day, "average_graid": avg_day, "image": img})
            
        case "playerActivityGraids":
            graidConn = connectDB("graid")
            graidCursor = graidConn.cursor()
            graidCursor.execute("SELECT value AS guilds FROM graid_data WHERE key = 'guild_raids'")
            confirmedGRaid = graidCursor.fetchone()
            if not confirmedGRaid:
                graidConn.close()
                return JSONResponse(status_code=404, content={"error": "Eligible guilds not found"})


            now = datetime.utcnow()
            cutoff = now - timedelta(days=14)

            # Step 1: Collect all timestamps the user appeared in
            timestamps = []
            for entries in confirmedGRaid.values():
                for entry in entries:
                    if name in entry.get("party", []):
                        ts = datetime.utcfromtimestamp(entry["timestamp"])
                        if ts >= cutoff:
                            timestamps.append(ts)

            timestamps.sort()
            if not timestamps:
                return None, None

            # Step 2: Build cumulative count
            cumulative_counts = list(range(1, len(timestamps) + 1))

            # Step 3: Daily stats
            day_counts = Counter(t.date() for t in timestamps)
            max_day = max(day_counts.values())
            avg_day = sum(day_counts.values()) / len(day_counts)
            total_raids = len(timestamps)

            plt.figure(figsize=(12, 6))
            plt.plot(timestamps, cumulative_counts, '-', label='Guild Raids', color=blue, lw=3)
            plt.fill_between(timestamps, 0, cumulative_counts, alpha=0.3)
            time_formatter = DateFormatter('%m/%d %H:%M')
            plt.gca().xaxis.set_major_formatter(time_formatter)
            plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x)}'))
            plt.ylim(0, max(cumulative_counts) + 5)
            plt.title(f'Guild Raid Activity - {name}', fontsize=14)
            plt.xlabel('Date (UTC)', fontsize=12)
            plt.ylabel('Total Guild Raids', fontsize=12)
            plt.grid(True, linestyle='-', alpha=0.5)
            plt.legend()
            plt.tight_layout()
            plt.margins(x=0.01)
            plt.text(1.0, -0.1, f"Generated at {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')} UTC.", 
                transform=plt.gca().transAxes, 
                fontsize=9, verticalalignment='bottom', 
                horizontalalignment='right',color='gray')
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=100)
            buf.seek(0)
            img = base64.b64encode(buf.getvalue()).decode()
            return JSONResponse({"total_graid": total_raids, "max_graid": max_day, "average_graid": avg_day, "image": img})

        case _: # Default case
            return JSONResponse(status_code=400, content={"error": "Please provide a correct leaderboard type."})


            
    guildConn.close()
    playerCursor.close()
    return Response(content=buf.getvalue(), media_type="image/png")
  
@mapRouter.get("/current") # Not a great name but its the current map
@cache_route(ttl=120) #2m cache
async def current_map():
    return mapCreator()

@mapRouter.get("/heatmap") # Not a great name but its the current map
@cache_route(ttl=600) #10m cache
async def heat_map(timeframe: str):
    if not timeframe:
        return JSONResponse(status_code=400, content={"error": "Please provide a valid timeframe."})
    return heatmapCreator(timeframe)
  
app.include_router(searchRouter)
app.include_router(graidRouter)
app.include_router(leaderboardRouter)
app.include_router(activityRouter)
app.include_router(mapRouter)
