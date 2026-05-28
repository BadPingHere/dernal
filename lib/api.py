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
import os
import matplotlib.cm as cm
import seaborn as sns
import matplotlib as mpl
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import io
from matplotlib.dates import HourLocator, DateFormatter, AutoDateLocator
from datetime import timezone
import base64
import logging
import logging.handlers
import sys
import re
import requests
import colorsys
from typing import Dict, List
import random
import unicodedata


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_FILE = os.path.join(BASE_DIR, "api.log")
CACHE_FILE = Path(__file__).resolve().parents[1] / "database" / "ing_cache.json"

logger = logging.getLogger('api')
logger.setLevel(logging.DEBUG)
handler = logging.handlers.RotatingFileHandler(
    filename=LOG_FILE,
    encoding='utf-8',
    maxBytes=256 * 1024 * 1024,  # 256 Mib
)
dt_fmt = '%Y-%m-%d %H:%M:%S'
formatter = logging.Formatter('{asctime} - {levelname:<8} - {name}: {message}', dt_fmt, style='{')
handler.setFormatter(formatter)
logger.addHandler(handler)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
ingToMobs: Dict[str, List[str]] = {}
ingRarity: Dict[str, int] = {}
mobCoords: Dict[str, List[List[int]]] = {}
priceCache: Dict[str, float] = {}
    
    
timeframeMap1 = { # Used for heatmap data
    "Season 24": ("04/18/25", "06/01/25"),
    "Season 25": ("06/06/25", "07/20/25"),
    "Season 26": ("07/25/25", "09/14/25"),
    "Season 27": ("09/19/25", "11/02/25"), 
    "Season 28": ("11/07/25", "12/20/25"), 
    "Season 29": ("01/02/26", "02/28/26"), 
    "Last 7 Days": None, # gotta handle ts outta dict
    "Everything": None
}


app = FastAPI(title="Dernal API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

route_cache = TTLCache(maxsize=200, ttl=300)  # default 5 min
searchRouter = APIRouter(prefix="/api/search", tags=["Search"])
leaderboardRouter = APIRouter(prefix="/api/leaderboard", tags=["Leaderboard"])
seasonRatingdRouter = APIRouter(prefix="/api/seasonRating", tags=["Leaderboard"])
activityRouter = APIRouter(prefix="/api/activity", tags=["Activity"])
mapRouter = APIRouter(prefix="/api/map", tags=["Maps"])

ACTIVITYDBPATH = Path(__file__).resolve().parents[1] / "database" / "activity.db"
TERRITORIESDBPATH = Path(__file__).resolve().parents[1] / "database" / "territories.db"
TERRITORIESPATH = Path(__file__).resolve().parents[1] / "lib" /  "documents" / "territories.json"
rootDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def mapCreator():
    map_img = Image.open("lib/documents/main-map.png").convert("RGBA")
    font = ImageFont.truetype("lib/documents/arial.ttf", 40)
    territoryCounts = defaultdict(int)
    namePrefixMap = {}

    def coordToPixel(x, z):
        return x + 2558, z + 6638 # if only wynntils was ACCURATE!!!

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
        if not prefix:
            prefix = "None"

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
    # Top left
    boxX = 50
    boxY = 50

    for i, (prefix, count) in enumerate(leaderboardGuilds):
        color_hex = color_map.get(prefix, "#FFFFFF")
        try:
            text_color = tuple(int(color_hex[i:i+2], 16) for i in (1, 3, 5))
        except:
            text_color = (255, 255, 255)
        
        text = f"{i+1}. {namePrefixMap[prefix]} ({prefix}) - {count} Territories"
        draw.text((boxX + legendPadding, boxY + legendPadding + i * lineHeight), text, font=font, fill=text_color)
    
    mapImg = Image.alpha_composite(map_img, overlay)
    scale_factor = 0.4
    new_size = (int(mapImg.width * scale_factor), int(mapImg.height * scale_factor))
    mapImg = mapImg.resize(new_size, Image.LANCZOS)
    mapBytes = BytesIO()
    mapImg.save(mapBytes, format='webp', optimize=True, compress_level=5)
    mapBytes.seek(0)
    return Response(content=mapBytes.getvalue(), media_type="image/webp")

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
        return x + 2558, z + 6638 # if only wynntils was ACCURATE!!!


    success, r = makeRequest("https://api.wynncraft.com/v3/guild/list/territory")
    territory_data = r.json()
    activityCount = defaultdict(int)

    conn = sqlite3.connect(TERRITORIESDBPATH)
    cur = conn.cursor()

    if timeframe == "Everything":
        cur.execute("""
        SELECT territory, SUM(count)
        FROM territory_changes
        GROUP BY territory
        """)
    else:
        cur.execute("""
        SELECT territory, SUM(count)
        FROM territory_changes
        WHERE date BETWEEN ? AND ?
        GROUP BY territory
        """, (startDate.isoformat(), endDate.isoformat()))

    for territory, total in cur.fetchall():
        activityCount[territory] = total

    conn.close()
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
    new_size = (int(mapImg.width * 0.4), int(mapImg.height * 0.4))
    mapImg = mapImg.resize(new_size, Image.LANCZOS)
    mapBytes = BytesIO()
    mapImg.save(mapBytes, format='PNG', optimize=True, compress_level=5)
    mapBytes.seek(0)
    return Response(content=mapBytes.getvalue(), media_type="image/png")

async def searchMaster(field, value):
    conn = connectDB()
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM guilds WHERE {field} = ? COLLATE NOCASE", (value,))

    guild = cursor.fetchone()
    if not guild:
        conn.close()
        return JSONResponse(status_code=404, content={"error": "Guild not found"})

    data = dict(guild)
    cursor.execute("""
        SELECT level, xp_percent, territories, wars, online_members, total_members, guild_raids, timestamp
        FROM guild_snapshots
        WHERE guild_uuid = ?
        ORDER BY timestamp DESC
        LIMIT 1
    """, (data["guild_uuid"],))
    snapshot = cursor.fetchone()
    if snapshot:
        data["latest_snapshot"] = dict(snapshot)
    else:
        data["latest_snapshot"] = None

    conn.close()
    return data

def connectDB():
    conn = sqlite3.connect(ACTIVITYDBPATH)
    conn.row_factory = sqlite3.Row
    return conn

def getTimeframe(timeframe, type="leaderboard"):
    if type == "leaderboard":
        endDate = datetime.now()
        if timeframe == "Last 14 Days":
            startDate = endDate - timedelta(days=14)
        elif timeframe == "Last 7 Days":
            startDate = endDate - timedelta(days=7)
        elif timeframe == "Last 3 Days":
            startDate = endDate - timedelta(days=3)
        elif timeframe == "Last 24 Hours":
            startDate = endDate - timedelta(hours=24)
        elif timeframe == "Last 30 Days":
            startDate = endDate - timedelta(days=30)
        else:  # "All Time" and fallback
            startDate = None
            endDate = None
        return startDate, endDate
    elif type == "activity": # All activity commands will fit into this
        if timeframe == "Last 14 Days":
            days = 14
        elif timeframe == "Last 7 Days":
            days = 7
        elif timeframe == "Last 3 Days":
            days = 3
        elif timeframe == "Last 24 Hours":
            days = 1
        elif timeframe == "Last 30 Days":
            days = 30
        else:  # fallback to 7 days
            days = 7
        return days

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

def saveCache():
    data = {
        "ingToMobs": ingToMobs,
        "mobCoords": mobCoords,
        "priceCache": priceCache,
        "ingRarity": ingRarity,
    }
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "w") as f:
        json.dump(data, f, indent=2)

def loadCache():
    global ingToMobs, mobCoords, priceCache, ingRarity

    if not CACHE_FILE.exists():
        return False

    try:
        with open(CACHE_FILE, "r") as f:
            data = json.load(f)

        ingToMobs = data.get("ingToMobs", {})
        mobCoords = data.get("mobCoords", {})
        priceCache = data.get("priceCache", {})
        ingRarity = data.get("ingRarity", {})
        return True

    except Exception as e:
        logger.error("Cache corrupted:", e)
        return False

def findIngCoords(ingToMobs, mobCoords, ingRarity):
    def cleanText(text): # helper function to remove all the shitass from names
        if not text:
            return text
        text = unicodedata.normalize("NFKD", text)
        text = text.encode("ascii", "ignore").decode("ascii")
        text = re.sub(r"§.", "", text)
        return text.strip()
    
    ingToMobs.clear()
    mobCoords.clear()
    ingRarity.clear()
    try:
        time.sleep(0.50)
        url = f"https://api.wynncraft.com/v3/item/search?fullResult"
        payload = {
            "type": ["ingredient"],
            "levelRange": [0, 131]
        }
        r = requests.post(url, json=payload)
        jsonData = r.json()
        results = jsonData.get("results", {})
        if isinstance(results, list):
            result_items = [(item.get("displayName") or item.get("internalName", ""), item) for item in results]
        elif isinstance(results, dict):
            result_items = list(results.items())
        for ingredientName, info in result_items:
            ingredientName = cleanText(ingredientName)
            droppedBy = info.get("droppedBy", []) # Gets the droppedBy data if applicable, some dont have it because of WE and whatnot
            ingToMobs.setdefault(ingredientName, [])
            raw_tier = info.get("tier", 0)
            if isinstance(raw_tier, str):
                raw_tier = int(raw_tier.split("_")[-1]) if "_" in raw_tier else 0
            ingRarity[ingredientName] = int(raw_tier)
                    
            for entry in droppedBy:
                mobName = cleanText(entry.get("name"))
                coords = entry.get("coords")
                if mobName:
                    ingToMobs[ingredientName].append(mobName)

                if not coords:
                    continue

                if isinstance(coords[0], list): # Account for multiple lists of coords
                    processed = [[c[0], c[2], c[3]] for c in coords]
                else:
                    processed = [[coords[0], coords[2], coords[3]]]

                if mobName not in mobCoords:
                    mobCoords[mobName] = []
                    
                mobCoords.setdefault(mobName, [])
                mobCoords[mobName].extend(processed)
    except Exception as e: # we hit end of pages
        logger.info(f"findIngCoords ran, hit end of pages. or errored. Potential error: {e}")

def ingredientMap(ingToMobs, mobCoords, ingSearch, price, priceCache, updatePriceCache, tier):
    #font = ImageFont.truetype("lib/documents/arial.ttf", 30)
    map_img = Image.open("lib/documents/main-map.png").convert("RGBA")
    if not price: # hotchpotch fix but we just set price to -1
        price = -1

    def coordToPixel(x, z):
        return x + 2558, z + 6638 # if only wynntils was ACCURATE!!!

    overlay = Image.new("RGBA", map_img.size)
    overlay_draw = ImageDraw.Draw(overlay)
    draw = ImageDraw.Draw(map_img)
    legend_items = []

    loweercaseIngs = {ing.lower(): ing for ing in ingToMobs.keys()}
    if ingSearch: # Ing supplied
        lookup = ingSearch.lower()
        if lookup in loweercaseIngs:
            targets = [loweercaseIngs[lookup]]
        else:
            return None
    else: # Ing not supplied
        if tier is None: # Tier not supplied, search all ings
            targets = list(ingToMobs.keys())
        else: # tier supplied search all ings with right tier
            targets = [ing for ing in ingToMobs if ingRarity.get(ing) == tier]
    #logger.info(f"Targets: {targets}")
    drawn_min_x = drawn_min_y = float('inf')
    drawn_max_x = drawn_max_y = float('-inf')

    for ing in targets:
        if not updatePriceCache and ing in priceCache:
            avgLowPrice = priceCache[ing]
        else:
            time.sleep(0.1)
            url = f"https://www.wynnventory.com/api/trademarket/history/{ing}"
            r = requests.get(url)
            jsonData = r.json()

            if jsonData:
                lowestPrices = [entry["lowest_price"] for entry in jsonData if entry["lowest_price"]]
                avgLowPrice = sum(lowestPrices) / len(lowestPrices) if lowestPrices else 0
            else:
                avgLowPrice = 0
            priceCache[ing] = avgLowPrice
            saveCache()
        h = int(hashlib.md5(ing.encode()).hexdigest(), 16)
        hue = (h % 360) / 360.0
        sat = 0.85
        val = 0.95
        r, g, b = colorsys.hsv_to_rgb(hue, sat, val)
        color_rgb = (int(r * 255), int(g * 255), int(b * 255))
        fill_color = (*color_rgb, 70)
        outline_color = (*color_rgb, 255)
        legend_items.append((ing, color_rgb))
        if avgLowPrice >= 64 * price:
            #print(f"Ing {ing} is good: {avgLowPrice}")
            mobs = ingToMobs.get(ing, [])
            #logger.info(ing)
            #logger.info(mobs)
            for mob in mobs:
                #logger.info(mob)
                coords_list = mobCoords.get(mob, [])
                for (x, z, radius) in coords_list:
                    px, py = coordToPixel(x, z)
                    pr = radius if radius > 0 else 5
                    box = [px - pr, py - pr, px + pr, py + pr]
                    overlay_draw.ellipse(box, fill=fill_color)
                    draw.ellipse(box, outline=outline_color, width=2)
                    drawn_min_x = min(drawn_min_x, px - pr)
                    drawn_min_y = min(drawn_min_y, py - pr)
                    drawn_max_x = max(drawn_max_x, px + pr)
                    drawn_max_y = max(drawn_max_y, py + pr)

    mapImg = Image.alpha_composite(map_img, overlay)

    if drawn_min_x != float('inf'):
        margin = 100
        crop_box = (
            max(0, drawn_min_x - margin),
            max(0, drawn_min_y - margin),
            min(mapImg.width, drawn_max_x + margin),
            min(mapImg.height, drawn_max_y + margin),
        )
        mapImg = mapImg.crop(crop_box)

    #mapImg.save("ingredient_map.webp", format="webp")
    mapBytes = BytesIO()
    mapImg.save(mapBytes, format='webp', quality=90)
    mapBytes.seek(0)
    return Response(content=mapBytes.getvalue(), media_type="image/webp")

def createPlot(
    x, 
    y,
    graphType,
    color,
    title,
    xlabel,
    ylabel,
    timeColor,
    ahxlineY=None,
    ahxlineLabel=None,
    fillBetween=None,
    legendName=None,
    timeframeDays=None,
):
    #* Parameter info (because this will be long for sure)
    # x: The x axis data (usually dates)
    # y: the y axis data (whatever we are measuring)
    # graphType: the type of graph (bar, line, pie)
    # color: color for the bars and line
    # title: title of the graph
    # xlabel: label for the x axis
    # ylabel: label for the y axis
    # timeColor: color for the timestamp
    # ahxlineY: for the plt.axhline that intersects, mostly for average line, the y value it should use.
    # ahxlineLabel: the label for the above line
    # fillBetween: some line graphs (like territories) want fill between under the line so it looks better, so this is either True or None.
    # legendName: on the legend for line and pie chart graphs the legend needs a name
    # timeframeDays: number of days the timeframe is set to
    fig, ax = plt.subplots(figsize=(12, 6))

    if graphType == "bar":
        ax.bar(x, y, width=0.8, color=color)
        if ahxlineY and ahxlineLabel:
            ax.axhline(y=ahxlineY, color='red', linestyle='-', label=ahxlineLabel)
        ax.xaxis.set_major_formatter(DateFormatter('%m/%d'))
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f'{v:,.0f}'))
        ax.set_xlabel(xlabel, fontsize=12)
        ax.set_ylabel(ylabel, fontsize=12)
        ax.grid(True, linestyle='-', alpha=0.5)
        ax.legend()
        if timeframeDays:
            now = datetime.utcnow()
            ax.set_xlim(now - timedelta(days=timeframeDays), now)

    elif graphType == "line":
        ax.plot(x, y, '-', label=legendName, color=color, lw=1.5)
        if fillBetween:
            ax.fill_between(x, 0, y, alpha=0.3, color=color)
        if ahxlineY and ahxlineLabel:
            ax.axhline(y=ahxlineY, color='red', linestyle='-', label=ahxlineLabel)
        if timeframeDays >= 7: # 7 or more days we remove the hour:min since its not needed
            ax.xaxis.set_major_formatter(DateFormatter('%m/%d'))
        else:
            ax.xaxis.set_major_formatter(DateFormatter('%m/%d %H:%M'))
            
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f'{int(v)}'))
        ax.set_xlabel(xlabel, fontsize=12)
        ax.set_ylabel(ylabel, fontsize=12)
        ax.grid(True, linestyle='-', alpha=0.5)
        ax.legend()
        ax.margins(x=0.01)
        if timeframeDays:
            now = datetime.utcnow()
            ax.set_xlim(now - timedelta(days=timeframeDays), now)

    elif graphType == "pie":
        colorMap = plt.cm.get_cmap('tab20c')
        cleanedLabels = [s.split(" — ", 1)[0] for s in x] # We remove this to get just dungeon name for colors to be consistent
        colors = [colorMap((int(hashlib.md5(label.encode()).hexdigest(), 16) % colorMap.N) / colorMap.N)for label in cleanedLabels]

        wedges, texts, autotexts = ax.pie(
            y,
            labels=x,
            autopct='%1.1f%%',
            startangle=90,
            colors=colors
        )
        ax.axis('equal')
        if legendName:
            ax.legend(wedges, x, title=legendName, loc="center left", bbox_to_anchor=(1, 0.5))
        plt.subplots_adjust(right=0.75)

    else:
        raise ValueError(f"Unknown graph type: {graphType}")

    plt.title(title, fontsize=14)
    plt.tight_layout()
    plt.text(
        1.0, -0.1,
        f"Generated at {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')} UTC.",
        transform=ax.transAxes,
        fontsize=9,
        verticalalignment='bottom',
        horizontalalignment='right',
        color=timeColor
    )

    buf = io.BytesIO()
    fig.savefig(buf, format='webp', bbox_inches='tight', dpi=100, pil_kwargs={'quality': 85})
    plt.close(fig)
    buf.seek(0)
    img = base64.b64encode(buf.getvalue()).decode()
    
    return img

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception as exc:
        logger.exception(f"Unhandled error during {request.method} {request.url.path}: {exc}")
        response = JSONResponse(status_code=500, content={"detail": "Internal server error"})
        
    process_time = time.perf_counter() - start_time
    response.headers["X-Process-Time"] = f"{process_time:.4f}s"
    logger.info(f"{request.method} {request.url} with status {response.status_code} in {process_time:.4f}s")
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
    
    return await searchMaster("guild_uuid", uuid)
    
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

    conn = connectDB()
    cursor = conn.cursor()

    # Get player_uuid from latest run
    cursor.execute("""
        SELECT player_uuid, username
        FROM user_history
        WHERE username = ? COLLATE NOCASE
        LIMIT 1
    """, (username,))
    user_row = cursor.fetchone()
    if not user_row:
        conn.close()
        return JSONResponse(status_code=404, content={"error": "Player not found"})

    player_uuid = user_row["player_uuid"]

    cursor.execute("""
        SELECT online, last_join, playtime, guild_uuid, timestamp
        FROM player_snapshots
        WHERE player_uuid = ?
        ORDER BY timestamp DESC
        LIMIT 1
    """, (player_uuid,))
    snapshot = cursor.fetchone()

    data = {
        "player_uuid": player_uuid,
        "username": user_row["username"],
    }
    if snapshot:
        data.update(dict(snapshot))

    cursor.execute("""
        SELECT wars, mobs_killed, total_dungeons, total_raids, total_graids
        FROM player_snapshots
        WHERE player_uuid = ?
        ORDER BY timestamp DESC
        LIMIT 1
    """, (player_uuid,))
    global_snap = cursor.fetchone()
    if global_snap:
        temp = dict(global_snap)
        cursor.execute("""
            SELECT dungeon_dict, raid_dict, graid_dict
            FROM player_current_stats
            WHERE player_uuid = ?
        """, (player_uuid,))
        stats_row = cursor.fetchone()
        if stats_row:
            for key in ("dungeon_dict", "raid_dict", "graid_dict"):
                val = stats_row[key]
                if val:
                    try:
                        temp[key] = ast.literal_eval(val)
                    except (ValueError, SyntaxError):
                        temp[key] = None
                else:
                    temp[key] = None
        data["globalData"] = temp
    else:
        data["globalData"] = None

    conn.close()
    return data

#TODO: Make sure leaderboard and activity does NOT rely on the 'users' table, and instead relies on 'user_history' as its never purged
#TODO: Test every leaderboard command to make sure it works with new timeframes
#TODO: Fix embeds having line wrap
@leaderboardRouter.get("/{leaderboardType}")
@cache_route(ttl=600) #10m cache
async def leaderboard(leaderboardType: str, timeframe: str | None = None, uuid: str | None = None):
    if not leaderboardType:
        return JSONResponse(status_code=400, content={"error": "Please provide a valid leaderboard type."})
    dbConn = connectDB()
    dbCursor = dbConn.cursor()
    if timeframe: # For commands which use it
        startDate, endDate = getTimeframe(timeframe)
    match leaderboardType:
        case "guildLeaderboardOnlineMembers": #? WORKS BUT NOTE: AL TIME DEFAULTS TO 30D BECAUSE OF A BAD SYSTEM.
            if timeframe == "All Time":
                startDate, endDate = getTimeframe("Last 30 Days") # all time is useless and i dont want to make an error for this specific issue.
            query = """
            WITH avg_online AS (
                SELECT
                    g.guild_uuid,
                    g.name,
                    g.prefix,
                    ROUND(CAST(SUM(gs.online_members) AS REAL) / COUNT(*), 2) AS avg_online_members
                FROM guilds g
                JOIN guild_snapshots gs ON g.guild_uuid = gs.guild_uuid
            """
            params = []
            if startDate and endDate:
                query += " WHERE gs.timestamp BETWEEN ? AND ? "
                params.extend([
                    startDate.strftime("%Y-%m-%d %H:%M:%S"),
                    endDate.strftime("%Y-%m-%d %H:%M:%S")
                ])
            query += """
                GROUP BY g.guild_uuid
                HAVING COUNT(*) >= 1
            )
            SELECT
                name || ' (' || prefix || ')' AS guild_display_name,
                avg_online_members
            FROM avg_online
            ORDER BY avg_online_members DESC
            LIMIT 100;
            """
            dbCursor.execute(query, params)
            data = dbCursor.fetchall()

        case "guildLeaderboardWars": #? WORKS
            if startDate and endDate:
                query = """
                WITH war_gains AS (
                    SELECT
                        g.guild_uuid,
                        g.name,
                        g.prefix,
                        MAX(gs.wars) - MIN(gs.wars) AS wars_gained
                    FROM guilds g
                    JOIN guild_snapshots gs ON g.guild_uuid = gs.guild_uuid
                    WHERE gs.timestamp BETWEEN ? AND ?
                    GROUP BY g.guild_uuid
                    HAVING COUNT(*) >= 2
                )
                SELECT
                    name || ' (' || prefix || ')' AS guild_display_name,
                    wars_gained
                FROM war_gains
                WHERE wars_gained > 0
                ORDER BY wars_gained DESC
                LIMIT 100;
                """
                params = [
                    startDate.strftime("%Y-%m-%d %H:%M:%S"),
                    endDate.strftime("%Y-%m-%d %H:%M:%S")
                ]
            else:
                query = """
                SELECT
                    g.name || ' (' || g.prefix || ')' AS guild_display_name,
                    MAX(gs.wars) AS wars_gained
                FROM guilds g
                JOIN guild_snapshots gs ON g.guild_uuid = gs.guild_uuid
                GROUP BY g.guild_uuid
                HAVING MAX(gs.wars) > 0
                ORDER BY wars_gained DESC
                LIMIT 100;
                """
                params = []
            dbCursor.execute(query, params)
            data = dbCursor.fetchall()

        case "guildLeaderboardXP": #! WORKS BUT INSANELY SLOW
            if startDate and endDate:
                query = """
                WITH member_bounds AS (
                    SELECT
                        guild_uuid,
                        player_uuid,
                        MIN(contribution) AS min_contribution,
                        MAX(contribution) AS max_contribution
                    FROM player_snapshots
                    WHERE timestamp BETWEEN ? AND ?
                    GROUP BY guild_uuid, player_uuid
                    HAVING COUNT(*) >= 2
                ),
                guild_totals AS (
                    SELECT
                        guild_uuid,
                        SUM(max_contribution - min_contribution) AS xp_gained
                    FROM member_bounds
                    GROUP BY guild_uuid
                )
                SELECT
                    g.name || ' (' || g.prefix || ')' AS guild_display_name,
                    gt.xp_gained
                FROM guild_totals gt
                JOIN guilds g ON g.guild_uuid = gt.guild_uuid
                WHERE gt.xp_gained > 0
                ORDER BY gt.xp_gained DESC
                LIMIT 100;
                """
                params = [
                    startDate.strftime("%Y-%m-%d %H:%M:%S"),
                    endDate.strftime("%Y-%m-%d %H:%M:%S")
                ]
            else:
                query = """
                WITH latest_ts AS (
                    SELECT player_uuid, MAX(timestamp) AS max_ts
                    FROM player_snapshots
                    WHERE timestamp >= datetime('now', '-3 days')
                    GROUP BY player_uuid
                ),
                member_max AS (
                    SELECT
                        ps.guild_uuid,
                        ps.player_uuid,
                        ps.contribution AS max_contribution
                    FROM player_snapshots ps
                    JOIN latest_ts lt ON ps.player_uuid = lt.player_uuid AND ps.timestamp = lt.max_ts
                    WHERE ps.contribution > 0
                ),
                guild_totals AS (
                    SELECT
                        guild_uuid,
                        SUM(max_contribution) AS xp_gained
                    FROM member_max
                    GROUP BY guild_uuid
                )
                SELECT
                    g.name || ' (' || g.prefix || ')' AS guild_display_name,
                    gt.xp_gained
                FROM guild_totals gt
                JOIN guilds g ON g.guild_uuid = gt.guild_uuid
                WHERE gt.xp_gained > 0
                ORDER BY gt.xp_gained DESC
                LIMIT 100;
                """
                params = []
            dbCursor.execute(query, params)
            data = dbCursor.fetchall()

        case "playerLeaderboardRaids":  #! WORKS BUT INSANELY SLOW
            if startDate and endDate:
                query = """
                WITH player_bounds AS (
                    SELECT
                        player_uuid,
                        MIN(total_raids) AS min_raids,
                        MAX(total_raids) AS max_raids
                    FROM player_snapshots
                    WHERE timestamp BETWEEN ? AND ?
                    GROUP BY player_uuid
                    HAVING COUNT(*) >= 2
                ),
                latest_username AS (
                    SELECT uh.player_uuid, uh.username
                    FROM user_history uh
                    JOIN (
                        SELECT player_uuid, MAX(timestamp) AS max_ts
                        FROM user_history
                        WHERE player_uuid IN (SELECT player_uuid FROM player_bounds)
                        GROUP BY player_uuid
                    ) mx ON uh.player_uuid = mx.player_uuid AND uh.timestamp = mx.max_ts
                )
                SELECT
                    lu.username,
                    (pb.max_raids - pb.min_raids) AS raids_gained
                FROM player_bounds pb
                JOIN latest_username lu ON lu.player_uuid = pb.player_uuid
                WHERE (pb.max_raids - pb.min_raids) > 0
                ORDER BY raids_gained DESC
                LIMIT 100;
                """
                params = [
                    startDate.strftime("%Y-%m-%d %H:%M:%S"),
                    endDate.strftime("%Y-%m-%d %H:%M:%S")
                ]
            else:
                query = """
                WITH active_players AS (
                    SELECT DISTINCT player_uuid
                    FROM player_snapshots
                    WHERE timestamp >= datetime('now', '-3 days')
                ),
                latest_username AS (
                    SELECT uh.player_uuid, uh.username
                    FROM user_history uh
                    JOIN (
                        SELECT player_uuid, MAX(timestamp) AS max_ts
                        FROM user_history
                        WHERE player_uuid IN (SELECT player_uuid FROM active_players)
                        GROUP BY player_uuid
                    ) mx ON uh.player_uuid = mx.player_uuid AND uh.timestamp = mx.max_ts
                )
                SELECT
                    lu.username,
                    (SELECT ps.total_raids FROM player_snapshots ps
                     WHERE ps.player_uuid = lu.player_uuid
                     ORDER BY ps.timestamp DESC LIMIT 1) AS total_raids
                FROM latest_username lu
                WHERE total_raids > 0
                ORDER BY total_raids DESC
                LIMIT 100;
                """
                params = []
            dbCursor.execute(query, params)
            data = dbCursor.fetchall()

        case "playerLeaderboardDungeons": #! WORKS BUT INSANELY SLOW
            if startDate and endDate:
                query = """
                WITH player_bounds AS (
                    SELECT
                        player_uuid,
                        MIN(total_dungeons) AS min_dungeons,
                        MAX(total_dungeons) AS max_dungeons
                    FROM player_snapshots
                    WHERE timestamp BETWEEN ? AND ?
                    GROUP BY player_uuid
                    HAVING COUNT(*) >= 2
                ),
                latest_username AS (
                    SELECT uh.player_uuid, uh.username
                    FROM user_history uh
                    JOIN (
                        SELECT player_uuid, MAX(timestamp) AS max_ts
                        FROM user_history
                        WHERE player_uuid IN (SELECT player_uuid FROM player_bounds)
                        GROUP BY player_uuid
                    ) mx ON uh.player_uuid = mx.player_uuid AND uh.timestamp = mx.max_ts
                )
                SELECT
                    lu.username,
                    (pb.max_dungeons - pb.min_dungeons) AS dungeons_gained
                FROM player_bounds pb
                JOIN latest_username lu ON lu.player_uuid = pb.player_uuid
                WHERE (pb.max_dungeons - pb.min_dungeons) > 0
                ORDER BY dungeons_gained DESC
                LIMIT 100;
                """
                params = [
                    startDate.strftime("%Y-%m-%d %H:%M:%S"),
                    endDate.strftime("%Y-%m-%d %H:%M:%S")
                ]
            else:
                query = """
                WITH active_players AS (
                    SELECT DISTINCT player_uuid
                    FROM player_snapshots
                    WHERE timestamp >= datetime('now', '-3 days')
                ),
                latest_username AS (
                    SELECT uh.player_uuid, uh.username
                    FROM user_history uh
                    JOIN (
                        SELECT player_uuid, MAX(timestamp) AS max_ts
                        FROM user_history
                        WHERE player_uuid IN (SELECT player_uuid FROM active_players)
                        GROUP BY player_uuid
                    ) mx ON uh.player_uuid = mx.player_uuid AND uh.timestamp = mx.max_ts
                )
                SELECT
                    lu.username,
                    (SELECT ps.total_dungeons FROM player_snapshots ps
                     WHERE ps.player_uuid = lu.player_uuid
                     ORDER BY ps.timestamp DESC LIMIT 1) AS total_dungeons
                FROM latest_username lu
                WHERE total_dungeons > 0
                ORDER BY total_dungeons DESC
                LIMIT 100;
                """
                params = []
            dbCursor.execute(query, params)
            data = dbCursor.fetchall()

        case "playerLeaderboardPlaytime":  #! WORKS BUT INSANELY SLOW, ALL TIME JUST TAKES WAYYY TOO LONG
            if startDate is None:
                query = """
                WITH active_players AS (
                    SELECT DISTINCT player_uuid
                    FROM player_snapshots
                    WHERE timestamp >= datetime('now', '-3 days')
                ),
                latest_username AS (
                    SELECT uh.player_uuid, uh.username
                    FROM user_history uh
                    JOIN (
                        SELECT player_uuid, MAX(timestamp) AS max_ts
                        FROM user_history
                        WHERE player_uuid IN (SELECT player_uuid FROM active_players)
                        GROUP BY player_uuid
                    ) mx ON uh.player_uuid = mx.player_uuid AND uh.timestamp = mx.max_ts
                )
                SELECT
                    lu.username,
                    (SELECT ps.playtime FROM player_snapshots ps
                     WHERE ps.player_uuid = lu.player_uuid
                     ORDER BY ps.timestamp DESC LIMIT 1) AS playtime
                FROM latest_username lu
                WHERE playtime > 0
                ORDER BY playtime DESC
                LIMIT 100;
                """
                dbCursor.execute(query)
            else: #TODO: Make sure that playtime CANNOT take 0 because i think it is currently
                query = """
                WITH playtime_diff AS (
                    SELECT player_uuid, (MAX(playtime) - MIN(playtime)) AS playtime_gained
                    FROM player_snapshots
                    WHERE timestamp BETWEEN ? AND ?
                    AND playtime > 0
                    GROUP BY player_uuid
                    HAVING COUNT(*) >= 2
                ),
                latest_username AS (
                    SELECT uh.player_uuid, uh.username
                    FROM user_history uh
                    JOIN (
                        SELECT player_uuid, MAX(timestamp) AS max_ts
                        FROM user_history
                        WHERE player_uuid IN (SELECT player_uuid FROM playtime_diff)
                        GROUP BY player_uuid
                    ) mx ON uh.player_uuid = mx.player_uuid AND uh.timestamp = mx.max_ts
                )
                SELECT lu.username, pd.playtime_gained
                FROM playtime_diff pd
                JOIN latest_username lu ON lu.player_uuid = pd.player_uuid
                WHERE pd.playtime_gained > 0
                ORDER BY pd.playtime_gained DESC
                LIMIT 100;
                """
                dbCursor.execute(query, [
                    startDate.strftime("%Y-%m-%d %H:%M:%S"),
                    endDate.strftime("%Y-%m-%d %H:%M:%S")
                ])
            data = dbCursor.fetchall()

        case "guildLeaderboardXPButGuildSpecific": #? WORKS 
            if startDate and endDate:
                query = """
                WITH member_bounds AS (
                    SELECT
                        player_uuid,
                        MIN(contribution) AS min_contribution,
                        MAX(contribution) AS max_contribution
                    FROM player_snapshots
                    WHERE guild_uuid = ?
                    AND timestamp BETWEEN ? AND ?
                    GROUP BY player_uuid
                    HAVING COUNT(*) >= 2
                ),
                latest_username AS (
                    SELECT uh.player_uuid, uh.username
                    FROM user_history uh
                    JOIN (
                        SELECT player_uuid, MAX(timestamp) AS max_ts
                        FROM user_history
                        WHERE player_uuid IN (SELECT player_uuid FROM member_bounds)
                        GROUP BY player_uuid
                    ) mx ON uh.player_uuid = mx.player_uuid AND uh.timestamp = mx.max_ts
                )
                SELECT
                    lu.username,
                    (mb.max_contribution - mb.min_contribution) AS xp_gained
                FROM member_bounds mb
                JOIN latest_username lu ON lu.player_uuid = mb.player_uuid
                WHERE (mb.max_contribution - mb.min_contribution) > 0
                ORDER BY xp_gained DESC
                LIMIT 100;
                """
                params = [uuid, startDate.strftime("%Y-%m-%d %H:%M:%S"), endDate.strftime("%Y-%m-%d %H:%M:%S")]
            else:
                query = """
                WITH latest_username AS (
                    SELECT uh.player_uuid, uh.username
                    FROM user_history uh
                    JOIN (
                        SELECT player_uuid, MAX(timestamp) AS max_ts
                        FROM user_history
                        WHERE player_uuid IN (
                            SELECT player_uuid FROM player_snapshots WHERE guild_uuid = ?
                        )
                        GROUP BY player_uuid
                    ) mx ON uh.player_uuid = mx.player_uuid AND uh.timestamp = mx.max_ts
                )
                SELECT
                    lu.username,
                    MAX(ps.contribution) AS xp_gained
                FROM player_snapshots ps
                JOIN latest_username lu ON lu.player_uuid = ps.player_uuid
                WHERE ps.guild_uuid = ?
                GROUP BY ps.player_uuid
                HAVING xp_gained > 0
                ORDER BY xp_gained DESC
                LIMIT 100;
                """
                params = [uuid, uuid]
            dbCursor.execute(query, params)
            data = dbCursor.fetchall()

        case "guildLeaderboardOnlineButGuildSpecific": #? WORKS 
            if startDate and endDate:
                query = """
                WITH playtime_diff AS (
                    SELECT player_uuid, (MAX(playtime) - MIN(playtime)) AS playtime_gained
                    FROM player_snapshots
                    WHERE guild_uuid = ?
                    AND playtime > 0
                    AND timestamp BETWEEN ? AND ?
                    GROUP BY player_uuid
                    HAVING COUNT(*) >= 2
                ),
                latest_username AS (
                    SELECT uh.player_uuid, uh.username
                    FROM user_history uh
                    JOIN (
                        SELECT player_uuid, MAX(timestamp) AS max_ts
                        FROM user_history
                        WHERE player_uuid IN (SELECT player_uuid FROM playtime_diff)
                        GROUP BY player_uuid
                    ) mx ON uh.player_uuid = mx.player_uuid AND uh.timestamp = mx.max_ts
                )
                SELECT lu.username, pd.playtime_gained
                FROM playtime_diff pd
                JOIN latest_username lu ON lu.player_uuid = pd.player_uuid
                ORDER BY pd.playtime_gained DESC
                LIMIT 150;
                """
                params = [uuid, startDate.strftime("%Y-%m-%d %H:%M:%S"), endDate.strftime("%Y-%m-%d %H:%M:%S")]
            else:
                query = """
                WITH latest_username AS (
                    SELECT uh.player_uuid, uh.username
                    FROM user_history uh
                    JOIN (
                        SELECT player_uuid, MAX(timestamp) AS max_ts
                        FROM user_history
                        WHERE player_uuid IN (
                            SELECT player_uuid FROM player_snapshots WHERE guild_uuid = ?
                        )
                        GROUP BY player_uuid
                    ) mx ON uh.player_uuid = mx.player_uuid AND uh.timestamp = mx.max_ts
                )
                SELECT lu.username, MAX(ps.playtime) AS playtime_gained
                FROM player_snapshots ps
                JOIN latest_username lu ON lu.player_uuid = ps.player_uuid
                WHERE ps.guild_uuid = ?
                AND ps.playtime > 0
                GROUP BY ps.player_uuid
                ORDER BY playtime_gained DESC
                LIMIT 150;
                """
                params = [uuid, uuid]
            dbCursor.execute(query, params)
            data = dbCursor.fetchall()

        case "guildLeaderboardWarsButGuildSpecific": #? WORKS  
            if startDate and endDate:
                query = """
                WITH war_bounds AS (
                    SELECT
                        player_uuid,
                        MIN(wars) AS min_wars,
                        MAX(wars) AS max_wars
                    FROM player_snapshots
                    WHERE guild_uuid = ?
                    AND timestamp BETWEEN ? AND ?
                    GROUP BY player_uuid
                    HAVING COUNT(*) >= 2
                ),
                latest_username AS (
                    SELECT uh.player_uuid, uh.username
                    FROM user_history uh
                    JOIN (
                        SELECT player_uuid, MAX(timestamp) AS max_ts
                        FROM user_history
                        WHERE player_uuid IN (SELECT player_uuid FROM war_bounds)
                        GROUP BY player_uuid
                    ) mx ON uh.player_uuid = mx.player_uuid AND uh.timestamp = mx.max_ts
                )
                SELECT
                    lu.username,
                    (wb.max_wars - wb.min_wars) AS wars_gained
                FROM war_bounds wb
                JOIN latest_username lu ON lu.player_uuid = wb.player_uuid
                WHERE (wb.max_wars - wb.min_wars) > 0
                ORDER BY wars_gained DESC
                LIMIT 100;
                """
                params = [uuid, startDate.strftime("%Y-%m-%d %H:%M:%S"), endDate.strftime("%Y-%m-%d %H:%M:%S")]
            else:
                query = """
                WITH latest_username AS (
                    SELECT uh.player_uuid, uh.username
                    FROM user_history uh
                    JOIN (
                        SELECT player_uuid, MAX(timestamp) AS max_ts
                        FROM user_history
                        WHERE player_uuid IN (
                            SELECT player_uuid FROM player_snapshots WHERE guild_uuid = ?
                        )
                        GROUP BY player_uuid
                    ) mx ON uh.player_uuid = mx.player_uuid AND uh.timestamp = mx.max_ts
                )
                SELECT
                    lu.username,
                    MAX(ps.wars) AS wars_gained
                FROM player_snapshots ps
                JOIN latest_username lu ON lu.player_uuid = ps.player_uuid
                WHERE ps.guild_uuid = ?
                GROUP BY ps.player_uuid
                HAVING wars_gained > 0
                ORDER BY wars_gained DESC
                LIMIT 100;
                """
                params = [uuid, uuid]
            dbCursor.execute(query, params)
            data = dbCursor.fetchall()

        case "guildLeaderboardGraids": #! JUST VERY WRONG BUT COULD BE API BUGS, LOOK INTO AFTER A WEEK OF API CHANGES, ALSO REALLY SLOW STILL
            if startDate and endDate:
                query = """
                WITH guild_raid_gains AS (
                    SELECT
                        guild_uuid,
                        MAX(guild_raids) - MIN(guild_raids) AS graid_delta
                    FROM guild_snapshots
                    WHERE guild_raids IS NOT NULL
                    AND timestamp BETWEEN ? AND ?
                    GROUP BY guild_uuid
                    HAVING COUNT(*) >= 2
                )
                SELECT
                    g.name || ' (' || g.prefix || ')' AS guild_display_name,
                    grg.graid_delta AS total_graids
                FROM guild_raid_gains grg
                JOIN guilds g ON g.guild_uuid = grg.guild_uuid
                WHERE grg.graid_delta > 0
                ORDER BY total_graids DESC
                LIMIT 100;
                """
                params = [startDate.strftime("%Y-%m-%d %H:%M:%S"), endDate.strftime("%Y-%m-%d %H:%M:%S")]
            else:
                query = """
                SELECT
                    g.name || ' (' || g.prefix || ')' AS guild_display_name,
                    MAX(gs.guild_raids) AS total_graids
                FROM guild_snapshots gs
                JOIN guilds g ON g.guild_uuid = gs.guild_uuid
                WHERE gs.guild_raids IS NOT NULL
                GROUP BY gs.guild_uuid
                HAVING total_graids > 0
                ORDER BY total_graids DESC
                LIMIT 100;
                """
                params = []
            dbCursor.execute(query, params)
            data = dbCursor.fetchall()

        case "guildLeaderboardGraidsButGuildSpecific": #! JUST VERY WRONG BUT COULD BE API BUGS, LOOK INTO AFTER A WEEK OF API CHANGES, ALSO REALLY SLOW STILL
            if startDate and endDate:
                query = """
                WITH player_deltas AS (
                    SELECT
                        player_uuid,
                        MAX(total_graids) - MIN(total_graids) AS graid_delta
                    FROM player_snapshots
                    WHERE guild_uuid = ?
                    AND timestamp BETWEEN ? AND ?
                    GROUP BY player_uuid
                    HAVING COUNT(*) >= 2 AND MIN(total_graids) >= 1
                ),
                latest_username AS (
                    SELECT uh.player_uuid, uh.username
                    FROM user_history uh
                    JOIN (
                        SELECT player_uuid, MAX(timestamp) AS max_ts
                        FROM user_history
                        WHERE player_uuid IN (SELECT player_uuid FROM player_deltas)
                        GROUP BY player_uuid
                    ) mx ON uh.player_uuid = mx.player_uuid AND uh.timestamp = mx.max_ts
                )
                SELECT
                    lu.username,
                    pd.graid_delta AS graids_done
                FROM player_deltas pd
                JOIN latest_username lu ON lu.player_uuid = pd.player_uuid
                WHERE pd.graid_delta > 0
                ORDER BY graids_done DESC
                LIMIT 100;
                """
                params = [uuid, startDate.strftime("%Y-%m-%d %H:%M:%S"), endDate.strftime("%Y-%m-%d %H:%M:%S")]
            else:
                query = """
                WITH guild_players AS (
                    SELECT player_uuid, MAX(total_graids) AS graids_done
                    FROM player_snapshots
                    WHERE guild_uuid = ?
                    GROUP BY player_uuid
                    HAVING graids_done > 0
                ),
                latest_username AS (
                    SELECT uh.player_uuid, uh.username
                    FROM user_history uh
                    JOIN (
                        SELECT player_uuid, MAX(timestamp) AS max_ts
                        FROM user_history
                        WHERE player_uuid IN (SELECT player_uuid FROM guild_players)
                        GROUP BY player_uuid
                    ) mx ON uh.player_uuid = mx.player_uuid AND uh.timestamp = mx.max_ts
                )
                SELECT
                    lu.username,
                    gp.graids_done
                FROM guild_players gp
                JOIN latest_username lu ON lu.player_uuid = gp.player_uuid
                ORDER BY graids_done DESC
                LIMIT 100;
                """
                params = [uuid]
            dbCursor.execute(query, params)
            data = dbCursor.fetchall()

        case "playerLeaderboardGraids":  #! JUST VERY WRONG BUT COULD BE API BUGS, LOOK INTO AFTER A WEEK OF API CHANGES, ALSO REALLY SLOW STILL
            if startDate and endDate:
                query = """
                WITH player_deltas AS (
                    SELECT
                        player_uuid,
                        MAX(total_graids) - MIN(total_graids) AS graid_delta
                    FROM player_snapshots
                    WHERE timestamp BETWEEN ? AND ?
                    GROUP BY player_uuid
                    HAVING COUNT(*) >= 2 AND MIN(total_graids) >= 1
                ),
                latest_username AS (
                    SELECT uh.player_uuid, uh.username
                    FROM user_history uh
                    JOIN (
                        SELECT player_uuid, MAX(timestamp) AS max_ts
                        FROM user_history
                        WHERE player_uuid IN (SELECT player_uuid FROM player_deltas)
                        GROUP BY player_uuid
                    ) mx ON uh.player_uuid = mx.player_uuid AND uh.timestamp = mx.max_ts
                )
                SELECT
                    lu.username,
                    pd.graid_delta AS graids_done
                FROM player_deltas pd
                JOIN latest_username lu ON lu.player_uuid = pd.player_uuid
                WHERE pd.graid_delta > 0
                ORDER BY graids_done DESC
                LIMIT 100;
                """
                params = [startDate.strftime("%Y-%m-%d %H:%M:%S"), endDate.strftime("%Y-%m-%d %H:%M:%S")]
            else:
                query = """
                WITH active_players AS (
                    SELECT DISTINCT player_uuid
                    FROM player_snapshots
                    WHERE timestamp >= datetime('now', '-3 days')
                ),
                latest_username AS (
                    SELECT uh.player_uuid, uh.username
                    FROM user_history uh
                    JOIN (
                        SELECT player_uuid, MAX(timestamp) AS max_ts
                        FROM user_history
                        WHERE player_uuid IN (SELECT player_uuid FROM active_players)
                        GROUP BY player_uuid
                    ) mx ON uh.player_uuid = mx.player_uuid AND uh.timestamp = mx.max_ts
                )
                SELECT
                    lu.username,
                    (SELECT ps.total_graids FROM player_snapshots ps
                     WHERE ps.player_uuid = lu.player_uuid
                     ORDER BY ps.timestamp DESC LIMIT 1) AS graids_done
                FROM latest_username lu
                WHERE graids_done > 0
                ORDER BY graids_done DESC
                LIMIT 100;
                """
                params = []
            dbCursor.execute(query, params)
            data = dbCursor.fetchall()

        case _: # Default case
            return JSONResponse(status_code=400, content={"error": "Please provide a valid leaderboard type."})


    dbCursor.close()
    return data

@seasonRatingdRouter.get("/")
@cache_route(ttl=600) #10m cache
async def seasonLeaderboard(season: int | None = None, uuid: str | None = None):
    if not season and not uuid:
        return JSONResponse(status_code=400, content={"error": "Please provide a valid season or guild uuid."})
    dbConn = connectDB()
    dbCursor = dbConn.cursor()
    if uuid: # Get the individual guild's data with rank per season
        dbCursor.execute("""
            WITH ranked AS (
                SELECT guild_uuid, season, rating,
                       RANK() OVER (PARTITION BY season ORDER BY rating DESC) AS rank
                FROM guild_season_ratings
            )
            SELECT g.name || ' (' || g.prefix || ')' AS guild, r.season, r.rating, r.rank
            FROM ranked r
            JOIN guilds g ON g.guild_uuid = r.guild_uuid
            WHERE r.guild_uuid = ?
            ORDER BY r.season DESC
        """, (uuid,))
        snapshots = dbCursor.fetchall()
        rows = [dict(r) for r in snapshots]
    else: # Get a season's rankings
        dbCursor.execute("""
            SELECT g.name || ' (' || g.prefix || ')' AS guild, gsr.rating
            FROM guild_season_ratings gsr
            JOIN guilds g ON g.guild_uuid = gsr.guild_uuid
            WHERE gsr.season = ?
            ORDER BY gsr.rating DESC
            LIMIT 100;
        """, (season,))
        snapshots = dbCursor.fetchall()
        rows = [dict(r) for r in snapshots]

    dbCursor.close()
    return rows

@activityRouter.get("/{activityType}")
@cache_route(ttl=600) #10m cache
async def activity(activityType: str, uuid: str | None = None, name: str | None = None, theme: str | None = None, timeframe: str | None = None): # Name can be either prefix or gname or player username
    if not activityType:
        return JSONResponse(status_code=400, content={"error": "Please provide a valid leaderboard type."})
    
    mpl.rcParams.update(mpl.rcParamsDefault)
    match theme:
        case "light":
            sns.set_style("whitegrid")
            mpl.use('Agg') # Backend without any gui popping up
            blue, = sns.color_palette("muted", 1)
            color = "black" # color to use for the lil generated at text
            
        case "dark":
            sns.set_theme(
                style="whitegrid",
                rc={
                    "axes.facecolor": "#121212",
                    "axes.edgecolor": "#444444",
                    "figure.facecolor": "#121212",
                    "grid.color": "#666666",
                    "text.color": "white",
                    "axes.labelcolor": "white",
                    "xtick.color": "white",
                    "ytick.color": "white",
                    "legend.facecolor": "#1e1e1e",
                    "legend.edgecolor": "#333333",
                }
            )
            mpl.use('Agg') # Backend without any gui popping up
            blue, = sns.color_palette("muted", 1)
            color = "white" # color to use for the lil generated at text
        case "discord":
            sns.set_theme(
                style="whitegrid",
                rc={
                    "axes.facecolor": "#323339",
                    "axes.edgecolor": "#7289da",
                    "figure.facecolor": "#323339",
                    "grid.color": "#c4c5c9",
                    "text.color": "white",
                    "axes.labelcolor": "white",
                    "xtick.color": "white",
                    "ytick.color": "white",
                    "legend.facecolor": "#323339",
                    "legend.edgecolor": "#c4c5c9",
                }
            )
            mpl.use('Agg') # Backend without any gui popping up
            blue, = sns.color_palette("muted", 1)
            color = "white" # color to use for the lil generated at text
        case _: # default, as of rn its just defaulting to light mode
            sns.set_style("whitegrid")
            mpl.use('Agg') # Backend without any gui popping up
            blue, = sns.color_palette("muted", 1)
            color = "black" # color to use for the lil generated at text
    
    guildConn = connectDB()
    playerConn = connectDB()
    guildCursor = guildConn.cursor()
    playerCursor = playerConn.cursor()

    numDays = getTimeframe(timeframe, type="activity")
    
    match activityType:
        case "guildActivityXP": #? WORKS
            guildCursor.execute("""
            WITH RECURSIVE dates(date) AS (
                SELECT date('now', printf('-%d days', ?))
                UNION ALL
                SELECT date(date, '+1 day')
                FROM dates
                WHERE date < date('now')
            ),
            xp_data AS (
                SELECT
                    date(timestamp) AS day,
                    MAX(contribution) - MIN(contribution) AS daily_xp
                FROM player_snapshots
                WHERE guild_uuid = ?
                AND timestamp >= datetime('now', printf('-%d days', ?))
                GROUP BY date(timestamp), player_uuid
            )
            SELECT
                dates.date,
                COALESCE(SUM(xp_data.daily_xp), 0) AS total_xp
            FROM dates
            LEFT JOIN xp_data ON dates.date = xp_data.day
            GROUP BY dates.date
            ORDER BY dates.date;
            """, (numDays - 1, uuid, numDays - 1))
            
            snapshots = guildCursor.fetchall()
            if not snapshots:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            dates = []
            xp_values = []
            for date_str, xp in snapshots:
                dates.append(datetime.strptime(date_str, '%Y-%m-%d'))
                xp_values.append(xp)

            total_xp = sum(xp_values)
            avg_daily_xp = total_xp / len(dates) if dates else 0
            max_daily_xp = max(xp_values) if xp_values else 0
            min_daily_xp = min(xp_values) if xp_values else 0
            
            img = createPlot(dates, xp_values, "bar", blue, f'Daily Guild XP Contribution - {name}', 'Date (UTC)', 'XP Gained', color, ahxlineY = avg_daily_xp, ahxlineLabel = f'Daily Average: {avg_daily_xp:,.0f} XP', timeframeDays=numDays)
            return JSONResponse({"total_xp": total_xp, "daily_average": avg_daily_xp, "highest_day": max_daily_xp, "lowest_day": min_daily_xp, "image": img})
        
        case "guildActivityTerritories": #? WORKS
            guildCursor.execute("""
                WITH RECURSIVE 
                timepoints AS (
                    SELECT datetime('now', printf('-%d days', ?)) AS timepoint
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
            """, (numDays, uuid,))
            snapshots = guildCursor.fetchall()
            if not snapshots or all(count == 0 for _, count in snapshots):
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})
            
            times = []
            territory_counts = []
            for timestamp_str, count in snapshots:
                try:
                    times.append(datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S'))
                    territory_counts.append(float(count) if count is not None else 0.0)
                except (ValueError, TypeError) as e: # shouldnt happen, but after the amount of errors i ran from this, idk
                    continue

            if not times or not territory_counts:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})
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

            img = createPlot(times, territory_counts, "line", blue, f'Territory Count - {name}', 'Date (UTC)', 'Number of Territories', color, ahxlineY = avg_territories, ahxlineLabel = f'Average: {avg_territories:.1f}', fillBetween = True, legendName='Territory Count', timeframeDays=numDays)
            return JSONResponse({"current_territories": current_territories, "maximum_territories": max_territories, "minimum_territories": min_territories, "average_territories": avg_territories, "image": img})
            
        case "guildActivityWars": #? WORKS
            guildCursor.execute("""
                WITH RECURSIVE 
                timepoints AS (
                    SELECT datetime('now', printf('-%d days', ?)) AS timepoint
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
            """, (numDays, uuid,))
            snapshots = guildCursor.fetchall()
            if not snapshots or all(count == 0 for _, count in snapshots):
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})
            
            times = []
            war_counts = []
            for timestamp_str, count in snapshots:
                try:
                    times.append(datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S'))
                    war_counts.append(float(count) if count is not None else 0.0)
                except (ValueError, TypeError) as e: # shouldnt happen, but after the amount of errors i ran from this, idk
                    continue

            if not times or not war_counts:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})
            non_zero_indices = [i for i, count in enumerate(war_counts) if count > 0]
            if non_zero_indices:
                start_idx = non_zero_indices[0]
                end_idx = non_zero_indices[-1] + 1
                times = times[start_idx:end_idx]
                war_counts = war_counts[start_idx:end_idx]

            current_war = war_counts[-1] if war_counts else 0
            max_war = max(war_counts) if war_counts else 0
            min_war = min(filter(lambda x: x > 0, war_counts)) if war_counts else 0

            img = createPlot(times, war_counts, "line", blue, f'War History - {name}', 'Date (UTC)', 'Number of Wars', color, legendName='War Count', timeframeDays=numDays)
            return JSONResponse({"current_war": current_war,  "image": img})
        
        case "guildActivityOnlineMembers": #? WORKS
            guildCursor.execute("""
                SELECT timestamp, online_members
                FROM guild_snapshots
                WHERE guild_uuid = ?
                AND timestamp >= datetime('now', printf('-%d days', ?))
                ORDER BY timestamp
            """, (uuid, numDays))
            snapshots = guildCursor.fetchall()
            
            if not snapshots:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})
            
            times = [datetime.fromisoformat(snapshot[0]) for snapshot in snapshots]
            raw_numbers = [snapshot[1] for snapshot in snapshots]
            
            overall_average = sum(raw_numbers) / len(raw_numbers) if raw_numbers else 0
            img = createPlot(times, raw_numbers, "line", blue, f'Online Members - {name}', 'Date (UTC)', 'Players Online', color, ahxlineY = overall_average, ahxlineLabel = f'Average: {overall_average:.1f} players', fillBetween = True, legendName='Average Online Member Count', timeframeDays=numDays)
            return JSONResponse({"max_players": max(raw_numbers), "min_players": min(raw_numbers), "average": overall_average, "image": img})
            
        case "guildActivityTotalMembers": #? WORKS
            guildCursor.execute("""
                SELECT timestamp, total_members
                FROM guild_snapshots
                WHERE guild_uuid = ?
                AND timestamp >= datetime('now', printf('-%d days', ?))
                ORDER BY timestamp
            """, (uuid, numDays))
            snapshots = guildCursor.fetchall()
            if not snapshots:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})
            
            times = [datetime.fromisoformat(snapshot[0]) for snapshot in snapshots]
            total_numbers = [snapshot[1] for snapshot in snapshots]
            overall_total = sum(total_numbers) / len(total_numbers) if total_numbers else 0
            img = createPlot(times, total_numbers, "line", blue, f'Member Count - {name}', 'Date (UTC)', 'Members', color, ahxlineY = overall_total, ahxlineLabel = f'Average: {overall_total:.1f} members', fillBetween = True, legendName='Total Members', timeframeDays=numDays)
            return JSONResponse({"max_players": max(total_numbers), "min_players": min(total_numbers), "average": overall_total, "image": img})
        
        case "playerActivityPlaytime": #? WORKS
            playerCursor.execute("""
            WITH RECURSIVE dates(day) AS (
                SELECT DATE('now', printf('-%d days', ?))
                UNION ALL
                SELECT DATE(day, '+1 day')
                FROM dates
                WHERE day < DATE('now')
            ),
            valid_playtime AS (
                SELECT player_uuid, timestamp,
                    CASE WHEN playtime <= 0 THEN NULL ELSE playtime END AS playtime
                FROM player_snapshots
            ),
            playtime_per_day AS (
                SELECT DATE(timestamp) AS day,
                    ROUND((MAX(playtime) - MIN(playtime)) * 60.0) AS playtime_minutes
                FROM valid_playtime
                WHERE player_uuid = ?
                AND DATE(timestamp) >= DATE('now', printf('-%d days', ?))
                GROUP BY DATE(timestamp)
                HAVING playtime_minutes > 0
            )
            SELECT d.day, p.playtime_minutes
            FROM dates d
            INNER JOIN playtime_per_day p ON d.day = p.day
            ORDER BY d.day;
            """, (numDays - 1, uuid, numDays - 1))
            daily_data = playerCursor.fetchall()

            if not daily_data:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            dailyPlaytimes = {
                datetime.strptime(day, '%Y-%m-%d').date(): minutes
                for day, minutes in daily_data
            }
            dates = sorted(dailyPlaytimes.keys())
            playtimeValues = [dailyPlaytimes[date] for date in dates]
            totalPlaytimeinMinutes = sum(playtimeValues)
            averageDailyPlaytime = totalPlaytimeinMinutes / len(dates) if dates else 0

            img = createPlot(dates, playtimeValues, "bar", blue, f'Daily Playtime - {name}', 'Date (UTC)', 'Minutes Played', color, ahxlineY = averageDailyPlaytime, ahxlineLabel = f'Daily Average: {averageDailyPlaytime:.0f} minutes', timeframeDays=numDays)
            return JSONResponse({"daily_average": averageDailyPlaytime, "max_day": max(playtimeValues) if playtimeValues else 0, "min_day": min(playtimeValues) if playtimeValues else 0, "image": img})
            
        case "playerActivityContributions": #? WORKS
            guildCursor.execute("""
            SELECT timestamp, contribution
            FROM player_snapshots
            WHERE player_uuid = ?
            AND timestamp >= datetime('now', printf('-%d days', ?))
            ORDER BY timestamp
            """, (uuid, numDays))
            snapshots = guildCursor.fetchall()

            if not snapshots:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            parsed_snapshots = [(datetime.fromisoformat(ts), xp) for ts, xp in snapshots]

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

            img = createPlot(timestamps[1:], daily_gains, "bar", blue, f'Daily XP Gain - {name}', 'Date (UTC)', 'XP Gained', color, ahxlineY = average, ahxlineLabel = f'Daily Average: {average:,.0f} XP', timeframeDays=numDays)
            return JSONResponse({"total_xp": totalGained, "max_xp": max(daily_gains) if daily_gains else 0, "min_xp": min(daily_gains) if daily_gains else 0,  "image": img})
        
        case "playerActivityDungeons": #? WORKS
            playerCursor.execute("""
            SELECT timestamp, total_dungeons
            FROM player_snapshots
            WHERE player_uuid = ?
                AND timestamp >= DATETIME('now', printf('-%d days', ?))
            ORDER BY timestamp;
            """, (uuid, numDays))
            snapshots = playerCursor.fetchall()

            if not snapshots:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            dates = [datetime.fromisoformat(row[0]) for row in snapshots]
            total_dungeons = [row[1] for row in snapshots]

            # Highest total and daily gain
            highestTotal = total_dungeons[-1] if total_dungeons else 0
            dungeons_by_day = defaultdict(list)
            for dt, count in zip(dates, total_dungeons):
                dungeons_by_day[dt.date()].append(count)
            dailyGain = [max(counts) - min(counts) for counts in dungeons_by_day.values() if len(counts) > 1]
            highestGain = max(dailyGain) if dailyGain else 0
            img = createPlot(dates, total_dungeons, "line", blue, f'Dungeon History - {name}', 'Date (UTC)', 'Number of Dungeon\'s completed', color, legendName='Dungeon Count', timeframeDays=numDays)
            return JSONResponse({"total_dungeons": highestTotal, "highest_gain": highestGain, "image": img})
            
        case "playerActivityTotalDungeons": #? WORKS
            playerCursor.execute("""
                SELECT dungeon_dict
                FROM player_current_stats
                WHERE player_uuid = ?
            """, (uuid,))
            snapshots = playerCursor.fetchall()

            if not snapshots:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})
            
            dungeons = ast.literal_eval(snapshots[0][0])

            sorted_dungeons = dict(sorted(dungeons.items(), key=lambda item: item[1], reverse=True))

            labels = list(sorted_dungeons.keys())
            sizes = list(sorted_dungeons.values())
            total = sum(sizes)
            percent_labels = [f"{label} — {size} ({(size / total * 100):.1f}%)" for label, size in zip(labels, sizes)]
            
            img = createPlot(percent_labels, sizes, "pie", None, f"Dungeon Pie Chart - {name}", None, None, color, legendName="Dungeons")
            return JSONResponse({"image": img}) # Technically we could just ship this out like how it is on other endpoints, just straight image, but all activity commands should and will b64 images for consistenty
        
        case "playerActivityRaids": #? WORKS
            playerCursor.execute("""
                SELECT timestamp, total_raids
                FROM player_snapshots
                WHERE player_uuid = ?
                    AND timestamp >= DATETIME('now', printf('-%d days', ?))
                ORDER BY timestamp;
            """, (uuid, numDays))
            snapshots = playerCursor.fetchall()

            if not snapshots:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            dates = [datetime.fromisoformat(row[0]) for row in snapshots]
            totalRaids = [row[1] for row in snapshots]

            # Highest total and daily gain
            highestTotal = totalRaids[-1] if totalRaids else 0
            raids_by_day = defaultdict(list)
            for dt, count in zip(dates, totalRaids):
                raids_by_day[dt.date()].append(count)
            dailyGain = [max(counts) - min(counts) for counts in raids_by_day.values() if len(counts) > 1]
            highestGain = max(dailyGain) if dailyGain else 0

            img = createPlot(dates, totalRaids, "line", blue, f'Raid History - {name}', 'Date (UTC)', 'Number of Raid\'s completed', color, legendName='Raid Count', timeframeDays=numDays)
            return JSONResponse({"total": highestTotal, "highest_gain": highestGain, "image": img})
            
        case "playerActivityTotalRaids": #? WORKS
            playerCursor.execute("""
                SELECT raid_dict
                FROM player_current_stats
                WHERE player_uuid = ?
            """, (uuid,))
            snapshots = playerCursor.fetchall()

            if not snapshots:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})
            
            raids = ast.literal_eval(snapshots[0][0])

            sortedRaids = dict(sorted(raids.items(), key=lambda item: item[1], reverse=True))

            labels = list(sortedRaids.keys())
            sizes = list(sortedRaids.values())
            total = sum(sizes)
            percent_labels = [f"{label} — {size} ({(size / total * 100):.1f}%)" for label, size in zip(labels, sizes)]

            img = createPlot(percent_labels, sizes, "pie", None, f"Raid Pie Chart - {name}", None, None, color, legendName="Raids")

            return JSONResponse({"image": img})
        
        case "playerActivityMobsKilled": #? WORKS
            playerCursor.execute("""
            SELECT timestamp, mobs_killed
            FROM player_snapshots
            WHERE player_uuid = ?
                AND timestamp >= DATETIME('now', printf('-%d days', ?))
            ORDER BY timestamp;
            """, (uuid, numDays))
            snapshots = playerCursor.fetchall()

            if not snapshots:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            dates = [datetime.fromisoformat(row[0]) for row in snapshots]
            totalKills = [row[1] for row in snapshots]
            highestTotal = totalKills[-1] if totalKills else 0
            kills_by_day = defaultdict(list)
            for dt, count in zip(dates, totalKills):
                kills_by_day[dt.date()].append(count)

            daily_gains = [max(counts) - min(counts) for counts in kills_by_day.values() if len(counts) > 1]
            highestGain = max(daily_gains) if daily_gains else 0
            img = createPlot(dates, totalKills, "line", blue, f'Mob Kill History - {name}', 'Date (UTC)', 'Number of Kill\'s', color, legendName='Mob Kill Count', timeframeDays=numDays)
            return JSONResponse({"total_kills": highestTotal, "highest_gain": highestGain, "image": img})
            
        case "playerActivityWars": #? WORKS
            playerCursor.execute("""
            SELECT timestamp, wars
            FROM player_snapshots
            WHERE player_uuid = ?
                AND timestamp >= DATETIME('now', printf('-%d days', ?))
            ORDER BY timestamp;
            """, (uuid, numDays))
            snapshots = playerCursor.fetchall()

            if not snapshots:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            dates = [datetime.fromisoformat(row[0]) for row in snapshots]
            totalWars = [row[1] for row in snapshots]
            highestTotal = totalWars[-1] if totalWars else 0
            wars_by_day = defaultdict(list)
            for dt, count in zip(dates, totalWars):
                wars_by_day[dt.date()].append(count)
            daily_gains = [max(counts) - min(counts) for counts in wars_by_day.values() if len(counts) > 1]
            highestGain = max(daily_gains) if daily_gains else 0

            img = createPlot(dates, totalWars, "line", blue, f'War Count History - {name}', 'Date (UTC)', 'Number of War\'s', color, legendName='War Count', timeframeDays=numDays)
            return JSONResponse({"total_wars": highestTotal, "highest_gain": highestGain, "image": img})
        
        case "guildActivityGraids": #? WORKS
            guildCursor.execute("""
            SELECT timestamp, guild_raids
            FROM guild_snapshots
            WHERE guild_uuid = ?
            AND timestamp >= datetime('now', printf('-%d days', ?))
            AND guild_raids IS NOT NULL
            ORDER BY timestamp
            """, (uuid, numDays))

            snapshots = guildCursor.fetchall()
            if not snapshots:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            dates = [datetime.fromisoformat(row[0]) for row in snapshots]
            totalGraids = [row[1] for row in snapshots]
            highestTotal = totalGraids[-1] if totalGraids else 0
            graids_by_day = defaultdict(list)
            for dt, count in zip(dates, totalGraids):
                graids_by_day[dt.date()].append(count)
            daily_gains = [max(counts) - min(counts) for counts in graids_by_day.values() if len(counts) > 1]
            highestGain = max(daily_gains) if daily_gains else 0
            avg_day = round(sum(daily_gains) / len(daily_gains), 2) if daily_gains else 0

            img = createPlot(dates, totalGraids, "line", blue, f'Guild Raid Activity - {name}', 'Date (UTC)', 'Total Guild Raids', color, legendName='Guild Raids', timeframeDays=numDays)
            return JSONResponse({"total_graid": highestTotal, "max_graid": highestGain, "average_graid": avg_day, "image": img})
        
        case "playerActivityGraids":  #? WORKS
            days = getTimeframe(timeframe, "activity")
            playerCursor.execute("""
                SELECT timestamp, total_graids
                FROM player_snapshots
                WHERE player_uuid = ?
                    AND timestamp >= DATETIME('now', printf('-%d days', ?))
                ORDER BY timestamp;
            """, (uuid, days))
            snapshots = playerCursor.fetchall()

            if not snapshots:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            dates = [datetime.fromisoformat(row[0]) for row in snapshots]
            totalGraids = [row[1] for row in snapshots]
            highestTotal = max(totalGraids)
            raids_by_day = defaultdict(list)
            for dt, count in zip(dates, totalGraids):
                raids_by_day[dt.date()].append(count)
            dailyGain = [max(counts) - min(counts) for counts in raids_by_day.values() if len(counts) > 1]
            highestGain = max(dailyGain) if dailyGain else 0

            avgGain = round(sum(dailyGain) / len(dailyGain), 2) if dailyGain else 0
            img = createPlot(dates, totalGraids, "line", blue, f'Guild Raid Activity - {name}', 'Date (UTC)', 'Total Guild Raids', color, legendName='Guild Raids', timeframeDays=numDays)
            return JSONResponse({"total_graid": highestTotal, "max_graid": highestGain, "average_graid": avgGain, "image": img})
        
        case "playerActivityGraidPie": #? WORKS
            playerCursor.execute("""
                SELECT graid_dict
                FROM player_current_stats
                WHERE player_uuid = ?
            """, (uuid,))
            snapshots = playerCursor.fetchone()

            if not snapshots:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            raids = ast.literal_eval(snapshots[0])

            sortedGraids = dict(sorted(raids.items(), key=lambda item: item[1], reverse=True))
            labels = list(sortedGraids.keys())
            sizes = list(sortedGraids.values())
            total = sum(sizes)
            percent_labels = [f"{label} — {size} ({(size / total * 100):.1f}%)" for label, size in zip(labels, sizes)]

            img = createPlot(percent_labels, sizes, "pie", None, f"Graid Pie Chart - {name}", None, None, color, legendName="Graids")

            return JSONResponse({"image": img})
        
        case "guildActivityGraidPie": #NOTE: Not exactly right, i think something is wrong but not sure yet, could be an issue of old data + raids // 4 its hard to tell
            guildCursor.execute("""
                SELECT pcs.graid_dict
                FROM player_current_stats pcs
                WHERE pcs.player_uuid IN (
                    SELECT DISTINCT player_uuid
                    FROM player_snapshots
                    WHERE guild_uuid = ?
                )
            """, (uuid,))

            rows = guildCursor.fetchall()

            if not rows:
                return JSONResponse(status_code=500, content={"error": "No guild data found."})

            combined_raids = {}

            for row in rows:
                if not row[0]:
                    continue

                try:
                    raid_data = ast.literal_eval(row[0])
                except (ValueError, SyntaxError):
                    continue

                for raid, count in raid_data.items():
                    combined_raids[raid] = combined_raids.get(raid, 0) + count

            combined_raids = {raid: count // 4 for raid, count in combined_raids.items()}
            sortedGraids = dict(sorted(combined_raids.items(), key=lambda item: item[1], reverse=True))
            labels = list(sortedGraids.keys())
            sizes = list(sortedGraids.values())
            total = sum(sizes)
            percent_labels = [f"{label} — {size} ({(size / total * 100):.1f}%)" for label, size in zip(labels, sizes)]

            img = createPlot(percent_labels, sizes, "pie", None, f"Graid Pie Chart - {name}", None, None, color, legendName="Graids")
            
            return JSONResponse({"image": img})
        
        case _: # Default case
            return JSONResponse(status_code=400, content={"error": "Please provide a correct activity type."})

    guildConn.close()
    playerConn.close()
    return Response(content=buf.getvalue(), media_type="image/webp") # im actually quite confident that i fucked this up and none of this gets hit anytime.
  
@mapRouter.get("/current") # Not a great name but its the current map
@cache_route(ttl=120) #2m cache
async def current_map():
    return mapCreator()

@mapRouter.get("/heatmap")
@cache_route(ttl=600) #10m cache
async def heat_map(timeframe: str):
    if not timeframe:
        return JSONResponse(status_code=400, content={"error": "Please provide a valid timeframe."})
    return heatmapCreator(timeframe)
  
@mapRouter.get("/ingmap")
@cache_route(ttl=3600) #1hr cache
async def ingredient_map(ingredient: str | None = None, price: int | None = None, tier: int | None = None):
    updateIngCache = False
    updatePriceCache = False
    ingRandomSeed = random.randint(0, 100) # ings rarely change so no need to update
    priceRandomSeed = random.randint(0, 20) #prices change sometimes so update a bit more
    if ingRandomSeed == 6:
        updateIngCache = True
    if priceRandomSeed == 6:
        updatePriceCache = True
     
    cacheLoaded = loadCache()
    if updateIngCache or not cacheLoaded:
        findIngCoords(ingToMobs, mobCoords, ingRarity)
        saveCache()

    return ingredientMap(ingToMobs, mobCoords, ingredient, price, priceCache, updatePriceCache, tier)

app.include_router(searchRouter)
app.include_router(leaderboardRouter)
app.include_router(activityRouter)
app.include_router(mapRouter)
app.include_router(seasonRatingdRouter)
