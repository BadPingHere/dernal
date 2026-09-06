from fastapi import FastAPI, APIRouter, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pathlib import Path
import time
from datetime import timedelta, datetime, timezone
from collections import defaultdict
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
from lib.makeRequest import makeRequest, internalMakeRequest
from lib.utils import timeframeMap
import os
import matplotlib.cm as cm
import seaborn as sns
import matplotlib as mpl
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import io
from matplotlib.dates import DateFormatter
import logging
import random
import asyncio
import sys
import json
import hashlib
import base64
import re
import requests
import colorsys
import unicodedata
from typing import Dict, List
from functools import wraps
from cachetools import TTLCache


route_cache = TTLCache(maxsize=200, ttl=300)  # default 5 min

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

            expiry_time = datetime.now(timezone.utc) + timedelta(seconds=ttl or route_cache.ttl)
            route_cache[key] = (result, expiry_time)

            if isinstance(result, Response):
                result.headers["X-Cache"] = "MISS"
                result.headers["X-Cache-Expires"] = expiry_time.isoformat() + "Z"

            return result
        return wrapper
    return decorator

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger('api')

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_FILE = Path(__file__).resolve().parents[1] / "database" / "ing_cache.json"
ingToMobs: Dict[str, List[str]] = {}
ingRarity: Dict[str, int] = {}
mobCoords: Dict[str, List[List[int]]] = {}
priceCache: Dict[str, float] = {}

app = FastAPI(title="Dernal API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

searchRouter = APIRouter(prefix="/api/search", tags=["Search"])
leaderboardRouter = APIRouter(prefix="/api/leaderboard", tags=["Leaderboard"])
seasonRatingdRouter = APIRouter(prefix="/api/seasonRating", tags=["Leaderboard"])
activityRouter = APIRouter(prefix="/api/activity", tags=["Activity"])
mapRouter = APIRouter(prefix="/api/map", tags=["Activity"])
seasonRouter = APIRouter(prefix="/api", tags=["Seasons"])

from lib.config import DSN
TERRITORIESPATH = Path(__file__).resolve().parents[1] / "lib" / "documents" / "territories.json"
rootDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

dbPool = None
poolLock = asyncio.Lock()

async def connectDB(): # Replaces the sqlite3 connect, pool is opened on first use
    global dbPool
    if dbPool is None:
        async with poolLock:
            if dbPool is None:
                dbPool = AsyncConnectionPool(DSN, min_size=1, max_size=10, open=False, kwargs={"row_factory": dict_row})
                await dbPool.open(wait=True, timeout=30)
    return dbPool

async def fetch(query, params=None): # cursor.execute + fetchall, but pooled
    pool = await connectDB()
    async with pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(query, params or ())
            return await cursor.fetchall()

async def fetchone(query, params=None):
    rows = await fetch(query, params)
    return rows[0] if rows else None

def mapCreator(type, focusTerritory = None):
    FOCUS_CROP_RADIUS = 200  # half-width/height of the crop box for type="test"
    map_img = Image.open("lib/documents/main-map.png").convert("RGBA")
    hq_img = Image.open("lib/documents/guild_headquarters.png").convert("RGBA")
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
    hq_pastes = []

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
            
        if type == "defense":
            defenseMap = {
                "very_low": "#006400",
                "low": "#90EE90",
                "medium": "#FFFF00",
                "high": "#FF0000",
                "very_high": "#00FFFF",
            }
            color_hex = defenseMap.get(info["defences"].lower(), "#FFFFFF")
            #logger.info(f"{name}: defences is {info["defences"]}")

        elif type == "treasury":
            treasuryMap = {
                "very_low": "#006400",
                "low": "#90EE90",
                "medium": "#FFFF00",
                "high": "#FF0000",
                "very_high": "#00FFFF",
            }
            color_hex = treasuryMap.get(info["treasury"].lower(), "#FFFFFF")
            #logger.info(f"{name}: treasury is {info["treasury"]}")
        
        elif type in ("map", "test"):
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
        isHQ = info["hq"]
        
        if isHQ:
            territory_w = xMax - xMin
            territory_h = yMax - yMin
            crown_size = int(min(territory_w, territory_h, 80) / 1.5)
            orig_w, orig_h = hq_img.size
            crown_w = crown_size
            crown_h = int(crown_size * orig_h / orig_w)
            crown_resized = hq_img.resize((crown_w, crown_h), Image.LANCZOS)
            crown_x = (xMin + xMax) // 2 - crown_w // 2
            crown_y = (yMin + yMax) // 2 - crown_h // 2
            hq_pastes.append((crown_resized, crown_x, crown_y))
        else:
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
    for crown_resized, crown_x, crown_y in hq_pastes:
        mapImg.paste(crown_resized, (crown_x, crown_y), crown_resized)

    if type == "test" and focusTerritory and focusTerritory in territory_data:
        focus_info = territory_data[focusTerritory]
        startX, startZ = focus_info["location"]["start"]
        endX, endZ = focus_info["location"]["end"]
        px1, py1 = coordToPixel(startX, startZ)
        px2, py2 = coordToPixel(endX, endZ)
        center_x = (px1 + px2) // 2
        center_y = (py1 + py2) // 2
        r = FOCUS_CROP_RADIUS
        crop_box = (
            max(0, center_x - r),
            max(0, center_y - r),
            min(mapImg.width, center_x + r),
            min(mapImg.height, center_y + r),
        )
        mapImg = mapImg.crop(crop_box)
    else:
        scale_factor = 0.4
        new_size = (int(mapImg.width * scale_factor), int(mapImg.height * scale_factor))
        mapImg = mapImg.resize(new_size, Image.LANCZOS)

    mapBytes = BytesIO()
    mapImg.save(mapBytes, format='webp', optimize=True, compress_level=5)
    mapBytes.seek(0)
    return Response(content=mapBytes.getvalue(), media_type="image/webp")


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
        logger.exception("Cache corrupted")
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
        logger.exception("findIngCoords ran, hit end of pages or errored")

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
    start=None,
    end=None,
):
    fig, ax = plt.subplots(figsize=(12, 6))

    if graphType == "bar":
        widths = 0.8
        if start is not None and end is not None:
            centers, widths = [], []
            for value in x:
                day = value.date() if isinstance(value, datetime) else value
                bucket = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
                left, right = max(start, bucket), min(end, bucket + timedelta(hours=24))
                centers.append(left + (right - left) / 2)
                widths.append((right - left).total_seconds() / 86400 * 0.8)
            x = centers
        ax.bar(x, y, width=widths, color=color)
        if ahxlineY and ahxlineLabel:
            ax.axhline(y=ahxlineY, color='red', linestyle='-', label=ahxlineLabel)
        ax.xaxis.set_major_formatter(DateFormatter('%m/%d'))
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f'{v:,.0f}'))
        ax.set_xlabel(xlabel, fontsize=12)
        ax.set_ylabel(ylabel, fontsize=12)
        ax.grid(True, linestyle='-', alpha=0.5)
        ax.legend()
        if start is not None and end is not None:
            ax.set_xlim(start, end)

    elif graphType == "line":
        ax.plot(x, y, '-', label=legendName, color=color, lw=1.5)
        if fillBetween:
            ax.fill_between(x, 0, y, alpha=0.3, color=color)
        if ahxlineY and ahxlineLabel:
            ax.axhline(y=ahxlineY, color='red', linestyle='-', label=ahxlineLabel)
        if start is not None and end is not None and (end - start).total_seconds() >= 604800: # 7 or more days we remove the hour:min since its not needed
            ax.xaxis.set_major_formatter(DateFormatter('%m/%d'))
        else:
            ax.xaxis.set_major_formatter(DateFormatter('%m/%d %H:%M'))
            
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f'{int(v)}'))
        ax.set_xlabel(xlabel, fontsize=12)
        ax.set_ylabel(ylabel, fontsize=12)
        ax.grid(True, linestyle='-', alpha=0.5)
        ax.legend()
        ax.margins(x=0.01)
        if start is not None and end is not None:
            ax.set_xlim(start, end)

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

async def heatmapCreator(timeframe):
    if timeframe == "Last 7 Days": # We handle it.
        endDate = datetime.now(timezone.utc)
        startDate = endDate - timedelta(days=7)
    elif timeframe != "Everything": # we deal with everything later on
        startDay, endDay = timeframeMap().get(timeframe, (None, None))
        startDate = datetime.strptime(startDay, "%m/%d/%y").replace(tzinfo=timezone.utc)
        endDate = datetime.strptime(endDay, "%m/%d/%y").replace(tzinfo=timezone.utc)

    map_img = Image.open("lib/documents/main-map.png").convert("RGBA")

    def coordToPixel(x, z):
        return x + 2558, z + 6638 # if only wynntils was ACCURATE!!!


    success, r = makeRequest("https://api.wynncraft.com/v3/guild/list/territory")
    territory_data = r.json()
    activityCount = defaultdict(int)

    if timeframe == "Everything":
        rows = await fetch("""
        SELECT territory, SUM(captures) AS total
        FROM ts.territory_captures_daily
        GROUP BY territory
        """)
    else:
        rows = await fetch("""
        SELECT territory, SUM(captures) AS total
        FROM ts.territory_captures_daily
        WHERE day BETWEEN %s AND %s
        GROUP BY territory
        """, (startDate, endDate))

    for row in rows:
        activityCount[row["territory"]] = int(row["total"])

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
    where = {
        "prefix": "lower(prefix) = lower(%s)",
        "name": "lower(name) = lower(%s)",
        "guild_uuid": "guild_uuid = %s::uuid",
    }.get(field)
    if where is None:
        return JSONResponse(status_code=400, content={"error": "Invalid search field."})

    guild = await fetchone(f"""
        SELECT guild_uuid::text, name, prefix, first_seen, last_seen, is_tracked
        FROM core.guilds
        WHERE {where}
        LIMIT 1
    """, (value,))

    if not guild:
        return JSONResponse(status_code=404, content={"error": "Guild not found"})

    data = dict(guild)
    snapshot = await fetchone("""
        SELECT level, xp_percent, territories, wars, online_members, total_members, guild_raids, ts AS timestamp
        FROM ts.guild_snapshots
        WHERE guild_uuid = %s::uuid
        ORDER BY ts DESC
        LIMIT 1
    """, (data["guild_uuid"],))
    if snapshot:
        data["latest_snapshot"] = dict(snapshot)
    else:
        data["latest_snapshot"] = None

    return data

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

    user_row = await fetchone("""
        SELECT player_uuid::text, username, online, last_join, guild_uuid::text, last_seen
        FROM core.players
        WHERE lower(username) = lower(%s)
        LIMIT 1
    """, (username,))
    if not user_row:
        return JSONResponse(status_code=404, content={"error": "Player not found"})

    player_uuid = user_row["player_uuid"]

    snapshot = await fetchone("""
        SELECT playtime, ts AS timestamp
        FROM ts.player_snapshots
        WHERE player_uuid = %s::uuid
        ORDER BY ts DESC
        LIMIT 1
    """, (player_uuid,))

    data = {
        "player_uuid": player_uuid,
        "username": user_row["username"],
        "online": user_row["online"],
        "last_join": user_row["last_join"],
        "guild_uuid": user_row["guild_uuid"],
        "timestamp": user_row["last_seen"],
    }
    if snapshot:
        data.update(dict(snapshot))

    global_snap = await fetchone("""
        SELECT wars, mobs_killed, total_dungeons, total_raids, total_graids
        FROM ts.player_snapshots
        WHERE player_uuid = %s::uuid
        ORDER BY ts DESC
        LIMIT 1
    """, (player_uuid,))
    if global_snap:
        temp = dict(global_snap)
        stats_row = await fetchone("""
            SELECT jsonb_object_agg(a.name, t.total) FILTER (WHERE a.kind = 'dungeon') AS dungeons,
                   jsonb_object_agg(a.name, t.total) FILTER (WHERE a.kind = 'raid') AS raids,
                   jsonb_object_agg(a.name, t.total) FILTER (WHERE a.kind = 'guild_raid') AS guild_raids
            FROM core.player_activity_totals t
            JOIN core.activities a USING (activity_id)
            WHERE t.player_uuid = %s::uuid
            GROUP BY t.player_uuid
        """, (player_uuid,))
        # JSONB, so psycopg already handed us dicts. No ast.literal_eval needed.
        if stats_row:
            temp["dungeon_dict"] = stats_row["dungeons"]
            temp["raid_dict"] = stats_row["raids"]
            temp["graid_dict"] = stats_row["guild_raids"]
        data["globalData"] = temp
    else:
        data["globalData"] = None

    return data

def leaderboardPlayerSource(total, rawGain, dailyGain):
    return f"""(
        SELECT day AS ts, player_uuid, guild_uuid, {total}, {dailyGain} AS gained
        FROM ts.player_daily
        WHERE day >= %(start)s AND day + INTERVAL '1 day' <= %(end)s
        UNION ALL
        SELECT ts, player_uuid, guild_uuid, {total}, {rawGain} AS gained
        FROM ts.player_snapshots
        WHERE ts >= %(start)s AND ts < %(end)s
          AND NOT (time_bucket('1 day', ts) >= %(start)s
                   AND time_bucket('1 day', ts) + INTERVAL '1 day' <= %(end)s)
    )"""


@leaderboardRouter.get("/{leaderboardType}")
@cache_route(ttl=600) #10m cache
async def leaderboard(leaderboardType: str, uuid: str | None = None, start: str | None = None, end: str | None = None):
    if not leaderboardType:
        return JSONResponse(status_code=400, content={"error": "Please provide a valid leaderboard type."})
    startDate, endDate = None, None
    if start is not None or end is not None:
        try:
            startDate = datetime.fromisoformat(start.replace("Z", "+00:00"))
            endDate = datetime.fromisoformat(end.replace("Z", "+00:00"))
            if startDate.tzinfo is None or endDate.tzinfo is None or startDate >= endDate:
                raise ValueError("Invalid range")
            startDate = startDate.astimezone(timezone.utc)
            endDate = endDate.astimezone(timezone.utc)
        except (AttributeError, TypeError, ValueError):
            return JSONResponse(status_code=400, content={
                "error": "Supply timezone-aware start and end timestamps, with start before end."
            })
    subDay = startDate is not None and (endDate - startDate).total_seconds() <= 86400
    match leaderboardType:
        case "guildLeaderboardOnlineMembers":
            query = """
            SELECT
                g.name || ' (' || g.prefix || ')' AS guild_display_name,
                ROUND(AVG(gs.online_members)::numeric, 2) AS avg_online_members
            FROM core.guilds g
            JOIN ts.guild_snapshots gs USING (guild_uuid)
            WHERE gs.ts >= %s AND gs.ts < %s
            GROUP BY g.guild_uuid, g.name, g.prefix
            HAVING COUNT(*) >= 1
            ORDER BY avg_online_members DESC, guild_display_name ASC
            LIMIT 100;
            """
            params = [startDate, endDate]
            data = await fetch(query, params)

        case "guildLeaderboardWars":
            if startDate and endDate:
                if subDay:
                    query = """
                    SELECT
                        g.name || ' (' || g.prefix || ')' AS guild_display_name,
                        MAX(gs.wars) - MIN(gs.wars) AS wars_gained
                    FROM core.guilds g
                    JOIN ts.guild_snapshots gs USING (guild_uuid)
                    WHERE gs.ts >= %s AND gs.ts < %s
                    GROUP BY g.guild_uuid, g.name, g.prefix
                    HAVING COUNT(*) >= 2 AND MAX(gs.wars) - MIN(gs.wars) > 0
                    ORDER BY wars_gained DESC, guild_display_name ASC
                    LIMIT 100;
                    """
                    params = [startDate, endDate]
                else:
                    query = """
                    SELECT
                        g.name || ' (' || g.prefix || ')' AS guild_display_name,
                        (last.wars - first.wars) AS wars_gained
                    FROM core.guilds g
                    JOIN LATERAL (
                        SELECT gs.wars FROM ts.guild_snapshots gs
                        WHERE gs.guild_uuid = g.guild_uuid AND gs.ts >= %s AND gs.ts < %s
                        ORDER BY gs.ts ASC LIMIT 1
                    ) first ON true
                    JOIN LATERAL (
                        SELECT gs.wars FROM ts.guild_snapshots gs
                        WHERE gs.guild_uuid = g.guild_uuid AND gs.ts >= %s AND gs.ts < %s
                        ORDER BY gs.ts DESC LIMIT 1
                    ) last ON true
                    WHERE last.wars - first.wars > 0
                    ORDER BY wars_gained DESC, guild_display_name ASC
                    LIMIT 100;
                    """
                    params = [startDate, endDate, startDate, endDate]
            else:
                query = """
                SELECT
                    g.name || ' (' || g.prefix || ')' AS guild_display_name,
                    MAX(gd.wars) AS wars_gained
                FROM core.guilds g
                JOIN ts.guild_daily gd USING (guild_uuid)
                GROUP BY g.guild_uuid, g.name, g.prefix
                HAVING MAX(gd.wars) > 0
                ORDER BY wars_gained DESC, guild_display_name ASC
                LIMIT 100;
                """
                params = []
            data = await fetch(query, params)

        case "guildLeaderboardXP":
            if startDate and endDate:
                source = leaderboardPlayerSource("contribution", "d_contribution", "gained_contribution")
                timeCol, gain = "ts", "gained"
                query = f"""
                SELECT
                    g.name || ' (' || g.prefix || ')' AS guild_display_name,
                    SUM(ps.{gain}) AS xp_gained
                FROM {source} ps
                JOIN core.guilds g ON g.guild_uuid = ps.guild_uuid
                WHERE ps.{timeCol} >= %(start)s AND ps.{timeCol} < %(end)s
                AND ps.{gain} > 0
                AND ps.contribution > 0
                AND ps.{gain} < ps.contribution * 0.99
                GROUP BY g.guild_uuid, g.name, g.prefix
                HAVING SUM(ps.{gain}) > 0
                ORDER BY xp_gained DESC, guild_display_name ASC
                LIMIT 100;
                """
                params = {"uuid": uuid, "start": startDate, "end": endDate}
            else:
                query = """
                WITH latest AS (
                    SELECT DISTINCT ON (pd.player_uuid)
                        pd.player_uuid, pd.guild_uuid, pd.contribution
                    FROM ts.player_daily pd
                    JOIN core.players p USING (player_uuid)
                    WHERE p.last_seen >= now() - INTERVAL '3 days'
                    AND pd.contribution > 0
                    ORDER BY pd.player_uuid, pd.day DESC
                )
                SELECT
                    g.name || ' (' || g.prefix || ')' AS guild_display_name,
                    SUM(latest.contribution) AS xp_gained
                FROM latest
                JOIN core.guilds g ON g.guild_uuid = latest.guild_uuid
                GROUP BY g.guild_uuid, g.name, g.prefix
                HAVING SUM(latest.contribution) > 0
                ORDER BY xp_gained DESC, guild_display_name ASC
                LIMIT 100;
                """
                params = []
            data = await fetch(query, params)

        case "playerLeaderboardRaids":
            if startDate and endDate:
                source = leaderboardPlayerSource("total_raids", "d_total_raids", "gained_raids")
                timeCol, gain = "ts", "gained"
                query = f"""
                SELECT
                    p.username,
                    SUM(ps.{gain}) AS raids_gained
                FROM {source} ps
                JOIN core.players p USING (player_uuid)
                WHERE ps.{timeCol} >= %(start)s AND ps.{timeCol} < %(end)s
                AND ps.{gain} > 0
                AND ps.total_raids > 0
                AND ps.{gain} < ps.total_raids * 0.99
                GROUP BY p.username
                HAVING SUM(ps.{gain}) > 0
                ORDER BY raids_gained DESC, p.username ASC
                LIMIT 100;
                """
                params = {"uuid": uuid, "start": startDate, "end": endDate}
            else:
                query = """
                WITH latest AS MATERIALIZED (
                    SELECT pd.player_uuid, MAX(pd.total_raids) AS total_raids
                    FROM ts.player_daily pd
                    WHERE pd.total_raids IS NOT NULL
                    GROUP BY pd.player_uuid
                )
                SELECT
                    p.username,
                    latest.total_raids
                FROM latest
                JOIN core.players p USING (player_uuid)
                WHERE p.last_seen >= now() - INTERVAL '3 days'
                AND latest.total_raids > 0
                ORDER BY latest.total_raids DESC, p.username ASC
                LIMIT 100;
                """
                params = []
            data = await fetch(query, params)

        case "playerLeaderboardDungeons":
            if startDate and endDate:
                source = leaderboardPlayerSource("total_dungeons", "d_total_dungeons", "gained_dungeons")
                timeCol, gain = "ts", "gained"
                query = f"""
                SELECT
                    p.username,
                    SUM(ps.{gain}) AS dungeons_gained
                FROM {source} ps
                JOIN core.players p USING (player_uuid)
                WHERE ps.{timeCol} >= %(start)s AND ps.{timeCol} < %(end)s
                AND ps.{gain} > 0
                AND ps.total_dungeons > 0
                AND ps.{gain} < ps.total_dungeons * 0.99
                GROUP BY p.username
                HAVING SUM(ps.{gain}) > 0
                ORDER BY dungeons_gained DESC, p.username ASC
                LIMIT 100;
                """
                params = {"uuid": uuid, "start": startDate, "end": endDate}
            else:
                query = """
                WITH latest AS MATERIALIZED (
                    SELECT pd.player_uuid, MAX(pd.total_dungeons) AS total_dungeons
                    FROM ts.player_daily pd
                    WHERE pd.total_dungeons IS NOT NULL
                    GROUP BY pd.player_uuid
                )
                SELECT
                    p.username,
                    latest.total_dungeons
                FROM latest
                JOIN core.players p USING (player_uuid)
                WHERE p.last_seen >= now() - INTERVAL '3 days'
                AND latest.total_dungeons > 0
                ORDER BY latest.total_dungeons DESC, p.username ASC
                LIMIT 100;
                """
                params = []
            data = await fetch(query, params)

        case "playerLeaderboardPlaytime":
            if startDate and endDate:
                source = leaderboardPlayerSource("playtime", "d_playtime", "gained_playtime")
                timeCol, gain = "ts", "gained"
                query = f"""
                SELECT
                    p.username,
                    SUM(ps.{gain}) AS playtime_gained
                FROM {source} ps
                JOIN core.players p USING (player_uuid)
                WHERE ps.{timeCol} >= %(start)s AND ps.{timeCol} < %(end)s
                AND ps.{gain} > 0
                AND ps.{gain} <= 24
                GROUP BY p.username
                HAVING SUM(ps.{gain}) > 0
                ORDER BY playtime_gained DESC, p.username ASC
                LIMIT 100;
                """
                params = {"uuid": uuid, "start": startDate, "end": endDate}
            else:
                query = """
                WITH latest AS MATERIALIZED (
                    SELECT pd.player_uuid, MAX(pd.playtime) AS playtime
                    FROM ts.player_daily pd
                    WHERE pd.playtime IS NOT NULL
                    GROUP BY pd.player_uuid
                )
                SELECT
                    p.username,
                    latest.playtime
                FROM latest
                JOIN core.players p USING (player_uuid)
                WHERE p.last_seen >= now() - INTERVAL '3 days'
                AND latest.playtime > 0
                ORDER BY latest.playtime DESC, p.username ASC
                LIMIT 100;
                """
                params = []
            data = await fetch(query, params)

        case "guildLeaderboardXPButGuildSpecific":
            if startDate and endDate:
                source = leaderboardPlayerSource("contribution", "d_contribution", "gained_contribution")
                timeCol, gain = "ts", "gained"
                query = f"""
                SELECT
                    p.username,
                    SUM(ps.{gain}) AS xp_gained
                FROM {source} ps
                JOIN core.players p USING (player_uuid)
                WHERE ps.guild_uuid = %(uuid)s::uuid
                AND ps.{timeCol} >= %(start)s AND ps.{timeCol} < %(end)s
                AND ps.{gain} > 0
                AND ps.contribution > 0
                AND ps.{gain} < ps.contribution * 0.99
                GROUP BY p.username
                HAVING SUM(ps.{gain}) > 0
                ORDER BY xp_gained DESC, p.username ASC
                LIMIT 100;
                """
                params = {"uuid": uuid, "start": startDate, "end": endDate}
            else:
                query = """
                SELECT
                    p.username,
                    latest.contribution AS xp_gained
                FROM core.players p
                JOIN LATERAL (
                    SELECT pd.contribution
                    FROM ts.player_daily pd
                    WHERE pd.player_uuid = p.player_uuid AND pd.contribution IS NOT NULL
                    ORDER BY pd.day DESC
                    LIMIT 1
                ) latest ON true
                WHERE p.guild_uuid = %s::uuid
                AND latest.contribution > 0
                ORDER BY xp_gained DESC, p.username ASC
                LIMIT 100;
                """
                params = [uuid]
            data = await fetch(query, params)

        case "guildLeaderboardOnlineButGuildSpecific":
            if startDate and endDate:
                source = leaderboardPlayerSource("playtime", "d_playtime", "gained_playtime")
                timeCol, gain = "ts", "gained"
                query = f"""
                SELECT
                    p.username,
                    SUM(ps.{gain}) AS playtime_gained
                FROM {source} ps
                JOIN core.players p USING (player_uuid)
                WHERE ps.guild_uuid = %(uuid)s::uuid
                AND ps.{timeCol} >= %(start)s AND ps.{timeCol} < %(end)s
                AND ps.{gain} > 0
                AND ps.{gain} <= 24
                GROUP BY p.username
                HAVING SUM(ps.{gain}) > 0
                ORDER BY playtime_gained DESC, p.username ASC
                LIMIT 150;
                """
                params = {"uuid": uuid, "start": startDate, "end": endDate}
            else:
                query = """
                SELECT
                    p.username,
                    latest.playtime AS playtime_gained
                FROM core.players p
                JOIN LATERAL (
                    SELECT pd.playtime
                    FROM ts.player_daily pd
                    WHERE pd.player_uuid = p.player_uuid AND pd.playtime IS NOT NULL
                    ORDER BY pd.day DESC
                    LIMIT 1
                ) latest ON true
                WHERE p.guild_uuid = %s::uuid
                AND latest.playtime > 0
                ORDER BY playtime_gained DESC, p.username ASC
                LIMIT 150;
                """
                params = [uuid]
            data = await fetch(query, params)

        case "guildLeaderboardWarsButGuildSpecific":
            if startDate and endDate:
                source = leaderboardPlayerSource("wars", "d_wars", "gained_wars")
                timeCol, gain = "ts", "gained"
                query = f"""
                SELECT
                    p.username,
                    SUM(ps.{gain}) AS wars_gained
                FROM {source} ps
                JOIN core.players p USING (player_uuid)
                WHERE ps.guild_uuid = %(uuid)s::uuid
                AND ps.{timeCol} >= %(start)s AND ps.{timeCol} < %(end)s
                AND ps.{gain} > 0
                AND ps.wars > 0
                AND ps.{gain} < ps.wars * 0.99
                GROUP BY p.username
                HAVING SUM(ps.{gain}) > 0
                ORDER BY wars_gained DESC, p.username ASC
                LIMIT 100;
                """
                params = {"uuid": uuid, "start": startDate, "end": endDate}
            else:
                query = """
                SELECT
                    p.username,
                    latest.wars AS wars_gained
                FROM core.players p
                JOIN LATERAL (
                    SELECT pd.wars
                    FROM ts.player_daily pd
                    WHERE pd.player_uuid = p.player_uuid AND pd.wars IS NOT NULL
                    ORDER BY pd.day DESC
                    LIMIT 1
                ) latest ON true
                WHERE p.guild_uuid = %s::uuid
                AND latest.wars > 0
                ORDER BY wars_gained DESC, p.username ASC
                LIMIT 100;
                """
                params = [uuid]
            data = await fetch(query, params)

        case "guildLeaderboardGraids":
            if startDate and endDate:
                if subDay:
                    query = """
                    SELECT
                        g.name || ' (' || g.prefix || ')' AS guild_display_name,
                        MAX(gs.guild_raids) - MIN(gs.guild_raids) AS total_graids
                    FROM core.guilds g
                    JOIN ts.guild_snapshots gs USING (guild_uuid)
                    WHERE gs.guild_raids IS NOT NULL
                    AND gs.ts >= %s AND gs.ts < %s
                    GROUP BY g.guild_uuid, g.name, g.prefix
                    HAVING COUNT(*) >= 2 AND MAX(gs.guild_raids) - MIN(gs.guild_raids) > 0
                    ORDER BY total_graids DESC, guild_display_name ASC
                    LIMIT 100;
                    """
                    params = [startDate, endDate]
                else:
                    query = """
                    SELECT
                        g.name || ' (' || g.prefix || ')' AS guild_display_name,
                        (last.guild_raids - first.guild_raids) AS total_graids
                    FROM core.guilds g
                    JOIN LATERAL (
                        SELECT gs.guild_raids FROM ts.guild_snapshots gs
                        WHERE gs.guild_uuid = g.guild_uuid AND gs.guild_raids IS NOT NULL
                          AND gs.ts >= %s AND gs.ts < %s
                        ORDER BY gs.ts ASC LIMIT 1
                    ) first ON true
                    JOIN LATERAL (
                        SELECT gs.guild_raids FROM ts.guild_snapshots gs
                        WHERE gs.guild_uuid = g.guild_uuid AND gs.guild_raids IS NOT NULL
                          AND gs.ts >= %s AND gs.ts < %s
                        ORDER BY gs.ts DESC LIMIT 1
                    ) last ON true
                    WHERE last.guild_raids - first.guild_raids > 0
                    ORDER BY total_graids DESC, guild_display_name ASC
                    LIMIT 100;
                    """
                    params = [startDate, endDate, startDate, endDate]
            else:
                query = """
                SELECT
                    g.name || ' (' || g.prefix || ')' AS guild_display_name,
                    MAX(gd.guild_raids) AS total_graids
                FROM core.guilds g
                JOIN ts.guild_daily gd USING (guild_uuid)
                WHERE gd.guild_raids IS NOT NULL
                GROUP BY g.guild_uuid, g.name, g.prefix
                HAVING MAX(gd.guild_raids) > 0
                ORDER BY total_graids DESC, guild_display_name ASC
                LIMIT 100;
                """
                params = []
            data = await fetch(query, params)

        case "guildLeaderboardGraidsButGuildSpecific":
            if startDate and endDate:
                source = leaderboardPlayerSource("total_graids", "d_total_graids", "gained_graids")
                timeCol, gain = "ts", "gained"
                query = f"""
                SELECT
                    p.username,
                    SUM(ps.{gain}) AS graids_done
                FROM {source} ps
                JOIN core.players p USING (player_uuid)
                WHERE ps.guild_uuid = %(uuid)s::uuid
                AND ps.{timeCol} >= %(start)s AND ps.{timeCol} < %(end)s
                AND ps.{gain} > 0
                AND ps.total_graids > 0
                AND ps.{gain} < ps.total_graids * 0.99
                GROUP BY p.username
                HAVING SUM(ps.{gain}) > 0
                ORDER BY graids_done DESC, p.username ASC
                LIMIT 100;
                """
                params = {"uuid": uuid, "start": startDate, "end": endDate}
            else:
                query = """
                SELECT
                    p.username,
                    latest.total_graids AS graids_done
                FROM core.players p
                JOIN LATERAL (
                    SELECT pd.total_graids
                    FROM ts.player_daily pd
                    WHERE pd.player_uuid = p.player_uuid AND pd.total_graids IS NOT NULL
                    ORDER BY pd.day DESC
                    LIMIT 1
                ) latest ON true
                WHERE p.guild_uuid = %s::uuid
                AND latest.total_graids > 0
                ORDER BY graids_done DESC, p.username ASC
                LIMIT 100;
                """
                params = [uuid]
            data = await fetch(query, params)

        case "playerLeaderboardGraids":
            if startDate and endDate:
                source = leaderboardPlayerSource("total_graids", "d_total_graids", "gained_graids")
                timeCol, gain = "ts", "gained"
                query = f"""
                SELECT
                    p.username,
                    SUM(ps.{gain}) AS graids_done
                FROM {source} ps
                JOIN core.players p USING (player_uuid)
                WHERE ps.{timeCol} >= %(start)s AND ps.{timeCol} < %(end)s
                AND ps.{gain} > 0
                AND ps.total_graids > 0
                AND ps.{gain} < ps.total_graids * 0.99
                GROUP BY p.username
                HAVING SUM(ps.{gain}) > 0
                ORDER BY graids_done DESC, p.username ASC
                LIMIT 100;
                """
                params = {"uuid": uuid, "start": startDate, "end": endDate}
            else:
                query = """
                WITH latest AS MATERIALIZED (
                    SELECT pd.player_uuid, MAX(pd.total_graids) AS total_graids
                    FROM ts.player_daily pd
                    WHERE pd.total_graids IS NOT NULL
                    GROUP BY pd.player_uuid
                )
                SELECT
                    p.username,
                    latest.total_graids AS graids_done
                FROM latest
                JOIN core.players p USING (player_uuid)
                WHERE p.last_seen >= now() - INTERVAL '3 days'
                AND latest.total_graids > 0
                ORDER BY latest.total_graids DESC, p.username ASC
                LIMIT 100;
                """
                params = []
            data = await fetch(query, params)

        case _: # Default case
            return JSONResponse(status_code=400, content={"error": "Please provide a valid leaderboard type."})


    return data

@seasonRatingdRouter.get("/")
@cache_route(ttl=600) #10m cache
async def seasonLeaderboard(season: int | None = None, uuid: str | None = None):
    if not season and not uuid:
        return JSONResponse(status_code=400, content={"error": "Please provide a valid season or guild uuid."})
    if uuid: # Get the individual guild's data with rank per season
        snapshots = await fetch("""
            WITH ranked AS (
                SELECT guild_uuid, season, rating,
                       RANK() OVER (PARTITION BY season ORDER BY rating DESC) AS rank
                FROM core.guild_season_ratings
            )
            SELECT g.name || ' (' || g.prefix || ')' AS guild, r.season, r.rating, r.rank
            FROM ranked r
            JOIN core.guilds g USING (guild_uuid)
            WHERE r.guild_uuid = %s::uuid
            ORDER BY r.season DESC
        """, (uuid,))
        rows = [dict(r) for r in snapshots]
    else: # Get a season's rankings
        snapshots = await fetch("""
            SELECT g.name || ' (' || g.prefix || ')' AS guild, gsr.rating
            FROM core.guild_season_ratings gsr
            JOIN core.guilds g USING (guild_uuid)
            WHERE gsr.season = %s
            ORDER BY gsr.rating DESC
            LIMIT 100;
        """, (season,))
        rows = [dict(r) for r in snapshots]

    return rows

async def activityGains(uuid, start, end, guild=False):
    entity = "guild_uuid" if guild else "player_uuid"
    return await fetch(f"""
        WITH selected AS (
            SELECT day, gained_playtime, gained_contribution, contribution
            FROM ts.player_daily
            WHERE {entity} = %(uuid)s::uuid
              AND day >= %(start)s AND day + INTERVAL '1 day' <= %(end)s
            UNION ALL
            SELECT time_bucket('1 day', ts) AS day, d_playtime, d_contribution, contribution
            FROM ts.player_snapshots
            WHERE {entity} = %(uuid)s::uuid AND ts >= %(start)s AND ts < %(end)s
              AND NOT (time_bucket('1 day', ts) >= %(start)s
                       AND time_bucket('1 day', ts) + INTERVAL '1 day' <= %(end)s)
        ), totals AS (
            SELECT day,
                ROUND((SUM(CASE WHEN gained_playtime > 0 AND gained_playtime <= 24
                               THEN gained_playtime ELSE 0 END) * 60)::numeric) AS playtime_minutes,
                SUM(CASE WHEN gained_contribution > 0 AND contribution > 0
                          AND gained_contribution < contribution * 0.99
                         THEN gained_contribution ELSE 0 END) AS xp
            FROM selected GROUP BY day
        ), buckets AS (
            SELECT generate_series(time_bucket('1 day', %(start)s::timestamptz),
                                   time_bucket('1 day', %(end)s::timestamptz - INTERVAL '1 microsecond'),
                                   INTERVAL '1 day') AS day
        )
        SELECT (b.day AT TIME ZONE 'UTC')::date AS day, COALESCE(t.playtime_minutes, 0) AS playtime_minutes,
               COALESCE(t.xp, 0) AS xp
        FROM buckets b LEFT JOIN totals t USING (day)
        WHERE %(guild)s OR t.day IS NOT NULL
        ORDER BY b.day
    """, {"uuid": uuid, "start": start, "end": end, "guild": guild})

@activityRouter.get("/{activityType}")
@cache_route(ttl=600) #10m cache
async def activity(activityType: str, uuid: str | None = None, name: str | None = None, theme: str | None = None, start: datetime | None = None, end: datetime | None = None ,): # Name can be either prefix or gname or player username
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

    match activityType:
        case "guildActivityXP":
            snapshots = await activityGains(uuid, start, end, guild=True)

            if not snapshots:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            dates = []
            xp_values = []
            for row in snapshots:
                dates.append(datetime.combine(row["day"], datetime.min.time()))
                xp_values.append(int(row["xp"]))

            total_xp = sum(xp_values)
            avg_daily_xp = total_xp / len(dates) if dates else 0
            max_daily_xp = max(xp_values) if xp_values else 0
            min_daily_xp = min(xp_values) if xp_values else 0

            img = createPlot(dates, xp_values, "bar", blue, f'Daily Guild XP Contribution - {name}', 'Date (UTC)', 'XP Gained', color, ahxlineY = avg_daily_xp, ahxlineLabel = f'Daily Average: {avg_daily_xp:,.0f} XP', start=start, end=end)
            return JSONResponse({"total_xp": total_xp, "daily_average": avg_daily_xp, "highest_day": max_daily_xp, "lowest_day": min_daily_xp, "image": img})

        case "guildActivityTerritories":
            snapshots = await fetch("""
                SELECT
                    timepoint,
                    COALESCE(territory_count, 0) AS territory_count
                FROM (
                    SELECT
                        time_bucket_gapfill('15 minutes', ts) AS timepoint,
                        locf(
                            last(territories, ts),
                            (SELECT gs2.territories FROM ts.guild_snapshots gs2
                             WHERE gs2.guild_uuid = %s::uuid
                             AND gs2.territories IS NOT NULL AND gs2.territories > 0
                             AND gs2.ts < %s
                             ORDER BY gs2.ts DESC LIMIT 1)
                        ) AS territory_count
                    FROM ts.guild_snapshots
                    WHERE guild_uuid = %s::uuid
                    AND territories IS NOT NULL
                    AND territories > 0
                    AND ts >= %s AND ts < %s
                    GROUP BY timepoint
                ) gapfilled
                ORDER BY timepoint;
            """, (uuid, start, uuid, start, end))
            if not snapshots or all(row["territory_count"] == 0 for row in snapshots):
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            times = []
            territory_counts = []
            for row in snapshots:
                times.append(row["timepoint"])
                territory_counts.append(float(row["territory_count"]))

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

            img = createPlot(times, territory_counts, "line", blue, f'Territory Count - {name}', 'Date (UTC)', 'Number of Territories', color, ahxlineY = avg_territories, ahxlineLabel = f'Average: {avg_territories:.1f}', fillBetween = True, legendName='Territory Count', start=start, end=end)
            return JSONResponse({"current_territories": current_territories, "maximum_territories": max_territories, "minimum_territories": min_territories, "average_territories": avg_territories, "image": img})

        case "guildActivityWars":
            snapshots = await fetch("""
                SELECT
                    timepoint,
                    COALESCE(war_count, 0) AS war_count
                FROM (
                    SELECT
                        time_bucket_gapfill('15 minutes', ts) AS timepoint,
                        locf(
                            last(wars, ts),
                            (SELECT gs2.wars FROM ts.guild_snapshots gs2
                             WHERE gs2.guild_uuid = %s::uuid
                             AND gs2.wars IS NOT NULL
                             AND gs2.ts < %s
                             ORDER BY gs2.ts DESC LIMIT 1)
                        ) AS war_count
                    FROM ts.guild_snapshots
                    WHERE guild_uuid = %s::uuid
                    AND wars IS NOT NULL
                    AND ts >= %s AND ts < %s
                    GROUP BY timepoint
                ) gapfilled
                ORDER BY timepoint;
            """, (uuid, start, uuid, start, end))
            if not snapshots or all(row["war_count"] == 0 for row in snapshots):
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            times = []
            war_counts = []
            for row in snapshots:
                times.append(row["timepoint"])
                war_counts.append(float(row["war_count"]))

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

            img = createPlot(times, war_counts, "line", blue, f'War History - {name}', 'Date (UTC)', 'Number of Wars', color, legendName='War Count', start=start, end=end)
            return JSONResponse({"current_war": current_war,  "image": img})

        case "guildActivityOnlineMembers":
            snapshots = await fetch("""
                SELECT ts AS timestamp, online_members
                FROM ts.guild_snapshots
                WHERE guild_uuid = %s::uuid
                AND ts >= %s AND ts < %s
                ORDER BY ts
            """, (uuid, start, end))

            if not snapshots:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            times = [row["timestamp"] for row in snapshots]
            raw_numbers = [row["online_members"] for row in snapshots]

            overall_average = sum(raw_numbers) / len(raw_numbers) if raw_numbers else 0
            img = createPlot(times, raw_numbers, "line", blue, f'Online Members - {name}', 'Date (UTC)', 'Players Online', color, ahxlineY = overall_average, ahxlineLabel = f'Average: {overall_average:.1f} players', fillBetween = True, legendName='Average Online Member Count', start=start, end=end)
            return JSONResponse({"max_players": max(raw_numbers), "min_players": min(raw_numbers), "average": overall_average, "image": img})

        case "guildActivityTotalMembers":
            snapshots = await fetch("""
                SELECT ts AS timestamp, total_members
                FROM ts.guild_snapshots
                WHERE guild_uuid = %s::uuid
                AND ts >= %s AND ts < %s
                ORDER BY ts
            """, (uuid, start, end))
            if not snapshots:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            times = [row["timestamp"] for row in snapshots]
            total_numbers = [row["total_members"] for row in snapshots]
            overall_total = sum(total_numbers) / len(total_numbers) if total_numbers else 0
            img = createPlot(times, total_numbers, "line", blue, f'Member Count - {name}', 'Date (UTC)', 'Members', color, ahxlineY = overall_total, ahxlineLabel = f'Average: {overall_total:.1f} members', fillBetween = True, legendName='Total Members', start=start, end=end)
            return JSONResponse({"max_players": max(total_numbers), "min_players": min(total_numbers), "average": overall_total, "image": img})

        case "playerActivityPlaytime":
            daily_data = await activityGains(uuid, start, end)

            if not daily_data:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            dailyPlaytimes = {row["day"]: float(row["playtime_minutes"]) for row in daily_data}
            dates = sorted(dailyPlaytimes.keys())
            playtimeValues = [dailyPlaytimes[date] for date in dates]
            totalPlaytimeinMinutes = sum(playtimeValues)
            averageDailyPlaytime = totalPlaytimeinMinutes / len(dates) if dates else 0

            img = createPlot(dates, playtimeValues, "bar", blue, f'Daily Playtime - {name}', 'Date (UTC)', 'Minutes Played', color, ahxlineY = averageDailyPlaytime, ahxlineLabel = f'Daily Average: {averageDailyPlaytime:.0f} minutes', start=start, end=end)
            return JSONResponse({"daily_average": averageDailyPlaytime, "max_day": max(playtimeValues) if playtimeValues else 0, "min_day": min(playtimeValues) if playtimeValues else 0, "image": img})

        case "playerActivityContributions":
            snapshots = await activityGains(uuid, start, end)

            if not snapshots:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            timestamps = [datetime.combine(row["day"], datetime.min.time()) for row in snapshots]
            daily_gains = [int(row["xp"]) for row in snapshots]

            totalGained = sum(daily_gains)
            average = totalGained / len(daily_gains) if daily_gains else 0

            img = createPlot(timestamps, daily_gains, "bar", blue, f'Daily XP Gain - {name}', 'Date (UTC)', 'XP Gained', color, ahxlineY = average, ahxlineLabel = f'Daily Average: {average:,.0f} XP', start=start, end=end)
            return JSONResponse({"total_xp": totalGained, "max_xp": max(daily_gains) if daily_gains else 0, "min_xp": min(daily_gains) if daily_gains else 0,  "image": img})

        case "playerActivityDungeons":
            snapshots = await fetch("""
            SELECT ts AS timestamp, total_dungeons
            FROM ts.player_snapshots
            WHERE player_uuid = %s::uuid
                AND ts >= %s AND ts < %s
                AND total_dungeons IS NOT NULL
            ORDER BY ts;
            """, (uuid, start, end))

            if not snapshots:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            dates = [row["timestamp"] for row in snapshots]
            total_dungeons = [row["total_dungeons"] for row in snapshots]

            # Highest total and daily gain
            highestTotal = total_dungeons[-1] if total_dungeons else 0
            dungeons_by_day = defaultdict(list)
            for dt, count in zip(dates, total_dungeons):
                dungeons_by_day[dt.date()].append(count)
            dailyGain = [max(counts) - min(counts) for counts in dungeons_by_day.values() if len(counts) > 1]
            highestGain = max(dailyGain) if dailyGain else 0
            img = createPlot(dates, total_dungeons, "line", blue, f'Dungeon History - {name}', 'Date (UTC)', 'Number of Dungeon\'s completed', color, legendName='Dungeon Count', start=start, end=end)
            return JSONResponse({"total_dungeons": highestTotal, "highest_gain": highestGain, "image": img})

        case "playerActivityTotalDungeons":
            snapshots = await fetchone("""
                SELECT jsonb_object_agg(a.name, t.total) AS dungeons
                FROM core.player_activity_totals t
                JOIN core.activities a USING (activity_id)
                WHERE t.player_uuid = %s::uuid AND a.kind = 'dungeon' AND t.total > 0
                GROUP BY t.player_uuid
            """, (uuid,))

            if not snapshots or not snapshots["dungeons"]:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            dungeons = snapshots["dungeons"] # JSONB, already a dict

            sorted_dungeons = dict(sorted(dungeons.items(), key=lambda item: item[1], reverse=True))

            labels = list(sorted_dungeons.keys())
            sizes = list(sorted_dungeons.values())
            total = sum(sizes)
            percent_labels = [f"{label} — {size} ({(size / total * 100):.1f}%)" for label, size in zip(labels, sizes)]

            img = createPlot(percent_labels, sizes, "pie", None, f"Dungeon Pie Chart - {name}", None, None, color, legendName="Dungeons")
            return JSONResponse({"image": img}) # Technically we could just ship this out like how it is on other endpoints, just straight image, but all activity commands should and will b64 images for consistenty

        case "playerActivityRaids":
            snapshots = await fetch("""
                SELECT ts AS timestamp, total_raids
                FROM ts.player_snapshots
                WHERE player_uuid = %s::uuid
                    AND ts >= %s AND ts < %s
                    AND total_raids IS NOT NULL
                ORDER BY ts;
            """, (uuid, start, end))

            if not snapshots:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            dates = [row["timestamp"] for row in snapshots]
            totalRaids = [row["total_raids"] for row in snapshots]

            # Highest total and daily gain
            highestTotal = totalRaids[-1] if totalRaids else 0
            raids_by_day = defaultdict(list)
            for dt, count in zip(dates, totalRaids):
                raids_by_day[dt.date()].append(count)
            dailyGain = [max(counts) - min(counts) for counts in raids_by_day.values() if len(counts) > 1]
            highestGain = max(dailyGain) if dailyGain else 0

            img = createPlot(dates, totalRaids, "line", blue, f'Raid History - {name}', 'Date (UTC)', 'Number of Raid\'s completed', color, legendName='Raid Count', start=start, end=end)
            return JSONResponse({"total": highestTotal, "highest_gain": highestGain, "image": img})

        case "playerActivityTotalRaids":
            snapshots = await fetchone("""
                SELECT jsonb_object_agg(a.name, t.total) AS raids
                FROM core.player_activity_totals t
                JOIN core.activities a USING (activity_id)
                WHERE t.player_uuid = %s::uuid AND a.kind = 'raid' AND t.total > 0
                GROUP BY t.player_uuid
            """, (uuid,))

            if not snapshots or not snapshots["raids"]:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            raids = snapshots["raids"]

            sortedRaids = dict(sorted(raids.items(), key=lambda item: item[1], reverse=True))

            labels = list(sortedRaids.keys())
            sizes = list(sortedRaids.values())
            total = sum(sizes)
            percent_labels = [f"{label} — {size} ({(size / total * 100):.1f}%)" for label, size in zip(labels, sizes)]

            img = createPlot(percent_labels, sizes, "pie", None, f"Raid Pie Chart - {name}", None, None, color, legendName="Raids")

            return JSONResponse({"image": img})

        case "playerActivityMobsKilled":
            snapshots = await fetch("""
            SELECT ts AS timestamp, mobs_killed
            FROM ts.player_snapshots
            WHERE player_uuid = %s::uuid
                AND ts >= %s AND ts < %s
                AND mobs_killed IS NOT NULL
            ORDER BY ts;
            """, (uuid, start, end))

            if not snapshots:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            dates = [row["timestamp"] for row in snapshots]
            totalKills = [row["mobs_killed"] for row in snapshots]
            highestTotal = totalKills[-1] if totalKills else 0
            kills_by_day = defaultdict(list)
            for dt, count in zip(dates, totalKills):
                kills_by_day[dt.date()].append(count)

            daily_gains = [max(counts) - min(counts) for counts in kills_by_day.values() if len(counts) > 1]
            highestGain = max(daily_gains) if daily_gains else 0
            img = createPlot(dates, totalKills, "line", blue, f'Mob Kill History - {name}', 'Date (UTC)', 'Number of Kill\'s', color, legendName='Mob Kill Count', start=start, end=end)
            return JSONResponse({"total_kills": highestTotal, "highest_gain": highestGain, "image": img})

        case "playerActivityWars":
            snapshots = await fetch("""
            SELECT ts AS timestamp, wars
            FROM ts.player_snapshots
            WHERE player_uuid = %s::uuid
                AND ts >= %s AND ts < %s
                AND wars IS NOT NULL
            ORDER BY ts;
            """, (uuid, start, end))

            if not snapshots:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            dates = [row["timestamp"] for row in snapshots]
            totalWars = [row["wars"] for row in snapshots]
            highestTotal = totalWars[-1] if totalWars else 0
            wars_by_day = defaultdict(list)
            for dt, count in zip(dates, totalWars):
                wars_by_day[dt.date()].append(count)
            daily_gains = [max(counts) - min(counts) for counts in wars_by_day.values() if len(counts) > 1]
            highestGain = max(daily_gains) if daily_gains else 0

            img = createPlot(dates, totalWars, "line", blue, f'War Count History - {name}', 'Date (UTC)', 'Number of War\'s', color, legendName='War Count', start=start, end=end)
            return JSONResponse({"total_wars": highestTotal, "highest_gain": highestGain, "image": img})

        case "guildActivityGraids":
            snapshots = await fetch("""
            SELECT ts AS timestamp, guild_raids
            FROM ts.guild_snapshots
            WHERE guild_uuid = %s::uuid
            AND ts >= %s AND ts < %s
            AND guild_raids IS NOT NULL
            ORDER BY ts
            """, (uuid, start, end))

            if not snapshots:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            dates = [row["timestamp"] for row in snapshots]
            totalGraids = [row["guild_raids"] for row in snapshots]
            highestTotal = totalGraids[-1] if totalGraids else 0
            graids_by_day = defaultdict(list)
            for dt, count in zip(dates, totalGraids):
                graids_by_day[dt.date()].append(count)
            daily_gains = [max(counts) - min(counts) for counts in graids_by_day.values() if len(counts) > 1]
            highestGain = max(daily_gains) if daily_gains else 0
            avg_day = round(sum(daily_gains) / len(daily_gains), 2) if daily_gains else 0

            img = createPlot(dates, totalGraids, "line", blue, f'Guild Raid Activity - {name}', 'Date (UTC)', 'Total Guild Raids', color, legendName='Guild Raids', start=start, end=end)
            return JSONResponse({"total_graid": highestTotal, "max_graid": highestGain, "average_graid": avg_day, "image": img})

        case "playerActivityGraids":
            snapshots = await fetch("""
                SELECT ts AS timestamp, total_graids
                FROM ts.player_snapshots
                WHERE player_uuid = %s::uuid
                    AND ts >= %s AND ts < %s
                    AND total_graids IS NOT NULL
                ORDER BY ts;
            """, (uuid, start, end))

            if not snapshots:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            dates = [row["timestamp"] for row in snapshots]
            totalGraids = [row["total_graids"] for row in snapshots]
            highestTotal = max(totalGraids)
            raids_by_day = defaultdict(list)
            for dt, count in zip(dates, totalGraids):
                raids_by_day[dt.date()].append(count)
            dailyGain = [max(counts) - min(counts) for counts in raids_by_day.values() if len(counts) > 1]
            highestGain = max(dailyGain) if dailyGain else 0

            avgGain = round(sum(dailyGain) / len(dailyGain), 2) if dailyGain else 0
            img = createPlot(dates, totalGraids, "line", blue, f'Guild Raid Activity - {name}', 'Date (UTC)', 'Total Guild Raids', color, legendName='Guild Raids', start=start, end=end)
            return JSONResponse({"total_graid": highestTotal, "max_graid": highestGain, "average_graid": avgGain, "image": img})

        case "playerActivityGraidPie":
            snapshots = await fetchone("""
                SELECT jsonb_object_agg(a.name, t.total) AS guild_raids
                FROM core.player_activity_totals t
                JOIN core.activities a USING (activity_id)
                WHERE t.player_uuid = %s::uuid AND a.kind = 'guild_raid' AND t.total > 0
                GROUP BY t.player_uuid
            """, (uuid,))

            if not snapshots or not snapshots["guild_raids"]:
                return JSONResponse(status_code=500, content={"error": "An error occured while achieving this request."})

            raids = snapshots["guild_raids"] # JSONB, already a dict

            sortedGraids = dict(sorted(raids.items(), key=lambda item: item[1], reverse=True))
            labels = list(sortedGraids.keys())
            sizes = list(sortedGraids.values())
            total = sum(sizes)
            percent_labels = [f"{label} — {size} ({(size / total * 100):.1f}%)" for label, size in zip(labels, sizes)]

            img = createPlot(percent_labels, sizes, "pie", None, f"Graid Pie Chart - {name}", None, None, color, legendName="Graids")

            return JSONResponse({"image": img})

        case "guildActivityGraidPie": #NOTE: Not exactly right, i think something is wrong but not sure yet, could be an issue of old data + raids // 4 its hard to tell
            rows = await fetch("""
                SELECT jsonb_object_agg(a.name, t.total) AS guild_raids
                FROM core.player_activity_totals t
                JOIN core.activities a USING (activity_id)
                JOIN core.players p USING (player_uuid)
                WHERE p.guild_uuid = %s::uuid AND a.kind = 'guild_raid' AND t.total > 0
                GROUP BY t.player_uuid
            """, (uuid,))

            if not rows:
                return JSONResponse(status_code=500, content={"error": "No guild data found."})

            combined_raids = {}

            for row in rows:
                if not row["guild_raids"]:
                    continue

                for raid, count in row["guild_raids"].items():
                    combined_raids[raid] = combined_raids.get(raid, 0) + count

            if not combined_raids:
                return JSONResponse(status_code=500, content={"error": "No guild data found."})

            combined_raids = {raid: count // 4 for raid, count in combined_raids.items() if count >= 4}
            if not combined_raids:
                return JSONResponse(status_code=500, content={"error": "No guild data found."})
            sortedGraids = dict(sorted(combined_raids.items(), key=lambda item: item[1], reverse=True))
            labels = list(sortedGraids.keys())
            sizes = list(sortedGraids.values())
            total = sum(sizes)
            percent_labels = [f"{label} — {size} ({(size / total * 100):.1f}%)" for label, size in zip(labels, sizes)]

            img = createPlot(percent_labels, sizes, "pie", None, f"Graid Pie Chart - {name}", None, None, color, legendName="Graids")

            return JSONResponse({"image": img})

        case _: # Default case
            return JSONResponse(status_code=400, content={"error": "Please provide a correct activity type."})

@mapRouter.get("/current") # Not a great name but its the current map
@cache_route(ttl=30) #30s cache
async def current_map(type: str, focusTerritory: str | None = None):
    return mapCreator(type, focusTerritory)

@mapRouter.get("/heatmap")
@cache_route(ttl=600) #10m cache
async def heat_map(timeframe: str):
    if not timeframe:
        return JSONResponse(status_code=400, content={"error": "Please provide a valid timeframe."})
    return await heatmapCreator(timeframe)

@mapRouter.get("/ingmap")
@cache_route(ttl=3600) #1hr cache
async def ingredient_map(ingredient: str | None = None, price: int | None = None, tier: int | None = None):
    updateIngCache = False
    updatePriceCache = False
    ingRandomSeed = random.randint(0, 20) # ings rarely change so no need to update
    priceRandomSeed = random.randint(0, 20) #prices change sometimes so update a bit more
    if ingRandomSeed == 1:
        updateIngCache = True
    if priceRandomSeed == 6:
        updatePriceCache = True

    cacheLoaded = loadCache()
    #TODO: if its only 1 ing, just search the ing to get most-updated shit.
    if updateIngCache or not cacheLoaded:
        findIngCoords(ingToMobs, mobCoords, ingRarity)
        saveCache()

    return ingredientMap(ingToMobs, mobCoords, ingredient, price, priceCache, updatePriceCache, tier)


@seasonRouter.get("/season")
@cache_route(ttl=86400) #24hr cache
async def getAllSeasons():
    success, r = makeRequest("https://api.wynncraft.com/v3/guild/seasons")
    if not success:
        return JSONResponse(status_code=400, content={"error": "A error occured while contacting the Wynn API. Please try again later."})
    jsonData = r.json()
    return jsonData

app.include_router(searchRouter)
app.include_router(leaderboardRouter)
app.include_router(activityRouter)
app.include_router(mapRouter)
app.include_router(seasonRatingdRouter)