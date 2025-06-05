import requests
import discord
from datetime import datetime, timezone
import json
from collections import Counter
import logging
import time
import sqlite3
from datetime import datetime
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.dates import HourLocator, DateFormatter
from collections import defaultdict, deque
import io
import seaborn as sns
import bisect
import random
import ast
import os
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import shelve
  
logger = logging.getLogger('discord')

ratelimitmultiplier = 1
ratelimitwait = 0.1 
cooldownHolder = {}
rootDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
territoryFilePath = os.path.join(rootDir, 'database', 'territory')

sns.set_style("whitegrid")
mpl.use('Agg') # Backend without any gui popping up
blue, = sns.color_palette("muted", 1)

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
            r = session.get(url)
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
                        logger.warning("We are under 12. PANIC!!")
                        time.sleep(2)
                    elif remaining < 30:
                        logger.warning("We are under 30. PANIC!!")
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
                logger.warning(f"{url} failed with {status}. Current retry is at {retries}.")
                retries += 1
                time.sleep(2)
                continue
            else:
                logger.error(f"Non-retryable error {status} for {url}: {err}")
                return False, {} 

    logger.error(f"Max retries exceeded for {url}")
    return False, {} 
    
def human_time_duration(seconds): # thanks guy from github https://gist.github.com/borgstrom/936ca741e885a1438c374824efb038b3
    TIME_DURATION_UNITS = (
        ('week', 60*60*24*7),
        ('day', 60*60*24),
        ('hour', 60*60),
        ('minute', 60),
        ('second', 1)
    )
    if seconds == 0:
        return 'Error while getting time! Report this to my github.' # it shouldnt ever be 0, but better safe than sorry
    parts = []
    for unit, div in TIME_DURATION_UNITS:
        amount, seconds = divmod(int(seconds), div)
        if amount > 0:
            parts.append('{} {}{}'.format(amount, unit, "" if amount == 1 else "s"))
    return ' '.join(parts)

def checkCooldown(userOrGuildID, cooldownSeconds): # We could theoretically save cooldowns to disk, but uh, we wont!
    now = time.time()
    #logger.info(cooldownHolder)

    lastUsed = cooldownHolder.get(userOrGuildID, 0)
    elapsed = now - lastUsed
    if elapsed < cooldownSeconds:
        remaining = round(cooldownSeconds - elapsed, 1)
        return remaining
    cooldownHolder[userOrGuildID] = now
    #logger.info(cooldownHolder)
    return True

def findAttackingMembers(attacker):
    success, r = makeRequest("https://api.wynncraft.com/v3/guild/prefix/"+str(attacker)) # Using nori api as main for less api usage + it shows online members easier
    if not success:
        logger.error("Unsuccessful request in findAttackingMembers - 1.")
        return [["Unknown", "Unknown", 1738]]
    jsonData = r.json()
    onlineMembers = []
    warringMembers = []

    for rank in jsonData["members"]: # this iterates through every rank like chief, owner, etc
        if isinstance(jsonData["members"][rank], dict):
            for member in jsonData["members"][rank].values(): 
                if member['online']: # checks if online is set to true or false
                    onlineMembers.append(member['uuid'])

    for i in onlineMembers:
        success, r = makeRequest("https://api.wynncraft.com/v3/player/"+str(i))
        if not success:
            logger.error("Unsuccessful request in findAttackingMembers - 2.")
            return [["Unknown", "Unknown", 1738]]
        json = r.json()
        #logger.info(f"json: {json}")
        if int(json["globalData"]["wars"]) > 20: # arbitrary number, imo 20 or more means youre prolly a full-time warrer
            warringMembers.append([json["username"], json['server'], int(json["globalData"]["wars"])])

    if not warringMembers: # if for some reason this comes back with no one (offline or sum)
        attackingMembers = [["Unknown", "Unknown", 1738]] # ay
        #(f"Attacking Members: {attackingMembers}")
        return attackingMembers
    
    worldStrings = [sublist[1] for sublist in warringMembers]
    mostCommonWorld = Counter(worldStrings).most_common(1) # finds the most common world between all warring members

    if len(mostCommonWorld) == 0 or mostCommonWorld[0][1] == 1:
        # no majority world, so we just send who has the most amount of wars
        highestWars = max(warringMembers, key=lambda x: x[2])
        attackingMembers = [highestWars]
    else:
        # majority world, just chop shit up and whatnot
        string = mostCommonWorld[0][0]
        attackingMembers = [sublist for sublist in warringMembers if sublist[1] == string]
    #logger.info(f"Attacking Members: {attackingMembers}")
    return attackingMembers
        
def sendEmbed(attacker, defender, terrInQuestion, timeLasted, attackerTerrBefore, attackerTerrAfter, defenderTerrBefore, defenderTerrAfter, guildPrefix, pingRoleID, intervalForPing, timesinceping, guildID):
    key = (guildID, guildPrefix)
    if key not in timesinceping:
        timesinceping[key] = 0  # setup 0 first, never again
        
    shouldPing = False
    if attacker != guildPrefix:
        attackingMembers = findAttackingMembers(attacker) # y6eah i give a shit.
        world = attackingMembers[0][1]
        username = [item[0] for item in attackingMembers]

    description = "### 🟢 **Gained Territory!**" if attacker == guildPrefix else "### 🔴 **Lost Territory!**"
    description += f"\n\n**{terrInQuestion}**\nAttacker: **{attacker}** ({attackerTerrBefore} -> {attackerTerrAfter})\nDefender: **{defender}** ({defenderTerrBefore} -> {defenderTerrAfter})\n\nThe territory lasted {timeLasted}." + ("" if attacker == guildPrefix else f"\n{world}: **{'**, **'.join(username)}**")
    embed = discord.Embed(
        description=description,
        color=0x00FF00 if attacker == guildPrefix else 0xFF0000  # Green for gain, red for loss
    )
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    
    # Check if we should ping
    if pingRoleID and intervalForPing and attacker != guildPrefix:
        current_time = time.time()
        if current_time - int(timesinceping[key]) >= intervalForPing*60:
            timesinceping[key] = current_time
            shouldPing = True
    #logger.info(f"Sending Embed - {'Gained Territory' if attacker == guildPrefix else 'Lost Territory'}, {terrInQuestion}, Attacker: {attacker} ({attackerTerrBefore} -> {attackerTerrAfter}), Defender: {defender} ({defenderTerrBefore} -> {defenderTerrAfter}), lasted {timeLasted}. {'{}: **{}**'.format(world, ', '.join(username)) if attacker != guildPrefix else ''}") # linux FUCKING SUCKS i hate the bird
    return {
        "embed": embed,
        "shouldPing": shouldPing,
        "roleID": pingRoleID if shouldPing  else None
    }

def checkterritories(untainteddata, untainteddataOLD, guildPrefix, pingRoleID, expectedterrcount, intervalForPing, hasbeenran, timesinceping, guildID):
    gainedTerritories = {}
    lostTerritories = {}
    terrcount = {}
    messagesToSend = []
    key = (guildID, guildPrefix)
    
    for territory, data in untainteddata.items():
        old_guild = untainteddataOLD[str(territory)]['guild']['prefix']
        new_guild = data['guild']['prefix']
        if old_guild == guildPrefix and new_guild != guildPrefix:
            lostTerritories[territory] = data
        elif old_guild != guildPrefix and new_guild == guildPrefix:
            gainedTerritories[territory] = data
    #logger.info(f"Gained Territories: {gainedTerritories}")
    #logger.info(f"Lost Territories: {lostTerritories}")
    #logger.info(f"historicalTerritories: {historicalTerritories}")
    if guildPrefix.lower() == 'global': # We check and then enter, never to leave again
        messagesToSend = []
        for territory, data in untainteddata.items():
            oldGuild = untainteddataOLD[str(territory)]['guild']['prefix']
            newGuild = data['guild']['prefix']
            if oldGuild != newGuild and newGuild != "None":
                reworkedDate = datetime.fromisoformat(untainteddataOLD[territory]['acquired'].replace("Z", "+00:00"))
                reworkedDateNew = datetime.fromisoformat(data['acquired'].replace("Z", "+00:00"))
                elapsed_time = int(reworkedDateNew.timestamp() - reworkedDate.timestamp())

                attackerOldCount = sum(1 for d in untainteddataOLD.values() if d["guild"]["prefix"] == newGuild)
                attackerNewCount = sum(1 for d in untainteddata.values() if d["guild"]["prefix"] == newGuild)
                defenderOldCount = sum(1 for d in untainteddataOLD.values() if d["guild"]["prefix"] == oldGuild)
                defenderNewCount = sum(1 for d in untainteddata.values() if d["guild"]["prefix"] == oldGuild)

                embed = discord.Embed(
                    description=f"### ⚪ **Territory Change**\n\n**{territory}**\nAttacker: **{newGuild}** ({attackerOldCount} -> {attackerNewCount})\nDefender: **{oldGuild}** ({defenderOldCount} -> {defenderNewCount})\n\nThe territory lasted {human_time_duration(elapsed_time)}.",
                    color=0xffffff
                )
                embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")

                shouldPing = False
                if pingRoleID and intervalForPing:
                    if key not in timesinceping:
                        timesinceping[key] = 0
                    current_time = time.time()
                    if current_time - int(timesinceping[key]) >= intervalForPing*60:
                        timesinceping[key] = current_time
                        shouldPing = True
                messagesToSend.append({
                    "embed": embed,
                    "shouldPing": shouldPing,
                    "roleID": pingRoleID if shouldPing  else None
                })
        return messagesToSend
    terrcount[guildPrefix] = expectedterrcount[guildPrefix] # this is what will fix (40 -> 38)
    if lostTerritories: # checks if its empty, no need to run if it is
        for i in lostTerritories:
            reworkedDate = datetime.fromisoformat(untainteddataOLD[i]['acquired'].replace("Z", "+00:00")) # gets the time from the old data
            timestamp = reworkedDate.timestamp()
            reworkedDateNew = datetime.fromisoformat(lostTerritories[i]['acquired'].replace("Z", "+00:00")) # gets the time from the new data
            timestampNew = reworkedDateNew.timestamp() 
            elapsed_time = int(timestampNew) - int(timestamp)
            
            opponentTerrCountBefore = sum(1 for data in untainteddataOLD.values()if data["guild"]["prefix"] == lostTerritories[str(i)]['guild']['prefix'])
            opponentTerrCountAfter = sum(1 for data in untainteddata.values()if data["guild"]["prefix"] == lostTerritories[str(i)]['guild']['prefix']) # this will maybe just be wrong if multiple were taken within 11s.
            terrcount[guildPrefix] -= 1
            embedInfo = sendEmbed(lostTerritories[i]['guild']['prefix'], guildPrefix, i, human_time_duration(elapsed_time), str(opponentTerrCountBefore), str(opponentTerrCountAfter), str(expectedterrcount[guildPrefix]), str(terrcount[guildPrefix]), guildPrefix, pingRoleID, intervalForPing, timesinceping, guildID)
            messagesToSend.append(embedInfo)
            expectedterrcount[guildPrefix] = terrcount[guildPrefix]
    if gainedTerritories: # checks if its empty, no need to run if it is
        for i in gainedTerritories:
            reworkedDate = datetime.fromisoformat(untainteddataOLD[i]['acquired'].replace("Z", "+00:00")) # gets the time from the old data
            timestamp = reworkedDate.timestamp()
            reworkedDateNew = datetime.fromisoformat(gainedTerritories[i]['acquired'].replace("Z", "+00:00")) # gets the time from the new data
            timestampNew = reworkedDateNew.timestamp() 
            elapsed_time = int(timestampNew)- int(timestamp)
            
            opponentTerrCountBefore = sum(1 for data in untainteddataOLD.values()if data["guild"]["prefix"] == untainteddataOLD[str(i)]['guild']['prefix'])
            opponentTerrCountAfter = sum(1 for data in untainteddata.values()if data["guild"]["prefix"] == untainteddataOLD[str(i)]['guild']['prefix']) # this will maybe just be wrong if multiple were taken within 11s.
            terrcount[guildPrefix]+=1
            embedInfo = sendEmbed(guildPrefix, untainteddataOLD[i]['guild']['prefix'], i, human_time_duration(elapsed_time),str(expectedterrcount[guildPrefix]), str(terrcount[guildPrefix]), str(opponentTerrCountBefore), str(opponentTerrCountAfter), guildPrefix, pingRoleID, intervalForPing, timesinceping, guildID)
            messagesToSend.append(embedInfo)
            expectedterrcount[guildPrefix] = terrcount[guildPrefix]
    if gainedTerritories or lostTerritories: # just for resetting our variables
        hasbeenran[key] = False
    else:
        hasbeenran[key] = True
    return messagesToSend

def printTop3(list, word, word2):
    output = ""
    for i, sublist in enumerate(list[:3], 1):
        output += f"{i}.{word} {sublist[1]}: **{sublist[0]}** {word2}\n"
    return output

def guildLookup(guildPrefixorName, r):
    jsonData = r.json()
    online_count = 0 # default
    ratingList = [] 
    contributingList = [] 

    for rank in jsonData["members"]: # this iterates through every rank like chief, owner, etc
        if isinstance(jsonData["members"][rank], dict):
            for member in jsonData["members"][rank].values(): 
                if member['online']: # checks if online is set to true or false
                    online_count += 1
       
    for index,content in enumerate(jsonData["seasonRanks"]):
        ratingList.append([jsonData["seasonRanks"][content]["rating"], content])
    
    #this is like if the top 2 for loops fucked and made this
    for rank in jsonData["members"]: 
        if isinstance(jsonData["members"][rank], dict):  
            for member_name, member_data in jsonData["members"][rank].items():  
                contributingList.append([member_data["contributed"], member_name])  
    
    ratingList.sort(reverse = True)
    contributingList.sort(reverse = True)
    warCount = jsonData.get("wars") or 0 # Accounts for null wars, or 0 wars
    formattedRatingList = [[f"{x:,}", y] for x, y in ratingList]
    formattedcontributingList = [[f"{x:,}", y] for x, y in contributingList]
    embed = discord.Embed(
        description=f"""
        {"## "+'🐝 **Fruman Bee (FUB)** 🐝' if jsonData['prefix'] == 'FUB' else '**'+jsonData['name']+' ('+jsonData['prefix']+')**'}
        \n‎\nOwned By: **{list(jsonData["members"]["owner"].keys())[0]}**
        Online: **{online_count}**/**{jsonData["members"]["total"]}**
        Guild Level: **{jsonData["level"]}** (**{jsonData.get("xpPercent", jsonData.get("xp_percent", "N/A"))}**% until level {int(jsonData["level"])+1})\n
        Territory Count: **{jsonData["territories"]}**
        Wars: **{"{:,}".format(warCount)}**\n
        Top Season Rankings:
        {printTop3(formattedRatingList, " Season", "SR")}
        Top Contributing Members:
        {printTop3(formattedcontributingList, "", "XP")}
        """,
        color=0xFFFF00  # i could make color specific to lookup command, but i wont until i can figure out how to get banner inside of the embed.
    )
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    logger.info(f"Print for {guildPrefixorName} was a success!")
    return(embed)

def getTerritoryNames(untainteddata, guildPrefix):
    with open('territories.json') as a:
        territoryData = json.load(a)

    ownedTerritories = {}
    if guildPrefix == None: # they want all territories
        ownedTerritories = territoryData
    else:
        for territory, data in untainteddata.items():
            ownerOfTerritory = data['guild']['prefix']
            if ownerOfTerritory is None: # sometimes shit be null idk
                ownerOfTerritory = "qwdiqwidjqwiodjqiodj" # garbage code btw
            if ownerOfTerritory == guildPrefix: # if the guild owns it, add to the dict
                ownedTerritories[territory] = data
    scorelist = {}
    otherData = {}
    for hqCandidate in ownedTerritories:
        connections = []
        externals = []
        visited = set()
        queue = [(hqCandidate, 0)]

        while queue:
            current, dist = queue.pop(0)
            if current in visited or dist > 3:
                continue
            visited.add(current)

            if dist == 1 and current in ownedTerritories: # thats a conn
                connections.append(current)

            if dist > 0 and current in ownedTerritories and current != hqCandidate: #  its a external
                externals.append(current)

            for conn in territoryData[current]["Trading Routes"]:
                if conn not in visited:
                    queue.append((conn, dist + 1))
        multiplier = (1.5 + (len(externals) * 0.25)) * (1.0 + (len(connections) * 0.30))
        score = int(multiplier * 100)
        scorelist[hqCandidate] = int(score)
        otherData[hqCandidate] = (len(connections), len(externals)) # i for sure couldve merged this with scorelist, but uhhhhh not worth my time!!
        externals = []
    scorelist = dict(reversed(sorted(scorelist.items(), key=lambda item: item[1]))) # sorts on top
    
    description = "## Best HQ Location:\n"
    for i, (location, score) in enumerate(scorelist.items()):
        if i >= 5:  # max 5 entries
            break
        connCount, externalCount = otherData[location] #like balatro!!!!!
        description += f"{i + 1}. **{location}**: {score}% - Connections: {connCount}, Externals: {externalCount}\n"
    description += "\n-# Note: HQ calculations are purely based on headquarter\n-# strength, not importance of territories or queue times."
    
    embed = discord.Embed(
        description=description,
        color=0x3457D5,
        )
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    logger.info(f"Ran HQ lookup successfully for {guildPrefix if guildPrefix else 'global map'}.")
    return embed

def lookupUser(memberList):
    inactivityDict = {
        "Four Week Inactive Users": [],
        "Three Week Inactive Users": [],
        "Two Week Inactive Users": [],
        "One Week Inactive Users": [],
        "Three Day Inactive Users": [],
        "Active Users": [],
    }
    for member in memberList:
        time.sleep(1) # Slow down inactivity because we need to preserve our ratelimits
        success, r = makeRequest("https://api.wynncraft.com/v3/player/"+str(member))
        if not success:
            logger.error("Unsuccessful request in lookupUser.")
            continue # i think thisll work
        jsonData = r.json()
        lastJoinDate = jsonData["lastJoin"]

        try: # This should hopefully fix the offchance that someone's last time has no millisecond
            joinTime = datetime.strptime(lastJoinDate, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            joinTime = datetime.strptime(lastJoinDate, "%Y-%m-%dT%H:%M:%SZ")
        joinTime = joinTime.replace(tzinfo=timezone.utc)
        currentTime = datetime.now(timezone.utc)
        timeDifference = currentTime - joinTime
        timeDifference = int(timeDifference.total_seconds())

        dt = datetime.fromisoformat(lastJoinDate.replace("Z", "+00:00"))  # Convert 'Z' to UTC
        epochTime = dt.timestamp()

        if timeDifference >= 86400 * 28:  # 28 days or more
            inactivityDict["Four Week Inactive Users"].append((jsonData["username"], int(epochTime)))
        elif timeDifference >= 86400 * 21:  # Between 21 and 27 days
            inactivityDict["Three Week Inactive Users"].append((jsonData["username"], int(epochTime)))
        elif timeDifference >= 86400 * 14:  # Between 14 and 20 days
            inactivityDict["Two Week Inactive Users"].append((jsonData["username"], int(epochTime)))
        elif timeDifference >= 86400 * 7:  # Between 7 and 13 days
            inactivityDict["One Week Inactive Users"].append((jsonData["username"], int(epochTime)))
        elif timeDifference >= 86400 * 3:  # Between 3 and 7 days
            inactivityDict["Three Day Inactive Users"].append((jsonData["username"], int(epochTime)))
        else:  # Less than 3 days
            inactivityDict["Active Users"].append((jsonData["username"], int(epochTime)))
    
    for key in inactivityDict:
        inactivityDict[key].sort(key=lambda x: x[1])  # Sort by timestamp, so we can go from oldest to newest
    return inactivityDict

def lookupGuild(r):
    jsonData = r.json()
    memberList = []
    for rank in jsonData["members"]: # this iterates through every rank like chief, owner, etc
        if isinstance(jsonData["members"][rank], dict): # checks if it has a rank i think so it knows people from non arrrays??
            for member, value in jsonData["members"][rank].items(): 
                memberList.append(value['uuid']) # we use uuid because name changes fuck up username lookups
    #logger.info(f"memberlist-2: {memberList}")
    return lookupUser(memberList)

# because not everything happens in a second.
def intvervalGrouping(timestamps):
    interval_seconds=30
    groups = deque()
    for ts in timestamps:
        if not groups or (ts - groups[-1][0]).total_seconds() > interval_seconds:
            groups.append([ts])
        else:
            groups[-1].append(ts)
    return groups

def guildActivityPlaytime(guild_uuid, name):
    logger.info(f"guild_uuid: {guild_uuid}, ActivityPlaytime")
    conn = sqlite3.connect('database/guild_activity.db')
    cursor = conn.cursor()

    cursor.execute("""
        SELECT member_uuid, timestamp, online
        FROM member_snapshots
        WHERE guild_uuid = ?
        AND timestamp >= datetime('now', '-1 day')
        ORDER BY timestamp
    """, (guild_uuid,))
    snapshots = cursor.fetchall()
    if not snapshots:
        conn.close()
        return None, None
    
    hourly_data = defaultdict(list)
    
    grouped_snapshots = intvervalGrouping([datetime.fromisoformat(snapshot[1]) for snapshot in snapshots])

    for group in grouped_snapshots:
        avg_online = sum(
            snapshot[2] for snapshot in snapshots 
            if datetime.fromisoformat(snapshot[1]) in group
        ) / len(group)
        midpoint_time = group[0] + (group[-1] - group[0]) / 2
        hourly_data[midpoint_time].append(avg_online)

    times = sorted(hourly_data.keys())
    averages = [sum(hourly_data[time]) / len(hourly_data[time]) * 100 for time in times]
    overall_average = sum(averages) / len(averages) if averages else 0

    # you cannot get me to try and understand what is happening here.
    plt.figure(figsize=(12, 6))
    plt.plot(times, averages, '-', label='Average Activity Playtime', color=blue, lw=3)
    plt.fill_between(times, 0, averages, alpha=0.3)
    plt.axhline(y=overall_average, color='red', linestyle='-', label=f'Average: {overall_average:.1f}%')
    time_formatter = DateFormatter('%H:%M')
    plt.gca().xaxis.set_major_formatter(time_formatter)
    hour_locator = HourLocator()
    plt.gca().xaxis.set_major_locator(hour_locator)
    plt.title(f'Playtime Activity - {name}', fontsize=14)
    plt.xlabel('Time (UTC)', fontsize=12)
    plt.ylabel('Players Online (%)', fontsize=12)
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
    plt.close()

    file = discord.File(buf, filename='playtime_graph.png')
    embed = discord.Embed(
        title=f"Playtime Analysis for {name}",
        description=f"Maximum player activity: {max(averages):.2f}%\nMinimum player activity: {min(averages):.2f}%\nAverage player activity: {overall_average:.2f}%",
        color=discord.Color.blue()
    )
    embed.set_image(url="attachment://playtime_graph.png")
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    conn.close()
    buf.close()
    return file, embed

def guildActivityXP(guild_uuid, name):
    logger.info(f"guild_uuid: {guild_uuid}, activityXP")
    conn = sqlite3.connect('database/guild_activity.db')
    cursor = conn.cursor()

    cursor.execute("""
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
    """, (guild_uuid,))
    
    snapshots = cursor.fetchall()
    if not snapshots:
        conn.close()
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
    buf.seek(0)
    plt.close()
    
    file = discord.File(buf, filename='xp_graph.png')
    embed = discord.Embed(
        title=f"XP Analysis for {name}",
        description=f"Total XP (14 days): {total_xp:,.0f}\nDaily Average: {avg_daily_xp:,.0f}\nHighest Day: {max_daily_xp:,.0f}\nLowest Day: {min_daily_xp:,.0f}",
        color=discord.Color.blue()
    )
    embed.set_image(url="attachment://xp_graph.png")
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    
    conn.close()
    buf.close()
    return file, embed

def guildActivityTerritories(guild_uuid, name):
    logger.info(f"guild_uuid: {guild_uuid}, activityTerritories")
    conn = sqlite3.connect('database/guild_activity.db')
    cursor = conn.cursor()

    cursor.execute("""
        WITH RECURSIVE 
        timepoints AS (
            SELECT datetime('now', '-3 days') as timepoint
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
    """, (guild_uuid,))
    snapshots = cursor.fetchall()
    if not snapshots or all(count == 0 for _, count in snapshots):
        conn.close()
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
        conn.close()
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
    
    file = discord.File(buf, filename='territory_graph.png')
    embed = discord.Embed(
        title=f"Territory Analysis for {name}",
        description=f"Current territories: {current_territories:.0f}\nMaximum territories: {max_territories:.0f}\nMinimum territories: {min_territories:.0f}\nAverage territories: {avg_territories:.0f}",
        color=discord.Color.blue()
    )
    embed.set_image(url="attachment://territory_graph.png")
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")

    buf.close()
    conn.close()
    return file, embed

def guildActivityWars(guild_uuid, name):
    logger.info(f"guild_uuid: {guild_uuid}, activityWars")
    conn = sqlite3.connect('database/guild_activity.db')
    cursor = conn.cursor()

    cursor.execute("""
        WITH RECURSIVE 
        timepoints AS (
            SELECT datetime('now', '-3 days') as timepoint
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
    """, (guild_uuid,))
    snapshots = cursor.fetchall()
    if not snapshots or all(count == 0 for _, count in snapshots):
        conn.close()
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
        conn.close()
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
    plt.close()
    
    file = discord.File(buf, filename='wars_graph.png')
    embed = discord.Embed(
        title=f"Warring Analysis for {name}",
        description=f"Current war count: {current_war:.0f}",
        color=discord.Color.blue()
    )
    embed.set_image(url="attachment://wars_graph.png")
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")

    buf.close()
    conn.close()
    return file, embed

def guildActivityOnlineMembers(guild_uuid, name):
    logger.info(f"guild_uuid: {guild_uuid}, activityOnlineMembers")
    conn = sqlite3.connect('database/guild_activity.db')
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT member_uuid, timestamp, online
        FROM member_snapshots
        WHERE guild_uuid = ?
        AND timestamp >= datetime('now', '-1 day')
        ORDER BY timestamp
    """, (guild_uuid,))
    snapshots = cursor.fetchall()
    
    if not snapshots:
        conn.close()
        return None, None
    
    hourly_data = defaultdict(list)
    
    grouped_snapshots = intvervalGrouping([datetime.fromisoformat(snapshot[1]) for snapshot in snapshots])

    for group in grouped_snapshots:
        total_online = sum(snapshot[2] for snapshot in snapshots if datetime.fromisoformat(snapshot[1]) in group)
        hourly_data[group[0]].append(total_online)
    times = sorted(hourly_data.keys())
    raw_numbers = [sum(hourly_data[time]) for time in times]
    overall_average = sum(raw_numbers) / len(raw_numbers) if raw_numbers else 0

    plt.figure(figsize=(12, 6))
    plt.plot(times, raw_numbers, '-', label='Average Online Member Count', color=blue, lw=3)
    plt.fill_between(times, 0, raw_numbers, alpha=0.3)
    plt.axhline(y=overall_average, color='red', linestyle='-', label=f'Average: {overall_average:.1f} players')
    time_formatter = DateFormatter('%H:%M')
    plt.gca().xaxis.set_major_formatter(time_formatter)
    hour_locator = HourLocator()
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
    plt.close()

    file = discord.File(buf, filename='playtime_graph.png')
    embed = discord.Embed(
        title=f"Online Members for {name}",
        description=(
            f"Maximum players online: {max(raw_numbers):.0f}\n"
            f"Minimum players online: {min(raw_numbers):.0f}\n"
            f"Average players online: {overall_average:.1f}"
        ),
        color=discord.Color.blue()
    )
    embed.set_image(url="attachment://playtime_graph.png")
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    
    conn.close()
    buf.close()
    
    return file, embed

def guildActivityTotalMembers(guild_uuid, name):
    logger.info(f"guild_uuid: {guild_uuid}, activityTotalMembers")
    conn = sqlite3.connect('database/guild_activity.db')
    cursor = conn.cursor()

    cursor.execute("""
        SELECT timestamp, total_members
        FROM guild_snapshots
        WHERE guild_uuid = ?
        AND timestamp >= datetime('now', '-1 day')
        ORDER BY timestamp
    """, (guild_uuid,))
    snapshots = cursor.fetchall()
    if not snapshots:
        conn.close()
        return None, None
    
    hourly_data = defaultdict(list)
    grouped_snapshots = intvervalGrouping([datetime.fromisoformat(snapshot[0]) for snapshot in snapshots])
    for group in grouped_snapshots:
        total_members = sum(snapshot[1] for snapshot in snapshots if datetime.fromisoformat(snapshot[0]) in group)
        hourly_data[group[0]].append(total_members)

    times = sorted(hourly_data.keys())
    total_numbers = [sum(hourly_data[time]) for time in times] 
    overall_total = sum(total_numbers) / len(total_numbers) if total_numbers else 0
    plt.figure(figsize=(12, 6))
    plt.plot(times, total_numbers, '-', label='Total Members', color=blue, lw=3)
    plt.fill_between(times, 0, total_numbers, alpha=0.3)
    plt.axhline(y=overall_total, color='r', linestyle='-', label=f'Average: {overall_total:.1f} members')
    time_formatter = DateFormatter('%H:%M')
    plt.gca().xaxis.set_major_formatter(time_formatter)
    hour_locator = HourLocator()
    plt.gca().xaxis.set_major_locator(hour_locator)
    plt.title(f'Total Members Activity - {name}', fontsize=14)
    plt.xlabel('Time (UTC)', fontsize=12)
    plt.ylabel('Total Members', fontsize=12)
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
    plt.close()

    file = discord.File(buf, filename='total_members_graph.png')
    embed = discord.Embed(
        title=f"Total Members Analysis for {name}",
        description=(
            f"Maximum total members: {max(total_numbers):.0f}\n"
            f"Minimum total members: {min(total_numbers):.0f}\n"
            f"Average total members: {overall_total:.0f}"
        ),
        color=discord.Color.blue()
    )
    embed.set_image(url="attachment://total_members_graph.png")
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    
    conn.close()
    buf.close()
    return file, embed

def guildLeaderboardOnlineMembers():
    logger.info(f"leaderboardOnlineMembers")
    conn = sqlite3.connect('database/guild_activity.db')
    cursor = conn.cursor()

    cursor.execute("""
    WITH avg_online_members AS (
        SELECT 
            g.name as guild_name,
            g.prefix as guild_prefix,
            g.uuid as guild_uuid,
            ROUND(AVG(gs.online_members), 2) as avg_online_members,
            COUNT(gs.id) as snapshot_count
        FROM guilds g
        JOIN guild_snapshots gs ON g.uuid = gs.guild_uuid
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
    LIMIT 10;
    """)
    snapshots = cursor.fetchall()
    if not snapshots:
        conn.close()

    counter = 0
    max_guild_length = max(len(guild) for guild, _, _ in snapshots)
    header = "```\n{:<3} {:<{guild_width}} {:<15}\n".format("#", "Guild", "Average Online", guild_width=max_guild_length)
    separator = "-" * (max_guild_length + 20) + "\n"

    description = header + separator
    for counter, (guild, avg_online, _) in enumerate(snapshots, 1):
        description += "{:<3} {:<{guild_width}} {:<15.2f}\n".format(
            counter,
            guild,
            avg_online,
            guild_width=max_guild_length
        )
    description += "```"
    embed = discord.Embed(
        title=f"Average Online Members Leaderboard",
        description=description,
        color=discord.Color.blue()
    )
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    return embed

def guildLeaderboardTotalMembers():
    logger.info(f"leaderboardTotalMembers")
    conn = sqlite3.connect('database/guild_activity.db')
    cursor = conn.cursor()

    cursor.execute("""
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
    LIMIT 10;
    """)
    snapshots = cursor.fetchall()
    if not snapshots:
        conn.close()
    
    counter = 0
    max_guild_length = max(len(guild) for guild, _, _ in snapshots)
    header = "```\n{:<3} {:<{guild_width}} {:<15}\n".format("#", "Guild", "Total Members", guild_width=max_guild_length)
    separator = "-" * (max_guild_length + 20) + "\n"

    description = header + separator
    for counter, (guild, totalMembers, _) in enumerate(snapshots, 1):
        description += "{:<3} {:<{guild_width}} {:<15.0f}\n".format(
            counter,
            guild,
            totalMembers,
            guild_width=max_guild_length
        )
    description += "```"
    embed = discord.Embed(
        title=f"Total Members Leaderboard",
        description=description,
        color=discord.Color.blue()
    )
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    return embed

def guildLeaderboardWars():
    logger.info(f"leaderboardWars")
    conn = sqlite3.connect('database/guild_activity.db')
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            g.name || ' (' || COALESCE(g.prefix, '') || ')' as guild_name,
            MAX(gs.wars) as total_wars,
            RANK() OVER (ORDER BY MAX(gs.wars) DESC) as war_rank
        FROM guilds g
        JOIN guild_snapshots gs ON g.uuid = gs.guild_uuid
        GROUP BY g.uuid, g.name, g.prefix
        ORDER BY total_wars DESC
        LIMIT 10;
    """)
    snapshots = cursor.fetchall()
    if not snapshots:
        conn.close()
        return None

    leaderboard_description = "```\n{:<3} {:<25} {:<10}\n".format("#", "Guild", "Wars")
    separator = "-" * 40 + "\n"
    leaderboard_description += separator
    for row in snapshots:
        guild_name = row[0]
        total_wars = row[1]
        rank = row[2]
        leaderboard_description += "{:<3} {:<25} {:<10}\n".format(rank, guild_name, total_wars)
    leaderboard_description += "```"

    embed = discord.Embed(
        title="Guild Wars Leaderboard",
        description=leaderboard_description,
        color=discord.Color.blue()
    )
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    conn.close()
    return embed

def guildLeaderboardXP():
    logger.info(f"leaderboardXP")
    conn = sqlite3.connect('database/guild_activity.db')
    cursor = conn.cursor()
    
    cursor.execute("""
        WITH time_bounds AS (
            SELECT 
                guild_uuid,
                member_uuid,
                MIN(timestamp) as min_time,
                MAX(timestamp) as max_time
            FROM member_snapshots
            WHERE timestamp >= datetime('now', '-1 day')
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
        LIMIT 10;
    """)
    
    snapshot = cursor.fetchall()
    if not snapshot:
        conn.close()
        return None

    leaderboard_description = "```\n{:<3} {:<25} {:<10}\n".format("#", "Guild", "XP Gained")
    separator = "-" * 40 + "\n"
    leaderboard_description += separator
    for row in snapshot:
        guild_name = row[0]
        xp_gained = row[1]
        rank = row[2]
        leaderboard_description += "{:<3} {:<25} {:<10,d}\n".format(rank, guild_name, xp_gained)
    leaderboard_description += "```"

    embed = discord.Embed(
        title="Guild XP Gained Leaderboard (Last 24 Hours)",
        description=leaderboard_description,
        color=discord.Color.blue()
    )
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    conn.close()
    return embed

def guildLeaderboardPlaytime():
    logger.info(f"leaderboardPlaytime")
    conn = sqlite3.connect('database/guild_activity.db')
    cursor = conn.cursor()
    
    cursor.execute("""
        WITH time_grouped_data AS (
            SELECT 
                g.uuid as guild_uuid,
                ms.timestamp,
                ms.online,
                datetime(strftime('%s', ms.timestamp) - strftime('%s', ms.timestamp) % 300, 'unixepoch') as interval_start
            FROM guilds g
            JOIN member_snapshots ms ON g.uuid = ms.guild_uuid
            WHERE ms.timestamp >= datetime('now', '-1 day')
        ),
        interval_averages AS (
            SELECT 
                guild_uuid,
                interval_start,
                AVG(CAST(online AS FLOAT)) * 100 as interval_avg
            FROM time_grouped_data
            GROUP BY guild_uuid, interval_start
        ),
        guild_averages AS (
            SELECT 
                g.name || ' (' || COALESCE(g.prefix, '') || ')' as guild_name,
                AVG(ia.interval_avg) as activity_percentage
            FROM interval_averages ia
            JOIN guilds g ON ia.guild_uuid = g.uuid
            GROUP BY g.uuid
            HAVING activity_percentage > 0
        )
        SELECT 
            guild_name,
            ROUND(activity_percentage, 2) as activity_percentage,
            RANK() OVER (ORDER BY activity_percentage DESC) as rank
        FROM guild_averages
        ORDER BY activity_percentage DESC
        LIMIT 10;
    """)
    results = cursor.fetchall()
    conn.close()
    if not results:
        return None
        
    leaderboard_description = "```\n{:<3} {:<25} {:<10}\n".format("#", "Guild", "Activity %")
    leaderboard_description += "-" * 40 + "\n"
    for guild_data in results:
        guild_name, activity, rank = guild_data
        leaderboard_description += "{:<3} {:<25} {:<10.2f}\n".format(
            rank, guild_name, activity
        )
    leaderboard_description += "```"
    

    embed = discord.Embed(
        title="Guild Activity Leaderboard (Last 24 Hours)",
        description=leaderboard_description,
        color=discord.Color.blue()
    )
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    return embed

def playerActivityPlaytime(player_uuid, name):
    logger.info(f"player_uuid: {player_uuid}, playerActivityPlaytime")
    conn = sqlite3.connect('database/player_activity.db')
    cursor = conn.cursor()

    cursor.execute("""
    WITH RECURSIVE dates(day) AS (
        SELECT DATE('now', '-13 days')
        UNION ALL
        SELECT DATE(day, '+1 day')
        FROM dates
        WHERE day < DATE('now')
    ),
    playtime_per_day AS (
        SELECT DATE(timestamp) AS day, 
            ROUND((MAX(playtime) - MIN(playtime)) * 60.0) AS playtime_minutes
        FROM users
        WHERE uuid = ?
        AND DATE(timestamp) >= DATE('now', '-14 days')
        GROUP BY DATE(timestamp)
    )
    SELECT d.day,
        COALESCE(p.playtime_minutes, 0) AS playtime_minutes
    FROM dates d
    LEFT JOIN playtime_per_day p ON d.day = p.day
    ORDER BY d.day;
    """, (player_uuid,))
    daily_data = cursor.fetchall()

    if not daily_data:
        conn.close()
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
    plt.close()
    
    file = discord.File(buf, filename='player_playtime_graph.png')
    embed = discord.Embed(
        title=f"Playtime Analysis for {name}",
        description=(
            f"Daily Average: {averageDailyPlaytime:.0f} min\n"
            f"Highest Day: {max(playtimeValues) if playtimeValues else 0} min\n"
            f"Lowest Day: {min(playtimeValues) if playtimeValues else 0} min\n"
        ),
        color=discord.Color.red()
    )
    embed.set_image(url="attachment://player_playtime_graph.png")
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    
    conn.close()
    buf.close()
    
    return file, embed

def playerActivityContributions(player_uuid, name):
    logger.info(f"player_uuid: {player_uuid}, playerActivityContributions")
    conn = sqlite3.connect('database/guild_activity.db')
    cursor = conn.cursor()

    # Fetch snapshots from the last 14 days
    cursor.execute("""
    SELECT timestamp, contribution
    FROM member_snapshots
    WHERE member_uuid = ?
    AND timestamp >= datetime('now', '-14 days')
    ORDER BY timestamp
    """, (player_uuid,))
    snapshots = cursor.fetchall()

    if not snapshots:
        conn.close()
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
    plt.close()
    
    file = discord.File(buf, filename='player_xp_graph.png')
    embed = discord.Embed(
        title=f"XP Gain for {name}",
        description=(
            f"Total XP (14 Days): {totalGained:,.0f} xp\n"
            f"Highest Day: {max(daily_gains) if daily_gains else 0} xp\n"
            f"Lowest Day: {min(daily_gains) if daily_gains else 0} xp"
        ),
        color=discord.Color.red()
    )
    embed.set_image(url="attachment://player_xp_graph.png")
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    
    conn.close()
    buf.close()
    return file, embed

def playerActivityDungeons(player_uuid, name):
    logger.info(f"player_uuid: {player_uuid}, playerActivityDungeons")
    conn = sqlite3.connect('database/player_activity.db')
    cursor = conn.cursor()

    cursor.execute("""
    SELECT u.timestamp, u.totalDungeons
    FROM users_global u
    WHERE u.uuid = ?
        AND u.timestamp >= DATETIME('now', '-7 days')
    ORDER BY u.timestamp;
    """, (player_uuid,))
    snapshots = cursor.fetchall()

    if not snapshots:
        conn.close()
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
    plt.close()

    file = discord.File(buf, filename='player_dungeon_graph.png')
    embed = discord.Embed(
        title=f"Dungeon Runs for {name}",
        description=(
            f"Total Dungeons: {highestTotal} dungeons\n"
            f"Highest Gain in One Day: {highestGain} dungeons\n"
        ),
        color=discord.Color.red()
    )
    embed.set_image(url="attachment://player_dungeon_graph.png")
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")

    conn.close()
    buf.close()
    return file, embed

def playerActivityTotalDungeons(player_uuid, name):
    logger.info(f"player_uuid: {player_uuid}, playerActivityTotalDungeons")
    conn = sqlite3.connect('database/player_activity.db')
    cursor = conn.cursor()

    cursor.execute("""
        SELECT dungeonsDict
        FROM users_global
        WHERE uuid = ?
        ORDER BY timestamp DESC
        LIMIT 1
    """, (player_uuid,))
    snapshots = cursor.fetchall()

    if not snapshots:
        conn.close()
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
    plt.close

    file = discord.File(buf, filename='player_dungeon_pie.png')
    embed = discord.Embed(
        title=f"Dungeon pie chart for {name}",
        color=discord.Color.red()
    )
    embed.set_image(url="attachment://player_dungeon_pie.png")
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    buf.close()
    return file, embed

def playerActivityRaids(player_uuid, name):
    logger.info(f"player_uuid: {player_uuid}, playerActivityRaids")
    conn = sqlite3.connect('database/player_activity.db')
    cursor = conn.cursor()

    cursor.execute("""
    SELECT u.timestamp, u.totalRaids
    FROM users_global u
    WHERE u.uuid = ?
        AND u.timestamp >= DATETIME('now', '-7 days')
    ORDER BY u.timestamp;
    """, (player_uuid,))
    snapshots = cursor.fetchall()

    if not snapshots:
        conn.close()
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
    plt.close()

    file = discord.File(buf, filename='player_raid_graph.png')
    embed = discord.Embed(
        title=f"Raid Runs for {name}",
        description=(
            f"Total Raids: {highestTotal} raids\n"
            f"Highest Gain in One Day: {highestGain} raids\n"
        ),
        color=discord.Color.red()
    )
    embed.set_image(url="attachment://player_raid_graph.png")
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")

    conn.close()
    buf.close()
    return file, embed

def playerActivityTotalRaids(player_uuid, name):
    logger.info(f"player_uuid: {player_uuid}, playerActivityTotalRaids")
    conn = sqlite3.connect('database/player_activity.db')
    cursor = conn.cursor()

    cursor.execute("""
        SELECT raidsDict
        FROM users_global
        WHERE uuid = ?
        ORDER BY timestamp DESC
        LIMIT 1
    """, (player_uuid,))
    snapshots = cursor.fetchall()

    if not snapshots:
        conn.close()
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
    plt.close

    file = discord.File(buf, filename='player_raid_pie.png')
    embed = discord.Embed(
        title=f"Raid pie chart for {name}",
        color=discord.Color.red()
    )
    embed.set_image(url="attachment://player_raid_pie.png")
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    buf.close()
    return file, embed

def playerActivityMobsKilled(player_uuid, name):
    logger.info(f"player_uuid: {player_uuid}, playerActivityMobsKilled")
    conn = sqlite3.connect('database/player_activity.db')
    cursor = conn.cursor()

    cursor.execute("""
    SELECT u.timestamp, u.killedMobs
    FROM users_global u
    WHERE u.uuid = ?
        AND u.timestamp >= DATETIME('now', '-7 days')
    ORDER BY u.timestamp;
    """, (player_uuid,))
    snapshots = cursor.fetchall()

    if not snapshots:
        conn.close()
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
    plt.close()

    file = discord.File(buf, filename='player_mob_kill_graph.png')
    embed = discord.Embed(
        title=f"Mob's killed for {name}",
        description=(
            f"Total Kills: {highestTotal} kills\n"
            f"Highest Gain in One Day: {highestGain} kills\n"
        ),
        color=discord.Color.red()
    )
    embed.set_image(url="attachment://player_mob_kill_graph.png")
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")

    conn.close()
    buf.close()
    return file, embed

def playerActivityWars(player_uuid, name):
    logger.info(f"player_uuid: {player_uuid}, playerActivityWars")
    conn = sqlite3.connect('database/player_activity.db')
    cursor = conn.cursor()

    cursor.execute("""
    SELECT u.timestamp, u.wars
    FROM users_global u
    WHERE u.uuid = ?
        AND u.timestamp >= DATETIME('now', '-7 days')
    ORDER BY u.timestamp;
    """, (player_uuid,))
    snapshots = cursor.fetchall()

    if not snapshots:
        conn.close()
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
    plt.close()

    file = discord.File(buf, filename='player_wars_graph.png')
    embed = discord.Embed(
        title=f"War Count for {name}",
        description=(
            f"Total Wars: {highestTotal} wars\n"
            f"Highest Gain in One Day: {highestGain} wars\n"
        ),
        color=discord.Color.red()
    )
    embed.set_image(url="attachment://player_wars_graph.png")
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")

    conn.close()
    buf.close()
    return file, embed

def playerLeaderboardRaids():
    logger.info(f"playerLeaderboardRaids")
    conn = sqlite3.connect('database/player_activity.db')
    cursor = conn.cursor()

    cursor.execute("""
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
    LIMIT 10;
    """)
    snapshots = cursor.fetchall()
    if not snapshots:
        conn.close()

    leaderboard_description = "```\n{:<3} {:<25} {:<10}\n".format("#", "Username", "Raids")
    separator = "-" * 40 + "\n"
    rankNum = 0
    leaderboard_description += separator
    for row in snapshots:
        rankNum+=1
        username = row[0]
        totalRaids = row[1]
        rank = rankNum
        leaderboard_description += "{:<3} {:<25} {:<10}\n".format(rank, username, totalRaids)
    leaderboard_description += "```"

    embed = discord.Embed(
        title="Player Raid Leaderboard",
        description=leaderboard_description,
        color=discord.Color.blue()
    )
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    conn.close()
    return embed

def playerLeaderboardDungeons():
    logger.info(f"playerLeaderboardDungeons")
    conn = sqlite3.connect('database/player_activity.db')
    cursor = conn.cursor()

    cursor.execute("""
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
    LIMIT 10;
    """)
    snapshots = cursor.fetchall()
    if not snapshots:
        conn.close()

    leaderboard_description = "```\n{:<3} {:<25} {:<10}\n".format("#", "Username", "Dungeons")
    separator = "-" * 40 + "\n"
    rankNum = 0
    leaderboard_description += separator
    for row in snapshots:
        rankNum+=1
        username = row[0]
        totalDungeons = row[1]
        rank = rankNum
        leaderboard_description += "{:<3} {:<25} {:<10}\n".format(rank, username, totalDungeons)
    leaderboard_description += "```"

    embed = discord.Embed(
        title="Player Dungeon Leaderboard",
        description=leaderboard_description,
        color=discord.Color.blue()
    )
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    conn.close()
    return embed

def playerLeaderboardPVPKills():
    logger.info(f"playerLeaderboardPVPKills")
    conn = sqlite3.connect('database/player_activity.db')
    cursor = conn.cursor()

    cursor.execute("""
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
    LIMIT 10;
    """)
    snapshots = cursor.fetchall()
    if not snapshots:
        conn.close()

    leaderboard_description = "```\n{:<3} {:<25} {:<10}\n".format("#", "Username", "PVP Kills")
    separator = "-" * 40 + "\n"
    rankNum = 0
    leaderboard_description += separator
    for row in snapshots:
        rankNum+=1
        username = row[0]
        kills = row[1]
        rank = rankNum
        leaderboard_description += "{:<3} {:<25} {:<10}\n".format(rank, username, kills)
    leaderboard_description += "```"

    embed = discord.Embed(
        title="Player PVP Kills Leaderboard",
        description=leaderboard_description,
        color=discord.Color.blue()
    )
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    conn.close()
    return embed

def playerLeaderboardTotalLevel():
    logger.info(f"playerLeaderboardTotalLevel")
    conn = sqlite3.connect('database/player_activity.db')
    cursor = conn.cursor()

    cursor.execute("""
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
    LIMIT 10;
    """)
    snapshots = cursor.fetchall()
    if not snapshots:
        conn.close()

    leaderboard_description = "```\n{:<3} {:<25} {:<10}\n".format("#", "Username", "Total Level")
    separator = "-" * 40 + "\n"
    rankNum = 0
    leaderboard_description += separator
    for row in snapshots:
        rankNum+=1
        username = row[0]
        totalLevel = row[1]
        rank = rankNum
        leaderboard_description += "{:<3} {:<25} {:<10}\n".format(rank, username, totalLevel)
    leaderboard_description += "```"

    embed = discord.Embed(
        title="Player Total Level Leaderboard",
        description=leaderboard_description,
        color=discord.Color.blue()
    )
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    conn.close()
    return embed

def playerLeaderboardPlaytime():
    logger.info(f"playerLeaderboardPlaytime")
    conn = sqlite3.connect('database/player_activity.db')
    cursor = conn.cursor()

    cursor.execute("""
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
    LIMIT 10;
    """)
    snapshots = cursor.fetchall()
    if not snapshots:
        conn.close()

    leaderboard_description = "```\n{:<3} {:<25} {:<10}\n".format("#", "Username", "Playtime (Hours)")
    separator = "-" * 46 + "\n"
    rankNum = 0
    leaderboard_description += separator
    for row in snapshots:
        rankNum+=1
        username = row[0]
        playtime = row[1]
        rank = rankNum
        leaderboard_description += "{:<3} {:<25} {:<10}\n".format(rank, username, round(playtime))
    leaderboard_description += "```"

    embed = discord.Embed(
        title="Player Playtime Leaderboard",
        description=leaderboard_description,
        color=discord.Color.blue()
    )
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    conn.close()
    return embed

def rollGiveaway(weeklyNames, rollcount):
    logger.info(f"Starting rollGiveaway with {len(weeklyNames)} players and {rollcount} rolls")
    
    conn = sqlite3.connect('database/guild_activity.db')
    cursor = conn.cursor()
    weeklyNames = list(weeklyNames)

    placeholders = ','.join(['?' for _ in weeklyNames])
    cursor.execute(f"""
        SELECT name, uuid 
        FROM members 
        WHERE name IN ({placeholders})
    """, weeklyNames)
    uuid_map = dict(cursor.fetchall())

    uuid_list = list(uuid_map.values())
    if not uuid_list:
        logger.warning("No valid UUIDs found for the provided player names")
        conn.close()
        return {}, []
        
    placeholders = ','.join(['?' for _ in uuid_list])
    cursor.execute(f"""
        SELECT member_uuid, timestamp, online, contribution
        FROM member_snapshots
        WHERE member_uuid IN ({placeholders})
        AND timestamp >= datetime('now', '-7 days')
        ORDER BY member_uuid, timestamp
    """, uuid_list)

    player_snapshots = defaultdict(list)
    for row in cursor.fetchall():
        player_snapshots[row[0]].append(row)
    
    chances = {}
    tickets = {}
    total_tickets = 0
    
    for player_name in weeklyNames:
        if player_name not in uuid_map:
            logger.warning(f"Player {player_name} not found in database") # shouldnt happen but whatnot
            continue
            
        player_uuid = uuid_map[player_name]
        snapshots = player_snapshots[player_uuid]
        
        if not snapshots:
            logger.warning(f"No snapshots found for player {player_name}")
            tickets[player_name] = 1 # they still get their completition tickets
            # TODO: Fix this code to get their chances
            continue
            
        # Calculate playtime
        playtimeMinutes = defaultdict(float)
        for i in range(1, len(snapshots)):
            if snapshots[i][2] == 1:  # online status
                curr_time = datetime.strptime(snapshots[i][1], '%Y-%m-%d %H:%M:%S')
                prev_time = datetime.strptime(snapshots[i-1][1], '%Y-%m-%d %H:%M:%S')
                minutes = (curr_time - prev_time).total_seconds() / 60
                playtimeMinutes[curr_time.date()] += minutes
        
        avgDailyPlaytime = sum(playtimeMinutes.values()) / 7 if playtimeMinutes else 0

        playtime_thresholds = [ # Tickets per minute ex. 300 mins playtime per day is 1, 60 is 0.5
            (300, 1.0), (240, 0.9), (120, 0.8), (90, 0.7),
            (75, 0.6), (60, 0.5), (45, 0.4), (30, 0.3),
            (20, 0.2), (10, 0.1), (0, 0.0)
        ]
        
        for threshold, ticket in playtime_thresholds: # goes down the list and sees if they qualify if they do break
            if avgDailyPlaytime >= threshold:
                playtimeTickets = ticket
                break

        weeklyXP = snapshots[-1][3] - snapshots[0][3] if len(snapshots) > 1 else 0

        xp_thresholds = [ # Tickets per xp ex. 50m xp contri the week is 1, 1.25m is 0.5
            (50_000_000, 1.0), (25_000_000, 0.9), (15_000_000, 0.8),
            (10_000_000, 0.7), (3_000_000, 0.6), (1_250_000, 0.5),
            (750_000, 0.4), (300_000, 0.3), (150_000, 0.2),
            (50_000, 0.1), (0, 0.0)
        ]
        
        for threshold, ticket in xp_thresholds: # goes down the list and sees if they qualify if they do break
            if weeklyXP >= threshold:
                xpTickets = ticket
                break

        completion_tickets = 1.0 # Base ticket count, everyone will have 1 ticket because they did weekly
        total_player_tickets = completion_tickets + playtimeTickets + xpTickets
        logger.info(f"total_player_tickets: {total_player_tickets}")
        total_tickets += total_player_tickets
        chances[player_name] = total_player_tickets
        tickets[player_name] = total_player_tickets
        logger.info(f"tickets[player_name]: {tickets[player_name]}")
        
        logger.info(f"Player {player_name} processing completed")
        logger.info(
            f"Player Stats - "
            f"Average Daily Playtime: {avgDailyPlaytime:.1f} minutes, "
            f"Weekly XP: {weeklyXP:,}"
        )
        logger.info(
            f"Tickets breakdown - "
            f"Completion: {completion_tickets}, "
            f"Playtime: {playtimeTickets} (from {avgDailyPlaytime:.1f} min/day), "
            f"XP: {xpTickets} (from {weeklyXP:,} XP)"
        )
    
    if total_tickets > 0: # redardancy and whjatnot
        chances = {name: (tickets[name]/total_tickets) * 100 for name in chances}
    
    eligible_players = [name for name, tickets in tickets.items() if tickets > 0]
    rollcount = min(rollcount, len(eligible_players))
    
    if rollcount == 0:
        logger.warning("No eligible players or rolls requested")
        conn.close()
        return chances, []

    names = []
    weights = []
    for name in eligible_players:
        names.append(name)
        weights.append(tickets[name])

    total_weight = sum(weights)
    if total_weight > 0:
        weights = [w/total_weight for w in weights]
    winners = []
    remaining_names = names.copy()
    remaining_weights = weights.copy()
    
    for _ in range(rollcount):
        if not remaining_names: # No one to pick from
            break
        cum_weights = []
        curr_sum = 0
        for w in remaining_weights:
            curr_sum += w
            cum_weights.append(curr_sum)
        winner_idx = bisect.bisect(cum_weights, random.random())
        winner = remaining_names[winner_idx]
        winners.append(winner)
        
        del remaining_names[winner_idx]
        del remaining_weights[winner_idx]
        
        if remaining_weights:
            weight_sum = sum(remaining_weights)
            remaining_weights = [w/weight_sum for w in remaining_weights]
    
    conn.close()
    logger.info(f"Tickets: {tickets}") # For logging purposes, so we dont have to do the math
    logger.info(f"Chances: {chances}")
    logger.info(f"Winners: {winners}")
    return chances, winners

def mapCreator():
    map_img = Image.open("lib/documents/main-map.png").convert("RGBA")
    font = ImageFont.truetype("lib/documents/arial.ttf", 40)
    territoryCounts = defaultdict(int)
    namePrefixMap = {}

    def coordToPixel(x, z):
        return x + 2383, z + 6572 # if only wynntils was ACCURATE!!!

    with open("territories.json", "r") as f:
        local_territories = json.load(f)

    color_map = {
        g["prefix"]: g.get("color", "#FFFFFF")
        for g in requests.get("https://athena.wynntils.com/cache/get/guildList").json()
        if g.get("prefix")}

    # get territory data
    territory_data = requests.get("https://api.wynncraft.com/v3/guild/list/territory").json()

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
    file = discord.File(mapBytes, filename="wynn_map.png")
    embed = discord.Embed(
        title=f"Current Territory Map",
        color=discord.Color.green()
    )
    embed.set_image(url="attachment://wynn_map.png")
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    return file, embed

def heatmapCreator():
    map_img = Image.open("lib/documents/main-map.png").convert("RGBA")

    def coordToPixel(x, z):
        return x + 2383, z + 6572 # if only wynntils was ACCURATE!!!

    with shelve.open(territoryFilePath) as territoryStorage:
        historicalTerritories = territoryStorage.get("historicalTerritories", {})

    territory_data = requests.get("https://api.wynncraft.com/v3/guild/list/territory").json()
    activityCount = defaultdict(int)
    for day in historicalTerritories.values():
        for territory, count in day.items():
            activityCount[territory] += count

    maxCount = max(activityCount.values(), default=1)

    def heatToColor(heat): # I'd like to make this better in the future
        heat = max(0.0, min(1.0, heat))
        r = int(255 * heat)
        g = 0
        b = int(255 * (1 - heat))
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
    file = discord.File(mapBytes, filename="wynn_heatmap.png")
    embed = discord.Embed(
        title=f"Current Territory Heatmap",
        color=discord.Color.green()
    )
    embed.set_image(url="attachment://wynn_heatmap.png")
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    return file, embed
