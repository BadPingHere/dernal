import discord
from datetime import datetime, timezone, timedelta
import json
from collections import Counter
import logging
import time
import sqlite3
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.dates import HourLocator, DateFormatter, AutoDateLocator
from collections import defaultdict
import io
import seaborn as sns
import bisect
import random
import ast
import os
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import shelve
import difflib
import matplotlib.cm as cm
from lib.makeRequest import makeRequest
from pathlib import Path
import re
  
logger = logging.getLogger('discord')

def getGraidDatabaseData(key):
    if key == "guild_raids":
        keyName = "guild_raids"
    elif key == "EligibleGuilds":
        keyName = "EligibleGuilds"

    graidFilePath = os.path.join(rootDir, 'database', 'graid.db')
    conn = sqlite3.connect(graidFilePath)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS graid_data (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """)
    cur.execute("SELECT value FROM graid_data WHERE key = ?", (keyName,))
    row = cur.fetchone()
    conn.close()
    if row:
        return json.loads(row[0])
    if key == "guild_raids":
        return {}
    elif key == "EligibleGuilds":
        return []
    return None

def writeGraidDatabaseData(key, data):
    graidFilePath = os.path.join(rootDir, 'database', 'graid.db')
    conn = sqlite3.connect(graidFilePath)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO graid_data (key, value) VALUES (?, ?)",
        (key, json.dumps(data))
    )
    conn.commit()
    conn.close()

cooldownHolder = {}
last_xp = {}  # {(guild_prefix, username): contributed}
rootDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
territoryFilePath = os.path.join(rootDir, 'database', 'territory')
confirmedGRaid = getGraidDatabaseData("guild_raids")

path = Path(__file__).resolve().parents[1] / '.env'
CONFIGDBPATH = Path(__file__).resolve().parents[1] / "database" / "config.db"
sns.set_style("whitegrid")
mpl.use('Agg') # Backend without any gui popping up
blue, = sns.color_palette("muted", 1)

timeframeMap1 = { # Used for heatmap data
    "Season 24": ("04/18/25", "06/01/25"),
    "Season 25": ("06/06/25", "07/20/25"),
    "Season 26": ("07/25/25", "09/14/25"),
    "Season 27": ("09/19/25", "12/25/25"), 
    "Last 7 Days": None, # gotta handle ts outta dict
    "Everything": None
}

timeframeMap2 = { # Used for graid data
    "Season 25": ("06/06/25", "07/20/25"),
    "Season 26": ("07/25/25", "09/14/25"),
    "Season 27": ("09/19/25", "12/25/25"), 
    "Last 14 Days": None, # gotta handle ts outta dict
    "Last 7 Days": None, # gotta handle ts outta dict
    "Last 24 Hours": None, # gotta handle ts outta dict
    "Everything": None
}

timeframeMap3 = { # Used for database data
    "Last 14 Days": None, # gotta handle ts outta dict
    "Last 7 Days": None, # gotta handle ts outta dict
    "Last 3 Days": None, # gotta handle ts outta dict
    "Last 24 Hours": None, # gotta handle ts outta dict
    "Everything": None # all data
}

    
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
    if userOrGuildID == 736028271153512489: # the owner (the lion) does not get a cooldown.
        return True
    lastUsed = cooldownHolder.get(userOrGuildID, 0)
    elapsed = now - lastUsed
    if elapsed < cooldownSeconds:
        remaining = round(cooldownSeconds - elapsed, 1)
        return remaining
    cooldownHolder[userOrGuildID] = now
    #logger.info(cooldownHolder)
    return True

def findAttackingMembers(attacker):
    if str(attacker) == "None":
        logger.error("Attacker None in findAttackingMembers.")
        return [["Unknown", "Unknown", 1738]] # ay
    success, r = makeRequest("https://api.wynncraft.com/v3/guild/prefix/"+str(attacker)) # Using nori api as main for less api usage + it shows online members easier
    if not success:
        logger.error("Unsuccessful request in findAttackingMembers - 1.")
        return [["Unknown", "Unknown", 1738]]
    jsonData = r.json()
    onlineMembers = []
    warringMembers = []
    onlineMembersButServersTooBecauseIDontWantToRewriteThisPartOfTheCodeToAccomdateTheNewDatabaseLookupPart = {}

    for rank in jsonData["members"]: # this iterates through every rank like chief, owner, etc
        if isinstance(jsonData["members"][rank], dict):
            for member in jsonData["members"][rank].values(): 
                if member['online']: # checks if online is set to true or false
                    onlineMembers.append(member['uuid'])
                    onlineMembersButServersTooBecauseIDontWantToRewriteThisPartOfTheCodeToAccomdateTheNewDatabaseLookupPart[member['uuid']] = member['server']
                    
    # Check if they are in database
    conn = sqlite3.connect('database/guild_activity.db')
    cursor = conn.cursor()
    cursor.execute("SELECT uuid FROM guilds WHERE prefix = ? COLLATE NOCASE", (str(attacker),))
    result = cursor.fetchone()
    conn.close()
    if not result: # Not in databse, manually check warcounts
        for i in onlineMembers:
            success, r = makeRequest("https://api.wynncraft.com/v3/player/"+str(i))
            if not success:
                logger.error("Unsuccessful request in findAttackingMembers - 2.")
                return [["Unknown", "Unknown", 1738]]
            json = r.json()
            #logger.info(f"json: {json}")
            if int(json.get("globalData", {}).get("wars", 0)) > 20: # arbitrary number, imo 20 or more means youre prolly a full-time warrer. also defaults to 21 because if you hide stats youre a sweat aka warrer
                warringMembers.append([json["username"], json['server'], int(json["globalData"]["wars"])])
    else: # In database, we can save resources
        conn = sqlite3.connect('database/player_activity.db')
        cursor = conn.cursor()
        placeholders = ','.join(['?' for _ in onlineMembers])
        query = f"""
        SELECT 
            u.username,
            u.uuid,
            u.wars
        FROM users_global u
        INNER JOIN (
            SELECT 
                uuid,
                MAX(timestamp) as latest_timestamp
            FROM users_global
            WHERE uuid IN ({placeholders})
            GROUP BY uuid
        ) latest ON u.uuid = latest.uuid AND u.timestamp = latest.latest_timestamp
        WHERE u.wars > 20;
        """

        cursor.execute(query, onlineMembers)
        result = cursor.fetchall()
        for username, uuid, wars in result:
            server = onlineMembersButServersTooBecauseIDontWantToRewriteThisPartOfTheCodeToAccomdateTheNewDatabaseLookupPart.get(uuid, 'Unknown')
            warringMembers.append([username, server, wars])

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

def checkterritories(untainteddata, untainteddataOLD, guildPrefix, pingRoleID, expectedterrcount, intervalForPing, timesinceping, guildID):
    gainedTerritories = {}
    lostTerritories = {}
    terrcount = {}
    messagesToSend = []
    key = (guildID, guildPrefix)
    
    if guildPrefix.lower() != 'global' and key not in expectedterrcount:
        expectedterrcount[key] = sum(
            1 for d in untainteddata.values() if d['guild']['prefix'] == guildPrefix
        )

    current = expectedterrcount.get(key, 0)
    
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
            if oldGuild != newGuild and newGuild and str(newGuild).lower() != "none": # This line has fucked me up 3-4
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
    terrcount[key] = expectedterrcount[key] # this is what will fix (40 -> 38)
    for t, data in lostTerritories.items():
        old_ts = datetime.fromisoformat(untainteddataOLD[t]['acquired'].replace('Z', '+00:00'))
        new_ts = datetime.fromisoformat(data['acquired'].replace('Z', '+00:00'))
        elapsed_time = int(new_ts.timestamp() - old_ts.timestamp())

        current -= 1

        opponent_before = sum(1 for d in untainteddataOLD.values() if d['guild']['prefix'] == data['guild']['prefix'])
        opponent_after = sum(1 for d in untainteddata.values() if d['guild']['prefix'] == data['guild']['prefix'])

        messagesToSend.append(sendEmbed(
                data['guild']['prefix'],
                guildPrefix,
                t,
                human_time_duration(elapsed_time),
                str(opponent_before),
                str(opponent_after),
                str(current + 1),
                str(current),
                guildPrefix,
                pingRoleID,
                intervalForPing,
                timesinceping,
                guildID,
            )
        )

    # GAINED territories
    for t, data in gainedTerritories.items():
        old_ts = datetime.fromisoformat(untainteddataOLD[t]['acquired'].replace('Z', '+00:00'))
        new_ts = datetime.fromisoformat(data['acquired'].replace('Z', '+00:00'))
        elapsed_time = int(new_ts.timestamp() - old_ts.timestamp())

        current += 1
        prev_owner = untainteddataOLD[t]['guild']['prefix']

        opponent_before = sum(1 for d in untainteddataOLD.values() if d['guild']['prefix'] == prev_owner)
        opponent_after = sum(1 for d in untainteddata.values() if d['guild']['prefix'] == prev_owner)

        messagesToSend.append(
            sendEmbed(
                guildPrefix,
                prev_owner,
                t,
                human_time_duration(elapsed_time),
                str(current - 1),
                str(current),
                str(opponent_before),
                str(opponent_after),
                guildPrefix,
                pingRoleID,
                intervalForPing,
                timesinceping,
                guildID,
            )
        )

    # update the shared counter for next tick
    if guildPrefix.lower() != 'global':
        expectedterrcount[key] = current


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
            if ownerOfTerritory.lower() == guildPrefix.lower(): # if the guild owns it, add to the dict
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
    #logger.info(f"scorelist: {scorelist}")
    listy = []
    for i, (location, score) in enumerate(scorelist.items()):
        if i >= 100:  # max 100 entries
            break
        connCount, externalCount = otherData[location] #like balatro!!!!! # how was this like balatro, my comments amaze me
        listy.append([location, f"{score}% - Conns: {connCount}, Exts: {externalCount}"])
    logger.info(f"Ran HQ lookup successfully for {guildPrefix if guildPrefix else 'global map'}.")
    return listy

def lookupUser(memberList, progressCallback=None):
    inactivityDict = {
        "Four Week Inactive Users": [],
        "Three Week Inactive Users": [],
        "Two Week Inactive Users": [],
        "One Week Inactive Users": [],
        "Three Day Inactive Users": [],
        "Active Users": [],
    }
    totalMembers = len(memberList)


    for i, member in enumerate(memberList):
        if progressCallback:
            progressCallback(i + 1, totalMembers)

        time.sleep(0.5) # Slow down inactivity because we need to preserve our ratelimits
        success, r = makeRequest("https://api.wynncraft.com/v3/player/"+str(member))
        #logger.info(f"username: {member}")
        if not success:
            logger.error("Unsuccessful request in lookupUser.")
            continue # i think thisll work
        jsonData = r.json()
        lastJoinDate = jsonData["lastJoin"]
        if not jsonData or jsonData.get("lastJoin") is None:
            joinTime = datetime(1960, 1, 1, tzinfo=timezone.utc) #pricks have their shit turned off
        else:
            lastJoinDate = jsonData["lastJoin"]
            try:
                joinTime = datetime.strptime(lastJoinDate, "%Y-%m-%dT%H:%M:%S.%fZ")
            except ValueError:
                joinTime = datetime.strptime(lastJoinDate, "%Y-%m-%dT%H:%M:%SZ")
            joinTime = joinTime.replace(tzinfo=timezone.utc)
        currentTime = datetime.now(timezone.utc)
        timeDifference = int((currentTime - joinTime).total_seconds())

        epochTime = joinTime.timestamp()

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

def lookupGuild(r, progressCallback=None):
    jsonData = r.json()
    memberList = []
    for rank in jsonData["members"]: # this iterates through every rank like chief, owner, etc
        if isinstance(jsonData["members"][rank], dict): # checks if it has a rank i think so it knows people from non arrrays??
            for member, value in jsonData["members"][rank].items(): 
                memberList.append(value['uuid']) # we use uuid because name changes fuck up username lookups
    #logger.info(f"memberlist-2: {memberList}")
    return lookupUser(memberList, progressCallback)

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
        SELECT timestamp, online_members
        FROM guild_snapshots
        WHERE guild_uuid = ?
        AND timestamp >= datetime('now', '-3 day')
        ORDER BY timestamp
    """, (guild_uuid,))
    snapshots = cursor.fetchall()
    
    if not snapshots:
        conn.close()
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
        AND timestamp >= datetime('now', '-7 day')
        ORDER BY timestamp
    """, (guild_uuid,))
    snapshots = cursor.fetchall()
    if not snapshots:
        conn.close()
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
    plt.close()

    file = discord.File(buf, filename='total_members_graph.png')
    embed = discord.Embed(
        title=f" Members Analysis for {name}",
        description=(
            f"Maximum Member Count: {max(total_numbers):.0f}\n"
            f"Minimum Member Count: {min(total_numbers):.0f}\n"
            f"Average Member Count: {overall_total:.0f}"
        ),
        color=discord.Color.blue()
    )
    embed.set_image(url="attachment://total_members_graph.png")
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    
    conn.close()
    buf.close()
    return file, embed

def guildLeaderboardOnlineMembers(timeframe):
    logger.info(f"leaderboardOnlineMembers timeframe: {timeframe}")
    conn = sqlite3.connect('database/guild_activity.db')
    cursor = conn.cursor()

    if timeframe == "Last 14 Days":
        endDate = datetime.now()
        startDate = endDate - timedelta(days=14)
    elif timeframe == "Last 7 Days":
        endDate = datetime.now()
        startDate = endDate - timedelta(days=7)
    elif timeframe == "Last 3 Days":
        endDate = datetime.now()
        startDate = endDate - timedelta(days=3)
    elif timeframe == "Last 24 Hours":
        endDate = datetime.now()
        startDate = endDate - timedelta(hours=24)
    elif timeframe == "Everything":
        startDate = None
        endDate = None
    else: # fallback area
        startDate = None
        endDate = None

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

    cursor.execute(query, params)
    snapshots = cursor.fetchall()
    conn.close()
    listy = []
    for row in snapshots:
        listy.append([row[0], row[1]]) #name, value

    return listy

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
    LIMIT 100;
    """)
    snapshots = cursor.fetchall()
    if not snapshots:
        conn.close()
    
    listy = []
    for row in snapshots:
        listy.append([row[0], row[1]]) #name, value

    return listy

def guildLeaderboardWars(timeframe):
    logger.info(f"leaderboardWars timeframe: {timeframe}")
    conn = sqlite3.connect('database/guild_activity.db')
    cursor = conn.cursor()

    if timeframe == "Last 14 Days":
        endDate = datetime.now()
        startDate = endDate - timedelta(days=14)
    elif timeframe == "Last 7 Days":
        endDate = datetime.now()
        startDate = endDate - timedelta(days=7)
    elif timeframe == "Last 3 Days":
        endDate = datetime.now()
        startDate = endDate - timedelta(days=3)
    elif timeframe == "Last 24 Hours":
        endDate = datetime.now()
        startDate = endDate - timedelta(hours=24)
    elif timeframe == "Everything":
        startDate = None
        endDate = None
    else: # fallback
        startDate = None
        endDate = None

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

    cursor.execute(query, params)
    snapshots = cursor.fetchall()
    conn.close()

    if not snapshots:
        return None

    listy = []
    for row in snapshots:
        listy.append([row[0], row[1]]) #name, value

    return listy

def guildLeaderboardXP(timeframe):
    logger.info(f"leaderboardXP timeframe: {timeframe}")
    conn = sqlite3.connect('database/guild_activity.db')
    cursor = conn.cursor()
    if timeframe == "Last 14 Days":
        endDate = datetime.now()
        startDate = endDate - timedelta(days=14)
    elif timeframe == "Last 7 Days":
        endDate = datetime.now()
        startDate = endDate - timedelta(days=7)
    elif timeframe == "Last 3 Days":
        endDate = datetime.now()
        startDate = endDate - timedelta(days=3)
    elif timeframe == "Last 24 Hours":
        endDate = datetime.now()
        startDate = endDate - timedelta(hours=24)
    elif timeframe == "Everything":
        startDate = None
        endDate = None
    else:  # fallback
        startDate = None
        endDate = None

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

    cursor.execute(query, params)
    snapshots = cursor.fetchall()
    conn.close()

    if not snapshots:
        return None

    listy = []
    for row in snapshots:
        listy.append([row[0], row[1]]) #name, value

    return listy

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

def playerLeaderboardRaids(timeframe):
    logger.info(f"playerLeaderboardRaids timeframe: {timeframe}")
    conn = sqlite3.connect('database/player_activity.db')
    cursor = conn.cursor()

    # Handle timeframe logic
    if timeframe == "Last 14 Days":
        endDate = datetime.now()
        startDate = endDate - timedelta(days=14)
    elif timeframe == "Last 7 Days":
        endDate = datetime.now()
        startDate = endDate - timedelta(days=7)
    elif timeframe == "Last 3 Days":
        endDate = datetime.now()
        startDate = endDate - timedelta(days=3)
    elif timeframe == "Last 24 Hours":
        endDate = datetime.now()
        startDate = endDate - timedelta(hours=24)
    elif timeframe == "Everything":
        startDate = None
        endDate = None
    else:  # fallback
        startDate = None
        endDate = None

    # Build the query with timeframe filtering
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

    cursor.execute(query, params)
    snapshots = cursor.fetchall()
    if not snapshots:
        conn.close()

    listy = []
    for row in snapshots:
        listy.append([row[0], row[1]]) #name, value

    return listy

def playerLeaderboardDungeons(timeframe):
    logger.info(f"playerLeaderboardDungeons timeframe: {timeframe}")
    conn = sqlite3.connect('database/player_activity.db')
    cursor = conn.cursor()

    # Handle timeframe logic
    if timeframe == "Last 14 Days":
        endDate = datetime.now()
        startDate = endDate - timedelta(days=14)
    elif timeframe == "Last 7 Days":
        endDate = datetime.now()
        startDate = endDate - timedelta(days=7)
    elif timeframe == "Last 3 Days":
        endDate = datetime.now()
        startDate = endDate - timedelta(days=3)
    elif timeframe == "Last 24 Hours":
        endDate = datetime.now()
        startDate = endDate - timedelta(hours=24)
    elif timeframe == "Everything":
        startDate = None
        endDate = None
    else:  # fallback
        startDate = None
        endDate = None

    # Build the query with timeframe filtering
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

    cursor.execute(query, params)
    snapshots = cursor.fetchall()
    if not snapshots:
        conn.close()

    listy = []
    for row in snapshots:
        listy.append([row[0], row[1]]) #name, value

    return listy

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
    LIMIT 100;
    """)
    snapshots = cursor.fetchall()
    if not snapshots:
        conn.close()

    listy = []
    for row in snapshots:
        listy.append([row[0], row[1]]) #name, value

    return listy

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
    LIMIT 100;
    """)
    snapshots = cursor.fetchall()
    if not snapshots:
        conn.close()

    listy = []
    for row in snapshots:
        listy.append([row[0], row[1]]) #name, value

    return listy

def playerLeaderboardPlaytime(timeframe):
    logger.info(f"playerLeaderboardPlaytime, timeframe: {timeframe}")
    conn = sqlite3.connect('database/player_activity.db')
    cursor = conn.cursor()

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

    if startDate is None:
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
        cursor.execute(query, [
            startDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            endDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        ])

    snapshots = cursor.fetchall()
    conn.close()

    if not snapshots:
        return None

    listy = []
    for row in snapshots:
        listy.append([row[0], row[1]]) 

    return listy

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
        #logger.info(f"total_player_tickets: {total_player_tickets}")
        total_tickets += total_player_tickets
        chances[player_name] = total_player_tickets
        tickets[player_name] = total_player_tickets
        #logger.info(f"tickets[player_name]: {tickets[player_name]}")
        
        #logger.info(f"Player {player_name} processing completed")
        #logger.info(f"Player Stats - "f"Average Daily Playtime: {avgDailyPlaytime:.1f} minutes, "f"Weekly XP: {weeklyXP:,}")
        #logger.info(f"Tickets breakdown - "f"Completion: {completion_tickets}, "f"Playtime: {playtimeTickets} (from {avgDailyPlaytime:.1f} min/day), "f"XP: {xpTickets} (from {weeklyXP:,} XP)")
    
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
    success, r = makeRequest("https://athena.wynntils.com/cache/get/guildList")
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
    file = discord.File(mapBytes, filename="wynn_map.png")
    embed = discord.Embed(
        title=f"Current Territory Map",
        color=discord.Color.green()
    )
    embed.set_image(url="attachment://wynn_map.png")
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    return file, embed

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
    file = discord.File(mapBytes, filename="wynn_heatmap.png")
    embed = discord.Embed(
        title=f"Heatmap for {timeframe}",
        color=discord.Color.green()
    )
    embed.set_image(url="attachment://wynn_heatmap.png")
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    return file, embed

def getHelp(arg):
    if not arg: # No arg, we can send out some basic shit
        commands = {
            "detector" : {
                "add": "Add a guild to detect.",
                "remove": "Remove a guild from being detected."
            },
            "giveaway" : {
                "configure": "Configure the giveaway system with a guild prefix.",
                "roll": "Roll for winners from the configured guild."
            },
            "guild" : {
                "activity": {
                    "xp": "Shows a bar graph displaying the total xp a guild has every day, for the past 2 weeks.",
                    "territories": "Shows a graph displaying the amount of territories a guild has for the past 7 days.",
                    "wars": "Shows a graph displaying the total amount of wars a guild has done over the past 7 days.",
                    "total_members": "Shows a graph displaying the total members a guild has for the past 7 days.",
                    "online_members": "Shows a graph displaying the average amount of online members a guild has for the past 3 days.",
                    "guild_raids": "Shows a graph displaying the amount of guild raids completed in the past 14 days.",
                },
                "leaderboard": {
                    "online_members": "Shows a leaderboard of the top 100 guild's average amount of online players.",
                    "total_members": "Shows a leaderboard of the top 100 guild's total members.",
                    "wars": "Shows a leaderboard of the top 100 guild's war amount.",
                    "xp": "Shows a leaderboard of the top 100 guild's xp gained over the past 24 hours.",
                    "guild_raids": "Shows a leaderboard of the level 100+ guild's guild raids for the past 14 days.",
                },
                "overview": "Configure the giveaway system with a guild prefix.",
                "inactivity": "Roll for winners from the configured guild."
            },
            "player" : {
                "activity": {
                    "playtime": "Shows the graph displaying the average amount of playtime every day over the past two weeks.",
                    "contribution": "Shows a graph displaying the amount of contributiond xp every day over the past two weeks.",
                    "dungeons": "Shows a graph displaying the amount of dungeons completed total every day for the past week.",
                    "dungeons_pie": "Shows a pie chart displaying the different dungeons's you have done.",
                    "raids": "Shows a graph displaying the amount of raids completed total every day for the past week.",
                    "raids_pie": "Shows a pie chart displaying the different raid's you have done.",
                    "mobs_killed": "Shows a graph displaying the amount of total mobs killed every day for the past week.",
                    "wars": "Shows a graph displaying the amount of total wars every day for the past week.",
                    "guild_raids": "Shows a graph with the amount of guild raids done over the past 2 weeks for supported players.",
                },
                "leaderboard": {
                    "raids": "Shows the leaderboard of the top 100 players with the highest total raids completed.",
                    "total_level": "Shows the leaderboard of the top 100 players with the highest total level.",
                    "dungeons": "Shows the leaderboard of the top 100 players with the highest dungeons completed.",
                    "playtime": "Shows the leaderboard of the top 100 players with the highest playtime in hours.",
                    "pvp_kills": "Shows the leaderboard of the top 100 players with the highest PvP Kills.",
                    "guild_raids": "Shows the leaderboard of the top 1000 players with the highest guild raids in the past 2 weeks.",
                },
            },
            "territory" : {
                "map": "Generates the current Wynncraft Territory Map.",
                "heatmap": "Generates the current Wynncraft Territory Heatmap."
            },
            "hq": "Outputs the top hq locations.",
            "help": "Provides help and info on commands.",
        }
        embed = discord.Embed(
            title="Help",
            color=0x016610E
        )
        
        fieldsToAdd = []
        
        for key in commands:
            if isinstance(commands[key], dict): # We check if there is more
                for subkey in commands[key]:
                    if isinstance(commands[key][subkey], dict): # multiple subcommands like guild leaderboard etc
                        fieldValue = ""
                        for subsubkey in commands[key][subkey]:
                            fieldValue += f"`{subsubkey}` - {commands[key][subkey][subsubkey]}\n"
                    
                        fieldName = f"🔵 {key.title()} {subkey.title()}"
                        fieldsToAdd.append((fieldName, fieldValue, True))
                    else: # sum like hq
                        if not any(isinstance(v, dict) for v in commands[key].values()):
                            # If this is the first non-dict item in this category, create the field
                            fieldValue = ""
                            for sk, sv in commands[key].items():
                                if not isinstance(sv, dict):
                                    fieldValue += f"`{sk}` - {sv}\n"
                            
                            fieldName = f"🟠 {key.title()}"
                            fieldsToAdd.append((fieldName, fieldValue, True))
                            break  # Only add this field once per category
            else:
                # This is just for commands like hq
                fieldsToAdd.append((f"🔴 {key.title()}", f"`{key}` - {commands[key]}", True))
        
        # Sort fields by length (longest first)
        fieldsToAdd.sort(key=lambda x: len(x[1]), reverse=True)
        
        # add to embed
        for fieldName, fieldValue, inline in fieldsToAdd:
            embed.add_field(name=fieldName, value=fieldValue, inline=inline)
        
        embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
        return embed, True
    else:
        commands = {
            "detector add": {
                "desc": "Add a guild to detect using Detector, a feature to 'detect' when a guild loses or gains territory. Allows for global detection aswell. Requires the 'Detector Permission' role for users to use this command.", 
                "usage": "detector add [channel] [guild_prefix] [role - optional] [interval - optional]",
                "options": {
                    "channel": "The channel you want the detector messages to be sent to.",
                    "guild_prefix": "The guild prefix you want to track, or 'Global' for global detection.",
                    "role": "The role you want pinged on territory loss. (optional)",
                    "interval": "The number of minutes cooldown between pings, if you are using role. (optional)"
                }
            },
            "detector remove": {
                "desc": "Removes a configuration from Detector. Requires the 'Detector Permission' role for users to use this command.", 
                "usage": "detector remove [prefix]",
                "options": {
                    "prefix": "The configuration of Detector you want to remove.",
                }
            },
            "giveaway configure": {
                "desc": "Add a guild configuration to use for Giveaways. Requires the 'Giveaway Permission' role for users to use this command.", 
                "usage": "giveaway configure [prefix]",
                "options": {
                    "prefix": "The guild prefix you want to add.",
                }
            },
            "giveaway roll": {
                "desc": "Rolls a giveaway with a predetermined amount of winners, using user's playtime and xp contributions as more 'tickets' in a raffle. Requires the 'Giveaway Permission' role for users to use this command.", 
                "usage": "giveaway roll [winners]",
                "options": {
                    "winners": "The amount of winners for the giveaway.",
                }
            },
            "guild activity xp": {
                "desc": "Shows a bar graph of the guild's total XP contributed each day for the past 2 weeks.",
                "usage": "guild activity xp [name]",
                "options": {
                    "name": "The name or prefix of the guild to check.",
                }
            },
            "guild activity territories": {
                "desc": "Displays a graph showing the number of territories owned by the guild over the past 7 days.",
                "usage": "guild activity territories [name]",
                "options": {
                    "name": "The name or prefix of the guild to check.",
                }
            },
            "guild activity wars": {
                "desc": "Shows a graph of the total wars the guild has done, with 7 days of history.", # The wording here sounds dumb but i didnt want it to sound like it wasnt TOTAL
                "usage": "guild activity wars [name]",
                "options": {
                    "name": "The name or prefix of the guild to check.",
                }
            },
            "guild activity members": {
                "desc": "Displays a graph showing the member count of the guild over the past 7 days.",
                "usage": "guild activity members [name]",
                "options": {
                    "name": "The name or prefix of the guild to check.",
                }
            },
            "guild activity online_members": {
                "desc": "Shows a graph of the average number of online members over the past 3 days.",
                "usage": "guild activity online_members [name]",
                "options": {
                    "name": "The name or prefix of the guild to check.",
                }
            },
            "guild activity guild_wars": {
                "desc": "Shows a graph displaying the amount of guild raids completed in the past 14 days.",
                "usage": "guild activity guild_wars [name]",
                "options": {
                    "name": "Prefix of the guild search Ex: TAq, Calvish.",
                }
            },
            "guild leaderboard guild_wars": {
                "desc": "Shows a leaderboard of the level 100+ guild's guild raids for the past 14 days.",
                "usage": "guild leaderboard guild_wars",
                "options": {
                    "name": "Prefix of the guild Ex: TAq, SEQ. Shows data for the past 14 days. (optional)",
                }
            },
            "guild leaderboard online_members": {
                "desc": "Displays a leaderboard of the top 100 guilds by average online member count.",
                "usage": "guild leaderboard online_members",
                "options": {
                    "name": "Prefix or Name of the guild Ex: TAq, Calvish. Shows data for the past 7 days.",
                }
            },
            "guild leaderboard members": {
                "desc": "Shows a leaderboard of the top 100 guilds by member count.",
                "usage": "guild leaderboard members",
                "options": {}
            },
            "guild leaderboard wars": {
                "desc": "Displays a leaderboard of the top 100 guilds by total war count.",
                "usage": "guild leaderboard wars",
                "options": {
                    "name": "Prefix or Name of the guild Ex: TAq, Calvish. Shows data for the past 7 days.",
                }
            },
            "guild leaderboard xp": {
                "desc": "Shows a leaderboard of the top 100 guilds by XP gained in the last 24 hours.",
                "usage": "guild leaderboard xp",
                "options": {
                    "name": "Prefix or Name of the guild Ex: TAq, Calvish. Shows data for the past 7 days. ",
                }
            },
            "guild overview": {
                "desc": "Shows a (kind of outdated) overview of a guild.",
                "usage": "guild overview [name]",
                "options": {
                    "name": "The name or prefix of the guild to check. (Case-Sensitive)",
                }
            },
            "guild inactivity": {
                "desc": "Shows a list of inactive guild members sorted by their last login date.",
                "usage": "guild inactivity [name]",
                "options": {
                    "name": "The name or prefix of the guild to check. (Case-Sensitive)",
                }
            },
            "player activity playtime": {
                "desc": "Shows a bar graph of a player's every day playtime over the past two weeks.",
                "usage": "player activity playtime [name]",
                "options": {
                    "name": "The username of the player to check.",
                }
            },
            "player activity contribution": {
                "desc": "Displays a graph of a player's every day contribution XP over the past two weeks.",
                "usage": "player activity contribution [name]",
                "options": {
                    "name": "The username of the player to check.",
                }
            },
            "player activity dungeons": {
                "desc": "Shows a graph of total dungeons completed every day over the past week.",
                "usage": "player activity dungeons [name]",
                "options": {
                    "name": "The username of the player to check.",
                }
            },
            "player activity dungeons_pie": {
                "desc": "Displays a pie chart showing the amount of different dungeons completed.",
                "usage": "player activity dungeons_pie [name]",
                "options": {
                    "name": "The username of the player to check.",
                }
            },
            "player activity raids": {
                "desc": "Shows a graph of total raids completed every day over the past week.",
                "usage": "player activity raids [name]",
                "options": {
                    "name": "The username of the player to check.",
                }
            },
            "player activity raids_pie": {
                "desc": "Displays a pie chart showing the amount of different raids completed.",
                "usage": "player activity raids_pie [name]",
                "options": {
                    "name": "The username of the player to check.",
                }
            },
            "player activity mobs_killed": {
                "desc": "Shows a graph of total mobs killed every day over the past week.",
                "usage": "player activity mobs_killed [name]",
                "options": {
                    "name": "The username of the player to check.",
                }
            },
            "player activity wars": {
                "desc": "Displays a graph of total wars won every day over the past week.",
                "usage": "player activity wars [name]",
                "options": {
                    "name": "The username of the player to check.",
                }
            },
            "player activity guild_wars": {
                "desc": "Shows a graph displaying the amount of guild raids completed in the past 14 days.",
                "usage": "player activity guild_wars [name]",
                "options": {
                    "name": "Username of the player search Ex: BadPingHere, Salted.",
                }
            },
            "player leaderboard guild_wars": {
                "desc": "Shows the leaderboard of the top 100 players with the highest guild raids in the past 2 weeks.",
                "usage": "player leaderboard guild_wars",
            },
            "player leaderboard raids": {
                "desc": "Shows the top 100 players with the highest total raid completions.",
                "usage": "player leaderboard raids",
                "options": {}
            },
            "player leaderboard total_level": {
                "desc": "Shows the top 100 players with the highest total level across all classes.",
                "usage": "player leaderboard total_level",
                "options": {}
            },
            "player leaderboard dungeons": {
                "desc": "Shows the top 100 players with the highest total dungeon completions.",
                "usage": "player leaderboard dungeons",
                "options": {}
            },
            "player leaderboard playtime": {
                "desc": "Displays the top 100 players with the highest total playtime in hours.",
                "usage": "player leaderboard playtime",
                "options": {}
            },
            "player leaderboard pvp_kills": {
                "desc": "Shows the top 100 players with the highest PvP kill count.",
                "usage": "player leaderboard pvp_kills",
                "options": {}
            },
            "territory map": {
                "desc": "Generates and displays the current Wynncraft territory map displaying what guild owns what.",
                "usage": "territory map",
                "options": {}
            },
            "territory heatmap": {
                "desc": "Generates and displays a heatmap showing territory activity.",
                "usage": "territory heatmap",
                "options": {
                    "timeframe": "The timeframe you wish to create a heatmap for, by season, days, or lifetime.",
                }
            },
            "hq": {
                "desc": "Lists the strongest headquarter territory location for a speciic guild or globally.",
                "usage": "hq [guild - optional]",
                "options": {"guild": "Prefix of the guild to check. (Case Sensitive), (optional)."
                }
            },
            "help": {
                "desc": "Displays help information about available commands and their usage.",
                "usage": "help [command - optional]]",
                "options": {
                    "command": "The specific command to get help for (optional).",
                }
            }
        }
        desc = ""
        arg = arg.lower()
        if arg in commands.keys():
            desc += f"**Description**: {commands[arg]['desc']}\n\n"
            desc += f"**Usage**: /{commands[arg]['usage']}\n\n"
            if commands[arg]["options"]: #Checks if there are options
                desc += f"**Options**:\n"
                for option in commands[arg]["options"]:
                    desc += f"- **{option}**: {commands[arg]['options'][option]}\n"
            embed = discord.Embed(
                title=arg,
                description=desc,
                color=0x016610E
            )
            return embed, True
        
        else:
            closeMatches = difflib.get_close_matches(arg, commands.keys(), n=1, cutoff=0.4)
            if closeMatches:
                suggestion = closeMatches[0]
                message = f"The command you inputted is not valid. Did you mean **{suggestion}**?"
            else:
                message = "The command you inputted is not valid. Please try again."
            return message, False

def guildLeaderboardXPButGuildSpecific(guild_uuid, timeframe):
    logger.info(f"guild_uuid: {guild_uuid}, guildLeaderboardXPButGuildSpecific, timeframe: {timeframe}")
    conn = sqlite3.connect('database/guild_activity.db')
    cursor = conn.cursor()

    if timeframe == "Last 14 Days":
        endDate = datetime.now()
        startDate = endDate - timedelta(days=14)
    elif timeframe == "Last 7 Days":
        endDate = datetime.now()
        startDate = endDate - timedelta(days=7)
    elif timeframe == "Last 3 Days":
        endDate = datetime.now()
        startDate = endDate - timedelta(days=3)
    elif timeframe == "Last 24 Hours":
        endDate = datetime.now()
        startDate = endDate - timedelta(hours=24)
    elif timeframe == "Everything":
        startDate = None
        endDate = None
    else:  # fallback
        startDate = None
        endDate = None
        
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
    params = [guild_uuid]

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

    cursor.execute(query, params)
    snapshots = cursor.fetchall()
    conn.close()

    if not snapshots:
        return None

    listy = []
    for row in snapshots:
        listy.append([row[0], row[1]]) #name, value

    return listy

def guildLeaderboardOnlineButGuildSpecific(guild_uuid, timeframe):
    logger.info(f"guild_uuid: {guild_uuid}, guildLeaderboardOnlineButGuildSpecific, timeframe: {timeframe}")
    conn = sqlite3.connect('database/player_activity.db')
    cursor = conn.cursor()

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

    # Build query dynamically
    query = f"""
        WITH recent_users AS (
            SELECT DISTINCT uuid, username
            FROM users
            WHERE guildUUID = ?
    """
    params = [guild_uuid]

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
    cursor.execute(query, params)
    snapshots = cursor.fetchall()
    conn.close()

    if not snapshots:
        return None

    listy = []
    for row in snapshots:
        listy.append([row[0], row[1]])

    return listy

def guildLeaderboardWarsButGuildSpecific(guild_uuid, timeframe):
    logger.info(f"guild_uuid: {guild_uuid}, guildLeaderboardWarsButGuildSpecific, timeframe: {timeframe}")
    conn = sqlite3.connect('database/player_activity.db')
    cursor = conn.cursor()

    if timeframe == "Last 14 Days":
        endDate = datetime.now()
        startDate = endDate - timedelta(days=14)
    elif timeframe == "Last 7 Days":
        endDate = datetime.now()
        startDate = endDate - timedelta(days=7)
    elif timeframe == "Last 3 Days":
        endDate = datetime.now()
        startDate = endDate - timedelta(days=3)
    elif timeframe == "Last 24 Hours":
        endDate = datetime.now()
        startDate = endDate - timedelta(hours=24)
    elif timeframe == "Everything":
        startDate = None
        endDate = None
    else:  # fallback
        startDate = None
        endDate = None

    query = """
        WITH recent_snapshots AS (
            SELECT *
            FROM users_global
            WHERE uuid IN (
                SELECT DISTINCT uuid 
                FROM users 
                WHERE guildUUID = ?
    """
    params = [guild_uuid]

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

    cursor.execute(query, params)
    snapshots = cursor.fetchall()
    conn.close()

    if not snapshots:
        return None

    listy = []
    for row in snapshots:
        listy.append([row[0], row[1]]) #name, value

    return listy

def guildLeaderGraids(timeframe):
    logger.info(f"guildLeaderGraids")
    
    confirmedGRaid = getGraidDatabaseData("guild_raids")
    #logger.info(f"confirmedGRaid: {confirmedGRaid}")
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
    leaderboard = {}
    for prefix, entries in confirmedGRaid.items():
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

def guildLeaderGraidsButGuildSpecific(prefix, timeframe):
    logger.info(f"guildLeaderGraidsButGuildSpecific. prefix is {prefix}")
    
    confirmedGRaid = getGraidDatabaseData("guild_raids")

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
            startDate = None
            endDate = None

    player_counter = Counter()

    for entry in confirmedGRaid[prefix]:
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

def playerLeaderboardGraids(timeframe):
    logger.info(f"playerLeaderboardGraids timeframe: {timeframe}")
    
    confirmedGRaid = getGraidDatabaseData("guild_raids")

    # Handle timeframe logic
    if timeframe == "Last 14 Days":
        endDate = datetime.now()
        startDate = endDate - timedelta(days=14)
        cutoff_timestamp = time.time() - (14*86400)
    elif timeframe == "Last 7 Days":
        endDate = datetime.now()
        startDate = endDate - timedelta(days=7)
        cutoff_timestamp = time.time() - (7*86400)
    elif timeframe == "Last 24 Hours":
        endDate = datetime.now()
        startDate = endDate - timedelta(hours=24)
        cutoff_timestamp = time.time() - (1*86400)
    elif timeframe == "Everything":
        startDate = None
        endDate = None
        cutoff_timestamp = 0
    else:  # fallback to 14 days
        cutoff_timestamp = time.time() - (14*86400)

    player_counter = Counter()

    for entries in confirmedGRaid.values():
        for entry in entries:
            if entry["timestamp"] >= cutoff_timestamp:
                for player in entry["party"]:
                    player_counter[player] += 1

    # Sort and display
    sortedPlayers = player_counter.most_common(100) # Limit to 100
    
    return sortedPlayers

def guildActivityGraids(prefix):
    logger.info(f"guildActivityGraids, prefix: {prefix}")
    
    confirmedGRaid = getGraidDatabaseData("guild_raids")

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
    plt.close()
    
    file = discord.File(buf, filename='graid_graph.png')
    embed = discord.Embed(
        title=f"Guild Raid Completion for {prefix}",
        description=f"Total Guild Raids: {total_raids}\nMost Guild Raids in One Day: {max_day}\nAverage Guild Raids per Day: {avg_day:.2f}",
        color=discord.Color.blue()
    )
    embed.set_image(url="attachment://graid_graph.png")
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")

    buf.close()
    return file, embed

def playerActivityGraids(username):
    logger.info(f"guildActivityGraids, username: {username}")
    confirmedGRaid = getGraidDatabaseData("guild_raids")

    now = datetime.utcnow()
    cutoff = now - timedelta(days=14)

    # Step 1: Collect all timestamps the user appeared in
    timestamps = []
    for entries in confirmedGRaid.values():
        for entry in entries:
            if username in entry.get("party", []):
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
    plt.title(f'Guild Raid Activity - {username}', fontsize=14)
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
    plt.close()
    
    file = discord.File(buf, filename='graid_graph.png')
    embed = discord.Embed(
        title=f"Guild Raid Completion for {username}",
        description=f"Total Guild Raids: {total_raids}\nMost Guild Raids in One Day: {max_day}\nAverage Guild Raids per Day: {avg_day:.2f}",
        color=discord.Color.blue()
    )
    embed.set_image(url="attachment://graid_graph.png")
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")

    buf.close()
    return file, embed

def detect_graids(eligibleGuilds): # i will credit this to slumbrous (+my additions) on disc, my shit did NOT fucking work first try.
    for prefix in eligibleGuilds:
        raidingUsers = []
        
        success, r = makeRequest(f"https://api.wynncraft.com/v3/guild/prefix/{prefix}")
        if not success:
            continue
        d = r.json()
        lvl = min(d.get("level", 0), 130)
        thr = round((20000 * sum(1.15**(n-1) for n in range(1, lvl+1)))/4000) # amount of xp the guld recieves for XP /4000 (since every person gets 1/4th of 1/1000 of total level req)
        for members in d["members"].values():
            if not isinstance(members, dict):
                continue
            for user, info in members.items():
                now = info.get("contributed", 0)
                prev = last_xp.get((prefix, user), now)
                if thr*0.95 <= now - prev <= (thr*1.01)+2500000: # successful graid (I do (thr*1.01)+2500000 because /guild xp can fuck up the calc of it on lower lvl guilds, itll be less accurate here but fixed at len(party) == 4)
                    raidingUsers.append(user)
                    #logger.info(f"[GRAID] {user}@{prefix} +{now-prev} XP, thr is {thr}, so a deviation of {(now - prev)-thr} XP.")
                last_xp[(prefix, user)] = now

        if prefix not in confirmedGRaid: #init it
                confirmedGRaid[prefix] = []
        if raidingUsers: # Raid happened, cut them up and put into confirmed raids
            splitRaidingUsers = [raidingUsers[i:i+4] for i in range(0, len(raidingUsers), 4)]
            for party in splitRaidingUsers:
                if len(party) == 4: # should fix false finds
                    confirmedGRaid[prefix].append({"timestamp": time.time(), "party": party})
                    writeGraidDatabaseData("guild_raids", confirmedGRaid)
                    #logger.info(f"confirmedGRaid: timestamp: {time.time()} party: {party}")

def validateValue(expectedType, value, guild: discord.Guild):
    roleRegex = re.compile(r"<@&(\d+)>")
    channelRegex = re.compile(r"<#(\d+)>")
    if expectedType == "number":
        try:
            return int(value)
        except ValueError:
            raise ValueError("Value must be a number.")
    elif expectedType == "bool":
        lowered = value.lower()
        if lowered in ("true", "yes", "1"): # idk what else would be used
            return True
        elif lowered in ("false", "no", "0"): 
            return False
        raise ValueError("Value must be true/false.")
    elif expectedType == "role":
        # Check if value is a mention
        match = roleRegex.fullmatch(value)
        if match:
            role_id = int(match.group(1))
            role = guild.get_role(role_id)
            if not role:
                raise ValueError(f"Role with ID {role_id} not found.")
            return str(role.id)
        else:
            # Fallback, try and find user name
            role = discord.utils.get(guild.roles, name=value)
            if not role:
                raise ValueError(f"Role '{value}' not found.")
            return str(role.id)
    elif expectedType == "channel":
        match = channelRegex.fullmatch(value)
        if match:
            channel_id = int(match.group(1))
            channel = guild.get_channel(channel_id)
            if not channel:
                raise ValueError(f"Channel with ID {channel_id} not found.")
            return str(channel.id)
        else: # fallback, try to find channel name
            channel = discord.utils.get(guild.channels, name=value)
            if not channel:
                raise ValueError(f"Channel '{value}' not found.")
            return str(channel.id)
    elif expectedType == "rank":
        valid_ranks = ["Recruit", "Recruiter", "Captain", "Strategist", "Chief", "Owner"]
        if value not in valid_ranks:
            raise ValueError(f"Value must be one of: {', '.join(valid_ranks)}.")
        return value
    elif expectedType == "prefix":
        success, r = makeRequest("https://api.wynncraft.com/v3/guild/list/guild")
        if not success: # Check if theyre a real user
            raise ValueError(f"An error occured while validating your prefix.")
        else: # theyre real
            jsonData = r.json()
            prefixList = [v["prefix"] for v in jsonData.values()]
            if value not in prefixList:
                raise ValueError(f"{value} is not a valid prefix.")
        return value
    else:
        return value

def updateConfig(serverID, configName, configValue):
    try:
        conn = sqlite3.connect(CONFIGDBPATH)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS config (
                serverID INTEGER,
                configName TEXT,
                configValue TEXT,
                PRIMARY KEY (serverID, configName)
            )
        """)
        cur.execute("""
            INSERT INTO config (serverID, configName, configValue)
            VALUES (?, ?, ?)
            ON CONFLICT(serverID, configName)
            DO UPDATE SET configValue = excluded.configValue
        """, (serverID, configName, configValue)) # updates or inserts
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"Failed to update config DB {serverID}: {e}")

def checkVerification(serverID):
    try:
        conn = sqlite3.connect(CONFIGDBPATH)
        cur = conn.cursor()
        cur.execute("""
            SELECT configName, configValue
            FROM config
            WHERE serverID = ? AND configName IN (?, ?, ?)
        """, (serverID, "verify_apps", "verify_rank", "log_channel"))
        rows = dict(cur.fetchall())
        conn.close()
        return rows.get("verify_apps", ""), rows.get("verify_rank"), rows.get("log_channel")
    except Exception as e:
        logger.warning(f"Failed to fetch info {serverID}: {e}")

def getDatabaseData(serverID):
    try:
        conn = sqlite3.connect(CONFIGDBPATH)
        cur = conn.cursor()
        cur.execute("SELECT configName, configValue FROM config WHERE serverID = ?", (serverID,))
        rows = dict(cur.fetchall())
        conn.close()
        return rows
    except Exception as e:
        logger.warning(f"Failed to fetch info2 {serverID}: {e}")

def listConfig(serverID, configTypes):
    try:
        conn = sqlite3.connect(CONFIGDBPATH)
        cur = conn.cursor()

        cur.execute("SELECT configName, configValue FROM config WHERE serverID = ?", (serverID,))
        rows = dict(cur.fetchall())
        conn.close()
        lines = [f"{name}: {rows.get(name, 'Unset')}" for name in configTypes]
        desc = "```\n" + "\n".join(lines) + "\n```"
        embed = discord.Embed(
            title=f"Server {serverID} Configuration.",
            description=desc,
            color=discord.Color.blue()
        )
        embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
        return embed
    except Exception as e:
        logger.warning(f"Failed to list config for server {serverID}: {e}")
        embed = discord.Embed(
            title="Error",
            description="Could not fetch config values.",
            color=discord.Color.red()
        )
        embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
        return embed

def storeVerifiedUser(serverID, discordUser, ingameUser):
    try:
        conn = sqlite3.connect(CONFIGDBPATH)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS verified_users (
                serverID INTEGER,
                discordUser TEXT,
                gameUser TEXT,
                PRIMARY KEY (serverID, discordUser)
            )
        """)
        cur.execute("""
            INSERT INTO verified_users (serverID, discordUser, gameUser)
            VALUES (?, ?, ?)
            ON CONFLICT(serverID, discordUser)
            DO UPDATE SET gameUser = excluded.gameUser
        """, (serverID, discordUser, ingameUser)) # updates or inserts
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"Failed to update config DB {serverID}: {e}")

def getAllDatabaseData():
    try:
        conn = sqlite3.connect(CONFIGDBPATH)
        cur = conn.cursor()
        cur.execute("SELECT serverID, discordUser, gameUser FROM verified_users")
        rows = cur.fetchall()
        conn.close()
        return rows
    except Exception as e:
        logger.warning(f"Failed to fetch info for getAllDatabaseData: {e}")
        return []
    
def playerGuildHistory(playerUUID, username):
    conn = sqlite3.connect('database/guild_activity.db')
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            gm.guild_uuid,
            g.name AS guild_name,
            g.prefix AS guild_prefix,
            gm.joined
        FROM guild_members gm
        JOIN guilds g ON gm.guild_uuid = g.uuid
        WHERE gm.member_uuid = ?;
    """, (playerUUID,))
    rows = cursor.fetchall()
    
    description = f"### **{username}**'s Guild History (Nov. 2024)"
    description += "\n\n"
    for i, row in enumerate(rows):
        guildUUID, guildName, guildPrefix, joinDate = row

        if i + 1 < len(rows):
            leaveDate = rows[i + 1][3]
        else: # Should be most recent output, we call api to get if they've left.
            success, r = makeRequest(f"https://api.wynncraft.com/v3/guild/uuid/{guildUUID}") # player guild join date isnt in player info sadly
            if not success:
                leaveDate = "Unknown"
            else:
                hasLeft = True
                jsonData = r.json()
                for rank in jsonData["members"]: # this iterates through every rank like chief, owner, etc
                    if isinstance(jsonData["members"][rank], dict): # checks if it has a rank i think so it knows people from non arrrays??
                        for member, value in jsonData["members"][rank].items(): 
                            if value['uuid'] == playerUUID:
                                hasLeft = False
                leaveDate = "Unknown, has left guild" if hasLeft else "Has not left" # mr coder
                
        def convert(isoTime):
            if not isoTime or not isinstance(isoTime, str) or "T" not in isoTime:
                return None
            try:
                dt = datetime.fromisoformat(isoTime.replace("Z", "+00:00"))
                return int(dt.timestamp())
            except Exception:
                return None
            
        joinEpoch = convert(joinDate)
        leaveEpoch = convert(leaveDate if isinstance(leaveDate, str) and "T" in leaveDate else None)

        join = f"<t:{joinEpoch}:D>" if joinEpoch else joinDate
        if isinstance(leaveDate, str) and "T" not in leaveDate:
            leave = leaveDate
        else:
            leave = f"<t:{leaveEpoch}:D>" if leaveEpoch else "Unknown"

        description += (
            f"**{guildName} ({guildPrefix}):**\n"
            f"Join Date: {join}, Leave Date: {leave}\n"
        )
    description += "\nNote: Leave dates for prev guilds are unknown, so they are set as their next guild's join date."
    embed = discord.Embed(
        description=description,
        color=discord.Color.blue()
    )
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")

    return embed

def guildOnline(name, r):
    jsonData = r.json()
    online = {}
    for rank, rank_members in jsonData["members"].items():
        if rank == "total":
            continue
        online[rank] = [username for username, info in rank_members.items() if info.get("online")]


    desc = f"## Online Members - {name}\n\n"
    totalOnline = sum(len(members) for members in online.values())
    for rank in online:
        desc += f"**{rank.capitalize()} ({len(online[rank])})**:\n"
        if online[rank]:
            desc += ", ".join(online[rank]) + "\n\n"
        else:
            desc += "N/A\n\n"
    desc += f"**Total Online**: {totalOnline}"
    embed = discord.Embed(
        description=desc,
        color=discord.Color.blue()
    )
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    
    return embed
