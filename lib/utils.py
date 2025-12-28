import discord
from datetime import datetime, timezone
import json
from collections import Counter
import logging
import time
import sqlite3
import matplotlib as mpl
from collections import defaultdict
import seaborn as sns
import bisect
import random
import os
from io import BytesIO
import difflib
from pathlib import Path
from lib.makeRequest import makeRequest, internalMakeRequest
import base64
  
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
TERRITORIESPATH = Path(__file__).resolve().parents[1] / "lib" /  "documents" / "territories.json"
sns.set_style("whitegrid")
mpl.use('Agg') # Backend without any gui popping up
blue, = sns.color_palette("muted", 1)

timeframeMap1 = { # Used for heatmap data
    "Season 24": ("04/18/25", "06/01/25"),
    "Season 25": ("06/06/25", "07/20/25"),
    "Season 26": ("07/25/25", "09/14/25"),
    "Season 27": ("09/19/25", "11/02/25"), 
    "Season 28": ("11/07/25", "12/20/25"), 
    "Season 29": ("01/01/26", "12/20/26"), 
    "Last 7 Days": None, # gotta handle ts outta dict
    "Everything": None
}

timeframeMap2 = { # Used for graid data, note to update it in api
    "Season 25": ("06/06/25", "07/20/25"),
    "Season 26": ("07/25/25", "09/14/25"),
    "Season 27": ("09/19/25", "11/02/25"), 
    "Season 28": ("11/07/25", "12/20/25"), 
    "Season 29": ("01/01/26", "12/20/26"), 
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
            if int(json.get("globalData", {}).get("wars", 0)) > 20: # arbitrary number, imo 20 or more means youre prolly a full-time warrer. also defaults to 0 for hidden stats
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
    with open(TERRITORIESPATH) as a:
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

        time.sleep(0.25) # Slow down inactivity because we need to preserve our ratelimits
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

def leaderboardBuilder(commandType, uuid = None, timeframe = None, prefix = None):
    if commandType == "guildLeaderboardGraidsButGuildSpecific": # too odd of design
        # Because of bad design choices, we need to convert our prefix into uuid, but we have a search tool now!
        success, r = internalMakeRequest(f"http://127.0.0.1:8080/api/search/prefix/{prefix}")
        if not success:
            logger.error(f"Error in guildLeaderboardGraidsButGuildSpecific search function, success is {success}, jsonData is {r.json()}")  
            return []
        searchJson = r.json()
        uuid = searchJson["uuid"]
        
    logger.info(f"{commandType}" + (f", uuid: {uuid}" if uuid else "") + (f", timeframe: {timeframe}" if timeframe else ""))
    # complicated code to create the url given the present parameters
    url = (f"http://127.0.0.1:8080/api/leaderboard/{commandType}"+ ("?" + "&".join(f"{k}={v}" for k, v in (("uuid", uuid), ("timeframe", timeframe)) if v)if any((uuid, timeframe)) else ""))
    success, r = internalMakeRequest(url)
    jsonData = r.json()
    if not success:
        logger.error(f"Error for {commandType} in leaderboardBuilder, success is {success}, jsonData is {jsonData}")
        return []
    listy = []
    for row in jsonData:
        extracted = extractValues(row)
        listy.append(extracted)

    return listy

def activityBuilder(commandType, uuid = None, name = None, theme = None, prefix = None, timeframe = None):
    if commandType == "guildActivityGraids": # too odd of design
        # Because of bad design choices, we need to convert our prefix into uuid, but we have a search tool now!
        success, r = internalMakeRequest(f"http://127.0.0.1:8080/api/search/prefix/{prefix}")
        
        if not success:
            logger.error(f"Error in guildLeaderboardGraidsButGuildSpecific search function, success is {success}, jsonData is {r.json()}")  
            return None, None
        searchJson = r.json()
        uuid = searchJson["uuid"]

    logger.info(f"{commandType}" + (f", uuid: {uuid}" if uuid else "") + (f", name: {name}" if name else "") + (f", theme: {theme}" if theme else ""))
    url = (f"http://127.0.0.1:8080/api/activity/{commandType}"+ ("?" + "&".join(f"{k}={v}" for k, v in (("uuid", uuid), ("name", name), ("theme", theme), ("timeframe", timeframe),) if v)if any((uuid, name, theme, timeframe)) else ""))
    success, r = internalMakeRequest(url)
    if not success:
        logger.error(f"Error for {commandType} in leaderboardBuilder, success is {success}, jsonData is {r.json()}")
        return None, None
    jsonData = r.json()
    buf = BytesIO(base64.b64decode(jsonData["image"]))
    
    match commandType:
        case "guildActivityXP":
            title=f"XP Analysis for {name}"
            description=f"Total XP (14 days): {jsonData['total_xp']:,.0f}\nDaily Average: {jsonData['daily_average']:,.0f}\nHighest Day: {jsonData['highest_day']:,.0f}\nLowest Day: {jsonData['lowest_day']:,.0f}"
        
        case "guildActivityTerritories":
            title=f"Territory Analysis for {name}"
            description=f"Current territories: {jsonData['current_territories']:.0f}\nMaximum territories: {jsonData['maximum_territories']:.0f}\nMinimum territories: {jsonData['minimum_territories']:.0f}\nAverage territories: {jsonData['average_territories']:.0f}"
            
        case "guildActivityWars":
            title=f"Warring Analysis for {name}"
            description=f"Current war count: {jsonData['current_war']:.0f}"
        
        case "guildActivityOnlineMembers":
            title=f"Online Members for {name}"
            description=(
                f"Maximum players online: {jsonData['max_players']:.0f}\n"
                f"Minimum players online: {jsonData['min_players']:.0f}\n"
                f"Average players online: {jsonData['average']:.1f}"
            )
            
        case "guildActivityTotalMembers":
            title=f"Members Analysis for {name}"
            description=(
                f"Maximum Member Count: {jsonData['max_players']:.0f}\n"
                f"Minimum Member Count: {jsonData['min_players']:.0f}\n"
                f"Average Member Count: {jsonData['average']:.0f}"
            )
        
        case "playerActivityPlaytime":
            title=f"Playtime Analysis for {name}"
            description=(
                f"Daily Average: {jsonData['daily_average']:.0f} min\n"
                f"Highest Day: {jsonData['max_day']} min\n"
                f"Lowest Day: {jsonData['min_day']} min\n"
            )
            
        case "playerActivityContributions":
            title=f"XP Gain for {name}"
            description=(
                f"Total XP (14 Days): {jsonData['total_xp']:,.0f} xp\n"
                f"Highest Day: {jsonData['max_xp']:} xp\n"
                f"Lowest Day: {jsonData['min_xp']:} xp"
            )
        
        case "playerActivityDungeons":
            title=f"Dungeon Runs for {name}"
            description=(
                f"Total Dungeons: {jsonData['total_dungeons']} dungeons\n"
                f"Highest Gain in One Day: {jsonData['highest_gain']} dungeons\n"
            )
            
        case "playerActivityTotalDungeons":
            title=f"Dungeon pie chart for {name}"
            description= None
        
        case "playerActivityRaids":
            title=f"Raid Runs for {name}"
            description=(
                f"Total Raids: {jsonData['total']} raids\n"
                f"Highest Gain in One Day: {jsonData['highest_gain']} raids\n"
            )
            
        case "playerActivityTotalRaids":
            title=f"Raid pie chart for {name}"
            description= None
        
        case "playerActivityMobsKilled":
            title=f"Mob's killed for {name}"
            description=(
                f"Total Kills: {jsonData['total_kills']} kills\n"
                f"Highest Gain in One Day: {jsonData['highest_gain']} kills\n"
            )
            
        case "playerActivityWars":
            title=f"War Count for {name}"
            description=(
                f"Total Wars: {jsonData['total_wars']} wars\n"
                f"Highest Gain in One Day: {jsonData['highest_gain']} wars\n"
            )
        
        case "guildActivityGraids":
            title=f"Guild Raid Completion for {prefix}"
            description=f"Total Guild Raids: {jsonData['total_graid']}\nMost Guild Raids in One Day: {jsonData['max_graid']}\nAverage Guild Raids per Day: {jsonData['average_graid']:.2f}"
            
        case "playerActivityGraids":
            title=f"Guild Raid Completion for {name}"
            description=f"Total Guild Raids: {jsonData['total_graid']}\nMost Guild Raids in One Day: {jsonData['max_graid']}\nAverage Guild Raids per Day: {jsonData['average_graid']:.2f}"

    file = discord.File(buf, filename=f'{commandType}.webp')
    embed = discord.Embed(
        title=title+f" - {timeframe}",
        description=description,
        color=discord.Color.blue()
    )
    embed.set_image(url=f"attachment://{commandType}.webp")
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    return file, embed

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
    success, r = internalMakeRequest(f"http://127.0.0.1:8080/api/map/current")
    mapBytes = BytesIO(r.content)
    mapBytes.seek(0)
    file = discord.File(mapBytes, filename="wynn_map.webp")
    embed = discord.Embed(
        title=f"Current Territory Map",
        color=discord.Color.green()
    )
    embed.set_image(url="attachment://wynn_map.webp")
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    return file, embed

def ingredientMapCreator(ingredient, price, tier):
    url = (f"http://127.0.0.1:8080/api/map/ingmap"+ ("?" + "&".join(f"{k}={v}" for k, v in (("ingredient", ingredient), ("price", price), ("tier", tier),) if v)if any((ingredient, price, tier)) else ""))
    success, r = internalMakeRequest(url)
    mapBytes = BytesIO(r.content)
    mapBytes.seek(0)
    file = discord.File(mapBytes, filename="ingredient_map.webp")
    embed = discord.Embed(
        title=f"Ingredient Map",
        description = (
            f"Ingredient: {ingredient if ingredient else 'None'}\n"
            f"Price: {(str(price) + 'EB') if price else 'None'}\n"
            f"Tier: {tier if tier else 'None'}\n"
        ),
        color=discord.Color.green()
    )
    embed.set_image(url="attachment://ingredient_map.webp")
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    return file, embed

def heatmapCreator(timeframe):
    success, r = internalMakeRequest(f"http://127.0.0.1:8080/api/map/heatmap?timeframe={timeframe}")
    mapBytes = BytesIO(r.content)
    mapBytes.seek(0)
    file = discord.File(mapBytes, filename="wynn_heatmap.webp")
    embed = discord.Embed(
        title=f"Heatmap for {timeframe}",
        color=discord.Color.green()
    )
    embed.set_image(url="attachment://wynn_heatmap.webp")
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
                if thr*0.99 <= now - prev <= (thr*1.01)+2500000: # successful graid (I do (thr*1.01)+2500000 because /guild xp can fuck up the calc of it on lower lvl guilds, itll be less accurate here but fixed at len(party) == 4)
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

def extractValues(row): # we use this for our internal api so we dont have to know the key names and instead copy paste code because thats easier!
    if isinstance(row, dict):
        name = next((v for k, v in row.items() if isinstance(v, str)), None)
        value = next((v for k, v in row.items() if isinstance(v, (int, float))), None)
        if name is not None and value is not None:
            return [name, value]
        items = list(row.items())
        if len(items) >= 2:
            return [items[0][1], items[1][1]]
    elif isinstance(row, (list, tuple)) and len(row) >= 2:
        return [row[0], row[1]]
    return None
