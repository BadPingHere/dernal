import requests
import asyncio
import discord
from datetime import datetime, timezone
import json
from collections import Counter
import logging
import time

logger = logging.getLogger('discord')

ratelimitmultiplier = 1
ratelimitwait = 0.1 

async def makeRequest(URL): # the world is built on nested if else statements.
    global ratelimitmultiplier
    global ratelimitwait
    while True:
        try:
            session = requests.Session()
            session.trust_env = False
            
            r = session.get(URL)
            r.raise_for_status()
        except requests.exceptions.RequestException as err:
            if r.status_code == 404: # dont repeat 404's cause they wont be there.
                logger.error("404 not found i guess.")
                await asyncio.sleep(3)
                return r
            logger.error(f"Error getting request: {err}")
            await asyncio.sleep(3)
            continue
        if r.ok:
            if int(r.headers['RateLimit-Remaining']) > 60:
                ratelimitmultiplier = 1
                ratelimitwait = 0.25
            else:
                if int(r.headers['RateLimit-Remaining']) < 60: # We making too many requests, slow it down
                    ratelimitmultiplier = 1.5
                    ratelimitwait = 0.70
                if int(r.headers['RateLimit-Remaining']) < 30: # We making too many requests, slow it down
                    ratelimitmultiplier = 2
                    ratelimitwait = 1.25
                if int(r.headers['RateLimit-Remaining']) < 10: # We making too many requests, slow it down
                    ratelimitmultiplier = 4
                    ratelimitwait = 3
            return r
        else:
            logger.error("Error making request.")
            await asyncio.sleep(3)
            continue
    
async def human_time_duration(seconds): # thanks guy from github https://gist.github.com/borgstrom/936ca741e885a1438c374824efb038b3
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

async def findAttackingMembers(attacker):
    r = await makeRequest("https://api.wynncraft.com/v3/guild/prefix/"+str(attacker))
    await asyncio.sleep(ratelimitwait)
    if r is None:
        return [["Unknown", "Unknown", 1738]]  # failed request, so just give a unknown. also ay.

    jsonData = r.json()
    onlineMembers = []
    warringMembers = []
    attackingMembers = []

    for rank in jsonData["members"]: # this iterates through every rank like chief, owner, etc
        if isinstance(jsonData["members"][rank], dict):
            for member in jsonData["members"][rank].values(): 
                if member['online']: # checks if online is set to true or false
                    onlineMembers.append([member['uuid'], member['server']])
    #logger.info(f"Online Members: {onlineMembers}")
    for i in onlineMembers:
        r = await makeRequest("https://api.wynncraft.com/v3/player/"+str(i[0]))
        json = r.json()
        if int(json["globalData"]["wars"]) > 20: # arbitrary number, imo 20 or more means youre prolly a full-time warrer
            warringMembers.append([json["username"], json['server'], int(json["globalData"]["wars"])])
    #logger.info(f"Warring Members: {warringMembers}")

    if not warringMembers: # if for some reason this comes back with no one (offline or sum)
        attackingMembers = [["Unknown", "Unknown", 1738]] # ay
        #logger.info(f"Attacking Members: {attackingMembers}")
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
        
async def sendEmbed(attacker, defender, terrInQuestion, timeLasted, attackerTerrBefore, attackerTerrAfter, defenderTerrBefore, defenderTerrAfter, guildPrefix, pingRoleID, channelForMessages, intervalForPing, timesinceping):
    if guildPrefix not in timesinceping:
        timesinceping[guildPrefix] = 0  # setup 0 first, never again
        
    if attacker != guildPrefix: # lost territory, so try to find attackers
        attackingMembers = await findAttackingMembers(attacker)
        world = attackingMembers[0][1]
        username = [item[0] for item in attackingMembers]
    description = "### 🟢 **Gained Territory!**" if attacker == guildPrefix else "### 🔴 **Lost Territory!**"
    description += f"\n\n**{terrInQuestion}**\nAttacker: **{attacker}** ({attackerTerrBefore} -> {attackerTerrAfter})\nDefender: **{defender}** ({defenderTerrBefore} -> {defenderTerrAfter})\n\nThe territory lasted {timeLasted}." + ("" if attacker == guildPrefix else f"\n{world}: **{'**, **'.join(username)}**")
    embed = discord.Embed(
        description=description,
        color=0x00FF00 if attacker == guildPrefix else 0xFF0000  # Green for gain, red for loss
    )
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now().strftime('%m/%d/%Y, %I:%M %p')}")
    
    try:
        logger.info(f"Embed: {embed.to_dict()}")
        await channelForMessages.send(embed=embed)
    except discord.DiscordException as err:
        logger.error(f"Error sending message: {err}")
        
    if pingRoleID and intervalForPing and channelForMessages and attacker != guildPrefix: #checks if we have everything required, and its a loss
        current_time = time.time()
        if current_time  - int(timesinceping[guildPrefix]) >= intervalForPing*60:
            timesinceping[guildPrefix] = current_time
            try:
                await channelForMessages.send(f"<@&{pingRoleID}>")
            except discord.DiscordException as err:
                logger.error(f"Error sending ping: {err}")

async def getTerrData(untainteddata, untainteddataOLD):
    r = await makeRequest("https://api.wynncraft.com/v3/guild/list/territory")
    await asyncio.sleep(ratelimitwait)
    #print("status", r.status_code)
    stringdata = str(r.json())
    if untainteddata: #checks if it was used before if not save the last one to a different variable. only useful for time when gaind a territory.
        untainteddataOLD = untainteddata
    untainteddata = r.json()
    return {"stringdata": stringdata, "untainteddataOLD": untainteddataOLD, "untainteddata": untainteddata}

async def checkterritories(untainteddata_butitchangestho, untainteddataOLD_butitchangestho, guildPrefix, pingRoleID, channelForMessages, expectedterrcount, intervalForPing, hasbeenran, timesinceping):
    returnData = await getTerrData(untainteddata_butitchangestho, untainteddataOLD_butitchangestho) # gets untainteddataOLD with info
    untainteddataOLD = returnData["untainteddataOLD"]
    untainteddata = returnData["untainteddata"]
    gainedTerritories = {}
    lostTerritories = {}
    terrcount = {}
    
    for territory, data in untainteddata.items():
        old_guild = untainteddataOLD[str(territory)]['guild']['prefix']
        new_guild = data['guild']['prefix']
        if old_guild == guildPrefix and new_guild != guildPrefix:
            lostTerritories[territory] = data
        elif old_guild != guildPrefix and new_guild == guildPrefix:
            gainedTerritories[territory] = data
    #logger.info(f"Gained Territories: {gainedTerritories}")
    #logger.info(f"Lost Territories: {lostTerritories}")
    terrcount[guildPrefix] = expectedterrcount[guildPrefix] # this is what will fix (40 -> 38)
    if lostTerritories: # checks if its empty, no need to run if it is
        for i in lostTerritories:
            reworkedDate = datetime.fromisoformat(untainteddataOLD[i]['acquired'].replace("Z", "+00:00")) # gets the time from the old data
            timestamp = reworkedDate.timestamp()
            reworkedDateNew = datetime.fromisoformat(lostTerritories[i]['acquired'].replace("Z", "+00:00")) # gets the time from the new data
            timestampNew = reworkedDateNew.timestamp() 
            elapsed_time = int(timestampNew) - int(timestamp)
            
            opponentTerrCountBefore = str(untainteddataOLD).count(lostTerritories[str(i)]['guild']['prefix'])
            opponentTerrCountAfter = str(untainteddata).count(lostTerritories[str(i)]['guild']['prefix']) # this will maybe just be wrong if multiple were taken within 11s.
            terrcount[guildPrefix] -= 1
            await sendEmbed(lostTerritories[i]['guild']['prefix'], guildPrefix, i, await human_time_duration(elapsed_time), str(opponentTerrCountBefore), str(opponentTerrCountAfter), str(expectedterrcount[guildPrefix]), str(terrcount[guildPrefix]), guildPrefix, pingRoleID, channelForMessages, intervalForPing, timesinceping)
            expectedterrcount[guildPrefix] = terrcount[guildPrefix]
    if gainedTerritories: # checks if its empty, no need to run if it is
        for i in gainedTerritories:
            reworkedDate = datetime.fromisoformat(untainteddataOLD[i]['acquired'].replace("Z", "+00:00")) # gets the time from the old data
            timestamp = reworkedDate.timestamp()
            reworkedDateNew = datetime.fromisoformat(gainedTerritories[i]['acquired'].replace("Z", "+00:00")) # gets the time from the new data
            timestampNew = reworkedDateNew.timestamp() 
            elapsed_time = int(timestampNew)- int(timestamp)
            
            opponentTerrCountBefore = str(untainteddataOLD).count(untainteddataOLD[str(i)]['guild']['prefix'])
            opponentTerrCountAfter = str(untainteddata).count(untainteddataOLD[str(i)]['guild']['prefix']) # this will maybe just be wrong if multiple were taken within 11s.
            terrcount[guildPrefix]+=1
            await sendEmbed(guildPrefix, untainteddataOLD[i]['guild']['prefix'], i, await human_time_duration(elapsed_time),str(expectedterrcount[guildPrefix]), str(terrcount[guildPrefix]), str(opponentTerrCountBefore), str(opponentTerrCountAfter), guildPrefix, pingRoleID, channelForMessages, intervalForPing, timesinceping)
            expectedterrcount[guildPrefix] = terrcount[guildPrefix]
    if gainedTerritories or lostTerritories: # just for resetting our variables
        hasbeenran[guildPrefix] = False
    else:
        hasbeenran[guildPrefix] = True

async def printTop3(list, word, word2):
    output = ""
    for i, sublist in enumerate(list[:3], 1):
        output += f"{i}.{word} {sublist[1]}: **{sublist[0]}** {word2}\n"
    return output

async def guildLookup(guildPrefixorName, r):
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
    
    formattedRatingList = [[f"{x:,}", y] for x, y in ratingList]
    formattedcontributingList = [[f"{x:,}", y] for x, y in contributingList]

    embed = discord.Embed(
        description=f"""
        {"## "+'🐝 **Fruman Bee (FUB)** 🐝' if jsonData['prefix'] == 'FUB' else '**'+jsonData['name']+' ('+jsonData['prefix']+')**'}
        \n‎\nOwned By: **{list(jsonData["members"]["owner"].keys())[0]}**
        Online: **{online_count}**/**{jsonData["members"]["total"]}**
        Guild Level: **{jsonData["level"]}** (**{jsonData["xpPercent"]}**% until {int(jsonData["level"])+1})\n
        Territory Count: **{jsonData["territories"]}**
        Wars: **{"{:,}".format(jsonData["wars"])}**\n
        Top Season Rankings:
        {await printTop3(formattedRatingList, " Season", "SR")}
        Top Contributing Members:
        {await printTop3(formattedcontributingList, "", "XP")}
        """,
        color=0xFFFF00  # i could make color specific to lookup command, but i wont until i can figure out how to get banner inside of the embed.
    )
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now().strftime('%m/%d/%Y, %I:%M %p')}")
    logger.info(f"Print for {guildPrefixorName} was a success!")
    return(embed)

async def getTerritoryNames(untainteddata, guildPrefix):
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
    for hqCandidate in ownedTerritories:
        connections = []
        externals = []
        for territories in list(territoryData[hqCandidate]["Trading Routes"]):
            if territories in ownedTerritories:
                connections.append(territories)
                externals.append(territories)
        lookedAt = set(externals)

        for _ in range(2): #run twice, first run is conns
            newExternals = []  # Temporary list to store new connections
            for territory in externals:
                for newConnections in territoryData[territory]["Trading Routes"]:
                    if newConnections in lookedAt or newConnections == hqCandidate or newConnections not in ownedTerritories: # skips if its hq canidate or is already in list, also if its owned by us
                        continue
                    newExternals.append(newConnections)
                    lookedAt.add(newConnections)
            externals.extend(newExternals)

        score = int((1 + (len(connections) * 0.30))*(1.5 + (len(externals)  * 0.25))*100)
        scorelist[hqCandidate] = int(score)
        externals = []
    scorelist = dict(reversed(sorted(scorelist.items(), key=lambda item: item[1]))) # sorts on top
    
    description = "## Best HQ Location:\n‎\n"
    for i, (location, score) in enumerate(scorelist.items()):
        if i >= 5:  # max 5 entries
            break
        description += f"{i + 1}. **{location}**: {score}%\n"
    description += "\n-# Note: HQ calculations are purely based on headquarter\n-# strength, not importance of territories or queue times."
    
    embed = discord.Embed(
        #title="# Best HQ location",
        description=description,
        color=0x3457D5,
        )
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now().strftime('%m/%d/%Y, %I:%M %p')}")
    logger.info(f"Ran HQ lookup successfully for {guildPrefix if guildPrefix else "NONE!!"}.")
    return (embed)


async def lookupUser(memberList):
    inactivityDict = {
        "Four Week Inactive Users": [],
        "Three Week Inactive Users": [],
        "Two Week Inactive Users": [],
        "One Week Inactive Users": [],
        "Three Day Inactive Users": [],
        "Active Users": [],
    }
    for member in memberList:
        URL = "https://api.wynncraft.com/v3/player/"+str(member)
        r = await makeRequest(URL)
        jsonData = r.json()
        lastJoinDate = jsonData["lastJoin"]

        joinTime = datetime.strptime(lastJoinDate, "%Y-%m-%dT%H:%M:%S.%fZ")
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
    return inactivityDict

async def lookupGuild(prefix):
    # All this does is gets all the users in the guild and puts them in a list
    URL = "https://api.wynncraft.com/v3/guild/prefix/"+str(prefix)
    r = await makeRequest(URL)
    jsonData = r.json()
    memberList = []
    for rank in jsonData["members"]: # this iterates through every rank like chief, owner, etc
        if isinstance(jsonData["members"][rank], dict): # checks if it has a rank i think so it knows people from non arrrays??
            for member, value in jsonData["members"][rank].items(): 
                memberList.append(value['uuid']) # we use uuid because name changes fuck up username lookups
    return await lookupUser(memberList)