import requests
import asyncio
import discord
from datetime import datetime, timezone
import json
from collections import Counter
import logging
import time
import sqlite3
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.dates import HourLocator, DateFormatter
from collections import defaultdict, deque
from datetime import timedelta
import io
import seaborn as sns

logger = logging.getLogger('discord')

ratelimitmultiplier = 1
ratelimitwait = 0.1 

sns.set_style("whitegrid")
blue, = sns.color_palette("muted", 1)

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
                logger.error(f"{URL} returned 404: {err}")
                await asyncio.sleep(3)
                return r
            logger.error(f"Error getting {URL}: {err}")
            await asyncio.sleep(3)
            continue
        if r.ok:
            if int(r.headers['RateLimit-Remaining']) > 60:
                ratelimitmultiplier = 1
                ratelimitwait = 0.25
            else:
                if int(r.headers['RateLimit-Remaining']) < 60: # We making too many requests, slow it down
                    logger.info(f"Ratelimit-Remaining is under 60.")
                    ratelimitmultiplier = 1.5
                    ratelimitwait = 0.70
                if int(r.headers['RateLimit-Remaining']) < 30: # We making too many requests, slow it down
                    logger.info(f"Ratelimit-Remaining is under 30.")
                    ratelimitmultiplier = 2
                    ratelimitwait = 1.25
                if int(r.headers['RateLimit-Remaining']) < 10: # We making too many requests, slow it down
                    logger.info(f"Ratelimit-Remaining is under 10.")
                    ratelimitmultiplier = 4
                    ratelimitwait = 3
            return r
        else:
            logger.error(f"Error getting {URL}. Status code is {r.status_code}")
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
        logger.error(f"R is None in findAttackingMembers. Here is r: {r}.")
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
    logger.info(f"Ran HQ lookup successfully for {guildPrefix if guildPrefix else 'global map'}.")
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

# because not everything happens in a second.
async def intvervalGrouping(timestamps):
    interval_seconds=30
    groups = deque()
    for ts in timestamps:
        if not groups or (ts - groups[-1][0]).total_seconds() > interval_seconds:
            groups.append([ts])
        else:
            groups[-1].append(ts)
    return groups

async def guildActivityPlaytime(guild_uuid, name):
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
    
    grouped_snapshots = await intvervalGrouping([datetime.fromisoformat(snapshot[1]) for snapshot in snapshots])

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
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now().strftime('%m/%d/%Y, %I:%M %p')}")
    conn.close()
    buf.close()
    return file, embed

async def guildActivityXP(guild_uuid, name):
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
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now().strftime('%m/%d/%Y, %I:%M %p')}")
    
    conn.close()
    buf.close()
    return file, embed

async def guildActivityTerritories(guild_uuid, name):
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
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now().strftime('%m/%d/%Y, %I:%M %p')}")

    buf.close()
    conn.close()
    return file, embed

async def guildActivityWars(guild_uuid, name):
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
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now().strftime('%m/%d/%Y, %I:%M %p')}")

    buf.close()
    conn.close()
    return file, embed

async def guildActivityOnlineMembers(guild_uuid, name):
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
    
    grouped_snapshots = await intvervalGrouping([datetime.fromisoformat(snapshot[1]) for snapshot in snapshots])

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
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now().strftime('%m/%d/%Y, %I:%M %p')}")
    
    conn.close()
    buf.close()
    
    return file, embed

async def guildActivityTotalMembers(guild_uuid, name):
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
    grouped_snapshots = await intvervalGrouping([datetime.fromisoformat(snapshot[0]) for snapshot in snapshots])
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
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now().strftime('%m/%d/%Y, %I:%M %p')}")
    
    conn.close()
    buf.close()
    return file, embed

async def guildLeaderboardOnlineMembers():
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
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now().strftime('%m/%d/%Y, %I:%M %p')}")
    return embed

async def guildLeaderboardTotalMembers():
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
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now().strftime('%m/%d/%Y, %I:%M %p')}")
    return embed

async def guildLeaderboardWars():
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
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now().strftime('%m/%d/%Y, %I:%M %p')}")
    conn.close()
    return embed

async def guildLeaderboardXP():
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
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now().strftime('%m/%d/%Y, %I:%M %p')}")
    conn.close()
    return embed

async def guildLeaderboardPlaytime():
    logger.info(f"leaderboardPlaytime")
    conn = sqlite3.connect('database/guild_activity.db')
    cursor = conn.cursor()
    
    cursor.execute("""
        WITH time_grouped_data AS (
            SELECT 
                g.uuid as guild_uuid,
                g.name || ' (' || COALESCE(g.prefix, '') || ')' as guild_name,
                ms.timestamp,
                ms.online,
                datetime(
                    strftime('%Y-%m-%d %H:', timestamp) || 
                    (cast(strftime('%M', timestamp) / 5 as int) * 5)
                ) as interval_start
            FROM guilds g
            JOIN member_snapshots ms ON g.uuid = ms.guild_uuid
            WHERE ms.timestamp >= datetime('now', '-1 day')
        ),
        interval_averages AS (
            SELECT 
                guild_uuid,
                guild_name,
                interval_start,
                AVG(CAST(online AS FLOAT)) * 100 as interval_avg
            FROM time_grouped_data
            GROUP BY guild_uuid, guild_name, interval_start
        ),
        guild_averages AS (
            SELECT 
                guild_name,
                AVG(interval_avg) as activity_percentage
            FROM interval_averages
            GROUP BY guild_name
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
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now().strftime('%m/%d/%Y, %I:%M %p')}")
    return embed