from typing import Optional, Union
import requests
from datetime import datetime
import time
import asyncio
import discord
import logging.handlers
from discord import app_commands

logger = logging.getLogger('discord')
logger.setLevel(logging.INFO)

handler = logging.handlers.RotatingFileHandler(
    filename='discord.log',
    encoding='utf-8',
    maxBytes=32 * 1024 * 1024,  # 32 MiB
    backupCount=5,  # Rotate through 5 files
)
dt_fmt = '%Y-%m-%d %H:%M:%S'
formatter = logging.Formatter('[{asctime}] [{levelname:<8}] {name}: {message}', dt_fmt, style='{')
handler.setFormatter(formatter)
logger.addHandler(handler)


# TODO: slash command that shows the territory hitory of your guild, sum like 'August 9th: 🔴 X was taken by SEQ newline here  🟢 X was taken from SEQ'
# TODO: store data on all guilds, and like have stats on them (Daily active users, Wars won, etc) available from a slash command 

background_task: Optional[asyncio.Task] = None
stop_event = asyncio.Event()
guildsBeingTracked = {}
timesinceping = {}
guildLookupCooldown = 0

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

async def sendEmbed(attacker, defender, terrInQuestion, timeLasted, attackerTerrBefore, attackerTerrAfter, defenderTerrBefore, defenderTerrAfter, guildPrefix, pingRoleID, channelForMessages, intervalForPing):
    global timesinceping
    if guildPrefix not in timesinceping:
        timesinceping[guildPrefix] = 0  # setup 0 first, never again
    embed = discord.Embed(
        title="🟢 **Gained Territory!**" if attacker == guildPrefix else "🔴 **Lost Territory!**",
        description=f"**{terrInQuestion}**\nAttacker: **{attacker}** ({attackerTerrBefore} -> {attackerTerrAfter})\nDefender: **{defender}** ({defenderTerrBefore} -> {defenderTerrAfter})\n\nThe territory lasted {timeLasted}.",
        color=0x00FF00 if attacker == guildPrefix else 0xFF0000  # Green for gain, red for loss
    )
    embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now().strftime('%m/%d/%Y, %I:%M %p')}")
    
    try:
        logger.info("Sending embed.")
        logger.debug(embed)
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
    URL = "https://beta-api.wynncraft.com/v3/guild/list/territory"
    try:
        r = requests.get(URL)
    except requests.exceptions.HTTPError as err:
        logger.error("Error getting request:", err)
    #print("status", r.status_code)
    stringdata = str(r.json())
    if untainteddata: #checks if it was used before if not save the last one to a different variable. only useful for time when gaind a territory.
        untainteddataOLD = untainteddata
    untainteddata = r.json()
    return {"stringdata": stringdata, "untainteddataOLD": untainteddataOLD, "untainteddata": untainteddata}

async def checkterritories(untainteddata_butitchangestho, untainteddataOLD_butitchangestho, guildPrefix, pingRoleID, channelForMessages, expectedterrcount, intervalForPing, hasbeenran):
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
    #print("Gained Territories: ", gainedTerritories)
    #print("Lost Territories: ", lostTerritories)
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
            await sendEmbed(lostTerritories[i]['guild']['prefix'], guildPrefix, i, human_time_duration(elapsed_time), str(opponentTerrCountBefore), str(opponentTerrCountAfter), str(expectedterrcount[guildPrefix]), str(terrcount[guildPrefix]), guildPrefix, pingRoleID, channelForMessages, intervalForPing)
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
            await sendEmbed(guildPrefix, untainteddataOLD[i]['guild']['prefix'], i, human_time_duration(elapsed_time),str(expectedterrcount[guildPrefix]), str(terrcount[guildPrefix]), str(opponentTerrCountBefore), str(opponentTerrCountAfter), guildPrefix, pingRoleID, channelForMessages, intervalForPing)
            expectedterrcount[guildPrefix] = terrcount[guildPrefix]
    if gainedTerritories or lostTerritories: # just for resetting our variables
        hasbeenran[guildPrefix] = False
    else:
        hasbeenran[guildPrefix] = True

def split_message(message, limit=2000): # chatgpt is #thegoat but i will be pissed when it takes my job
    parts = []
    while len(message) > limit:
        split_pos = message.rfind('\n', 0, limit)
        if split_pos == -1:
            split_pos = limit
        parts.append(message[:split_pos])
        message = message[split_pos:].lstrip()
    parts.append(message)
    return parts     

async def printTop3(list, word, word2):
    output = ""
    for i, sublist in enumerate(list[:3], 1):
        output += f"{i}.{word} {sublist[1]}: **{sublist[0]}** {word2}\n"
    return output

async def guildLookup(guildPrefixorName, request):
    r = request
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
        title=f"{'🐝 **Fruman Bee (FUB)** 🐝' if jsonData['prefix'] == 'FUB' else '**'+jsonData['name']+' ('+jsonData['prefix']+')**'}",
        description=f"""
        Owned By: **{list(jsonData["members"]["owner"].keys())[0]}**
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


class MyClient(discord.Client):
    def __init__(self, *, intents: discord.Intents):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        await self.tree.sync()
        print("Command Tree is synced.")


    async def runBackgroundDetector(self, guildsBeingTracked, guild_prefix):
        logger.info(f"War detector now running in background for guild {guild_prefix}.")

        guildPrefix = guild_prefix 
        pingRoleID = guildsBeingTracked[guild_prefix]["pingRoleID"]
        channelForMessages = guildsBeingTracked[guild_prefix]["channelForMessages"]
        intervalForPing = guildsBeingTracked[guild_prefix]["intervalForPing"]
        untainteddata = {}
        untainteddataOLD = {}
        hasbeenran =  {}
        expectedterrcount = {}

        while True:
            guilds_to_check = list(guildsBeingTracked.keys())
            for guild_prefix in guilds_to_check:
                if guild_prefix in guildsBeingTracked:
                    if hasbeenran.get(guild_prefix):
                        pass
                    else:
                        returnData = await getTerrData(untainteddata, untainteddataOLD)
                        expectedterrcount[guild_prefix] = returnData["stringdata"].count(guild_prefix)
                        untainteddata = returnData["untainteddata"]
                        untainteddataOLD = returnData["untainteddataOLD"]
                        hasbeenran[guild_prefix] = True
            #print("---------------------")
            #print(guild_prefix)
            #print(expectedterrcount)
            await checkterritories(untainteddata, untainteddataOLD, guildPrefix, pingRoleID, channelForMessages, expectedterrcount, intervalForPing, hasbeenran) # this changes back to false to simulate change becuase this wasnt complicated enough
            await asyncio.sleep(20/len(guilds_to_check)) # this will make it so EVERY guild is checked every 30s. max requests a minute is 180 for api


        
intents = discord.Intents.default()
client = MyClient(intents=intents)

@client.event
async def on_ready():
    print(f'Logged in as {client.user} (ID: {client.user.id})')
    print('------')


@client.tree.command(description="Configuration for the Dernal War Detector.")
@app_commands.describe(
    channel='Channel to set',
    guild_prefix='Prefix of the guild to track Ex: SEQ, ICo.',
    role='Role to be pinged (optional)',
    interval='The cooldown on the pings in minutes (optional)',
)
async def detector(interaction: discord.Interaction, channel: Union[discord.TextChannel], guild_prefix: str, role: Optional[discord.Role] = None, interval: Optional[int] = None):
    global guildsBeingTracked
    message = (f'<#{channel.id}> now set! No role will be pinged when territory is lost.')
    if len(guildsBeingTracked) > 15: # this is a number out of my ass, just makes sense that we shouldnt have 15+ guilds being tracked, as that would use a lot of our ratelimit of 120 a minute.
        message = (f'You have too many guilds being tracked with Detector! The maximum limit you can have is 15. There is no way to delete them right now besides restarting the bot. So tough shit I guess.')
        success = False
    elif role and interval: # This is for when both are present
        message = (f'<#{channel.id}> now set! Role "{role}" will be pinged every time you lose territory, with a cooldown of {interval} minutes.')
        success = True
    else: # success, but also yknow no role
        message = (f'<#{channel.id}> now set! No role will be pinged when territory is lost.')
        success = True
    
    if success: # With this we should proceed with adding it to the queue.
        #print(guildsBeingTracked)
        guildsBeingTracked[guild_prefix] = {'channelForMessages': channel, 'pingRoleID': str(role.id) if role else "", 'intervalForPing': interval if interval else ""}
        #print(guildsBeingTracked)
        if 'task' in globals() and task and not task.done(): # all to restart when detector is running
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                logger.error("Task was cancelled. Not sure how.")
        task = asyncio.create_task(client.runBackgroundDetector(guildsBeingTracked, guild_prefix))
        await interaction.response.send_message(message)

@client.tree.command(description="Shows stats and information about the selected guild")
@app_commands.describe(
    name='Prefix or Name of the guild search Ex: TAq, Calvish.',
)
async def guild(interaction: discord.Interaction, name: str):
    global guildLookupCooldown
    current_time = time.time()
    if int(current_time - guildLookupCooldown) <= 3: # this sets a cooldown so only 20 requests can be made per minute.
        await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {current_time - guildLookupCooldown} seconds.", ephemeral=True)
    
    if len(name) >= 5: # this is checking if its a name
        URL = "https://beta-api.wynncraft.com/v3/guild/"+str(name)
    else:
        URL = "https://beta-api.wynncraft.com/v3/guild/prefix/"+str(name)
    try:
        request = requests.get(URL)
    except requests.exceptions.HTTPError as err:
        logger.error("Error getting request:", err)
    if request.ok: # real guild
        await interaction.response.send_message(embed=await guildLookup(name, request))
    else:
        await interaction.response.send_message(f"'{name}' is a unknown prefix or guild name.", ephemeral=True)

client.run('Bot Token Here')