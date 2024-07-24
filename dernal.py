import requests
from datetime import datetime
import time
from dateutil.relativedelta import relativedelta as rd

# TODO: maybe make it a bot or sum; write in code that fixes the code maybe breaking if 2 or more terrs was taken within 1 check.

guildPrefix = "FUB" #self explanatory
initTerrMessae = True # incase you want to turn off the first message you get when starting.
pingRoleID = "1181028659780399144" # needs to be a role, a person could be used but that is dumb and requires change, also remove if you dont want pings
webhookURL = "https://discord.com/api/webhooks/1264772391960055829/vo6zvQo3EZjn1NSt2_R4o8lF2qr3K6O7MDQwbALarSk0eBLMT81d1Ofa3zqN_Qt46FIz" #discord webhook URL
timesinceping = 0


untainteddata = []
intervals = ['days','hours','minutes','seconds']

def sendEmbed(attacker, defender, terrInQuestion, timeLasted, attackerTerrBefore, attackerTerrAfter, defenderTerrBefore, defenderTerrAfter):
    data = { # we need it sadly i think idk i ripped this code off someones github
    "content" : "",
    }
    if attacker == guildPrefix:
        data["embeds"] = [
            {
                "title" : "🟢 **Gained Territory!**", 
                "description" : "  **"+terrInQuestion+"**\nAttacker: **"+attacker+"** ("+attackerTerrBefore+" -> "+attackerTerrAfter+")\nDefender: **"+defender+"** ("+defenderTerrBefore+" -> "+defenderTerrAfter+")\n\nThe territory lasted "+timeLasted+".",
                "footer": {
                    "text": "https://github.com/badpinghere/dernal  •  "+datetime.now().strftime("%m/%d/%Y, %I:%M %p")+"" # i didnt like working with timestamp for embed so here is better way
                },
                "color": "5763719",
            }
        ]
        requests.post(webhookURL, json=data)
    else:
        data["embeds"] = [
            {
                "title" : "🔴 **Lost Territory!**", 
                "description" : " **"+terrInQuestion+"**\nAttacker: **"+attacker+"** ("+attackerTerrBefore+" -> "+attackerTerrAfter+")\nDefender: **"+defender+"** ("+defenderTerrBefore+" -> "+defenderTerrAfter+")\n\nThe territory lasted "+timeLasted+".",
                "footer": {
                    "text": "https://github.com/badpinghere/dernal  •  "+datetime.now().strftime("%m/%d/%Y, %I:%M %p")+""
                },
                "color": "15548997",
            }
        ]
        requests.post(webhookURL, json=data)
        if pingRoleID:
            if int(time.time) - timesinceping >= 900:
                timesinceping = datetime.now()
                requests.post(webhookURL, json={"content": "<@&"+pingRoleID+">"})
    
        

def storeteritories(jsondata, guildPrefix, resetdata):
    global territoryInfoVariable
    global territoryInfo
    territoryInfoVariable = []
    if resetdata == True:
        territoryInfo = []
        for terrname, inner in jsondata.items():
            if inner['guild']['prefix'] == guildPrefix:
                isoIime = str(inner['acquired']) # they do some like iso 8061 shit fuck if i know
                reworkedDate = datetime.fromisoformat(isoIime.replace("Z", "+00:00"))
                timestamp = reworkedDate.timestamp() #messy code but works
                territoryInfo.append([terrname, str(timestamp)])
    else:
        for terrname, inner in jsondata.items():
            if inner['guild']['prefix'] == guildPrefix:
                isoIime = str(inner['acquired']) # they do some like iso 8061 shit fuck if i know
                reworkedDate = datetime.fromisoformat(isoIime.replace("Z", "+00:00"))
                timestamp = reworkedDate.timestamp() #messy code but works
                territoryInfoVariable.append([terrname, str(timestamp)])
    
def getTerrData(firstTime):
    global untainteddata
    global untainteddataOLD
    URL = "https://api.wynncraft.com/v3/guild/list/territory"
    r = requests.get(URL)
    stringdata = str(r.json())
    if untainteddata: #checks if it was used before if not save the last one to a different variable. only useful for time when gaind a territory.
        untainteddataOLD = untainteddata
    untainteddata = r.json()
    storeteritories(r.json(), guildPrefix, firstTime)
    return stringdata

def checkterritories(): # im about 95% sure this will break if a guild took or lost 2 territories within a check, so i would advise not losing 2+ territories within 11s. i also cant check it because its rare.
    global expectedterrcount
    time.sleep(11) # waits 11s, not tryna get ratelimited
    terrcount = getTerrData(False).count(guildPrefix)
    if expectedterrcount > terrcount: # lost a territory
        set1 = set(tuple(item) for item in territoryInfo)
        set2 = set(tuple(item) for item in territoryInfoVariable)
        lostTerritoriesDiff = set1.symmetric_difference(set2)
        lostTerritoriesList = [list(item) for item in lostTerritoriesDiff]
        for i in lostTerritoriesList:
            reworkedDate = datetime.fromisoformat(untainteddata[i[0]]['acquired'].replace("Z", "+00:00"))
            timestamp = reworkedDate.timestamp() 
            elapsed_time = int(timestamp) - int(float(i[1]))
            x = rd(seconds=elapsed_time)
            opponentTerrCountBefore = str(untainteddataOLD).count(untainteddata[i[0]]['guild']['prefix'])
            opponentTerrCountAfter = str(untainteddata).count(untainteddata[i[0]]['guild']['prefix']) # this will maybe just be wrong if multiple were taken within 11s.
            sendEmbed(untainteddata[i[0]]['guild']['prefix'], guildPrefix, i[0], ' '.join('{} {}'.format(getattr(x,k),k) for k in intervals if getattr(x,k)), str(opponentTerrCountBefore), str(opponentTerrCountAfter), str(expectedterrcount), str(terrcount))
        expectedterrcount = getTerrData(True).count(guildPrefix)
        
    elif expectedterrcount < terrcount: #gained a territory
        set1 = set(tuple(item) for item in territoryInfo)
        set2 = set(tuple(item) for item in territoryInfoVariable)
        gainedTerritoriesDiff = set1.symmetric_difference(set2)
        gainedTerritoriesList = [list(item) for item in gainedTerritoriesDiff]
        for i in gainedTerritoriesList:
            reworkedDate = datetime.fromisoformat(untainteddataOLD[i[0]]['acquired'].replace("Z", "+00:00"))
            timestamp = reworkedDate.timestamp() 
            elapsed_time = int(float(i[1])) - int(timestamp)
            x = rd(seconds=elapsed_time)
            opponentTerrCountBefore = str(untainteddataOLD).count(untainteddataOLD[i[0]]['guild']['prefix'])
            opponentTerrCountAfter = str(untainteddata).count(untainteddataOLD[i[0]]['guild']['prefix']) # this will maybe just be wrong if multiple were taken within 11s.
            sendEmbed(guildPrefix, untainteddataOLD[i[0]]['guild']['prefix'], i[0], ' '.join('{} {}'.format(getattr(x,k),k) for k in intervals if getattr(x,k)), str(expectedterrcount), str(terrcount), str(opponentTerrCountBefore), str(opponentTerrCountAfter))
        expectedterrcount = getTerrData(True).count(guildPrefix)

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

# for inital setup.
message = ""
expectedterrcount = getTerrData(True).count(guildPrefix)
for i in territoryInfo:
    # for some reason i have to change the fucking float to int for the timestamp to work, sick work.
    timestamp = int(float(i[1]))
    message += f"\nTerritory Name: {i[0]}   Active since: <t:{timestamp}:F>"
realmessagetho = "Dernal.py is set up! Here are your guild's current territories:"+message
printthing = split_message(realmessagetho)
print("Everything is likely working!")
if initTerrMessae == True:
    for x in printthing:
        data = {"content": x}
        requests.post(webhookURL, json=data)


while True:
    checkterritories()