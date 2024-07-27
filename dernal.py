import requests
from datetime import datetime
import time
from dateutil.relativedelta import relativedelta as rd
import configparser

# TODO: maybe make it a bot or sum; fix it saying like (40 -> 38)  (40 -> 38)  instead of (40 -> 39) (39 -> 38); check if days or similar is '1' because 1 days is improper grammar

config = configparser.ConfigParser()
config.read('config.ini')

guildPrefix = config['SETTINGS']['guildPrefix'] 
initTerrMessae = config['SETTINGS']['initTerrMessae'] 
pingRoleID = config['SETTINGS']['pingRoleID']
webhookURL = config['SETTINGS']['webhookURL']
timesinceping = 0


untainteddata = {}
untainteddataOLD = {}
intervals = ['days','hours','minutes','seconds']

def sendEmbed(attacker, defender, terrInQuestion, timeLasted, attackerTerrBefore, attackerTerrAfter, defenderTerrBefore, defenderTerrAfter):
    global timesinceping
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
            current_time = time.time()
            if current_time  - int(timesinceping) >= 900:
                timesinceping = current_time
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

def checkterritories():
    global expectedterrcount
    time.sleep(60)  # Waits 10s to avoid rate-limiting
    getTerrData(False) # gets untainteddataOLD with info
    gainedTerritories = {}
    lostTerritories = {}
    for territory, data in untainteddata.items():
        old_guild = untainteddataOLD[str(territory)]['guild']['prefix']
        new_guild = data['guild']['prefix']
        if old_guild == guildPrefix and new_guild != guildPrefix:
            lostTerritories[territory] = data
        elif old_guild != guildPrefix and new_guild == guildPrefix:
            gainedTerritories[territory] = data
    #print("Gained Territories: ", gainedTerritories)
    #print("Lost Territories: ", lostTerritories)
    terrcount = expectedterrcount # this is what will fix (40 -> 38)
    if lostTerritories: # checks if its empty, no need to run if it is
        for i in lostTerritories:
            reworkedDate = datetime.fromisoformat(untainteddataOLD[i]['acquired'].replace("Z", "+00:00")) # gets the time from the old data
            timestamp = reworkedDate.timestamp()
            reworkedDateNew = datetime.fromisoformat(lostTerritories[i]['acquired'].replace("Z", "+00:00")) # gets the time from the new data
            timestampNew = reworkedDateNew.timestamp() 
            elapsed_time = int(timestampNew) - int(timestamp)
            x = rd(seconds=elapsed_time)
            
            opponentTerrCountBefore = str(untainteddataOLD).count(lostTerritories[str(i)]['guild']['prefix'])
            opponentTerrCountAfter = str(untainteddata).count(lostTerritories[str(i)]['guild']['prefix']) # this will maybe just be wrong if multiple were taken within 11s.
            terrcount -= 1
            sendEmbed(lostTerritories[i]['guild']['prefix'], guildPrefix, i, ' '.join('{} {}'.format(getattr(x,k),k) for k in intervals if getattr(x,k)), str(opponentTerrCountBefore), str(opponentTerrCountAfter), str(expectedterrcount), str(terrcount))
            expectedterrcount = terrcount
    if gainedTerritories: # checks if its empty, no need to run if it is
        for i in gainedTerritories:
            reworkedDate = datetime.fromisoformat(untainteddataOLD[i]['acquired'].replace("Z", "+00:00")) # gets the time from the old data
            timestamp = reworkedDate.timestamp()
            reworkedDateNew = datetime.fromisoformat(gainedTerritories[i]['acquired'].replace("Z", "+00:00")) # gets the time from the new data
            timestampNew = reworkedDateNew.timestamp() 
            elapsed_time = int(timestampNew)- int(timestamp)
            x = rd(seconds=elapsed_time)
            
            #opponentTerrCountBefore = str(untainteddataOLD).count(untainteddataOLD[str(i)]['guild']['prefix'])
            #opponentTerrCountAfter = str(untainteddata).count(untainteddataOLD[str(i)]['guild']['prefix']) # this will maybe just be wrong if multiple were taken within 11s.
            terrcount+=1
            sendEmbed(guildPrefix, untainteddataOLD[i]['guild']['prefix'], i, ' '.join('{} {}'.format(getattr(x,k),k) for k in intervals if getattr(x,k)),str(expectedterrcount), str(terrcount), str(opponentTerrCountBefore), str(opponentTerrCountAfter))
            expectedterrcount = terrcount
    if gainedTerritories or lostTerritories: # just for resetting our variables
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
if initTerrMessae == "True": # .ini cant do true or false values so gotta be string
    for x in printthing:
        data = {"content": x}
        requests.post(webhookURL, json=data)

while True:
    checkterritories()