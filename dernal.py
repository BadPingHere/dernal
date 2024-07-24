import requests
from datetime import datetime
import time
from dateutil.relativedelta import relativedelta as rd

# TODO: cool embed, basically copy boxfot or whatver it is. also maybe make it a bot or sum

guildPrefix = "" #self explanatory
pingRoleID = "" # needs to be a role, a person could be used but that is dumb and requires change, also remove if you dont want pings
webhookURL = "" #discord webhook URL


untainteddata = []
intervals = ['days','hours','minutes','seconds']

def storeteritories(jsondata, guildPrefix, firstTime):
    global territoryInfoVariable
    global territoryInfo
    territoryInfoVariable = []
    if firstTime == True:
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
    time.sleep(15) # waits 15s, not tryna get ratelimited
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
            data = {"content": ""+i[0]+" was taken by "+untainteddata[i[0]]['guild']['name']+" ("+untainteddata[i[0]]['guild']['prefix']+")! "+i[0]+" lasted "+' '.join('{} {}'.format(getattr(x,k),k) for k in intervals if getattr(x,k))+"."}
            requests.post(webhookURL, json=data)
            if pingRoleID:
                requests.post(webhookURL, json={"content": "<@&"+pingRoleID+">"})
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
            data = {"content": ""+untainteddata[i[0]]['guild']['name']+" ("+guildPrefix+") has taken control of "+i[0]+"! "+i[0]+" lasted "+' '.join('{} {}'.format(getattr(x,k),k) for k in intervals if getattr(x,k))+"."}
            requests.post(webhookURL, json=data)
            if pingRoleID:
                requests.post(webhookURL, json={"content": "<@&"+pingRoleID+">"})
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
for x in printthing:
    data = {"content": x}
    requests.post(webhookURL, json=data)


while True:
    checkterritories()