# Depending on the sleep variable, this can take between 3-24hrs. At 1 it will take around 6hrs for reference
from alive_progress import alive_bar
import time
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from lib.makeRequest import makeRequest
import csv
import json


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATABASE_DIR = PROJECT_ROOT / "database"
CSVFILE = DATABASE_DIR / "guildlist.csv" # all of this just to get autowrite to database folder
JSONFILE = DATABASE_DIR / "allguilds.json" # all of this just to get autowrite to database folder

SLEEP = 1

def main(collect=False, write=False):
    #? Collect mode: Writes allGuilds to json, to easily parse through data.
    #? Write mode: Writes suitableGuilds to csv
    
    suitableGuilds = []
    allGuilds = {}
    success, r = makeRequest("https://api.wynncraft.com/v3/guild/list/guild")
    if not success:
        print("Error getting guild list. Try again later.")
    else:
        jsonData = r.json()
        countOfGuilds = len(jsonData)
        with alive_bar(countOfGuilds) as bar:
            for prefix, guild_info in jsonData.items():
                uuid = guild_info["uuid"]
                success, r = makeRequest(f"https://api.wynncraft.com/v3/guild/uuid/{uuid}")
                if not success:
                    print(f"Unknown guild uuid, skipping: {uuid}")
                    bar()
                    continue
                jsonData = r.json()
                allGuilds[uuid] = {"prefix": prefix, "level": jsonData["level"], "totalMembers": jsonData["members"]["total"]}
                bar()
                time.sleep(SLEEP)

        count = {"suitable": 0, "totalPlayers": 0}
        for uuid, guildData in allGuilds.items():
            if guildData["level"] >= 1 and int(guildData["totalMembers"]) >= 7: # Any guild with 7 or more members
                suitableGuilds.append(uuid)
                count["suitable"] += 1
                count["totalPlayers"] += int(guildData["totalMembers"])
      
        print(f"""
                Total Guilds: {countOfGuilds}
                Suitable guilds: {count["suitable"]} | Total Players: {count["totalPlayers"]}
        """)

        if write:
            with CSVFILE.open("w", newline="") as myfile:
                wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
                for guild in suitableGuilds:
                    wr.writerow([guild])  # Write each UUID in a separate row
                    
        if collect:
            with JSONFILE.open("w", encoding='utf-8') as f:
                json.dump(allGuilds, f, ensure_ascii=False, indent=4)
            
def search(): # helper function to figure out who to collect from
    with open(JSONFILE) as json_data:
        allGuilds = json.load(json_data)
        
    countOfGuilds = len(allGuilds)
    count_1 = {"suitable": 0, "totalPlayers": 0}
    count_2 = {"suitable": 0, "totalPlayers": 0}
    count_3 = {"suitable": 0, "totalPlayers": 0}
    count_4 = {"suitable": 0, "totalPlayers": 0}
    for uuid, guildData in allGuilds.items():
        if guildData["level"] >= 30 and int(guildData["totalMembers"]) >= 1: # Current
            count_1["suitable"] += 1
            count_1["totalPlayers"] += int(guildData["totalMembers"])
        
        if guildData["level"] >= 25 and int(guildData["totalMembers"]) >= 7: # Low Level, High Player Req
            count_2["suitable"] += 1
            count_2["totalPlayers"] += int(guildData["totalMembers"])
        
        if guildData["level"] >= 1 and int(guildData["totalMembers"]) >= 10: # High Level, low player req
            count_3["suitable"] += 1
            count_3["totalPlayers"] += int(guildData["totalMembers"])

        if guildData["level"] >= 1 and int(guildData["totalMembers"]) >= 3: # Lower Current
            count_4["suitable"] += 1
            count_4["totalPlayers"] += int(guildData["totalMembers"])
            
    print(f"""
            Total Guilds: {countOfGuilds}
            Suitable guilds - 1: {count_1["suitable"]} | Average Players: {round(count_1["totalPlayers"] / count_1["suitable"], 2)}| Total Players: {count_1["totalPlayers"]}
            Suitable guilds - 2: {count_2["suitable"]} | Average Players: {round(count_2["totalPlayers"] / count_2["suitable"], 2)} | Total Players: {count_2["totalPlayers"]}
            Suitable guilds - 3: {count_3["suitable"]} | Average Players: {round(count_3["totalPlayers"] / count_3["suitable"], 2)} | Total Players: {count_3["totalPlayers"]}
            Suitable guilds - 4: {count_4["suitable"]} | Average Players: {round(count_4["totalPlayers"] / count_4["suitable"], 2)} | Total Players: {count_4["totalPlayers"]}
    """)
    
#main(collect=False, write=True)
search()