# This script will on average take 2+ hours! Have fun!

import requests
from alive_progress import alive_bar
import time
import csv

def makeRequest(URL): # the world is built on nested if else statements.
    while True:
        try:
            session = requests.Session()
            session.trust_env = False
            
            r = session.get(URL)
            r.raise_for_status()
        except requests.exceptions.RequestException as err:
            print("error", err)
            time.sleep(3)
            continue
        if r.ok:
            return r
        else:
            print("not ok", r.status_code)
            time.sleep(3)
            continue

def main():
    suitableGuilds = []
    r = makeRequest("https://api.wynncraft.com/v3/guild/list/guild")
    jsonData = r.json()
    countOfGuilds = len(jsonData)
    with alive_bar(countOfGuilds) as bar:
        for prefix, guild_info in jsonData.items():
            uuid = guild_info["uuid"]
            r = makeRequest(f"https://api.wynncraft.com/v3/guild/uuid/{uuid}")
            jsonData = r.json()
            if int(jsonData["level"]) > 30 and int(jsonData["members"]["total"]) > 15: # so we can thin the herd. because storage will be a BITCH, along with api requests.
                suitableGuilds.append(uuid)
            bar()
            time.sleep(0.2)
    print(countOfGuilds, len(suitableGuilds))
    with open('guildlist.csv', 'w', newline='') as myfile:
        wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
        for guild in suitableGuilds:
            wr.writerow([guild])  # Write each UUID in a separate row
            
main()