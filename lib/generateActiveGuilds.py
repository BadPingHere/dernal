# This script will on average take 1-3 hours! Have fun!
from alive_progress import alive_bar
import time
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from lib.makeRequest import makeRequest
import csv


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATABASE_DIR = PROJECT_ROOT / "database"
FILE = DATABASE_DIR / "guildlist.csv" # all of this just to get autowrite to database folder

def main():
    suitableGuilds = []
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
                    continue
                jsonData = r.json()
                if int(jsonData["level"]) >= 45 and int(jsonData["members"]["total"]) >= 3: # so we can thin the herd. because storage will be a BITCH, along with api requests.
                    suitableGuilds.append(uuid)
                bar()
                time.sleep(3)
        print(countOfGuilds, len(suitableGuilds))
        with FILE.open("w", newline="") as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            for guild in suitableGuilds:
                wr.writerow([guild])  # Write each UUID in a separate row
            
main()