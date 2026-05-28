import pytz
import time
import csv
import sqlite3
from datetime import datetime, timedelta
import os
import asyncio
import zipfile
import platform
import logging
import logging.handlers
import sys
import json
from dateutil.relativedelta import relativedelta
from pathlib import Path
import threading
sys.path.append(str(Path(__file__).resolve().parent.parent))
from lib.makeRequest import makeRequest
def get_utc_now():
    if platform.system() == "Windows":
        return datetime.utcnow()
    return datetime.now(pytz.UTC)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATABASE_DIR = os.path.join(BASE_DIR, "database")
BACKUP_DIR = os.path.join(DATABASE_DIR, "backups")
LOG_FILE = os.path.join(BASE_DIR, "activitySQL.log")

logger = logging.getLogger('activity')
logger.setLevel(logging.DEBUG)
handler = logging.handlers.RotatingFileHandler(
    filename=LOG_FILE,
    encoding='utf-8',
    maxBytes=256 * 1024 * 1024,  # 256 Mib
)
dt_fmt = '%Y-%m-%d %H:%M:%S'
formatter = logging.Formatter('{asctime} - {levelname:<8} - {name}: {message}', dt_fmt, style='{')
handler.setFormatter(formatter)
logger.addHandler(handler)

def connectDB():
    logger.info("Connecting to activity database...")
    os.makedirs(DATABASE_DIR, exist_ok=True)
    db_path = os.path.join(DATABASE_DIR, "activity.db")
    conn = sqlite3.connect(db_path, isolation_level=None)

    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA auto_vacuum=INCREMENTAL")
    conn.execute("PRAGMA wal_autocheckpoint=2000")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA mmap_size=536870912;")  # 512MB  mmap
    conn.execute("PRAGMA cache_size=-40000")  # 40MB cache
    conn.execute("PRAGMA temp_store=MEMORY")

    if not conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='guilds'").fetchone():
        createTables(conn)

    return conn

def createTables(conn):
    logger.info("Creating database tables...")
    schema_path = os.path.join(DATABASE_DIR, "schema.sql")
    with open(schema_path, "r") as schema_file:
        schema_script = schema_file.read()
        conn.executescript(schema_script)
    logger.info("Tables created successfully")

def storeGuildData(conn, jsonData, run_id):
    # Get necessary data for all inserts
    guild_uuid = jsonData.get("uuid")
    guild_name = jsonData.get("name")
    guild_prefix = jsonData.get("prefix")
    guild_xp = jsonData.get("xpPercent")
    guild_level = jsonData.get("level")
    guild_territories = jsonData.get("territories")
    guild_wars = jsonData.get("wars") or 0
    guild_onlineMembers = jsonData.get("online")
    guild_totalMembers = jsonData.get("members").get("total")
    guild_guildRaids = jsonData.get("raids") or 0
    
    # Store guild table info
    conn.execute(
    """
    INSERT INTO guilds(guild_uuid, name, prefix, timestamp)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(guild_uuid) DO UPDATE SET name=excluded.name, prefix=excluded.prefix, timestamp=excluded.timestamp
    """,
        (guild_uuid, guild_name, guild_prefix, get_utc_now().isoformat())
    )
    
    # Store guild_snapshots table info
    conn.execute(
    """
    INSERT INTO guild_snapshots(guild_uuid, timestamp, level, xp_percent, territories, wars, online_members, total_members, guild_raids)
    VALUES (?, ?, ?, ?, ? ,?, ?, ?, ?)
    """,
        (guild_uuid, get_utc_now().isoformat(), guild_level, guild_xp, guild_territories, guild_wars, guild_onlineMembers, guild_totalMembers, guild_guildRaids)
    )
    
    # Store guild_season_ratings table info
    season_ranks = jsonData.get("seasonRanks") or {}
    season_params = [
        (guild_uuid, int(season), int(info["rating"]))
        for season, info in season_ranks.items()
        if int(info.get("rating") or 0) > 0
    ]
    if season_params:
        conn.executemany("""
        INSERT INTO guild_season_ratings(guild_uuid, season, rating)
        VALUES (?, ?, ?)
        ON CONFLICT(guild_uuid, season) DO UPDATE SET rating = excluded.rating
        """, season_params)

    # Store users table info
    for role, members in jsonData["members"].items():
        if role != "total":
            for name, member in members.items():
                player_uuid = member.get("uuid")
                username = name
                
                conn.execute(
                """
                INSERT OR IGNORE INTO users(player_uuid, username, run_id)
                VALUES (?, ?, ?)
                """,
                    (player_uuid, username, run_id)
                )

def storePlayerData(conn, jsonData, location):
    #! Things missing in player endpoint vs guild endpoint: player contribution
    rows = []
    try:
        if location == "guild":  # Get necessary data for all inserts via guild endpoint
            for role, members in jsonData["members"].items():
                if role != "total":
                    for name, member in members.items():
                        # First, we check restrictions
                        restrictions = member.get("restrictions", {})
                        if restrictions.get("mainAccess") is True: # globalData gone, firstJoin gone, playtime gone, so we dont do those
                            playtime = mobs_killed = wars = total_dungeons = dungeon_dict = total_raids = raid_dict = total_graids = graid_dict = online = None
                        else:
                            globalData = member.get("globalData") or {}
                            playtime = globalData.get("playtime")
                            wars = globalData.get("wars")
                            mobs_killed = globalData.get("mobsKilled")
                            total_dungeons = (globalData.get("dungeons") or {}).get("total") # this should, if no dungeons done at all, default to None
                            dungeon_dict = (globalData.get("dungeons") or {}).get("list") # this should, if no dungeons done at all, default to None
                            total_raids = (globalData.get("raids") or {}).get("total") # this should, if no raids done at all, default to None
                            raid_dict = (globalData.get("raids") or {}).get("list") # this should, if no raids done at all, default to None
                            total_graids = (globalData.get("guildRaids") or {}).get("total") # this should, if no graids done at all, default to None
                            graid_dict = (globalData.get("guildRaids") or {}).get("list") # this should, if no graids done at all, default to None
                            online = member.get("online")
                            
                        guild_uuid = jsonData.get("uuid")
                        guild_name = jsonData.get("name")
                        guild_prefix = jsonData.get("prefix")
                        player_uuid = member.get("uuid")
                        last_join = member.get("lastJoin")
                        contribution = member.get("contributed")
                        username = name
                        
                        rows.append({
                            "guild_uuid": guild_uuid,
                            "guild_name": guild_name,
                            "guild_prefix": guild_prefix,
                            "player_uuid": player_uuid,
                            "online": online,
                            "last_join": last_join,
                            "playtime": playtime,
                            "contribution": contribution,
                            "wars": wars,
                            "mobs_killed": mobs_killed,
                            "total_dungeons": total_dungeons,
                            "dungeon_dict": dungeon_dict,
                            "total_raids": total_raids,
                            "raid_dict": raid_dict,
                            "total_graids": total_graids,
                            "graid_dict": graid_dict,
                            "restrictions": restrictions,
                            "username": username,
                        })

        elif location == "player":  # Get necessary data for all inserts via player endpoint
            # First, we check restrictions
            restrictions = jsonData.get("restrictions", {})
            if restrictions.get("mainAccess") is True: # globalData gone, firstJoin gone, playtime gone, so we dont do those
                playtime = mobs_killed = wars = total_dungeons = dungeon_dict = total_raids = raid_dict = total_graids = graid_dict = None
            else:
                playtime = jsonData.get("playtime")
                wars = jsonData["globalData"].get("wars")
                mobs_killed = jsonData["globalData"].get("mobsKilled")
                total_dungeons = (jsonData["globalData"].get("dungeons") or {}).get("total") # this should, if no dungeons done at all, default to None
                dungeon_dict = (jsonData["globalData"].get("dungeons") or {}).get("list") # this should, if no dungeons done at all, default to None
                total_raids = (jsonData["globalData"].get("raids") or {}).get("total") # this should, if no raids done at all, default to None
                raid_dict = (jsonData["globalData"].get("raids") or {}).get("list") # this should, if no raids done at all, default to None
                total_graids = (jsonData["globalData"].get("guildRaids") or {}).get("total") # this should, if no graids done at all, default to None
                graid_dict = (jsonData["globalData"].get("guildRaids") or {}).get("list") # this should, if no graids done at all, default to None

            if restrictions.get("mainAccess") is True: # i lowk forgot this was a thing, i think hides only online? maybe lastjoin too?
                online = None
            else:
                online = jsonData.get("online")
                
            guild_uuid = (jsonData.get("guild") or {}).get("uuid")
            guild_name = (jsonData.get("guild") or {}).get("name")
            guild_prefix = (jsonData.get("guild") or {}).get("prefix")
            username = jsonData.get("username")
            player_uuid = jsonData.get("uuid")
            last_join = jsonData.get("lastJoin")
            contribution = None # contribution not avaible for non-tracked guilds

            rows.append({
                "guild_uuid": guild_uuid,
                "guild_name": guild_name,
                "guild_prefix": guild_prefix,
                "player_uuid": player_uuid,
                "online": online,
                "last_join": last_join,
                "playtime": playtime,
                "contribution": contribution,
                "wars": wars,
                "mobs_killed": mobs_killed,
                "total_dungeons": total_dungeons,
                "dungeon_dict": dungeon_dict,
                "total_raids": total_raids,
                "raid_dict": raid_dict,
                "total_graids": total_graids,
                "graid_dict": graid_dict,
                "restrictions": restrictions,
                "username": username,
            })
        # Insert all rows of players

        snapshotParams = [
            (
                row["guild_uuid"], row["player_uuid"], get_utc_now().isoformat(),
                row["online"], row["last_join"], row["playtime"], row["contribution"],
                row["wars"], row["mobs_killed"], row["total_dungeons"],
                row["total_raids"], row["total_graids"]
            )
            for row in rows
        ]

        conn.executemany("""
        INSERT INTO player_snapshots(guild_uuid, player_uuid, timestamp, online, last_join, playtime, contribution, wars, mobs_killed, total_dungeons, total_raids, total_graids)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, snapshotParams)

        currentStatsParams = [
            (
                row["player_uuid"], json.dumps(row["dungeon_dict"]), json.dumps(row["raid_dict"]),
                json.dumps(row["graid_dict"]), json.dumps(row["restrictions"]), get_utc_now().isoformat()
            )
            for row in rows
        ]

        conn.executemany("""
        INSERT INTO player_current_stats(player_uuid, dungeon_dict, raid_dict, graid_dict, restrictions, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(player_uuid) DO UPDATE SET
            dungeon_dict = excluded.dungeon_dict,
            raid_dict    = excluded.raid_dict,
            graid_dict   = excluded.graid_dict,
            restrictions = excluded.restrictions,
            updated_at   = excluded.updated_at
        """, currentStatsParams)

        historyParams = []
        
        for row in rows: # We use this to see if they need a update in history
            last = conn.execute(
                "SELECT guild_uuid FROM user_history WHERE player_uuid = ? ORDER BY timestamp DESC LIMIT 1",
                (row["player_uuid"],)
            ).fetchone()
            
            if last is None or last[0] != row["guild_uuid"]:
                historyParams.append((
                    row["player_uuid"], row["username"], row["guild_uuid"],
                    row["guild_name"], row["guild_prefix"], get_utc_now().isoformat()
                ))

        if historyParams:
            conn.executemany("""
                INSERT INTO user_history(player_uuid, username, guild_uuid, guild_name, guild_prefix, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """, historyParams)
    except Exception as e:
        logger.error(f"Failed to fetch/store via {location}: {e}")
        logger.error(f"JsonData If applicable: {jsonData}")

def cleanDatabase(conn):
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
        logger.info("Database cleaned up and trunucated.")
    except Exception:
        logger.exception("Error during database cleanup:")

def cleanupOldData(conn, run_id, batchSize=1000):
    cutoffDate = (get_utc_now() - timedelta(days=30)).isoformat() # Cleanup all data older than 30d
    logger.info(f"Starting data cleanup for records older than {cutoffDate}")
    activityCur = conn.cursor()

    def batchDelete(cursor, tableName, cutoff, batchSize, conn):
        total_deleted = 0
        logger.info(f"Starting cleanup for {tableName}...")
        while True:
            cursor.execute(f"""
                DELETE FROM {tableName}
                WHERE timestamp < ?
                AND rowid IN (
                    SELECT rowid FROM {tableName}
                    WHERE timestamp < ?
                    LIMIT ?
                )
            """, (cutoff, cutoff, batchSize))
            deleted = cursor.rowcount
            total_deleted += deleted
            if deleted > 0:
                logger.info(f"Deleted {deleted} records from {tableName} (Total: {total_deleted})")
                conn.commit()
            if deleted < batchSize:
                break
        return total_deleted
    
    def deleteUsers(cursor, run_id):
        logger.info(f"Starting cleanup for users...")
        cursor.execute(f"""
            DELETE FROM users
            WHERE run_id != ?
        """, (run_id,))
        deleted = cursor.rowcount
        return deleted
    
    guildSnapshotCount = memberSnapshotCount = guildCount = userCount = -1 #incase anything breaks
    try:
        guildSnapshotCount = batchDelete(activityCur, "guild_snapshots", cutoffDate, batchSize, conn)
        memberSnapshotCount = batchDelete(activityCur, "player_snapshots", cutoffDate, batchSize, conn)
        guildCount = batchDelete(activityCur, "guilds", cutoffDate, batchSize, conn)
        userCount = deleteUsers(activityCur, run_id)
        
    except Exception as e:
        logger.exception(f"There was an error while cleaning up old data:")
    finally:
        activityCur.close()
        logger.info(
            f"Cleanup Stats:\n"
            f"  Guild Snapshots: {guildSnapshotCount}\n"
            f"  Member Snapshots: {memberSnapshotCount}\n"
            f"  Guilds: {guildCount}\n"
            f"  Users: {userCount}"
        )       

def createBackup():
    backup_flag_file = os.path.join(BACKUP_DIR, "last_backup.txt")
    current_month = datetime.now().strftime("%Y_%m")
    lastMonth = (datetime.now() - relativedelta(months=1)).strftime("%Y_%m") # We need last month because we are creating a bakcup for the last month so we want it named right
    
    # Check if we already did backup this month
    if os.path.exists(backup_flag_file):
        with open(backup_flag_file, "r") as f:
            last_backup = f.read().strip()
            if last_backup == current_month:
                logger.info("Monthly backup already exists, skipping...")
                return

    # Only backup on day 1
    if datetime.now().day != 1:
        return

    logger.info("Starting monthly backup creation...")
    os.makedirs(BACKUP_DIR, exist_ok=True)
    activityPath = os.path.join(DATABASE_DIR, "activity.db")
    activityZipPath = os.path.join(BACKUP_DIR, f"activity_backup_{lastMonth}.zip")

    try: # create backups
        logger.info("Creating activityPath backup...")
        with zipfile.ZipFile(activityZipPath, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(activityPath, os.path.basename(activityPath))

        with open(backup_flag_file, "w") as f:
            f.write(current_month)

        backup_size = os.path.getsize(activityZipPath) / (1024 * 1024)
        logger.info(f"Guild backup created: {activityZipPath} (Size: {backup_size:.2f} MB)")
    except Exception as e:
        logger.info(f"Error creating monthly backup: {e}")

def getUntrackedPlayers(conn, players_dict): # gets who isnt in the users table
    #start = time.perf_counter()
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS incoming_players(username TEXT PRIMARY KEY)")
    conn.execute("DELETE FROM incoming_players")
    
    conn.executemany(
        "INSERT OR IGNORE INTO incoming_players(username) VALUES (?)",
        [(username,) for username in players_dict.keys()]
    )
    
    rows = conn.execute("""
        SELECT i.username
        FROM incoming_players i
        LEFT JOIN users u ON i.username = u.username
        WHERE u.player_uuid IS NULL
    """).fetchall()
    
    conn.execute("DROP TABLE incoming_players")
    #end = time.perf_counter()
    #logger.info(f"getUntrackedPlayers time elapsed: {end - start:.6f} seconds.")
    return [row[0] for row in rows]

def main():
    run_id = int(time.time())
    logger.info("Starting main data collection...")
    guildlist_path = os.path.join(DATABASE_DIR, "guildlist.csv")

    with open(guildlist_path, mode="r") as file:
        reader = csv.reader(file)
        uuids = [row[0] for row in reader]

    logger.info(f"Found {len(uuids)} guilds to process")

    conn = connectDB()
    try:
        # we do all this ratio bullshit to alleviate pressure on api endpoints, make them do 100/m for 20m rather than 200/m for 10m then 0/m for 10m.
        success, r = makeRequest(f"https://api.wynncraft.com/v3/player")
        if not success:
            logger.info("Player list unavailable, giving up.") # this for sure wont happen but we gotta rock with it
            players = []
        else:
            playerDict = r.json().get("players", {})
            players = getUntrackedPlayers(conn, playerDict)

        num_guilds = len(uuids)
        num_players = len(players)

        if num_guilds == 0:
            ratio = 0
        else:
            ratio = num_players / num_guilds
        guild_index = 0
        player_index = 0
        accumulator = 0.0
        logger.info(f"Processing {num_guilds} guilds and {num_players} players (ratio ≈ {ratio:.2f})")
        while guild_index < num_guilds or player_index < num_players:
            if guild_index < num_guilds:
                uuid = uuids[guild_index]
                if guild_index % 100 == 0: # so we dont spam log...
                    logger.info(f"Processing guild {guild_index+1}/{num_guilds} (UUID: {uuid})")
                success, r = makeRequest(f"https://api.wynncraft.com/v3/guild/uuid/{uuid}")
                if not success:
                    logger.info(f"Skipping guild {uuid} as it no longer exists")
                    logger.info(f"r: {r}")
                else:
                    guild_data = r.json()
                    storeGuildData(conn, guild_data, run_id)
                    storePlayerData(conn, guild_data, "guild")
                    conn.commit()
                    time.sleep(0.05) # Sleep every guild so we can stretch this out to 20m ish
                guild_index += 1
                accumulator += ratio

            # process as many players as the accumulator says are due
            while accumulator >= 1.0 and player_index < num_players:
                username = players[player_index]
                success, r = makeRequest(f"https://api.wynncraft.com/v3/player/{username}?fullResult")
                if not success:
                    logger.error(f"Unsuccessful request! Success is {success}, r is {r}")
                    player_index += 1
                    accumulator -= 1.0
                    continue
                try:
                    jsonData = r.json()
                    storePlayerData(conn, jsonData, "player")
                    conn.commit()
                except Exception as e:
                    logger.error(f"Unsuccessful request! Success is {success}")
                    logger.error(f"Failed to fetch username {username}: {e}")
                
                player_index += 1
                accumulator -= 1.0

            # if guilds are exhausted, drain remaining players
            if guild_index >= num_guilds and player_index < num_players:
                while player_index < num_players:
                    username = players[player_index]
                    success, r = makeRequest(f"https://api.wynncraft.com/v3/player/{username}?fullResult")
                    if not success:
                        logger.error(f"Unsuccessful request! Success is {success}, r is {r}")
                        player_index += 1
                        continue
                    try:
                        jsonData = r.json()
                        storePlayerData(conn, jsonData, "player")
                        conn.commit()
                    except Exception as e:
                        logger.error(f"Unsuccessful request! Success is {success}")
                        logger.error(f"Failed to fetch username {username}: {e}")
                    
                    player_index += 1

    except Exception:
        logger.exception("Error in main data collection:")
    finally:
        logger.info("Final WAL checkpoint for activity.db...")
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        cleanDatabase(conn)
        return run_id

def vacuumDatabase(conn):
    try:
        logger.info("Starting database VACUUM...")
        conn.execute("PRAGMA incremental_vacuum(10000);")
        logger.info("Incremental VACUUM complete.")
    except Exception as e:
        logger.exception(f"Error during VACUUM: {e}")

def storeTerritories(stop_event=None):
    conn = connectDB()
    while True:
        if stop_event and stop_event.is_set():
            logger.info("storeTerritories: stop event received, exiting.")
            break
        
        success, r = makeRequest("https://api.wynncraft.com/v3/guild/list/territory")
        if not success:
            logger.error("Error getting territory data from Wynncraft API.")
        else:
            try:
                jsonData = r.json()
                rows = []
                for territory, data in jsonData.items():
                    guild = data.get("guild") or {}
                    guild_uuid   = guild.get("uuid")
                    guild_name   = guild.get("name")
                    guild_prefix = guild.get("prefix")
                    acquired     = data.get("acquired")
                    rows.append((territory, guild_uuid, guild_name, guild_prefix, acquired, territory, acquired))

                conn.executemany("""
                    INSERT INTO territory_changes (territory, guild_uuid, guild_name, guild_prefix, acquired)
                    SELECT ?, ?, ?, ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM territory_changes
                        WHERE territory = ?
                        AND acquired = ?
                    )
                """, rows)
                conn.commit()
            except Exception:
                logger.exception("storeTerritories: failed to process territory data.")

        time.sleep(15)

async def scheduledMainScript():
    vacuum = False
    while True:
        start_time = datetime.now()
        logger.info("Starting scheduled run...")

        try:
            run_id = main() # This is for putting all data in db
            conn = connectDB()
            cleanupOldData(conn, run_id)
            createBackup()
            if datetime.now().day == 1 and not vacuum: # Vacuum once a month
                vacuumDatabase(conn)
                vacuum = True
            elif datetime.now().day != 1:
                vacuum = False
            cleanDatabase(conn)
            logger.info("Scheduled run completed successfully")
        except Exception as e:
            logger.error(f"Error during scheduled run: {e}")
        
        execution_time = (datetime.now() - start_time).total_seconds()
        wait_time = max(1200 - execution_time, 0)  # 20 minutes
        logger.info(f"Execution took {execution_time:.2f} seconds")
        logger.info(f"Waiting {wait_time:.2f} seconds until next run")
        await asyncio.sleep(wait_time)

if __name__ == "__main__":
    logger.info("Starting production collector...")

    stop_event = threading.Event()
    
    territory_thread = threading.Thread(
        target=storeTerritories,
        args=(stop_event,),
        daemon=True,
        name="TerritoryThread"
    )
    territory_thread.start()
    try:
        asyncio.run(scheduledMainScript())
    except KeyboardInterrupt:
        stop_event.set()
        territory_thread.join(timeout=20)
        logger.info("Scheduled data collection stopped by user")