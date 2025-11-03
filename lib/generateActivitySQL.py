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
import math
import sys
from dateutil.relativedelta import relativedelta
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from lib.makeRequest import makeRequest
def get_utc_now():
    if platform.system() == "Windows":
        return datetime.utcnow()
    return datetime.now(pytz.UTC)

#TODO: Eventuall rewrite this entire script, as to fix shit, make it easier to understand, and whatnot

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

def connectGuildDB():
    logger.info("Connecting to guild database...")
    os.makedirs(DATABASE_DIR, exist_ok=True)
    db_path = os.path.join(DATABASE_DIR, "guild_activity.db")
    conn = sqlite3.connect(db_path, isolation_level=None)

    # Optimized database configuration
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA auto_vacuum=INCREMENTAL")
    conn.execute("PRAGMA wal_autocheckpoint=2000")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA mmap_size=536870912;")  # 512MB  mmap
    conn.execute("PRAGMA cache_size=-40000")  # 40MB cache
    conn.execute("PRAGMA temp_store=MEMORY")

    if not checkTablesExist(conn):
        createTables(conn)
    
    return conn

def connectPlayerDB():
    logger.info("Connecting to player database...")
    os.makedirs(DATABASE_DIR, exist_ok=True)
    db_path = os.path.join(DATABASE_DIR, "player_activity.db")
    conn = sqlite3.connect(db_path, isolation_level=None)

    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA auto_vacuum=INCREMENTAL")
    conn.execute("PRAGMA wal_autocheckpoint=2000")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA mmap_size=536870912;")  # 512MB  mmap
    conn.execute("PRAGMA cache_size=-40000")  # 40MB cache
    conn.execute("PRAGMA temp_store=MEMORY")
    
    # We are too lazy to create a schema.
    conn.execute('''
    CREATE TABLE IF NOT EXISTS users (
        username TEXT NOT NULL,
        uuid TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        online INTEGER NOT NULL,
        server TEXT,
        firstJoin TEXT,
        lastJoin TEXT,
        playtime INTEGER,
        guildUUID TEXT,
        publicprofile INTEGER,
        forumLink TEXT,
        PRIMARY KEY (uuid, timestamp)
    );
    ''')

    conn.execute('''
    CREATE TABLE IF NOT EXISTS users_global (
        username TEXT NOT NULL,
        uuid TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        wars INTEGER,
        totalLevel INTEGER,
        killedMobs INTEGER,
        chestsFound INTEGER,
        totalDungeons INTEGER,
        dungeonsDict TEXT,
        totalRaids INTEGER,
        raidsDict TEXT,
        completedQuests INTEGER,
        pvpKills INTEGER,
        pvpDeaths INTEGER,
        PRIMARY KEY (uuid, timestamp)
    );
    ''')

    conn.execute('''
    CREATE TABLE IF NOT EXISTS users_characters (
        username TEXT NOT NULL,
        uuid TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        characterUUID TEXT NOT NULL,
        characterDict TEXT,
        PRIMARY KEY (uuid, timestamp, characterUUID)
    );
    ''')

    conn.execute('''
    CREATE TABLE IF NOT EXISTS users_guilds (
        username TEXT NOT NULL,
        uuid TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        guildUUID TEXT,
        guildName TEXT,
        guildPrefix TEXT,
        PRIMARY KEY (uuid, timestamp)
    );
    ''')

    conn.execute('CREATE INDEX IF NOT EXISTS idx_users_uuid_timestamp ON users(uuid, timestamp);')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_usersglobal_uuid_timestamp ON users_global(uuid, timestamp);')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_userscharacters_uuid_timestamp ON users_characters(uuid, timestamp);')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_usersguilds_uuid_timestamp ON users_guilds(uuid, timestamp DESC);')

    return conn

def storePlayerData(player_db_conn, username):
    success, r = makeRequest(f"https://api.wynncraft.com/v3/player/{username}?fullResult")
    if not success:
        logger.error(f"Unsuccessful request! Success is {success}, r is {r}, r.json() is {r.json()}")
    try:
        jsonData = r.json()
        restrictionsDict = jsonData["restrictions"]
        # if restrictionsDict.get("characterBuildAccess") is True: # sp will be "skillPoints":{"error":"This player limits their skill points visibility."} 

        if restrictionsDict.get("mainAccess") is True: # globalData gone, firstJoin gone, playtime gone, so we dont do those
            firstJoin = None
            playtime = -1 # I could None this too... but i dont want to.
        else: # we have global data and whatnot, so we can get this shit done
            firstJoin = jsonData["firstJoin"]
            playtime = jsonData["playtime"]
            dungeonsDict = str(jsonData["globalData"]["dungeons"]["list"])
            raidsDict = str(jsonData["globalData"]["raids"]["list"])
            player_db_conn.execute(
            """
            INSERT INTO users_global (username, uuid, timestamp, wars, totalLevel, killedMobs, chestsFound, totalDungeons, dungeonsDict, totalRaids, raidsDict, completedQuests, pvpKills, pvpDeaths)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (jsonData["username"], jsonData["uuid"], get_utc_now().isoformat(), jsonData["globalData"]["wars"], jsonData["globalData"]["totalLevel"], jsonData["globalData"]["mobsKilled"], jsonData["globalData"]["chestsFound"], jsonData["globalData"]["dungeons"]["total"], dungeonsDict, jsonData["globalData"]["raids"]["total"], raidsDict, jsonData["globalData"]["completedQuests"], jsonData["globalData"]["pvp"]["kills"], jsonData["globalData"]["pvp"]["deaths"] )
            )
            
        if restrictionsDict.get("characterDataAccess") is False: # if true no character data, no reason to input
            if 1 == 0: # uh just turning this off because we dont need this as it stands, more for future-proofing
                for character in jsonData["characters"]:
                    characterUUID =  character
                    characterDict = str(jsonData["characters"][character])
                    player_db_conn.execute(
                        """
                        INSERT INTO users_characters (username, uuid, timestamp, characterUUID, characterDict)
                        VALUES (?, ?, ?, ?, ?)
                    """,
                        (jsonData["username"], jsonData["uuid"], get_utc_now().isoformat(), characterUUID, characterDict)
                            )
        if str(jsonData["online"]) == "True":
            online = 1
        else:
            online = 0
        if online == 1:
            server = jsonData["server"]
        else:
            server = None

        if jsonData["guild"] is None:
            guildUUID = guildName = guildPrefix = "None"
        else:
            guildUUID = jsonData["guild"]["uuid"]
            guildName = jsonData["guild"]["name"]
            guildPrefix = jsonData["guild"]["prefix"]

        player_db_conn.execute(
            """
            INSERT INTO users (username, uuid, timestamp, online, server, firstJoin, lastJoin, playtime, guildUUID, restrictions)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (jsonData["username"], jsonData["uuid"], get_utc_now().isoformat(), online, server, firstJoin, jsonData["lastJoin"], playtime, guildUUID, str(jsonData["restrictions"]))
        )

        player_db_conn.execute( # Only inserts if the most recent timestamp entry is different uuid
            """
            INSERT INTO users_guilds (username, uuid, timestamp, guildUUID, guildName, guildPrefix)
            SELECT ?, ?, ?, ?, ?, ?
            WHERE ? != COALESCE(
                (SELECT guildUUID FROM users_guilds WHERE uuid = ? ORDER BY timestamp DESC LIMIT 1),
                ''
            )
            """,
            (jsonData["username"], jsonData["uuid"], get_utc_now().isoformat(), guildUUID, guildName, guildPrefix,guildUUID, jsonData["uuid"])
        )

    except Exception as e:
        logger.error(f"Unsuccessful request2! Success is {success}, r.json() is {r.json()}.")
        logger.error(f"Failed to fetch/store uuid {jsonData['uuid']}: {e}")

def cleanguildDatabase(conn):
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
        logger.info("Database cleaned up and trunucated.")
    except Exception:
        logger.exception("Error during database cleanup:")

def cleanupOldData(conn, batchSize=1000):
    cutoffDate = get_utc_now() - timedelta(days=30) # Cleanup all data older than 30d
    logger.info(f"Starting data cleanup for records older than {cutoffDate}")
    guildCur = conn.cursor()
    playerConn = connectPlayerDB()
    playerCur = playerConn.cursor()
    
    def index(cursor, indexStatements, database):
        logger.info(f"Ensuring indexes exist for {database}...")
        for stmt in indexStatements:
            cursor.execute(stmt)

    index(
        guildCur, 
        ["CREATE INDEX IF NOT EXISTS idx_guild_snapshots_timestamp ON guild_snapshots(timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_member_snapshots_timestamp ON member_snapshots(timestamp)"], 
        "guild_activity.db"
    )
    index(
        playerCur, 
        ["CREATE INDEX IF NOT EXISTS idx_users_timestamp ON users(timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_usersglobal_timestamp ON users_global(timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_userscharacters_timestamp ON users_characters(timestamp)"], 
        "player_activity.db"
    )

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
    try:
        guildSnapshotCount = batchDelete(guildCur, "guild_snapshots", cutoffDate, batchSize, conn)
        memberSnapshotCount = batchDelete(guildCur, "member_snapshots", cutoffDate, batchSize, conn)

        userCount = batchDelete(playerCur, "users", cutoffDate, batchSize, playerConn)
        userGlobalCount = batchDelete(playerCur, "users_global", cutoffDate, batchSize, playerConn)
        characterCount = batchDelete(playerCur, "users_characters", cutoffDate, batchSize, playerConn)
    except Exception as e:
        logger.exception(f"There was an error while cleaning up old data:")
    finally:
        playerConn.close()
        logger.info(
            f"Cleanup Stats:\n"
            f"  Guild Snapshots: {guildSnapshotCount}\n"
            f"  Member Snapshots: {memberSnapshotCount}\n"
            f"  Users: {userCount}\n"
            f"  Users Global: {userGlobalCount}\n"
            f"  Users Characters: {characterCount}"
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
    guildPath = os.path.join(DATABASE_DIR, "guild_activity.db")
    guildZipPath = os.path.join(BACKUP_DIR, f"guild_activity_backup_{lastMonth}.zip")
    playerPath = os.path.join(DATABASE_DIR, "player_activity.db")
    playerZipPath = os.path.join(BACKUP_DIR, f"player_activity_backup_{lastMonth}.zip")

    try: # create backups
        logger.info("Creating guild backup...")
        with zipfile.ZipFile(guildZipPath, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(guildPath, os.path.basename(guildPath))

        with open(backup_flag_file, "w") as f:
            f.write(current_month)

        backup_size = os.path.getsize(guildZipPath) / (1024 * 1024)
        logger.info(f"Guild backup created: {guildZipPath} (Size: {backup_size:.2f} MB)")

        logger.info("Creating player backup...")
        with zipfile.ZipFile(playerZipPath, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(playerPath, os.path.basename(playerPath))

        with open(backup_flag_file, "w") as f:
            f.write(current_month)

        backup_size = os.path.getsize(playerZipPath) / (1024 * 1024)
        logger.info(f"Player backup created: {playerZipPath} (Size: {backup_size:.2f} MB)")
    except Exception as e:
        logger.info(f"Error creating monthly backup: {e}")

def checkTablesExist(conn): 
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='guilds'")
    return cursor.fetchone() is not None

def createTables(conn):
    logger.info("Creating database tables...")
    schema_path = os.path.join(DATABASE_DIR, "schema.sql")
    with open(schema_path, "r") as schema_file:
        schema_script = schema_file.read()
        conn.executescript(schema_script)
    logger.info("Tables created successfully")

def updateGuildList(conn, guild_data):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO guilds (uuid, name, prefix, created)
        VALUES (?, ?, ?, ?)
    """,
        (
            guild_data["uuid"],
            guild_data["name"],
            guild_data["prefix"],
            guild_data["created"],
        ),
    )

def insertGuildSnapshot(conn, guild_data):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO guild_snapshots (guild_uuid, level, xp_percent, territories, wars, online_members, total_members)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """,
        (
            guild_data["uuid"],
            guild_data["level"],
            guild_data["xpPercent"],
            guild_data["territories"],
            guild_data.get("wars", 0),
            guild_data["online"],
            guild_data["members"]["total"],
        ),
    )

def insertMemberSnapshot(conn, guild_uuid, members_data):
    cur = conn.cursor()
    for role, members in members_data.items():
        if role != "total":
            for name, member in members.items():
                cur.execute(
                    """
                    INSERT OR REPLACE INTO members (uuid, name)
                    VALUES (?, ?)
                """,
                    (member["uuid"], name),
                )
                cur.execute(
                    """
                    INSERT OR REPLACE INTO guild_members (guild_uuid, member_uuid, joined)
                    VALUES (?, ?, ?)
                """,
                    (guild_uuid, member["uuid"], member["joined"]),
                )
                cur.execute(
                    """
                    INSERT INTO member_snapshots (guild_uuid, member_uuid, contribution, contribution_rank, online, server)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        guild_uuid,
                        member["uuid"],
                        member["contributed"],
                        member["contributionRank"],
                        1 if member["online"] else 0,
                        member["server"],
                    ),
                )

def main():
    logger.info("Starting main data collection...")
    guildlist_path = os.path.join(DATABASE_DIR, "guildlist.csv")

    with open(guildlist_path, mode="r") as file:
        reader = csv.reader(file)
        uuids = [row[0] for row in reader]

    logger.info(f"Found {len(uuids)} guilds to process")

    conn = connectGuildDB()
    player_db_conn = connectPlayerDB()
    try:
        # we do all this ratio bullshit to alleviate pressure on api endpoints, make them do 100/m for 20m rather than 200/m for 10m then 0/m for 10m.
        success, r = makeRequest(f"https://api.wynncraft.com/v3/player")
        if not success:
            logger.info("Player list unavailable, giving up.") # this for sure wont happen but we gotta rock with it
            players = []
        else:
            players_dict = r.json().get("players", {})
            players = list(players_dict.keys()) 

        num_guilds = len(uuids)
        num_players = len(players)

        if num_guilds == 0:
            ratio = 0
        else:
            ratio = num_players / num_guilds
        guild_index = 0
        player_index = 0
        logger.info(f"Processing {num_guilds} guilds and {num_players} players (ratio ≈ {ratio:.2f})")
        while guild_index < num_guilds or player_index < num_players:
            if guild_index < num_guilds:
                uuid = uuids[guild_index]
                if guild_index % 25 == 0: # so we dont spam log...
                    logger.info(f"Processing guild {guild_index+1}/{num_guilds} (UUID: {uuid})")
                success, r = makeRequest(f"https://api.wynncraft.com/v3/guild/uuid/{uuid}")
                if not success:
                    logger.info(f"Skipping guild {uuid} as it no longer exists")
                else:
                    guild_data = r.json()
                    updateGuildList(conn, guild_data)
                    insertGuildSnapshot(conn, guild_data)
                    insertMemberSnapshot(conn, guild_data["uuid"], guild_data["members"])
                    conn.commit()
                    time.sleep(0.3) # Sleep every guild so we can stretch this out to 20m ish
                guild_index += 1

            num_players_to_do = math.ceil(ratio)
            for _ in range(num_players_to_do):
                if player_index >= num_players:
                    break
                username = players[player_index]
                #logger.info(f"USERNAME: {username}")
                storePlayerData(player_db_conn, username)
                player_index += 1

    except Exception:
        logger.exception("Error in main data collection:")
    finally:
        logger.info("Final WAL checkpoint for guild_activity.db...")
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)")

        logger.info("Final WAL checkpoint for player_activity.db...")
        player_db_conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        cleanguildDatabase(conn)
        cleanguildDatabase(player_db_conn)

def vacuumDatabase(conn):
    try:
        logger.info("Starting database VACUUM...")
        conn.execute("PRAGMA incremental_vacuum(10000);")
        logger.info("Incremental VACUUM complete.")
    except Exception as e:
        logger.exception(f"Error during VACUUM: {e}")

async def scheduledMainScript():
    vacuum = False
    while True:
        start_time = datetime.now()
        logger.info("Starting scheduled run...")

        try:
            main() # This is for putting all data in db
            guildConn = connectGuildDB()
            cleanupOldData(guildConn)
            createBackup()
 
            if datetime.now().day != 1:
                vacuum = False
            if datetime.now().day == 1 and not vacuum:
                vacuumDatabase(guildConn) #TODO: this is only for guild database atm
                vacuum = True
            cleanguildDatabase(guildConn) #TODO: this is only for guild database atm
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
    try:
        asyncio.run(scheduledMainScript())
    except KeyboardInterrupt:
        logger.info("Scheduled data collection stopped by user")