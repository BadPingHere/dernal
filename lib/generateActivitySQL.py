import pytz
import requests
import time
import csv
import sqlite3
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os
import asyncio
import zipfile
import platform
import logging
import logging.handlers
def get_utc_now():
    if platform.system() == "Windows":
        return datetime.utcnow()
    return datetime.now(pytz.UTC)

#TODO: Fix when nori ratelimit hits and we use too all retries the script just stopping
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



def makeRequest(url): # the world is built on nested if else statements.
    session = requests.Session()
    session.trust_env = False
    apiSwapDict = { # ts pmo icl
        "https://api.wynncraft.com/v3/guild/list/territory{}": "https://api.wynncraft.com/v3/guild/list/territory{}", # they call me the goat of hacks.
        "https://api.wynncraft.com/v3/guild/uuid/{}": "https://api.wynncraft.com/v3/guild/uuid/{}", # wait, i got one more in me
        "https://api.wynncraft.com/v3/guild/prefix/{}": "https://nori.fish/api/guild/{}",
        "https://api.wynncraft.com/v3/guild/{}": "https://nori.fish/api/guild/{}",
        "https://api.wynncraft.com/v3/player/{}": "https://nori.fish/api/player/{}",# Currently i'd like to save this for player activity sql
    }
        
    usingWynnAPI = True  # Default to official
    for wynn, nori in apiSwapDict.items():
        wynnPrefix = wynn.split("{}")[0] # just the beginning of url before {}
        if url.startswith(wynnPrefix):
            suffix = url[len(wynnPrefix):]  # Extract suffix from official URL
            url = nori.format(suffix)
            usingWynnAPI = False
            break

    retries = 0
    maxRetries = 10

    while retries < maxRetries:
        try:
            r = session.get(url)
            # Nori currently doesnt support multiple responses, itll just go code 500
            if r.status_code == 300: # they say we all got multiple choices in life. be a dog or get pissed on.
                if "/guild/" in url: # In guild endpoint, just select the first option.
                    jsonData = r.json()
                    prefix = jsonData[next(iter(jsonData))]["prefix"]
                    success, r = makeRequest(f"https://api.wynncraft.com/v3/guild/prefix/{prefix}")
                    return success, r
                if "/player/" in url: # In player endpoint, we should select the recently active one, but I dont care! we select the last one.
                    jsonData = r.json()
                    username = jsonData[list(jsonData)[-1]]["storedName"]
                    success, r = makeRequest(f"https://api.wynncraft.com/v3/player/{username}")   
                    return success, r

            elif r.status_code >= 400:
                raise requests.exceptions.HTTPError(request=r) # we send the bad requests to hell
            else:
                if usingWynnAPI:
                    remaining = int(r.headers.get("ratelimit-remaining", 120)) # if we cant get it, act like its 120
                    if remaining < 12: # theyre saying that this lowkey look like saddam hussein hiding spot
                        logger.warning("We are under 12. PANIC!!")
                        time.sleep(2)
                    elif remaining < 30:
                        logger.warning("We are under 30. PANIC!!")
                        time.sleep(0.75)
                    elif remaining < 60:
                        time.sleep(0.25)
                else: # Nori api doesnt have ratelimit headers yet, but we know ratelimits are usually 3/s
                    if "API rate limit exceeded" in str(r.json()): # Nori doesnt like to tell us when we've hit our limit, so we gotta infer
                        retries += 1
                        time.sleep(2.5)
                        continue
                    time.sleep(0.6) 
                return True, r
        except requests.exceptions.RequestException as err:
            status = getattr(err.response, 'status_code', None)
            retryable = [408, 425, 429, 500, 502, 503, 504]
            if status in retryable: # if its retryable, retry. idk why i had to make this comment.
                logger.warning(f"{url} failed with {status}. Current retry is at {retries}.")
                retries += 1
                time.sleep(2)
                continue
            else:
                logger.error(f"Non-retryable error {status} for {url}: {err}")
                return False, {} 

    logger.error(f"Max retries exceeded for {url}")
    return False, {} 


def connect_to_db():
    logger.info("Connecting to database...")
    os.makedirs(DATABASE_DIR, exist_ok=True)
    db_path = os.path.join(DATABASE_DIR, "guild_activity.db")
    conn = sqlite3.connect(db_path, isolation_level=None)

    # Optimized database configuration
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA auto_vacuum=FULL")
    conn.execute("PRAGMA wal_autocheckpoint=1000")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-20000")  # 20MB cache
    conn.execute("PRAGMA temp_store=MEMORY")

    if not check_tables_exist(conn):
        create_tables(conn)
    
    logger.info("Database connection established")
    return conn

def connect_to_player_db():
    os.makedirs(DATABASE_DIR, exist_ok=True)
    db_path = os.path.join(DATABASE_DIR, "player_activity.db")
    conn = sqlite3.connect(db_path, isolation_level=None)

    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA auto_vacuum=FULL")
    conn.execute("PRAGMA wal_autocheckpoint=1000")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-20000")  # 20MB cache
    conn.execute("PRAGMA temp_store=MEMORY")

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

    # Corrected table names in index creation
    conn.execute('CREATE INDEX IF NOT EXISTS idx_users_uuid_timestamp ON users(uuid, timestamp);')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_usersglobal_uuid_timestamp ON users_global(uuid, timestamp);')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_userscharacters_uuid_timestamp ON users_characters(uuid, timestamp);')
    return conn

def fetch_and_store_player_data(player_db_conn, uuid):
    success, r = makeRequest(f"https://api.wynncraft.com/v3/player/{uuid}")
    if not success:
        logger.error(f"Unsuccessful request! Success is {success}, r is {r}, r.json() is {r.json()}")
    try:
            jsonData = r.json()

            if str(jsonData["online"]) == "True":
                online = 1
            else:
                online = 0
            if online == 1:
                server = jsonData["server"]
            else:
                server = None

            if jsonData["guild"] is None:
                guildUUID = None
            else:
                guildUUID = jsonData["guild"]["uuid"]

            if str(jsonData["publicProfile"]) == "True":
                publicProfile = 1
            else:
                publicProfile = 0

            player_db_conn.execute(
                """
                INSERT INTO users (username, uuid, timestamp, online, server, firstJoin, lastJoin, playtime, guildUUID, publicprofile, forumLink)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (jsonData["username"], uuid, get_utc_now().isoformat(), online, server, jsonData["firstJoin"], jsonData["lastJoin"], jsonData["playtime"], guildUUID, publicProfile, jsonData["forumLink"] )
            )


            dungeonsDict = str(jsonData["globalData"]["dungeons"]["list"])
            raidsDict = str(jsonData["globalData"]["raids"]["list"])
            player_db_conn.execute(
                """
                INSERT INTO users_global (username, uuid, timestamp, wars, totalLevel, killedMobs, chestsFound, totalDungeons, dungeonsDict, totalRaids, raidsDict, completedQuests, pvpKills, pvpDeaths)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (jsonData["username"], uuid, get_utc_now().isoformat(), jsonData["globalData"]["wars"], jsonData["globalData"]["totalLevel"], jsonData["globalData"]["killedMobs"], jsonData["globalData"]["chestsFound"], jsonData["globalData"]["dungeons"]["total"], dungeonsDict, jsonData["globalData"]["raids"]["total"], raidsDict, jsonData["globalData"]["completedQuests"], jsonData["globalData"]["pvp"]["kills"], jsonData["globalData"]["pvp"]["deaths"] )
            )

            for character in jsonData["characters"]:
                characterUUID =  character
                characterDict = str(jsonData["characters"][character])
                player_db_conn.execute(
                    """
                    INSERT INTO users_characters (username, uuid, timestamp, characterUUID, characterDict)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (jsonData["username"], uuid, get_utc_now().isoformat(), characterUUID, characterDict)
                )
    except Exception as e:
        logger.error(f"Unsuccessful request2! Success is {success}, r is {r}, r.json() is {r.json()}")
        logger.error(f"Failed to fetch/store player {uuid}: {e}")

def analyze_database_performance(conn):
    try:
        cursor = conn.cursor()
        
        # Check database statistics
        integrity_check = cursor.execute("PRAGMA integrity_check").fetchone()
        logger.info(f"Database Integrity Check: {integrity_check}")
        
        # Analyze table sizes
        cursor.execute("""
            SELECT 
                'guild_snapshots' as table_name, 
                COUNT(*) as row_count, 
                (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM guild_snapshots)) as percentage
            FROM guild_snapshots
            UNION ALL
            SELECT 
                'member_snapshots' as table_name, 
                COUNT(*) as row_count, 
                (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM member_snapshots)) as percentage
            FROM member_snapshots
        """)
        
        for row in cursor.fetchall():
            logger.info(f"Table {row[0]}: {row[1]} rows ({row[2]:.2f}%)")
    
    except Exception as e:
        logger.error(f"Database performance analysis failed: {e}")

def cleanup_database(conn):
    logger.info("Starting database cleanup...")
    try:
        logger.info("Performing WAL checkpoint...")
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

        logger.info("Closing database connection...")
        conn.close()

        # Clean up WAL and SHM files if they exist
        db_path = os.path.join(DATABASE_DIR, "guild_activity.db")
        wal_path = f"{db_path}-wal"
        shm_path = f"{db_path}-shm"

        if os.path.exists(wal_path):
            try:
                os.remove(wal_path)
                logger.info("WAL file removed")
            except OSError as e:
                logger.info(f"Warning: Could not remove WAL file: {e}")

        if os.path.exists(shm_path):
            try:
                os.remove(shm_path)
                logger.info("SHM file removed")
            except OSError as e:
                logger.info(f"Warning: Could not remove SHM file: {e}")

        logger.info("Database cleanup completed")

    except Exception as e:
        logger.info(f"Error during database cleanup: {e}")


def cleanup_old_data(conn, batch_size=500):
    cutoff_date = get_utc_now() - timedelta(days=30)
    logger.info(f"Starting data cleanup for records older than {cutoff_date}")
    cur = conn.cursor()

    # Create indexes if they don't exist
    logger.info("Ensuring indexes exist...")
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_guild_snapshots_timestamp 
        ON guild_snapshots(timestamp)
    """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_member_snapshots_timestamp 
        ON member_snapshots(timestamp)
    """
    )

    # Cleanup guild_snapshots in batches
    total_guild_deleted = 0
    logger.info("Starting guild_snapshots cleanup...")
    while True:
        cur.execute(
            """
            DELETE FROM guild_snapshots 
            WHERE timestamp < ? 
            AND rowid IN (
                SELECT rowid FROM guild_snapshots 
                WHERE timestamp < ? 
                LIMIT ?
            )
        """,
            (cutoff_date, cutoff_date, batch_size),
        )

        deleted = cur.rowcount
        total_guild_deleted += deleted
        if deleted > 0:
            logger.info(
                f"Deleted {deleted} guild snapshot records (Total: {total_guild_deleted})"
            )
            conn.commit()

        if deleted < batch_size:
            break

    # Cleanup member_snapshots in batches
    total_member_deleted = 0
    logger.info("Starting member_snapshots cleanup...")
    while True:
        cur.execute(
            """
            DELETE FROM member_snapshots 
            WHERE timestamp < ? 
            AND rowid IN (
                SELECT rowid FROM member_snapshots 
                WHERE timestamp < ? 
                LIMIT ?
            )
        """,
            (cutoff_date, cutoff_date, batch_size),
        )

        deleted = cur.rowcount
        total_member_deleted += deleted
        if deleted > 0:
            logger.info(
                f"Deleted {deleted} member snapshot records (Total: {total_member_deleted})"
            )
            conn.commit()

        if deleted < batch_size:
            break
        logger.info("Starting player database cleanup...")
    player_conn = connect_to_player_db()
    player_cur = player_conn.cursor()
    
    try:
        # Create indexes for player tables
        logger.info("Ensuring player indexes exist...")
        player_cur.execute("CREATE INDEX IF NOT EXISTS idx_users_timestamp ON users(timestamp)")
        player_cur.execute("CREATE INDEX IF NOT EXISTS idx_usersglobal_timestamp ON users_global(timestamp)")
        player_cur.execute("CREATE INDEX IF NOT EXISTS idx_userscharacters_timestamp ON users_characters(timestamp)")

        # Cleanup users table
        total_users_deleted = 0
        logger.info("Cleaning users table...")
        while True:
            player_cur.execute("""
                DELETE FROM users 
                WHERE timestamp < ? 
                AND rowid IN (
                    SELECT rowid FROM users 
                    WHERE timestamp < ? 
                    LIMIT ?
                )
            """, (cutoff_date, cutoff_date, batch_size))
            deleted = player_cur.rowcount
            total_users_deleted += deleted
            if deleted > 0:
                logger.info(f"Deleted {deleted} user records (Total: {total_users_deleted})")
                player_conn.commit()
            if deleted < batch_size:
                break

        # Cleanup users_global table
        total_global_deleted = 0
        logger.info("Cleaning users_global table...")
        while True:
            player_cur.execute("""
                DELETE FROM users_global 
                WHERE timestamp < ? 
                AND rowid IN (
                    SELECT rowid FROM users_global 
                    WHERE timestamp < ? 
                    LIMIT ?
                )
            """, (cutoff_date, cutoff_date, batch_size))
            deleted = player_cur.rowcount
            total_global_deleted += deleted
            if deleted > 0:
                logger.info(f"Deleted {deleted} global records (Total: {total_global_deleted})")
                player_conn.commit()
            if deleted < batch_size:
                break

        # Cleanup users_characters table
        total_characters_deleted = 0
        logger.info("Cleaning users_characters table...")
        while True:
            player_cur.execute("""
                DELETE FROM users_characters 
                WHERE timestamp < ? 
                AND rowid IN (
                    SELECT rowid FROM users_characters 
                    WHERE timestamp < ? 
                    LIMIT ?
                )
            """, (cutoff_date, cutoff_date, batch_size))
            deleted = player_cur.rowcount
            total_characters_deleted += deleted
            if deleted > 0:
                logger.info(f"Deleted {deleted} character records (Total: {total_characters_deleted})")
                player_conn.commit()
            if deleted < batch_size:
                break

        logger.info(f"Player cleanup completed. Deleted - Users: {total_users_deleted}, Global: {total_global_deleted}, Characters: {total_characters_deleted}")

    finally:
        logger.info("Closing player database connection...")
        player_conn.close()


def create_monthly_backup():
    backup_flag_file = os.path.join(BACKUP_DIR, "last_backup.txt")
    current_month = datetime.now().strftime("%Y_%m")

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
    guildZipPath = os.path.join(BACKUP_DIR, f"guild_activity_backup_{current_month}.zip")
    playerPath = os.path.join(DATABASE_DIR, "player_activity.db")
    playerZipPath = os.path.join(BACKUP_DIR, f"player_activity_backup_{current_month}.zip")

    try:
        logger.info("Creating zip backup...")
        with zipfile.ZipFile(guildZipPath, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(guildPath, os.path.basename(guildPath))

        with open(backup_flag_file, "w") as f:
            f.write(current_month)

        backup_size = os.path.getsize(guildZipPath) / (1024 * 1024)
        logger.info(f"Monthly backup created: {guildZipPath} (Size: {backup_size:.2f} MB)")
    except Exception as e:
        logger.info(f"Error creating monthly backup: {e}")

    try:
        logger.info("Creating zip backup...")
        with zipfile.ZipFile(playerZipPath, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(playerPath, os.path.basename(playerPath))

        with open(backup_flag_file, "w") as f:
            f.write(current_month)

        backup_size = os.path.getsize(playerZipPath) / (1024 * 1024)
        logger.info(f"Monthly backup created: {playerZipPath} (Size: {backup_size:.2f} MB)")
    except Exception as e:
        logger.info(f"Error creating monthly backup: {e}")


def check_tables_exist(conn):
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='guilds'"
    )
    return cursor.fetchone() is not None


def create_tables(conn):
    logger.info("Creating database tables...")
    schema_path = os.path.join(DATABASE_DIR, "schema.sql")
    with open(schema_path, "r") as schema_file:
        schema_script = schema_file.read()
        conn.executescript(schema_script)
    logger.info("Tables created successfully")


def insert_or_update_guild(conn, guild_data):
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


def insert_guild_snapshot(conn, guild_data):
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


def insert_or_update_members(conn, guild_uuid, members_data):
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

    conn = connect_to_db()
    player_db_conn = connect_to_player_db()

    try:
        for i, uuid in enumerate(uuids, 1):
            logger.info(f"Processing guild {i}/{len(uuids)} (UUID: {uuid})")
            success, r = makeRequest(f"https://api.wynncraft.com/v3/guild/uuid/{uuid}")
            if not success:
                logger.info(f"Skipping guild {uuid} as it no longer exists")
                continue
            guild_data = r.json()
            insert_or_update_guild(conn, guild_data)
            insert_guild_snapshot(conn, guild_data)
            insert_or_update_members(conn, guild_data["uuid"], guild_data["members"])
            conn.commit()

            # Fetch data for online players
            #logger.info(f'guild_data["members"]: {guild_data["members"]}')
            for role, members in guild_data["members"].items():
                #logger.info(f'role: {role}')
                #logger.info(f'members: {members}')
                if role == "total":
                    continue
                for member in members.values():
                    #logger.info(f'member: {member}')
                    if member["online"]:
                        fetch_and_store_player_data(player_db_conn, member["uuid"])

            time.sleep(0.33)  # Rate limit ourselves

    except Exception as e:
        logger.info(f"Error in main data collection: {e}")
    finally:
        logger.info("Final WAL checkpoint for guild_activity.db...")
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)")

        logger.info("Final WAL checkpoint for player_activity.db...")
        player_db_conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        cleanup_database(conn)
        cleanup_database(player_db_conn)

def vacuum_database(conn):
    # This function is a bitch on linux but finally works.
    try:
        logger.info("Starting database VACUUM preparation...")
        
        # First check available disk space
        db_path = os.path.join(DATABASE_DIR, "guild_activity.db")
        db_size = os.path.getsize(db_path)
        
        # Get free space
        stat = os.statvfs(DATABASE_DIR)
        free_space = stat.f_frsize * stat.f_bavail
        
        logger.info(f"Current database size: {db_size / (1024*1024):.2f} MB")
        logger.info(f"Available disk space: {free_space / (1024*1024):.2f} MB")
        required_space = db_size * 1.1
        if free_space < required_space:
            logger.error(f"Insufficient disk space for VACUUM. Need at least {required_space/(1024*1024):.2f} MB free")
            return
        logger.info("Preparing system for VACUUM")
        conn.close()
        
        vacuum_conn = sqlite3.connect(db_path, isolation_level=None)
        vacuum_conn.execute("PRAGMA journal_mode=OFF")
        vacuum_conn.execute("PRAGMA synchronous=OFF")
        vacuum_conn.execute("PRAGMA cache_size=-2000") 
        vacuum_conn.execute("PRAGMA busy_timeout=3600000") # linux is slow but shouldnt ever take more than 1hr
        logger.info("Executing VACUUM")
        start_time = time.time()
        vacuum_conn.execute("VACUUM")
        duration = time.time() - start_time
        size_after = os.path.getsize(db_path)
        saved = db_size - size_after
        
        logger.info(f"Database VACUUM completed successfully:")
        logger.info(f"Duration: {duration/60:.1f} minutes")
        logger.info(f"Size before: {db_size / (1024*1024):.2f} MB")
        logger.info(f"Size after: {size_after / (1024*1024):.2f} MB")
        logger.info(f"Saved: {saved / (1024*1024):.2f} MB")
        
        # Close vacuum connection
        vacuum_conn.close()
        return connect_to_db()
        
    except sqlite3.OperationalError as e:
        logger.error(f"SQLite operational error during VACUUM: {str(e)}")
    except OSError as e:
        logger.error(f"OS error during VACUUM (possibly disk space related): {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error during VACUUM: {str(e)}")
        logger.error(f"Error type: {type(e)}")
        
    try:
        return connect_to_db()
    except:
        logger.error("Could not re-establish database connection after VACUUM error")
        raise

async def scheduled_main():
    vacuum = False
    while True:
        start_time = datetime.now()
        logger.info("Starting scheduled run...")

        try:
            main()
            conn = connect_to_db()
            cleanup_old_data(conn)
            create_monthly_backup()
            if datetime.now().day != 1:
                vacuum = False
            if datetime.now().day == 1 and not vacuum:
                vacuum_database(conn)
                vacuum = True
            if datetime.now().day % 7 == 0:
                analyze_database_performance(conn)
            cleanup_database(conn)
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
        asyncio.run(scheduled_main())
    except KeyboardInterrupt:
        logger.info("Scheduled data collection stopped by user")