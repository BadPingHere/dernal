# i think chatgpt is a blessing and a curse. because god smite me before I have to deal with this shit.
import pytz
import requests
import time
import csv
import sqlite3
from datetime import datetime, timedelta
import os
import asyncio
import zipfile
import platform


def get_utc_now():
    if platform.system() == "Windows":
        return datetime.utcnow()
    return datetime.now(pytz.UTC)


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATABASE_DIR = os.path.join(BASE_DIR, "database")
BACKUP_DIR = os.path.join(DATABASE_DIR, "backups")


def log_progress(message):
    """Log with timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def makeRequest(URL):
    while True:
        try:
            session = requests.Session()
            session.trust_env = False
            r = session.get(URL)

            # If we get a 404, the guild doesn't exist anymore
            if r.status_code == 404:
                log_progress(f"Guild not found (404) for URL: {URL}")
                return None

            r.raise_for_status()
        except requests.exceptions.RequestException as err:
            if "404" in str(err):  # Catch 404s that might raise as exceptions
                log_progress(f"Guild not found (404) for URL: {URL}")
                return None
            log_progress(f"Request failed, retrying in 3s... Error: {err}")
            time.sleep(3)
            continue

        if r.ok:
            return r
        else:
            log_progress("Request not OK, retrying in 3s...")
            time.sleep(3)
            continue


def connect_to_db():
    """
    Connect to the database with proper WAL mode settings
    """
    log_progress("Connecting to database...")
    os.makedirs(DATABASE_DIR, exist_ok=True)
    db_path = os.path.join(DATABASE_DIR, "guild_activity.db")
    conn = sqlite3.connect(db_path, isolation_level=None)

    # Configure WAL mode with conservative settings for slower servers
    log_progress("Configuring database settings...")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA auto_vacuum=FULL")
    conn.execute("PRAGMA wal_autocheckpoint=1000")
    conn.execute("PRAGMA busy_timeout=30000")

    if not check_tables_exist(conn):
        create_tables(conn)
    log_progress("Database connection established")
    return conn


def cleanup_database(conn):
    """
    Properly cleanup the database, checkpoint WAL, and close connection
    """
    log_progress("Starting database cleanup...")
    try:
        log_progress("Performing WAL checkpoint...")
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

        log_progress("Closing database connection...")
        conn.close()

        # Clean up WAL and SHM files if they exist
        db_path = os.path.join(DATABASE_DIR, "guild_activity.db")
        wal_path = f"{db_path}-wal"
        shm_path = f"{db_path}-shm"

        if os.path.exists(wal_path):
            try:
                os.remove(wal_path)
                log_progress("WAL file removed")
            except OSError as e:
                log_progress(f"Warning: Could not remove WAL file: {e}")

        if os.path.exists(shm_path):
            try:
                os.remove(shm_path)
                log_progress("SHM file removed")
            except OSError as e:
                log_progress(f"Warning: Could not remove SHM file: {e}")

        log_progress("Database cleanup completed")

    except Exception as e:
        log_progress(f"Error during database cleanup: {e}")


def cleanup_old_data(conn, batch_size=500):
    """
    Cleanup old data using batched deletions
    """
    cutoff_date = get_utc_now() - timedelta(days=30)
    log_progress(f"Starting data cleanup for records older than {cutoff_date}")
    cur = conn.cursor()

    # Create indexes if they don't exist
    log_progress("Ensuring indexes exist...")
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
    log_progress("Starting guild_snapshots cleanup...")
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
            log_progress(
                f"Deleted {deleted} guild snapshot records (Total: {total_guild_deleted})"
            )
            conn.commit()

        if deleted < batch_size:
            break

    # Cleanup member_snapshots in batches
    total_member_deleted = 0
    log_progress("Starting member_snapshots cleanup...")
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
            log_progress(
                f"Deleted {deleted} member snapshot records (Total: {total_member_deleted})"
            )
            conn.commit()

        if deleted < batch_size:
            break

    log_progress(
        f"Cleanup completed. Total records deleted - Guild: {total_guild_deleted}, Member: {total_member_deleted}"
    )


def create_monthly_backup():
    backup_flag_file = os.path.join(BACKUP_DIR, "last_backup.txt")
    current_month = datetime.now().strftime("%Y_%m")

    # Check if we already did backup this month
    if os.path.exists(backup_flag_file):
        with open(backup_flag_file, "r") as f:
            last_backup = f.read().strip()
            if last_backup == current_month:
                log_progress("Monthly backup already exists, skipping...")
                return

    # Only backup on day 2
    if datetime.now().day != 1:
        return

    log_progress("Starting monthly backup creation...")
    os.makedirs(BACKUP_DIR, exist_ok=True)
    db_path = os.path.join(DATABASE_DIR, "guild_activity.db")
    zip_path = os.path.join(BACKUP_DIR, f"guild_activity_backup_{current_month}.zip")

    try:
        log_progress("Creating zip backup...")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(db_path, os.path.basename(db_path))

        with open(backup_flag_file, "w") as f:
            f.write(current_month)

        backup_size = os.path.getsize(zip_path) / (1024 * 1024)  # Convert to MB
        log_progress(f"Monthly backup created: {zip_path} (Size: {backup_size:.2f} MB)")
    except Exception as e:
        log_progress(f"Error creating monthly backup: {e}")


def check_tables_exist(conn):
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='guilds'"
    )
    return cursor.fetchone() is not None


def create_tables(conn):
    log_progress("Creating database tables...")
    schema_path = os.path.join(DATABASE_DIR, "schema.sql")
    with open(schema_path, "r") as schema_file:
        schema_script = schema_file.read()
        conn.executescript(schema_script)
    log_progress("Tables created successfully")


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


def vacuum_database(conn):
    try:
        log_progress("Starting database VACUUM...")
        db_path = os.path.join(DATABASE_DIR, "guild_activity.db")
        size_before = os.path.getsize(db_path)
        conn.execute("VACUUM")
        size_after = os.path.getsize(db_path)
        saved = size_before - size_after
        log_progress(f"Database VACUUM completed:")
        log_progress(f"Size before: {size_before / 1024:.2f} KB")
        log_progress(f"Size after: {size_after / 1024:.2f} KB")
        log_progress(f"Saved: {saved / 1024:.2f} KB")
    except Exception as e:
        log_progress(f"VACUUM failed: {str(e)}")


def main():
    log_progress("Starting main data collection...")
    guildlist_path = os.path.join(DATABASE_DIR, "guildlist.csv")

    # Read guild list
    with open(guildlist_path, mode="r") as file:
        reader = csv.reader(file)
        uuids = [row[0] for row in reader]
    log_progress(f"Found {len(uuids)} guilds to process")

    conn = connect_to_db()
    try:
        for i, uuid in enumerate(uuids, 1):
            log_progress(f"Processing guild {i}/{len(uuids)} (UUID: {uuid})")
            response = makeRequest(f"https://api.wynncraft.com/v3/guild/uuid/{uuid}")

            # Skip this guild if we got a 404
            if response is None:
                log_progress(f"Skipping guild {uuid} as it no longer exists")
                continue

            guild_data = response.json()

            insert_or_update_guild(conn, guild_data)
            insert_guild_snapshot(conn, guild_data)
            insert_or_update_members(conn, guild_data["uuid"], guild_data["members"])
            conn.commit()

            # Checkpoint WAL periodically
            if i % 10 == 0:  # Every 10 guilds
                log_progress("Performing periodic WAL checkpoint...")
                conn.execute("PRAGMA wal_checkpoint(PASSIVE)")

            time.sleep(1.25)  # Rate limiting

    except Exception as e:
        log_progress(f"Error in main data collection: {e}")
    finally:
        cleanup_database(conn)


async def scheduled_main():
    while True:
        start_time = datetime.now()
        log_progress("Starting scheduled run...")

        try:
            main()
            conn = connect_to_db()
            cleanup_old_data(conn)
            create_monthly_backup()
            vacuum_database(conn)
            cleanup_database(conn)
            log_progress("Scheduled run completed successfully")

        except Exception as e:
            log_progress(f"Error during scheduled run: {e}")

        execution_time = (datetime.now() - start_time).total_seconds()
        wait_time = max(1200 - execution_time, 0)  # 20 minutes

        log_progress(f"Execution took {execution_time:.2f} seconds")
        log_progress(f"Waiting {wait_time:.2f} seconds until next run")
        await asyncio.sleep(wait_time)


if __name__ == "__main__":
    log_progress("Starting production collector...")
    try:
        asyncio.run(scheduled_main())
    except KeyboardInterrupt:
        log_progress("Scheduled data collection stopped by user")
