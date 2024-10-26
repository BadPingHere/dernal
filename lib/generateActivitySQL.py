# im going to keep it a bean, like half of this shit is chatgpt. fuck databases.
import requests
import time
import csv
import sqlite3
from datetime import datetime, timedelta
import os
import asyncio
import shutil
import zipfile

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATABASE_DIR = os.path.join(BASE_DIR, 'database')
BACKUP_DIR = os.path.join(DATABASE_DIR, 'backups')

def makeRequest(URL):
    while True:
        try:
            session = requests.Session()
            session.trust_env = False
            r = session.get(URL)
            r.raise_for_status()
        except requests.exceptions.RequestException as err:
            time.sleep(3)
            continue
        if r.ok:
            return r
        else:
            time.sleep(3)
            continue

def cleanup_old_data(conn):
    cutoff_date = datetime.now() - timedelta(days=30)
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM guild_snapshots 
        WHERE timestamp < ?
    """, (cutoff_date,))
    cur.execute("""
        DELETE FROM member_snapshots 
        WHERE timestamp < ?
    """, (cutoff_date,))
    conn.commit()
    print(f"Cleaned up data older than {cutoff_date}")

def create_monthly_backup():
    if datetime.now().day != 1:
        return
    os.makedirs(BACKUP_DIR, exist_ok=True)
    db_path = os.path.join(DATABASE_DIR, 'guild_activity.db')
    timestamp = datetime.now().strftime('%Y_%m')
    backup_db = os.path.join(BACKUP_DIR, f'guild_activity_backup_{timestamp}.db')
    
    try:
        shutil.copy2(db_path, backup_db)
        zip_path = os.path.join(BACKUP_DIR, f'guild_activity_backup_{timestamp}.zip')
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(backup_db, os.path.basename(backup_db))
        os.remove(backup_db)
        backup_size = os.path.getsize(zip_path) / (1024 * 1024)  # Convert to MB
        print(f"Monthly backup created: {zip_path} (Size: {backup_size:.2f} MB)")
    except Exception as e:
        print(f"Error creating monthly backup: {e}")

def connect_to_db():
    os.makedirs(DATABASE_DIR, exist_ok=True)
    db_path = os.path.join(DATABASE_DIR, 'guild_activity.db')
    conn = sqlite3.connect(db_path, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA auto_vacuum=FULL")
    if not check_tables_exist(conn):
        create_tables(conn)
    return conn

def vacuum_database(conn):
    try:
        db_path = os.path.join(DATABASE_DIR, 'guild_activity.db')
        size_before = os.path.getsize(db_path)
        conn.execute("VACUUM")
        size_after = os.path.getsize(db_path)
        saved = size_before - size_after
        print(f"Database compressed using VACUUM")
        print(f"Size before: {size_before / 1024:.2f} KB")
        print(f"Size after: {size_after / 1024:.2f} KB")
        print(f"Saved: {saved / 1024:.2f} KB")
    except Exception as e:
        print(f"VACUUM failed: {str(e)}")

def check_tables_exist(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='guilds'")
    return cursor.fetchone() is not None

def create_tables(conn):
    print("Creating tables...")
    schema_path = os.path.join(DATABASE_DIR, 'schema.sql')
    with open(schema_path, 'r') as schema_file:
        schema_script = schema_file.read()
        conn.executescript(schema_script)
    print("Tables created successfully.")

def insert_or_update_guild(conn, guild_data):
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO guilds (uuid, name, prefix, created)
        VALUES (?, ?, ?, ?)
    """, (guild_data['uuid'], guild_data['name'], guild_data['prefix'], guild_data['created']))

def insert_guild_snapshot(conn, guild_data):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO guild_snapshots (guild_uuid, level, xp_percent, territories, wars, online_members, total_members)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (guild_data['uuid'], guild_data['level'], guild_data['xpPercent'], guild_data['territories'],
          guild_data.get('wars', 0), guild_data['online'], guild_data['members']['total']))

def insert_or_update_members(conn, guild_uuid, members_data):
    cur = conn.cursor()
    for role, members in members_data.items():
        if role != 'total':
            for name, member in members.items():
                cur.execute("""
                    INSERT OR REPLACE INTO members (uuid, name)
                    VALUES (?, ?)
                """, (member['uuid'], name))
                cur.execute("""
                    INSERT OR REPLACE INTO guild_members (guild_uuid, member_uuid, joined)
                    VALUES (?, ?, ?)
                """, (guild_uuid, member['uuid'], member['joined']))
                cur.execute("""
                    INSERT INTO member_snapshots (guild_uuid, member_uuid, contribution, contribution_rank, online, server)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (guild_uuid, member['uuid'], member['contributed'], member['contributionRank'],
                      1 if member['online'] else 0, member['server']))

def main():
    conn = connect_to_db()
    guildlist_path = os.path.join(DATABASE_DIR, 'guildlist.csv')
    with open(guildlist_path, mode='r') as file:
        reader = csv.reader(file)
        uuids = [row[0] for row in reader]
    try:
        for uuid in uuids:
            r = makeRequest(f"https://api.wynncraft.com/v3/guild/uuid/{uuid}")
            time.sleep(1.25)  # should be around 48 requests per minute
            guild_data = r.json()

            insert_or_update_guild(conn, guild_data)
            insert_guild_snapshot(conn, guild_data)
            insert_or_update_members(conn, guild_data['uuid'], guild_data['members'])

            conn.commit()

    except Exception as e:
        print(f"Error in main data collection: {e}")
    finally:
        conn.close()

async def scheduled_main():
    while True:
        start_time = datetime.now()
        try:
            main()
            conn = connect_to_db()
            cleanup_old_data(conn)
            create_monthly_backup()
            vacuum_database(conn)
            conn.close()
            print(f"Data collection and maintenance completed at {datetime.now()}")
        except Exception as e:
            print(f"Error during execution: {e}")
        execution_time = (datetime.now() - start_time).total_seconds()
        wait_time = max(1200 - execution_time, 0)
        print(f"Execution took {execution_time:.2f} seconds")
        print(f"Waiting {wait_time:.2f} seconds until next run")
        await asyncio.sleep(wait_time)

if __name__ == "__main__":
    print("Starting scheduled data collection...")
    print(f"First run starting at {datetime.now()}")
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(scheduled_main())
    except KeyboardInterrupt:
        print("\nScheduled data collection stopped by user")
    finally:
        loop.close()