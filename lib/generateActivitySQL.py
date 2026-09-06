import time
import csv
import psycopg
from psycopg.rows import dict_row
from datetime import datetime, timezone
import os
import asyncio
import subprocess
import logging
import logging.handlers
import sys
from dateutil.relativedelta import relativedelta
from pathlib import Path
import threading
sys.path.append(str(Path(__file__).resolve().parent.parent))
from lib.makeRequest import makeRequest
def get_utc_now():
    return datetime.now(timezone.utc)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATABASE_DIR = os.path.join(BASE_DIR, "database")
BACKUP_DIR = os.path.join(DATABASE_DIR, "backups")
BACKUP_RETRY_COOLDOWN = 6 * 60 * 60 
LOG_FILE = os.path.join(BASE_DIR, "activitySQL.log")

from lib.config import DSN, PG_DB, PG_USER, TIMESCALE_CONTAINER as CONTAINER

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
    conn = psycopg.connect(DSN, row_factory=dict_row)
    createTables(conn)
    return conn

def createTables(conn):
    exists = conn.execute("SELECT to_regclass('core.guilds') AS t").fetchone()["t"]
    if exists is None:
        raise RuntimeError(
            "Schema is missing. Apply it first:\n"
            "  psql -d dernal -v ON_ERROR_STOP=1 -f database/timescale_schema.sql"
        )

def storeGuildData(conn, jsonData):
    # Get necessary data for all inserts
    guild_uuid = jsonData.get("uuid")
    guild_name = jsonData.get("name")
    guild_prefix = jsonData.get("prefix")
    guild_xp = jsonData.get("xpPercent")
    if guild_xp is not None and not 0 <= guild_xp <= 100: # there are like 2 broken guilds. so i need this. YAAAY!
        guild_xp = None

    guild_level = jsonData.get("level")
    guild_territories = jsonData.get("territories")
    guild_wars = jsonData.get("wars") or 0
    guild_onlineMembers = jsonData.get("online")
    guild_totalMembers = jsonData.get("members").get("total")
    guild_guildRaids = jsonData.get("raids") or 0
    now = get_utc_now()

    conn.execute(
    """
    INSERT INTO core.guilds(guild_uuid, name, prefix, first_seen, last_seen, is_tracked)
    VALUES (%s, %s, %s, %s, %s, true)
    ON CONFLICT(guild_uuid) DO UPDATE SET name=excluded.name, prefix=excluded.prefix, last_seen=excluded.last_seen, is_tracked=true
    """,
        (guild_uuid, guild_name, guild_prefix, now, now)
    )

    conn.execute(
    """
    INSERT INTO ts.guild_snapshots(guild_uuid, ts, level, xp_percent, territories, wars, online_members, total_members, guild_raids)
    VALUES (%s, %s, %s, %s, %s ,%s, %s, %s, %s)
    """,
        (guild_uuid, now, guild_level, guild_xp, guild_territories, guild_wars, guild_onlineMembers, guild_totalMembers, guild_guildRaids)
    )

    season_ranks = jsonData.get("seasonRanks") or {}
    season_params = [
        (guild_uuid, int(season), int(info["rating"]))
        for season, info in season_ranks.items()
        if int(info.get("rating") or 0) > 0
    ]
    if season_params:
        with conn.cursor() as cur:
            cur.executemany("""
            INSERT INTO core.guild_season_ratings(guild_uuid, season, rating)
            VALUES (%s, %s, %s)
            ON CONFLICT(guild_uuid, season) DO UPDATE SET rating = excluded.rating
            """, season_params)

    memberParams = []
    for role, members in jsonData["members"].items():
        if role != "total":
            for name, member in members.items():
                memberParams.append((member.get("uuid"), name, guild_uuid, now, now))

    if memberParams:
        with conn.cursor() as cur:
            cur.executemany("""
            INSERT INTO core.players(player_uuid, username, guild_uuid, first_seen, last_seen)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT(player_uuid) DO UPDATE SET
                username   = excluded.username,
                guild_uuid = excluded.guild_uuid,
                last_seen  = excluded.last_seen
            """, memberParams)

def ensureGuilds(conn, guilds):
    params = [
        (uuid, name or "Unknown", prefix or "???", get_utc_now(), get_utc_now())
        for uuid, name, prefix in guilds if uuid
    ]
    if not params:
        return
    with conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO core.guilds(guild_uuid, name, prefix, first_seen, last_seen, is_tracked)
        VALUES (%s, %s, %s, %s, %s, false)
        ON CONFLICT(guild_uuid) DO UPDATE SET last_seen = excluded.last_seen
        """, params)

def storePlayerActivities(conn, rows, now):
    observations = {}
    for row in rows:
        if not row["player_uuid"]:
            continue
        for kind, field in (("raid", "raid_dict"), ("dungeon", "dungeon_dict"),
                            ("guild_raid", "graid_dict")):
            counters = row[field]
            if not isinstance(counters, dict):
                continue
            for name, total in counters.items():
                if isinstance(name, str) and type(total) is int and total >= 0:
                    observations[(row["player_uuid"], kind, name)] = total
    if not observations:
        return

    activities = {
        (activity["kind"], activity["name"]): activity["activity_id"]
        for activity in conn.execute(
            "SELECT activity_id, kind, name FROM core.activities"
        ).fetchall()
    }
    for kind, name in sorted({(kind, name) for _, kind, name in observations}):
        if (kind, name) not in activities:
            activity = conn.execute("""
                INSERT INTO core.activities(kind, name) VALUES (%s, %s)
                ON CONFLICT (kind, name) DO UPDATE SET name = excluded.name
                RETURNING activity_id
            """, (kind, name)).fetchone()
            activities[(kind, name)] = activity["activity_id"]

    uuids = list({player_uuid for player_uuid, _, _ in observations})
    previous = {
        (row["player_uuid"], row["activity_id"]): row["total"]
        for row in conn.execute("""
            SELECT player_uuid::text AS player_uuid, activity_id, total
            FROM core.player_activity_totals
            WHERE player_uuid = ANY(%s::uuid[])
        """, (uuids,)).fetchall()
    }
    changes = []
    baselines = []
    for (player_uuid, kind, name), total in observations.items():
        activity_id = activities[(kind, name)]
        old = previous.get((player_uuid, activity_id))
        if old == total:
            continue
        gained = total - old if old is not None and total >= old else None
        changes.append((now, player_uuid, activity_id, total, gained))
        baselines.append((player_uuid, activity_id, total, now))

    if changes: # We write only on change to save storage.
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO ts.player_activity_changes(ts, player_uuid, activity_id, total, gained)
                VALUES (%s, %s, %s, %s, %s)
            """, changes)
            cur.executemany("""
                INSERT INTO core.player_activity_totals(player_uuid, activity_id, total, updated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (player_uuid, activity_id) DO UPDATE SET
                    total = excluded.total, updated_at = excluded.updated_at
            """, baselines)

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

        if not rows:
            return

        now = get_utc_now()
        ensureGuilds(conn, {(r["guild_uuid"], r["guild_name"], r["guild_prefix"]) for r in rows if r["guild_uuid"]})

        playerParams = [
            (
                row["player_uuid"], row["username"], row["guild_uuid"],
                row["online"], row["last_join"], now, now
            )
            for row in rows
        ]
        with conn.cursor() as cur:
            cur.executemany("""
            INSERT INTO core.players(player_uuid, username, guild_uuid, online, last_join, first_seen, last_seen)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(player_uuid) DO UPDATE SET
                username   = excluded.username,
                guild_uuid = excluded.guild_uuid,
                online     = excluded.online,
                last_join  = excluded.last_join,
                last_seen  = excluded.last_seen
            """, playerParams)

        uuids = [row["player_uuid"] for row in rows if row["player_uuid"]]
        previous = {}
        if uuids:
            for prev in conn.execute("""
                SELECT DISTINCT ON (player_uuid)
                    player_uuid::text AS player_uuid, guild_uuid::text AS guild_uuid,
                    playtime, contribution, wars, mobs_killed,
                    total_dungeons, total_raids, total_graids, online
                FROM ts.player_snapshots
                WHERE player_uuid = ANY(%s::uuid[])
                ORDER BY player_uuid, ts DESC
            """, (uuids,)).fetchall():
                previous[prev["player_uuid"]] = prev

        snapshotParams = []
        for row in rows:
            player_uuid = row["player_uuid"]
            if not player_uuid:
                continue

            current = (
                row["playtime"], row["contribution"], row["wars"], row["mobs_killed"],
                row["total_dungeons"], row["total_raids"], row["total_graids"],
            )
            prev = previous.get(player_uuid)
            # Get gained
            if prev is None:
                deltas = (None, None, None, None, None, None, None)
            else:
                prevTuple = (
                    prev["playtime"], prev["contribution"], prev["wars"], prev["mobs_killed"],
                    prev["total_dungeons"], prev["total_raids"], prev["total_graids"],
                )
                samePlaytime = (round(current[0], 2) if current[0] is not None else None)                             == (round(prevTuple[0], 2) if prevTuple[0] is not None else None)
                if (samePlaytime and current[1:] == prevTuple[1:]
                        and prev["guild_uuid"] == row["guild_uuid"]
                        and prev["online"] == row["online"]):
                    continue
                deltas = tuple(
                    None if c is None or p is None else c - p
                    for c, p in zip(current, prevTuple)
                )

            snapshotParams.append((
                now, player_uuid, row["guild_uuid"],
                current[0], deltas[0],
                current[1], deltas[1],
                current[2], deltas[2],
                current[3], deltas[3],
                current[4], deltas[4],
                current[5], deltas[5],
                current[6], deltas[6], row["online"],
            ))

        if snapshotParams:
            with conn.cursor() as cur:
                cur.executemany("""
                INSERT INTO ts.player_snapshots(ts, player_uuid, guild_uuid,
                    playtime, d_playtime, contribution, d_contribution, wars, d_wars,
                    mobs_killed, d_mobs_killed, total_dungeons, d_total_dungeons,
                    total_raids, d_total_raids, total_graids, d_total_graids, online)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, snapshotParams)

        storePlayerActivities(conn, rows, now)

        openIntervals = {}
        if uuids:
            for row in conn.execute("""
                SELECT DISTINCT ON (player_uuid)
                    player_uuid::text AS player_uuid, guild_uuid::text AS guild_uuid, joined_at
                FROM core.player_guild_history
                WHERE player_uuid = ANY(%s::uuid[])
                ORDER BY player_uuid, joined_at DESC
            """, (uuids,)).fetchall():
                openIntervals[row["player_uuid"]] = row

        closeParams = []
        historyParams = []
        for row in rows: # We use this to see if they need a update in history
            last = openIntervals.get(row["player_uuid"])

            if last is None or last["guild_uuid"] != row["guild_uuid"]:
                if last is not None:
                    closeParams.append((now, row["player_uuid"], last["joined_at"]))
                historyParams.append((row["player_uuid"], row["guild_uuid"], now))

        if closeParams:
            with conn.cursor() as cur:
                cur.executemany("""
                    UPDATE core.player_guild_history
                    SET left_at = %s
                    WHERE player_uuid = %s AND joined_at = %s AND left_at IS NULL
                """, closeParams)

        if historyParams:
            with conn.cursor() as cur:
                cur.executemany("""
                    INSERT INTO core.player_guild_history(player_uuid, guild_uuid, joined_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (player_uuid, joined_at) DO NOTHING
                """, historyParams)
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to fetch/store via {location}: {e}")
        logger.error(f"JsonData If applicable: {jsonData}")
        return False

def cleanDatabase(conn):
    try:
        conn.commit()
        conn.close()
        logger.info("Database cleaned up and trunucated.")
    except Exception:
        logger.exception("Error during database cleanup:")

def createBackup():
    backup_flag_file = os.path.join(BACKUP_DIR, "last_backup.txt")
    current_month = datetime.now().strftime("%Y_%m")
    lastMonth = (datetime.now() - relativedelta(months=1)).strftime("%Y_%m") # We need last month because we are creating a bakcup for the last month so we want it named right

    if os.path.exists(backup_flag_file):
        with open(backup_flag_file, "r") as f:
            last_backup = f.read().strip()
            if last_backup == current_month:
                logger.info("Monthly backup already exists, skipping...")
                return

    attempt_file = os.path.join(BACKUP_DIR, "last_backup_attempt.txt")
    if os.path.exists(attempt_file):
        age = time.time() - os.path.getmtime(attempt_file)
        if age < BACKUP_RETRY_COOLDOWN:
            logger.info(
                "Backup attempted %.1f h ago and has not succeeded; waiting for "
                "the %.0f h cooldown.", age / 3600, BACKUP_RETRY_COOLDOWN / 3600
            )
            return

    logger.info("Starting monthly backup creation...")
    os.makedirs(BACKUP_DIR, exist_ok=True)
    dumpPath = os.path.join(BACKUP_DIR, f"dernal_backup_{lastMonth}.dump")
    tempPath = dumpPath + ".partial"

    try: # create backups
        logger.info("Creating dernal database backup...")
        with open(attempt_file, "w") as f:
            f.write(datetime.now(timezone.utc).isoformat())

        with open(tempPath, "wb") as out:
            result = subprocess.run(
                ["docker", "exec", CONTAINER, "pg_dump", "-U", PG_USER, "-Fc", PG_DB],
                stdout=out, stderr=subprocess.PIPE, timeout=3600
            )

        if result.returncode != 0:
            logger.error(f"pg_dump failed ({result.returncode}): {result.stderr.decode(errors='replace')[:500]}")
            return

        if os.path.getsize(tempPath) == 0:
            logger.error("pg_dump exited 0 but produced an empty file; not publishing it.")
            return

        os.replace(tempPath, dumpPath)

        with open(backup_flag_file, "w") as f:
            f.write(current_month)

        backup_size = os.path.getsize(dumpPath) / (1024 * 1024)
        logger.info(f"Database backup created: {dumpPath} (Size: {backup_size:.2f} MB)")
    except FileNotFoundError:
        logger.error("docker not found on PATH, skipping backup.")
    except Exception as e:
        logger.info(f"Error creating monthly backup: {e}")
    finally:
        if os.path.exists(tempPath):
            try:
                os.remove(tempPath)
            except OSError:
                logger.warning("Could not remove partial backup %s", tempPath)

def getUntrackedPlayers(conn, players_dict, tracked_guilds):
    rows = conn.execute("""
        SELECT i.username
        FROM unnest(%s::text[]) AS i(username)
        WHERE NOT EXISTS (
            SELECT 1 FROM core.players p
            WHERE lower(p.username) = lower(i.username)
              AND p.guild_uuid = ANY(%s::uuid[])
        )
    """, (list(players_dict.keys()), tracked_guilds)).fetchall()
    return [row["username"] for row in rows]

def main():
    logger.info("Starting main data collection...")
    guildlist_path = os.path.join(DATABASE_DIR, "guildlist.csv")
    with open(guildlist_path, mode="r") as file:
        uuids = [row[0] for row in csv.reader(file)]
    logger.info(f"Found {len(uuids)} guilds to process")

    conn = connectDB()
    try:
        success, r = makeRequest("https://api.wynncraft.com/v3/player")
        playerDict = r.json().get("players", {}) if success else {}
        if not success:
            logger.info("Failed to fetch player list, collecting guilds only.")

        players = getUntrackedPlayers(conn, playerDict, uuids)
        immediate_names = {name.lower() for name in players}
        deferred = [name for name in playerDict if name.lower() not in immediate_names]
        covered_players = set()
        requested_players = set()

        def requestPlayer(username):
            key = username.lower()
            if key in covered_players or key in requested_players:
                return
            requested_players.add(key)
            try:
                success, response = makeRequest(f"https://api.wynncraft.com/v3/player/{username}?fullResult")
                if not success:
                    logger.error("Failed to fetch player %s", username)
                    return
                if storePlayerData(conn, response.json(), "player"):
                    conn.commit()
            except Exception:
                conn.rollback()
                logger.exception("Failed to fetch/store player %s", username)

        num_guilds = len(uuids)
        ratio = len(players) / num_guilds if num_guilds else 0
        player_index = 0
        accumulator = 0.0
        logger.info(
            "Processing %s guilds and %s players, with %s players deferred to check if theyre in the correct guild (ratio %.2f)",
            num_guilds, len(players), len(deferred), ratio,
        )
        for guild_index, uuid in enumerate(uuids):
            if guild_index % 100 == 0:
                logger.info("Processing guild %s/%s (UUID: %s)", guild_index + 1, num_guilds, uuid)
            try:
                success, response = makeRequest(f"https://api.wynncraft.com/v3/guild/uuid/{uuid}")
                if not success:
                    logger.warning("Guild %s unavailable; uncovered online members will be fetched individually", uuid)
                else:
                    guild_data = response.json()
                    member_names = {
                        name.lower()
                        for role, members in guild_data["members"].items()
                        if role != "total"
                        for name in members
                    }
                    storeGuildData(conn, guild_data)
                    if storePlayerData(conn, guild_data, "guild"):
                        conn.commit()
                        covered_players.update(member_names)
            except Exception:
                conn.rollback()
                logger.exception("Failed to collect guild %s; leaving members eligible for individual requests", uuid)

            # Spread priority player requests across the existing guild requests.
            accumulator += ratio
            while accumulator >= 1.0 and player_index < len(players):
                requestPlayer(players[player_index])
                player_index += 1
                accumulator -= 1.0

        # Drain rounding leftovers, then resolve stale membership and failed guilds.
        for username in players[player_index:]:
            requestPlayer(username)
        for username in deferred:
            requestPlayer(username)
        logger.info(
            "Player collection: %s online, %s covered by guilds, %s individual requests attempted",
            len(playerDict),
            sum(name.lower() in covered_players for name in playerDict),
            len(requested_players),
        )
    except Exception:
        logger.exception("Error in main data collection:")
    finally:
        cleanDatabase(conn)

def vacuumDatabase(conn):
    try:
        logger.info("Starting database ANALYZE...")
        conn.execute("ANALYZE")
        conn.commit()
        logger.info("ANALYZE complete.")
    except Exception as e:
        logger.exception(f"Error during ANALYZE: {e}")

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
                guilds = set()
                for territory, data in jsonData.items():
                    guild = data.get("guild") or {}
                    guild_uuid   = guild.get("uuid")
                    guild_name   = guild.get("name")
                    guild_prefix = guild.get("prefix")
                    acquired     = data.get("acquired")
                    if guild_uuid:
                        guilds.add((guild_uuid, guild_name, guild_prefix))
                    rows.append((acquired, territory, guild_uuid, get_utc_now(), territory, acquired))

                ensureGuilds(conn, guilds)

                with conn.cursor() as cur:
                    cur.executemany("""
                        INSERT INTO ts.territory_ownership (acquired, territory, guild_uuid, observed_at)
                        SELECT %s, %s, %s, %s
                        WHERE NOT EXISTS (
                            SELECT 1 FROM ts.territory_ownership
                            WHERE territory = %s
                            AND acquired = %s
                        )
                    """, rows)
                conn.commit()
            except Exception:
                conn.rollback()
                logger.exception("storeTerritories: failed to process territory data.")

        time.sleep(15)

async def scheduledMainScript():
    vacuum = False
    while True:
        start_time = datetime.now()
        logger.info("Starting scheduled run...")

        try:
            main()
            conn = connectDB()
            createBackup()
            if datetime.now().day == 1 and not vacuum: # Analyze once a month
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

#TODO: Add "isHQ" to territory takes
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