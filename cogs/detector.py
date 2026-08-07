import discord
from discord.ext import commands, tasks
from discord import app_commands
from typing import Optional, Union
import logging
import os
from dotenv import load_dotenv
from lib.utils import warComponentBuilder, checkWorldEventDiff, sendWorldEventMessage, human_time_duration
from lib.api import connectDB
from lib.makeRequest import makeRequest
import asyncio
from datetime import datetime, timezone, timedelta
import sqlite3
import time
import math
from pathlib import Path

logger = logging.getLogger('discord')
load_dotenv()

@app_commands.allowed_installs(guilds=True, users=False)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=True)
class Detector(commands.GroupCog, name="detector"):
    def __init__(self, bot):
        self.bot = bot

        # War detector data
        self.guildsBeingTracked = {}
        self.lastAcquiredTime = None
        self.ACTIVITYDBPATH = Path(__file__).resolve().parents[1] / "database" / "activity.db"

        # World Event data
        self.weOldData = None
        self.weBeingTracked = {}
        self.weConfigDirty = True
        self.anniePings = set()

        self.serverID = int(os.getenv("SERVER_ID") or 0)
        rootDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.detectorDBPath = os.path.join(rootDir, 'database', 'detector.db')
        self.territoryDBPath = os.path.join(rootDir, "database", "territories.db")
        self.initDetectorDB()
        self.loadTrackedGuilds()
        try:
            conn = connectDB()
            cur = conn.cursor()
            cur.execute("SELECT acquired FROM territory_changes ORDER BY acquired DESC LIMIT 1")
            row = cur.fetchone()
            conn.close()
            if row:
                self.lastAcquiredTime = row["acquired"]
        except Exception:
            logger.exception("Failed to initialize lastAcquiredTime from DB.")
        #logger.info(f"self.guildsBeingTracked: {self.guildsBeingTracked}")
        self.backgroundDetector.start()

    def connectDetectorDB(self):
        os.makedirs(os.path.dirname(self.detectorDBPath), exist_ok=True)
        conn = sqlite3.connect(self.detectorDBPath, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    def initDetectorDB(self):
        conn = self.connectDetectorDB()
        schema_path = os.path.join(os.path.dirname(self.detectorDBPath), "detector_schema.sql")
        with open(schema_path, "r") as f:
            conn.executescript(f.read())
        conn.close()

    def loadTrackedGuilds(self):
        conn = self.connectDetectorDB()
        cur = conn.cursor()
        cur.execute("SELECT serverID, channelForMessages, guildPrefix, pingRoleID, intervalForPing, lastPing FROM wars")
        rows = cur.fetchall()
        conn.close()
        self.guildsBeingTracked = {}
        for serverID, channelForMessages, guildPrefix, pingRoleID, intervalForPing, lastPing in rows:
            key = str(serverID)
            if key not in self.guildsBeingTracked:
                self.guildsBeingTracked[key] = []
            self.guildsBeingTracked[key].append({
                'channelForMessages': channelForMessages,
                'guildPrefix': guildPrefix,
                'pingRoleID': str(pingRoleID) if pingRoleID else "",
                'intervalForPing': intervalForPing if intervalForPing else "",
                'lastPing': lastPing if lastPing else ""
            })

    def shouldPing(self, data, currentEpochTime):
        # backgroundDetector no longer reloads tracked guilds every tick, so the interval
        # is gated on data['lastPing'], which recordPing keeps in sync with the DB.
        if not data.get("pingRoleID") or not data.get("intervalForPing"):
            return False
        lastPing = data.get("lastPing")
        if not lastPing:
            return True
        return (currentEpochTime - int(lastPing)) > (int(data["intervalForPing"]) * 60)

    def recordPing(self, serverID, data, currentEpochTime):
        data["lastPing"] = currentEpochTime
        conn = self.connectDetectorDB()
        conn.execute(
            "UPDATE wars SET lastPing = ? WHERE serverID = ? AND guildPrefix = ?",
            (currentEpochTime, int(serverID), data["guildPrefix"])
        )
        conn.close()

    def loadTrackedWorldEvents(self):
        conn = self.connectDetectorDB()
        cur = conn.cursor()
        cur.execute("SELECT serverID, channelForMessages, event, pingRoleID, anniePingTimer FROM world_events")
        rows = cur.fetchall()
        conn.close()
        self.weBeingTracked = {}
        for serverID, channelForMessages, event, pingRoleID, anniePingTimer in rows:
            key = str(serverID)
            if key not in self.weBeingTracked:
                self.weBeingTracked[key] = []
            self.weBeingTracked[key].append({
                'channelForMessages': channelForMessages,
                'event': event,
                'pingRoleID': str(pingRoleID) if pingRoleID else "",
                'anniePingTimer': anniePingTimer if anniePingTimer else None
            })

    @tasks.loop(seconds=5)
    async def backgroundDetector(self):
        latestRow = None
        try:
            if not self.guildsBeingTracked:
                return

            conn = connectDB()
            cur = conn.cursor()
            cur.execute("SELECT acquired FROM territory_changes ORDER BY acquired DESC LIMIT 1")
            latestRow = cur.fetchone()

            if latestRow and latestRow["acquired"] != self.lastAcquiredTime:
                newWars = {}
                cur.execute(
                    "SELECT * FROM territory_changes WHERE acquired > ? ORDER BY acquired ASC",
                    (self.lastAcquiredTime,)
                )
                rows = cur.fetchall()

                for row in rows:
                    acquiredDatetime = datetime.fromisoformat(row["acquired"].replace("Z", "+00:00"))
                    newWars[row["territory"]] = {
                        "Attacker": f"{row['guild_name']} ({row['guild_prefix']})",
                        "AttackerPrefix": row["guild_prefix"],
                        "AttackerUUID": row["guild_uuid"],
                        "Defender": None,
                        "DefenderPrefix": None,
                        "DefenderUUID": None,
                        "AttackerBefore": None,
                        "AttackerAfter": None,
                        "DefenderBefore": None,
                        "DefenderAfter": None,
                        "TimeLasted": acquiredDatetime,
                        "AcquiredAt": row["acquired"],
                    }

                # We got new wars, now we need to fill out all required data.
                for territoryName, warData in newWars.items():
                    cur.execute("""
                        SELECT *
                        FROM territory_changes
                        WHERE territory = ?
                        ORDER BY acquired DESC
                        LIMIT 1 OFFSET 1;
                    """, (territoryName,))
                    row = cur.fetchone()
                    newWars[territoryName]["Defender"] = f"{row['guild_name']} ({row['guild_prefix']})"
                    newWars[territoryName]["DefenderUUID"] = row["guild_uuid"]
                    newWars[territoryName]["DefenderPrefix"] = row["guild_prefix"]

                    lastAcquired = datetime.fromisoformat(row["acquired"].replace("Z", "+00:00"))
                    timeLastedSeconds = (newWars[territoryName]["TimeLasted"] - lastAcquired).total_seconds()
                    newWars[territoryName]["TimeLasted"] = await asyncio.to_thread(human_time_duration, timeLastedSeconds)

                    cur.execute("""
                        SELECT
                            guild_uuid,
                            COUNT(*) AS matching_territories
                        FROM territory_changes
                        WHERE (territory, acquired) IN (
                            SELECT territory, MAX(acquired)
                            FROM territory_changes
                            GROUP BY territory
                        )
                        AND guild_uuid IN (?, ?)
                        GROUP BY guild_uuid;
                    """, (newWars[territoryName]["AttackerUUID"], newWars[territoryName]["DefenderUUID"],))
                    rows = cur.fetchall()
                    count_by_uuid = {row["guild_uuid"]: row["matching_territories"] for row in rows}
                    newWars[territoryName]["AttackerAfter"] = count_by_uuid.get(newWars[territoryName]["AttackerUUID"], 0)
                    newWars[territoryName]["DefenderAfter"] = count_by_uuid.get(newWars[territoryName]["DefenderUUID"], 0)

                    cur.execute("""
                        SELECT
                            guild_uuid,
                            COUNT(*) AS matching_territories
                        FROM territory_changes
                        WHERE (territory, acquired) IN (
                            SELECT territory, MAX(acquired)
                            FROM territory_changes
                            WHERE acquired <= ?
                            GROUP BY territory
                        )
                        AND guild_uuid IN (?, ?)
                        GROUP BY guild_uuid;
                    """, (self.lastAcquiredTime, newWars[territoryName]["AttackerUUID"], newWars[territoryName]["DefenderUUID"],))
                    rows = cur.fetchall()
                    count_by_uuid = {row["guild_uuid"]: row["matching_territories"] for row in rows}
                    newWars[territoryName]["AttackerBefore"] = count_by_uuid.get(newWars[territoryName]["AttackerUUID"], 0)
                    newWars[territoryName]["DefenderBefore"] = count_by_uuid.get(newWars[territoryName]["DefenderUUID"], 0)

                conn.close()
                #logger.info(f"New Wars: {newWars}")
                for serverID, guildList in self.guildsBeingTracked.items():
                    for data in guildList:
                        for territoryName, warData in newWars.items():
                            if data["guildPrefix"] == warData["AttackerPrefix"] or data["guildPrefix"] == warData["DefenderPrefix"] or data["guildPrefix"] == "Global":
                                try:
                                    view, file, isAttacker = await asyncio.to_thread(warComponentBuilder, territoryName, warData, data["guildPrefix"])

                                    guild = await self.bot.fetch_guild(serverID)
                                    channel = await guild.fetch_channel(data["channelForMessages"])

                                    await channel.send(view=view, file=file)

                                    if not isAttacker: # i honestly didnt think about it needing to be defender, it should be isDefender
                                        currentEpochTime = int(time.time())
                                        if self.shouldPing(data, currentEpochTime):
                                            await channel.send(f"<@&{data['pingRoleID']}>")
                                            self.recordPing(serverID, data, currentEpochTime)
                                except Exception:
                                    #logger.exception(f"Failed to send war notification to server {serverID}, channel {data['channelForMessages']}.")
                                    wcode=1 # we could log but like genuinely ts spams my shit because im too lazy to remove inactive channels

            else:
                conn.close()

        except Exception:
            logger.exception(f"Unhandled exception in War Detector.")
        finally:
            if latestRow:
                self.lastAcquiredTime = latestRow["acquired"]
        
        weNewData = None
        try: # World Event tracking; THIS LOOKS LIKE NESTED DOGSHIT.
            success, r = await asyncio.to_thread(makeRequest, "https://api.wynncraft.com/v3/map/world-events")
            if not success:
                logger.error("Error while getting World Event data.")
                return

            weNewData = r.json()
            #logger.info(f"weNewData: {weNewData}")
            if self.weOldData:
                #newWorldEvents = await asyncio.to_thread(checkWorldEventDiff, weNewData, self.weOldData) we will do this once we get reliable data
                newWorldEvents = await asyncio.to_thread(checkWorldEventDiff, self.weOldData, weNewData)
                if self.weConfigDirty:
                    self.loadTrackedWorldEvents()
                    self.weConfigDirty = False

                if newWorldEvents:
                    #logger.info(f"Events found: {newWorldEvents}")
                    # Match tracked entries against the live API names tolerantly (trailing spaces/casing drift on the API).
                    apiByNorm = {str(name).strip().casefold(): name for name in newWorldEvents}
                    for serverID, entries in self.weBeingTracked.items(): # Loop through every entry, see if its one of the new world events
                        for entry in entries:
                            dbEvent = entry["event"]
                            channelForMessages = entry["channelForMessages"]
                            normEvent = str(dbEvent).strip().casefold()

                            if normEvent == "global":
                                matchedEvents = list(newWorldEvents)
                            elif normEvent in apiByNorm:
                                matchedEvents = [apiByNorm[normEvent]]
                            else:
                                #logger.info(f"World events {newWorldEvents} started but tracked entry '{dbEvent}' (server {serverID}) did not match.")
                                continue

                            for apiName in matchedEvents: # apiName is the real, current API name so the file/schedule lookup lines up
                                try:
                                    files, view = await asyncio.to_thread(sendWorldEventMessage, apiName, weNewData)

                                    guild = await self.bot.fetch_guild(serverID)
                                    channel = await guild.fetch_channel(channelForMessages)

                                    await channel.send(view=view, files=files)
                                    if entry["pingRoleID"]:
                                        await channel.send(f"<@&{entry['pingRoleID']}>")
                                except Exception:
                                    logger.exception(f"Failed to send world event '{apiName}' to server {serverID}, channel {channelForMessages}.")

                # Now we need to check for annie ping timers
                for eventJSON in weNewData:
                    if eventJSON["name"] == "Prelude to Annihilation" and eventJSON["schedule"] is not None:
                        # Find time til 
                        annieTime = datetime.fromisoformat(eventJSON["schedule"])
                        currentTime = datetime.now(timezone.utc)
                        
                        diffSeconds = (annieTime - currentTime).total_seconds()
                        diffMinutes = math.ceil(diffSeconds / 60)
                        #logger.info(f"diffMinutes: {diffMinutes}")
                        for serverID, entries in self.weBeingTracked.items():
                            for entry in entries:
                                dbEvent = entry["event"]
                                anniePingTimer = entry["anniePingTimer"]
                                channelForMessages = entry["channelForMessages"]

                                if str(dbEvent) == "Prelude to Annihilation" and anniePingTimer: # Checking for all annie's with a ping timer
                                    try:
                                        timerMinutes = int(anniePingTimer)
                                    except (TypeError, ValueError):
                                        continue
                                    if 0 < diffMinutes <= timerMinutes:
                                        pingKey = (serverID, "Prelude to Annihilation", annieTime.isoformat(), anniePingTimer)

                                        if pingKey in self.anniePings:
                                            continue

                                        self.anniePings.add(pingKey)
                                        #logger.info(f"DETECTION: World Event {entry['event']} is starting!")
                                        try:
                                            files, view = await asyncio.to_thread(sendWorldEventMessage, dbEvent, weNewData)

                                            guild = await self.bot.fetch_guild(serverID)
                                            channel = await guild.fetch_channel(channelForMessages)

                                            await channel.send(view=view, files=files)
                                            if entry["pingRoleID"]:
                                                await channel.send(f"<@&{entry['pingRoleID']}>")
                                        except Exception:
                                            pass

        except Exception:
            logger.exception(f"Unhandled exception in World Event Detector.")
        
        finally: # even if it errors, we still update old data.
            if weNewData is not None:
                self.weOldData = weNewData

        try: # Lootpool Tracking
            # On startup, set a variable to next friday 1pm and 2pm est for lootpool changes
            # Once we loop and we check that we passed that, send out the corresponding lootpools and then update to the next friday
            if 1 == 0:
                print(1) # bs code to keep this alive no errors
        except Exception:
            logger.exception(f"Unhandled exception in Lootpool Detector.")

    detectorAddCommands = app_commands.Group(name="add", description="this is never seen, yet discord flips the x out if its not here.",)
    @detectorAddCommands.command(name="wars", description="Detect when a guild, or globally, loses or gains a territory.")
    @app_commands.describe(
        channel='Channel to set for detection messages.',
        guild_prefix='Prefix of the guild to track Ex: SEQ, ICo. (Case Sensitive); Or \'Global\' for global detection.',
        role='Role to be pinged on territory loss (optional)',
        interval='The cooldown of the pings in minutes (optional)',
    )
    async def addwars(self, interaction: discord.Interaction, channel: Union[discord.TextChannel], guild_prefix: str, role: Optional[discord.Role] = None, interval: Optional[int] = None):
        requiredRoleName = "Detector Permission"
        permission = 0
        for role_Check in interaction.user.roles:
            if role_Check.name.lower() == requiredRoleName.lower():
                permission = 1
        if permission != 1:
            await interaction.response.send_message(f"You do not have the required role to use this command! If you are a server owner, create a role named '{requiredRoleName}', and give it to people who need to run this command.", ephemeral=True)
            return

        logger.info(f"Command /detector add wars was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter channel is: {channel}, guild_prefix is {guild_prefix}, role is {role}, interval is {interval}.")

        message = f'<#{channel.id}> now set! No role will be pinged when territory is lost.'
        success = False

        if guild_prefix in self.guildsBeingTracked:
            if role and interval:
                message += f' Role "{role}" will be pinged every time you lose territory, with a cooldown of {interval} minutes.'
            success = True
        else:
            if role and interval:
                message = f'<#{channel.id}> now set! Role "{role}" will be pinged every time you lose territory, with a cooldown of {interval} minutes.'
            success = True

        if success:
            serverID = str(interaction.guild.id)
            if serverID not in self.guildsBeingTracked:
                self.guildsBeingTracked[serverID] = []
            existing = [cfg for cfg in self.guildsBeingTracked[serverID] if cfg['guildPrefix'] == guild_prefix]
            new_config = {
                'channelForMessages': channel.id,
                'guildPrefix': guild_prefix,
                'pingRoleID': str(role.id) if role else "",
                'intervalForPing': interval if interval else "",
                'lastPing': existing[0].get('lastPing', "") if existing else "" # carry the cooldown over so re-running this doesn't reset it
            }
            if existing: # We check if prefix is already there, if it is we replace
                self.guildsBeingTracked[serverID] = [cfg for cfg in self.guildsBeingTracked[serverID] if cfg['guildPrefix'] != guild_prefix]
            self.guildsBeingTracked[serverID].append(new_config) # append
            logger.info(self.guildsBeingTracked)
            conn = self.connectDetectorDB()
            cur = conn.cursor()
            cur.execute( # upsert rather than INSERT OR REPLACE, which would null out lastPing
                """INSERT INTO wars (serverID, channelForMessages, guildPrefix, pingRoleID, intervalForPing)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(serverID, guildPrefix) DO UPDATE SET
                       channelForMessages = excluded.channelForMessages,
                       pingRoleID = excluded.pingRoleID,
                       intervalForPing = excluded.intervalForPing""",
                (int(serverID), channel.id, guild_prefix, str(role.id) if role else None, interval if interval else None)
            )
            conn.close()
            logger.info(f"War detector now running in background for guild prefix {guild_prefix} for guild id {interaction.guild.id}")

        await interaction.response.send_message(message)

    @detectorAddCommands.command(name="world_events",description="Get notified about upcoming World Events.")
    @app_commands.describe(
        channel='Channel to set for detection messages.',
        event='World event to track (e.g."Prelude to Annihilation"). Use \'Global\' to track all world events. Case Sensitive.',
        role='Role to be pinged when the event is detected (optional)',
        annie_ping_timer='Secondary ping X minutes before annie, only applies if the selected event is annie (optional)',
    )
    async def addworldevents(self, interaction: discord.Interaction, channel: Union[discord.TextChannel], event: str, role: Optional[discord.Role] = None, annie_ping_timer: Optional[int] = None):
        requiredRoleName = "Detector Permission"
        permission = 0
        for role_Check in interaction.user.roles:
            if role_Check.name.lower() == requiredRoleName.lower():
                permission = 1
        if permission != 1:
            await interaction.response.send_message(f"You do not have the required role to use this command! If you are a server owner, create a role named '{requiredRoleName}', and give it to people who need to run this command.", ephemeral=True)
            return

        logger.info(f"Command /detector add world-events was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter channel is: {channel}, event is {event}, role is {role}, annie_ping_timer is {annie_ping_timer}.")

        serverID = int(interaction.guild.id)
        conn = self.connectDetectorDB()
        cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO world_events (serverID, channelForMessages, event, pingRoleID, anniePingTimer) VALUES (?, ?, ?, ?, ?)",
            (serverID, channel.id, event, str(role.id) if role else None, annie_ping_timer if annie_ping_timer else None)
        )
        conn.commit()
        conn.close()
        self.weConfigDirty = True

        message = f'<#{channel.id}> now set for world event "{event}".'
        if role:
            message += f' Role "{role}" will be pinged.'
        if annie_ping_timer and event.lower() == "annie":
            message += f' Secondary ping {annie_ping_timer} minutes before annie.'
        await interaction.response.send_message(message)

    # @detectorAddCommands.command(name="lootpools", description="Get notified about lootpool changes for raids or lootruns.")
    # @app_commands.describe(
    #     channel='Channel to set for detection messages.',
    #     lootpool='Lootpool to track: raids or lootrun.',
    #     role='Role to be pinged when the lootpool changes (optional)',
    # )
    # async def addlootpools(self, interaction: discord.Interaction, channel: Union[discord.TextChannel], lootpool: str, role: Optional[discord.Role] = None):
    #     requiredRoleName = "Detector Permission"
    #     permission = 0
    #     for role_Check in interaction.user.roles:
    #         if role_Check.name.lower() == requiredRoleName.lower():
    #             permission = 1
    #     if permission != 1:
    #         await interaction.response.send_message(f"You do not have the required role to use this command! If you are a server owner, create a role named '{requiredRoleName}', and give it to people who need to run this command.", ephemeral=True)
    #         return

    #     logger.info(f"Command /detector add lootpools was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter channel is: {channel}, lootpool is {lootpool}, role is {role}.")

    #     serverID = int(interaction.guild.id)
    #     conn = self.connectDetectorDB()
    #     cur = conn.cursor()
    #     cur.execute(
    #         "INSERT OR REPLACE INTO lootpools (serverID, channelForMessages, lootpool, pingRoleID) VALUES (?, ?, ?, ?)",
    #         (serverID, channel.id, lootpool, str(role.id) if role else None)
    #     )
    #     conn.close()

    #     message = f'<#{channel.id}> now set for lootpool "{lootpool}".'
    #     if role:
    #         message += f' Role "{role}" will be pinged.'
    #     await interaction.response.send_message(message)

    detectorRemoveCommands = app_commands.Group(name="remove", description="this is never seen, yet discord flips the x out if its not here.",)

    @detectorRemoveCommands.command(name="wars", description="Remove a guild from being detected.")
    async def removewars(self, interaction: discord.Interaction, prefix: str):
        requiredRoleName = "Detector Permission"
        permission = 0
        for role in interaction.user.roles:
            if role.name.lower() == requiredRoleName.lower():
                permission = 1
        if permission != 1:
            await interaction.response.send_message(f"You do not have the required role to use this command! If you are a server owner, create a role named '{requiredRoleName}', and give it to people who need to run this command.", ephemeral=True)
            return

        logger.info(f"Command /detector remove wars was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter prefix is: {prefix}.")
        serverID = str(interaction.guild.id)

        if serverID not in self.guildsBeingTracked:
            await interaction.response.send_message(f"No guilds are currently being tracked in this server.", ephemeral=True)
            return

        trackedList = self.guildsBeingTracked[serverID]
        newTrackedList = [config for config in trackedList if config.get('guildPrefix') != prefix]
        if len(trackedList) == len(newTrackedList): # Checks if the inputted prefix is even in there
            await interaction.response.send_message(f"{prefix} not found for this server.", ephemeral=True)
            return

        if newTrackedList:
            self.guildsBeingTracked[serverID] = newTrackedList
        else:
            del self.guildsBeingTracked[serverID]

        conn = self.connectDetectorDB()
        cur = conn.cursor()
        cur.execute("DELETE FROM wars WHERE serverID = ? AND guildPrefix = ?", (int(serverID), prefix))
        conn.close()
        await interaction.response.send_message(f"{prefix} is no longer being detected.")

    @detectorRemoveCommands.command(name="world_events", description="Remove a world event from being detected.")
    async def removeworldevents(self, interaction: discord.Interaction, event: str):
        requiredRoleName = "Detector Permission"
        permission = 0
        for role in interaction.user.roles:
            if role.name.lower() == requiredRoleName.lower():
                permission = 1
        if permission != 1:
            await interaction.response.send_message(f"You do not have the required role to use this command! If you are a server owner, create a role named '{requiredRoleName}', and give it to people who need to run this command.", ephemeral=True)
            return

        logger.info(f"Command /detector remove world-events was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter event is: {event}.")
        serverID = int(interaction.guild.id)

        conn = self.connectDetectorDB()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM world_events WHERE serverID = ? AND event = ?", (serverID, event))
        exists = cur.fetchone()
        if not exists:
            conn.close()
            await interaction.response.send_message(f'World event "{event}" not found for this server.', ephemeral=True)
            return

        cur.execute("DELETE FROM world_events WHERE serverID = ? AND event = ?", (serverID, event))
        conn.close()
        self.weConfigDirty = True
        await interaction.response.send_message(f'World event "{event}" is no longer being detected.')

    # @detectorRemoveCommands.command(name="lootpools", description="Remove a lootpool from being detected.")
    # async def removelootpools(self, interaction: discord.Interaction, lootpool: str):
    #     requiredRoleName = "Detector Permission"
    #     permission = 0
    #     for role in interaction.user.roles:
    #         if role.name.lower() == requiredRoleName.lower():
    #             permission = 1
    #     if permission != 1:
    #         await interaction.response.send_message(f"You do not have the required role to use this command! If you are a server owner, create a role named '{requiredRoleName}', and give it to people who need to run this command.", ephemeral=True)
    #         return

    #     logger.info(f"Command /detector remove lootpools was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter lootpool is: {lootpool}.")
    #     serverID = int(interaction.guild.id)

    #     conn = self.connectDetectorDB()
    #     cur = conn.cursor()
    #     cur.execute("SELECT 1 FROM lootpools WHERE serverID = ? AND lootpool = ?", (serverID, lootpool))
    #     exists = cur.fetchone()
    #     if not exists:
    #         conn.close()
    #         await interaction.response.send_message(f'Lootpool "{lootpool}" not found for this server.', ephemeral=True)
    #         return

    #     cur.execute("DELETE FROM lootpools WHERE serverID = ? AND lootpool = ?", (serverID, lootpool))
    #     conn.close()
    #     await interaction.response.send_message(f'Lootpool "{lootpool}" is no longer being detected.')

    detectorConfigCommands = app_commands.Group(name="configurations", description="this is never seen, yet discord flips the x out if its not here.",)

    @removewars.autocomplete('prefix')
    async def autocomplete_remove_wars(self, interaction: discord.Interaction, current: str):
        choices = []

        requiredRoleName = "Detector Permission"
        permission = 0
        for role in interaction.user.roles:
            if role.name.lower() == requiredRoleName.lower():
                permission = 1
        if permission != 1:
            return choices # This is so the user in question doesnt get info on the currently-running detector

        serverID = str(interaction.guild.id)  # This gets the current server ID
        if serverID not in self.guildsBeingTracked:
            return choices
        def truncate(text: str, max_length: int = 15) -> str:
            return text if len(text) <= max_length else text[:12] + "..."
        for config in self.guildsBeingTracked[serverID]:
            guildPrefix = config.get('guildPrefix', '')
            if current.lower() in guildPrefix.lower():
                roleID = config.get('pingRoleID', '')
                roleName = "No Role"
                if roleID:
                    role = interaction.guild.get_role(int(roleID))
                    if role:
                        roleName = truncate(role.name)
                    else: # If they delete it or similar
                        roleName = "Unknown Role"
                interval = config.get('intervalForPing', 'No Interval')
                channelID = config.get('channelForMessages', '') # This should always be there, but redundancy type shit
                choices.append(
                    app_commands.Choice(
                        name=f"Guild Prefix: {guildPrefix} | Channel ID: {channelID} | Role Name: {roleName} | Interval: {interval}",
                        value=guildPrefix
                    )
                )

        return choices

    @removeworldevents.autocomplete('event')
    async def autocomplete_remove_world_events(self, interaction: discord.Interaction, current: str):
        choices = []

        requiredRoleName = "Detector Permission"
        permission = 0
        for role in interaction.user.roles:
            if role.name.lower() == requiredRoleName.lower():
                permission = 1
        if permission != 1:
            return choices

        serverID = int(interaction.guild.id)
        conn = self.connectDetectorDB()
        cur = conn.cursor()
        cur.execute("SELECT event, channelForMessages, pingRoleID FROM world_events WHERE serverID = ?", (serverID,))
        rows = cur.fetchall()
        conn.close()

        def truncate(text: str, max_length: int = 15) -> str:
            return text if len(text) <= max_length else text[:12] + "..."

        for event, channelID, roleID in rows:
            if current.lower() in event.lower():
                roleName = "No Role"
                if roleID:
                    r = interaction.guild.get_role(int(roleID))
                    if r:
                        roleName = truncate(r.name)
                    else:
                        roleName = "Unknown Role"
                choices.append(
                    app_commands.Choice(
                        name=f"Event: {event} | Channel ID: {channelID} | Role: {roleName}",
                        value=event
                    )
                )

        return choices

    # @removelootpools.autocomplete('lootpool')
    # async def autocomplete_remove_lootpools(self, interaction: discord.Interaction, current: str):
    #     choices = []

    #     requiredRoleName = "Detector Permission"
    #     permission = 0
    #     for role in interaction.user.roles:
    #         if role.name.lower() == requiredRoleName.lower():
    #             permission = 1
    #     if permission != 1:
    #         return choices

    #     serverID = int(interaction.guild.id)
    #     conn = self.connectDetectorDB()
    #     cur = conn.cursor()
    #     cur.execute("SELECT lootpool, channelForMessages, pingRoleID FROM lootpools WHERE serverID = ?", (serverID,))
    #     rows = cur.fetchall()
    #     conn.close()

    #     def truncate(text: str, max_length: int = 15) -> str:
    #         return text if len(text) <= max_length else text[:12] + "..."

    #     for lootpool, channelID, roleID in rows:
    #         if current.lower() in lootpool.lower():
    #             roleName = "No Role"
    #             if roleID:
    #                 r = interaction.guild.get_role(int(roleID))
    #                 if r:
    #                     roleName = truncate(r.name)
    #                 else:
    #                     roleName = "Unknown Role"
    #             choices.append(
    #                 app_commands.Choice(
    #                     name=f"Lootpool: {lootpool} | Channel ID: {channelID} | Role: {roleName}",
    #                     value=lootpool
    #                 )
    #             )

    #     return choices

async def setup(bot):
    await bot.add_cog(Detector(bot))