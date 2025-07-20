import discord
from discord.ext import commands, tasks
from discord import app_commands
from typing import Optional, Union
import logging
import os
import shelve
from dotenv import load_dotenv
from lib.utils import checkterritories, makeRequest, detect_graids
import asyncio
from datetime import datetime

logger = logging.getLogger('discord')
load_dotenv()

@app_commands.allowed_installs(guilds=True, users=False)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=True)
class Detector(commands.GroupCog, name="detector"):
    def __init__(self, bot):
        self.bot = bot
        self.guildsBeingTracked = {}
        self.timesinceping = {}
        self.expectedterrcount = {}
        self.untainteddata = {}
        self.untainteddataOLD = {}
        self.EligibleGuilds = []
        self.serverID = int(os.getenv("SERVER_ID") or 0)
        rootDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.detectorFilePath = os.path.join(rootDir, 'database', 'detector')
        self.territoryFilePath = os.path.join(rootDir, 'database', 'territory')
        self.graidFilePath = os.path.join(rootDir, 'database', 'graid')
        with shelve.open(self.detectorFilePath) as db:
            self.guildsBeingTracked = dict(db)
            #logger.info(f"self.guildsBeingTracked: {self.guildsBeingTracked}")
        with shelve.open(self.territoryFilePath) as territoryStorage:
            self.historicalTerritories = territoryStorage.get("historicalTerritories", {})
        self.backgroundDetector.start()
        self.collectGraidData.start()

            

    @tasks.loop(seconds=15)
    async def backgroundDetector(self):
        try:
            if not self.guildsBeingTracked:
                return

            with shelve.open(self.detectorFilePath) as detectorStorage:
                self.guildsBeingTracked = dict(detectorStorage)
        

            # Get new data
            success, r = await asyncio.to_thread(makeRequest, "https://api.wynncraft.com/v3/guild/list/territory")
            if not success:
                logger.error("Error while getting territory data.")
                return
                
            new_data = r.json()
            
            
            # Only process territory checks if we have both current and old data
            if self.untainteddata:  # Only if we already have some data
                # Store current data as old before processing
                old_data = self.untainteddata
                
                # Process data for each guild
                guilds_to_check = list(self.guildsBeingTracked.keys())
                
                # Get Heatmap data, sadlt the best place to run it at.
                dateMonth = str(datetime.now().month)+"/"+str(datetime.now().day)
                for territory, data in new_data.items():
                    oldGuild = old_data[str(territory)]['guild']['prefix']
                    newGuild = data['guild']['prefix']
                    if dateMonth not in self.historicalTerritories: # init today's date
                        self.historicalTerritories[dateMonth] = {}
                    if territory not in self.historicalTerritories[dateMonth]: # Init territory to 0
                        self.historicalTerritories[dateMonth][territory] = 0
                    if oldGuild != newGuild: # Means a change of hands, we add to our heatmap shit
                        self.historicalTerritories[dateMonth][territory] += 1
                with shelve.open(self.territoryFilePath) as territoryStorage:
                    territoryStorage["historicalTerritories"] = self.historicalTerritories

                for guildID in guilds_to_check:
                    if guildID not in self.guildsBeingTracked:
                        continue  # thanks dvs for crashing my shit for like 6 hours ebcauser of this
                    configList = self.guildsBeingTracked[guildID]
                    for config in configList:
                        guildPrefix = config['guildPrefix']
                        key = (guildID, guildPrefix)
                        if guildPrefix.lower() != "global" and key not in self.expectedterrcount:
                            self.expectedterrcount[key] = sum(1 for d in new_data.values()if d["guild"]["prefix"] == guildPrefix)

                        pingRoleID = config["pingRoleID"]
                        #logger.info(f"guildPrefix: {guildPrefix}")
                        try:
                            channel_id = config['channelForMessages']
                            guild = await self.bot.fetch_guild(guildID)
                            channelForMessages = await guild.fetch_channel(channel_id)
                        except Exception as e: # if someone kicks the bot or similar, theyll enter here and always be pinging discord servers every 20s. As much as I care about discord's api, i dont care enough to change the code.
                            continue

                        intervalForPing = config["intervalForPing"]
                        
                        # Check territories using current and old data
                        messagesToSend = await asyncio.to_thread(checkterritories, new_data, old_data, guildPrefix, pingRoleID, self.expectedterrcount, intervalForPing, self.timesinceping, guildID)
                        #logger.info(f"messagesToSend: {messagesToSend}")
                        if messagesToSend:
                            for message_info in messagesToSend:
                                try:
                                    await channelForMessages.send(embed=message_info['embed'])
                                        
                                    if message_info["shouldPing"]:
                                        await channelForMessages.send(f"<@&{message_info['roleID']}>")
                                except discord.DiscordException as err:
                                    logger.error(f"Error sending message: {err}")
            # Update the current data for next iteration
            self.untainteddata = new_data
        except Exception as e: # For one of the many errors
            logger.error(f"Unhandled exception in Detector: {e}", exc_info=True)

    @tasks.loop(seconds=60)
    async def collectGraidData(self): # technically this doesnt belong in detector... but its for sure detecting shit.
        try:
            if not self.EligibleGuilds: #init lvl 100 guilds
                success, r = await asyncio.to_thread(makeRequest, "https://api.wynncraft.com/v3/leaderboards/guildLevel")
                if not success:
                    logger.error(f"Unsucessful request in collectGraidData: {success}")
                for num, data in (r.json()).items():
                    if int(data["level"]) >= 100:
                        self.EligibleGuilds.append(data["prefix"])
                    else:
                        break
                with shelve.open(self.graidFilePath) as db:
                    db['EligibleGuilds'] = self.EligibleGuilds
            await asyncio.to_thread(detect_graids, self.EligibleGuilds)
        except Exception as e: # For one of the many errors
            logger.error(f"Unhandled exception in collectGraidData: {e}", exc_info=True)

    @app_commands.command(name="remove", description="Remove a guild from being detected.")
    async def remove(self, interaction: discord.Interaction, prefix: str):
        requiredRoleName = "Detector Permission"
        #logger.info(f"interaction.user.roles: {interaction.user.roles}")
        permission = 0
        for role in interaction.user.roles:
            if role.name.lower() == requiredRoleName.lower():
                permission = 1
        if permission != 1:
            await interaction.response.send_message(f"You do not have the required role to use this command! If you are a server owner, create a role named '{requiredRoleName}', and give it to people who need to run this command.", ephemeral=True)
            return
        
        logger.info(f"Command /detector remove was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter prefix is: {prefix}.")
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

        with shelve.open(self.detectorFilePath) as detectorStorage:
            if newTrackedList:
                detectorStorage[serverID] = newTrackedList
            else:
                if serverID in detectorStorage:
                    del detectorStorage[serverID]
        await interaction.response.send_message(f"{prefix} is no longer being detected.")

    @remove.autocomplete('prefix')
    async def autocomplete_remove(self, interaction: discord.Interaction, current: str):
        choices = []

        requiredRoleName = "Detector Permission"
        #logger.info(f"interaction.user.roles: {interaction.user.roles}")
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
        #logger.info(f"self.guildsBeingTracked.items(): {self.guildsBeingTracked.items()}")
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

    @app_commands.command(description="Add a guild to detect.")
    @app_commands.describe(
        channel='Channel to set',
        guild_prefix='Prefix of the guild to track Ex: SEQ, ICo. (Case Sensitive); Or \'Global\' for global detection.',
        role='Role to be pinged on territory loss (optional)',
        interval='The cooldown of the pings in minutes (optional)',
    )  
    async def add(self, interaction: discord.Interaction, channel: Union[discord.TextChannel], guild_prefix: str, role: Optional[discord.Role] = None, interval: Optional[int] = None):
        requiredRoleName = "Detector Permission"
        #logger.info(f"interaction.user.roles: {interaction.user.roles}")
        permission = 0
        for role_Check in interaction.user.roles:
            if role_Check.name.lower() == requiredRoleName.lower():
                #logger.info(f"role.name: {role.name}")
                permission = 1
        if permission != 1:
            await interaction.response.send_message(f"You do not have the required role to use this command! If you are a server owner, create a role named '{requiredRoleName}', and give it to people who need to run this command.", ephemeral=True)
            return
        
        logger.info(f"Command /detector add was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter channel is: {channel}, guild_prefix is {guild_prefix}, role is {role}, interval is {interval}.")
        
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
            new_config = {
                'channelForMessages': channel.id,
                'guildPrefix': guild_prefix,
                'pingRoleID': str(role.id) if role else "",
                'intervalForPing': interval if interval else ""
            }
            if serverID not in self.guildsBeingTracked:
                self.guildsBeingTracked[serverID] = []
            existing = [cfg for cfg in self.guildsBeingTracked[serverID] if cfg['guildPrefix'] == guild_prefix] 
            if existing: # We check if prefix is already there, if it is re replace
                self.guildsBeingTracked[serverID] = [cfg for cfg in self.guildsBeingTracked[serverID] if cfg['guildPrefix'] != guild_prefix]
            self.guildsBeingTracked[serverID].append(new_config) # append
            logger.info(self.guildsBeingTracked)
            with shelve.open(self.detectorFilePath) as detectorStorage:
                detectorStorage[str(serverID)] = self.guildsBeingTracked[serverID]
            logger.info(f"War detector now running in background for guild prefix {guild_prefix} for guild id {interaction.guild.id}")

        await interaction.response.send_message(message)

async def setup(bot):
    await bot.add_cog(Detector(bot))