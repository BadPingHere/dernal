import discord
from discord.ext import commands, tasks
from discord import app_commands
from typing import Optional, Union
import logging
import os
import json
import shelve
from dotenv import load_dotenv
from lib.utils import checkterritories, makeRequest
import asyncio

logger = logging.getLogger('discord')
load_dotenv()

@app_commands.allowed_installs(guilds=True, users=False)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=True)
class Detector(commands.GroupCog, name="detector"):
    def __init__(self, bot):
        self.bot = bot
        self.guildsBeingTracked = {}
        self.timesinceping = {}
        self.hasbeenran = {}
        self.expectedterrcount = {}
        self.untainteddata = {}
        self.untainteddataOLD = {}
        self.serverID = int(os.getenv("SERVER_ID") or 0)
        rootDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.detectorFilePath = os.path.join(rootDir, 'database', 'detector')
        with shelve.open(self.detectorFilePath) as db:
            self.guildsBeingTracked = dict(db)
        self.backgroundDetector.start()

            

    @tasks.loop(seconds=15)
    async def backgroundDetector(self):
        if not self.guildsBeingTracked:
            return

        with shelve.open(self.detectorFilePath) as detectorStorage:
            self.guildsBeingTracked = dict(detectorStorage)
        
        # Get new data
        success, r = await asyncio.to_thread(makeRequest, "https://api.wynncraft.com/v3/guild/list/territory")
        if not success:
            logger.error("Error while getting territory data.")
            return
            
        stringdata = str(r.json())
        new_data = r.json()
        
        # Initialize expected territory counts for all tracked guilds
        for guild_id in self.guildsBeingTracked:
            config = self.guildsBeingTracked[guild_id]
            guildPrefix = config['guildPrefix']
            if guildPrefix not in self.expectedterrcount:
                self.expectedterrcount[guildPrefix] = stringdata.count(guildPrefix)
                self.hasbeenran[guild_id] = True
        
        # Only process territory checks if we have both current and old data
        if self.untainteddata:  # Only if we already have some data
            # Store current data as old before processing
            old_data = self.untainteddata
            
            # Process data for each guild
            guilds_to_check = list(self.guildsBeingTracked.keys())
            
            for guild_id in guilds_to_check:
                config = self.guildsBeingTracked[guild_id]
                guildPrefix = config['guildPrefix']
                pingRoleID = config["pingRoleID"]
                #logger.info(f"guildPrefix: {guildPrefix}")
                try:
                    channel_id = config['channelForMessages']
                    guild = await self.bot.fetch_guild(guild_id)
                    channelForMessages = await guild.fetch_channel(channel_id)
                except Exception as e: # if someone kicks the bot or similar, theyll enter here and always be pinging discord servers every 20s. As much as I care about discord's api, i dont care enough to change the code.
                    continue

                intervalForPing = config["intervalForPing"]
                
                # Check territories using current and old data
                messagesToSend = await asyncio.to_thread(checkterritories, new_data, old_data, guildPrefix, pingRoleID, self.expectedterrcount, intervalForPing, self.hasbeenran, self.timesinceping)
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

        if serverID in self.guildsBeingTracked: # Search the guild id in tracked guilds
            trackedData = self.guildsBeingTracked[serverID]
            if trackedData.get('guildPrefix') == prefix: # check if the prefix is the same, which it should be
                del self.guildsBeingTracked[serverID] # deletes from running
                with shelve.open(self.detectorFilePath) as detectorStorage:
                    if serverID in detectorStorage:
                        del detectorStorage[serverID] # deletes from storage
                await interaction.response.send_message(f"{prefix} is no longer being detected.")
                return

        await interaction.response.send_message(f"{prefix} not found for this server.", ephemeral=True)

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
        #logger.info(f"self.guildsBeingTracked.items(): {self.guildsBeingTracked.items()}")
        for server_id, data in self.guildsBeingTracked.items():
            if server_id == serverID: # make sure we are only showing results for the server we are in
                guildPrefix = data.get('guildPrefix', '')
                if current.lower() in guildPrefix.lower():  # Match prefix based on current input
                    roleID = data.get('pingRoleID', '')
                    roleName = "No Role" if not roleID else (interaction.guild.get_role(int(roleID)).name or "Unknown Role")
                    interval = data.get('intervalForPing', 'No Interval')
                    choices.append(
                        app_commands.Choice(
                            name=f"Guild: {guildPrefix} | Channel: {data['channelForMessages']} | Role: {roleName} | Interval: {interval}",
                            value=guildPrefix
                        )
                    )
        
        if not choices:
            logger.info(f"No autocomplete options found for current input: {current} in guild {serverID}")
        
        return choices

    @app_commands.command(description="Add a guild to detect.")
    @app_commands.describe(
        channel='Channel to set',
        guild_prefix='Prefix of the guild to track Ex: SEQ, ICo. (Case Sensitive)',
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
            message = f'This guild is already being detected, so we will change its configurations.\n<#{channel.id}> now set!'
            if role and interval:
                message += f' Role "{role}" will be pinged every time you lose territory, with a cooldown of {interval} minutes.'
            success = True
        elif len(self.guildsBeingTracked) > 10:
            message = f'You have too many guilds being tracked with Detector! The maximum limit you can have is 10. You can remove tracked guilds with /detector remove.'
        else:
            if role and interval:
                message = f'<#{channel.id}> now set! Role "{role}" will be pinged every time you lose territory, with a cooldown of {interval} minutes.'
            success = True

        if success:
            self.guildsBeingTracked[interaction.guild.id] = {
                'channelForMessages': channel.id,
                'guildPrefix': guild_prefix,
                'pingRoleID': str(role.id) if role else "",
                'intervalForPing': interval if interval else ""
            }
            logger.info(self.guildsBeingTracked)
            with shelve.open(self.detectorFilePath) as detectorStorage:
                detectorStorage[str(interaction.guild.id)] = self.guildsBeingTracked[interaction.guild.id]
            logger.info(f"War detector now running in background for guild prefix {guild_prefix} for guild id {interaction.guild.id}")

        await interaction.response.send_message(message)

async def setup(bot):
    await bot.add_cog(Detector(bot))