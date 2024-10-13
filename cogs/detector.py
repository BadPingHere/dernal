import discord
from discord.ext import commands, tasks
from discord import app_commands
from typing import Optional, Union
import logging
from lib.utils import checkterritories, getTerrData

logger = logging.getLogger('discord')

class Detector(commands.GroupCog, name="detector"):
    def __init__(self, bot):
        self.bot = bot
        self.guildsBeingTracked = {}
        self.timesinceping = {}
        self.hasbeenran = {}
        self.expectedterrcount = {}
        self.untainteddata = {}
        self.untainteddataOLD = {}
        self.backgroundDetector.start()

    @tasks.loop(seconds=20)
    async def backgroundDetector(self):
        if not self.guildsBeingTracked:
            return

        guilds_to_check = list(self.guildsBeingTracked.keys())
        for guildPrefix in guilds_to_check:
            pingRoleID = self.guildsBeingTracked[guildPrefix]["pingRoleID"]
            channelForMessages = self.guildsBeingTracked[guildPrefix]["channelForMessages"]
            intervalForPing = self.guildsBeingTracked[guildPrefix]["intervalForPing"]
            if guildPrefix in self.guildsBeingTracked:
                if not self.hasbeenran.get(guildPrefix):
                    returnData = await getTerrData(self.untainteddata, self.untainteddataOLD)
                    self.expectedterrcount[guildPrefix] = returnData["stringdata"].count(guildPrefix)
                    self.untainteddata = returnData["untainteddata"]
                    self.untainteddataOLD = returnData["untainteddataOLD"]
                    self.hasbeenran[guildPrefix] = True
                await checkterritories(self.untainteddata, self.untainteddataOLD, guildPrefix, pingRoleID, channelForMessages, self.expectedterrcount, intervalForPing, self.hasbeenran, self.timesinceping)

    @app_commands.command(name="remove", description="Remove a guild from being detected.")
    async def remove(self, interaction: discord.Interaction, prefix: str):
        if prefix in self.guildsBeingTracked:
            del self.guildsBeingTracked[prefix]
            await interaction.response.send_message(f"{prefix} is no longer being detected.")
        else:
            await interaction.response.send_message(f"{prefix} not found.", ephemeral=True)

    @remove.autocomplete('prefix')
    async def autocomplete_remove(self, interaction: discord.Interaction, current: str):
        choices = []
        for key, value in self.guildsBeingTracked.items():
            if current.lower() in key.lower():
                roleID = value['pingRoleID']
                roleName = "No Role" if roleID == "" else interaction.guild.get_role(int(roleID)).name
                interval = value['intervalForPing'] if value['intervalForPing'] else "No Interval"
                choices.append(
                    app_commands.Choice(
                        name=f"Guild: {key} | Channel: {value['channelForMessages']} | Role: {roleName} | Interval: {interval}",
                        value=key
                    )
                )
        return choices

    @app_commands.command(description="Add a guild to detect.")
    @app_commands.describe(
        channel='Channel to set',
        guild_prefix='Prefix of the guild to track Ex: SEQ, ICo.',
        role='Role to be pinged (optional)',
        interval='The cooldown on the pings in minutes (optional)',
    )
    async def add(self, interaction: discord.Interaction, channel: Union[discord.TextChannel], guild_prefix: str, role: Optional[discord.Role] = None, interval: Optional[int] = None):
        message = f'<#{channel.id}> now set! No role will be pinged when territory is lost.'
        success = False

        if guild_prefix in self.guildsBeingTracked:
            message = f'This guild is already being detected, so we will change its configurations.\n<#{channel.id}> now set!'
            if role and interval:
                message += f' Role "{role}" will be pinged every time you lose territory, with a cooldown of {interval} minutes.'
            success = True
        elif len(self.guildsBeingTracked) > 15:
            message = f'You have too many guilds being tracked with Detector! The maximum limit you can have is 15. You can remove tracked guilds with /detector remove.'
        else:
            if role and interval:
                message = f'<#{channel.id}> now set! Role "{role}" will be pinged every time you lose territory, with a cooldown of {interval} minutes.'
            success = True

        if success:
            self.guildsBeingTracked[guild_prefix] = {
                'channelForMessages': channel,
                'pingRoleID': str(role.id) if role else "",
                'intervalForPing': interval if interval else ""
            }
            logger.info(f"War detector now running in background for guild prefix {guild_prefix}")

        await interaction.response.send_message(message)

async def setup(bot):
    await bot.add_cog(Detector(bot))