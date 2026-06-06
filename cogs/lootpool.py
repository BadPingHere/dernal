import discord
from discord.ext import commands
from discord import app_commands
from discord.ui import View, Button
from typing import Optional
from lib.utils import getLootrunComponent, getRaidComponent
from lib.makeRequest import makeRequest
import logging
import asyncio
from datetime import datetime, timezone

logger = logging.getLogger('discord')

class Lootpool(commands.GroupCog , name="lootpool"):
    def __init__(self, bot):
        self.bot = bot
        self.lootrunCamps = [
            "Canyon of the Lost Excursion (South)",
            "The Corkus Traversal",
            "Molten Heights Hike",
            "Sky Islands Exploration",
            "Silent Expanse Expedition",
            "The Fruma Foray (West)",
            "The Fruma Foray (East)",]
        self.raidCamp = [
            "Nest of the Grootslangs",
            "Orphion's Nexus of Light",
            "The Canyon Colossus",
            "The Nameless Anomaly",
            "The Wartorn Palace",]

    async def autocompleteLootrun(self, interaction: discord.Interaction, current: str):
        return [app_commands.Choice(name=k, value=k)for k in self.lootrunCamps if current.lower() in k.lower()][:25]
    
    async def autocompleteRaids(self, interaction: discord.Interaction, current: str):
        return [app_commands.Choice(name=k, value=k)for k in self.raidCamp if current.lower() in k.lower()][:25]

    @app_commands.allowed_installs(guilds=True, users=True)
    @app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)  
    @app_commands.command(description="Outputs the lootpool of all Lootrun camps (Mythics+ and Wards)")
    @app_commands.describe(camp='The specific camp you wish to get the lootpool of (Shows all items)',)
    async def lootrun(self, interaction: discord.Interaction, camp: Optional[str]):
        if camp and camp not in self.lootrunCamps: # invalid!
            await interaction.response.send_message("Invalid camp given. Please use the autocompleted camps.", ephemeral=True)
            
        view = await asyncio.to_thread(getLootrunComponent, camp if camp else None)
        await interaction.response.send_message(view=view)


    @app_commands.allowed_installs(guilds=True, users=True)
    @app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)  
    @app_commands.command(description="Outputs the lootpool of all raid camps (Aspects only)")
    @app_commands.describe(camp='The specific raid camp you wish to get the lootpool of (Shows all items)',)
    async def raid(self, interaction: discord.Interaction, camp: Optional[str]):
        if camp and camp not in self.raidCamp: # invalid!
            await interaction.response.send_message("Invalid camp given. Please use the autocompleted camps.", ephemeral=True)
            
        view = await asyncio.to_thread(getRaidComponent, camp if camp else None)
        await interaction.response.send_message(view=view)

    lootrun.autocomplete("camp")(autocompleteLootrun)
    raid.autocomplete("camp")(autocompleteRaids)


async def setup(bot):
    await bot.add_cog(Lootpool(bot))