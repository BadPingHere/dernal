import discord
from discord.ext import commands
from discord import app_commands
from typing import Optional
from lib.utils import makeRequest, getTerritoryNames
import logging
import asyncio

logger = logging.getLogger('discord')

class HQ(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.allowed_installs(guilds=True, users=True)
    @app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)  
    @app_commands.command(description="Outputs the top hq locations.")
    @app_commands.describe(
        guild='Prefix of the guild Ex: TAq, ICo. (Case Sensitive)',
    )
    async def hq(self, interaction: discord.Interaction, guild: Optional[str]):
        logger.info(f"Command /hq was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {guild}.")
        URL = "https://api.wynncraft.com/v3/guild/list/territory"
        success, r = await asyncio.to_thread(makeRequest, URL)
        if not success:
            logger.error("Error while getting request in /hq")
            await interaction.response.send_message("There was an error while getting data from the API. If this issue is persistent, please report it on my github.", ephemeral=True)
            return
        untainteddata = r.json()
        embed = await asyncio.to_thread(getTerritoryNames, untainteddata, guild if guild else None)
        await interaction.response.send_message(embed=embed)

async def setup(bot):
    await bot.add_cog(HQ(bot))