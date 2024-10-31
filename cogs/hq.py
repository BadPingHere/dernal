import discord
from discord.ext import commands
from discord import app_commands
from typing import Optional
from lib.utils import makeRequest, getTerritoryNames
import logging

logger = logging.getLogger('discord')

class HQ(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(description="Outputs the top hq locations.")
    @app_commands.describe(
        guild='Prefix of the guild Ex: TAq, ICo.',
    )
    async def hq(self, interaction: discord.Interaction, guild: Optional[str]):
        logger.info(f"Command /hq was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {guild}.")
        URL = "https://api.wynncraft.com/v3/guild/list/territory"
        r = await makeRequest(URL)
        untainteddata = r.json()
        embed = await getTerritoryNames(untainteddata, guild if guild else None)
        await interaction.response.send_message(embed=embed)

async def setup(bot):
    await bot.add_cog(HQ(bot))