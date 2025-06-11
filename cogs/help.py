import discord
from discord.ext import commands
from discord import app_commands
from typing import Optional
from lib.utils import getHelp
import logging
import asyncio

logger = logging.getLogger('discord')

class Help(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.allowed_installs(guilds=True, users=True)
    @app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)  
    @app_commands.command(description="Provides help and info on commands.")
    @app_commands.describe(
        command='Command that you need help with.',
    )
    async def help(self, interaction: discord.Interaction, command: Optional[str]):
        logger.info(f"Command /help was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter command is: {command}.")
        
        embedOrMessage, success = await asyncio.to_thread(getHelp, command if command else None)
        if success:
            await interaction.response.send_message(embed=embedOrMessage)
        else:
            await interaction.response.send_message(embedOrMessage)


async def setup(bot):
    await bot.add_cog(Help(bot))