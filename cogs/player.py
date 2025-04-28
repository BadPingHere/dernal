import discord
from discord.ext import commands
from discord import app_commands
from discord.ui import View, Button
import time
from lib.utils import playerActivityPlaytime, playerActivityXP
import sqlite3
import logging
import asyncio

logger = logging.getLogger('discord')

@app_commands.allowed_installs(guilds=True, users=True)
@app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
class Player(commands.GroupCog, name="player"):
    def __init__(self, bot):
        self.bot = bot
        self.guildLookupCooldown = 0
    activityCommands = app_commands.Group(name="activity", description="this is never seen, yet discord flips the fuck out if its not here.")
    
    @activityCommands.command(name="playtime", description="Shows the graph displaying the average amount of playtime every day over the past two weeks.")
    @app_commands.describe(name='Username of the player search Ex: BadPingHere, Salted.',)
    async def activityPlaytime(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /player activity playtime was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter username is: {name}.")
        current_time = time.time()
        elapsed = current_time - self.guildLookupCooldown

        if elapsed <= 5:
            remaining = 5 - elapsed
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {remaining:.1f} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        self.guildLookupCooldown = current_time
        conn = sqlite3.connect('database/guild_activity.db')
        cursor = conn.cursor()
        
        cursor.execute("SELECT uuid FROM members WHERE name = ? COLLATE NOCASE", (name,))
        result = cursor.fetchone()
        if not result:
            cursor.execute("SELECT uuid FROM members WHERE name = ? COLLATE NOCASE", (name,))
            result = cursor.fetchone()
    
        if not result:
            await interaction.followup.send(f"No data found for username: {name}", ephemeral=True)
            conn.close()
            return
            
        file, embed = await asyncio.to_thread(playerActivityPlaytime, result[0], name)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 14 days.")
    
    @activityCommands.command(name="xp", description="Shows the graph displaying the average amount of xp gain every day over the past two weeks.")
    @app_commands.describe(name='Username of the player search Ex: BadPingHere, Salted.',)
    async def activityXP(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /player activity xp was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter username is: {name}.")
        current_time = time.time()
        elapsed = current_time - self.guildLookupCooldown

        if elapsed <= 5:
            remaining = 5 - elapsed
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {remaining:.1f} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        self.guildLookupCooldown = current_time
        conn = sqlite3.connect('database/guild_activity.db')
        cursor = conn.cursor()
        
        cursor.execute("SELECT uuid FROM members WHERE name = ? COLLATE NOCASE", (name,))
        result = cursor.fetchone()
        if not result:
            cursor.execute("SELECT uuid FROM members WHERE name = ? COLLATE NOCASE", (name,))
            result = cursor.fetchone()
    
        if not result:
            await interaction.followup.send(f"No data found for username: {name}", ephemeral=True)
            conn.close()
            return
            
        file, embed = await asyncio.to_thread(playerActivityXP, result[0], name)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 14 days.")
    
  
async def setup(bot):
    await bot.add_cog(Player(bot))