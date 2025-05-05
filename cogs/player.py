import discord
from discord.ext import commands
from discord import app_commands
import time
from lib.utils import checkCooldown, playerActivityPlaytime, playerActivityContributions, playerActivityDungeons, playerActivityTotalDungeons, playerActivityRaids, playerActivityTotalRaids, playerActivityMobsKilled, playerActivityWars, playerLeaderboardRaids, playerLeaderboardTotalLevel, playerLeaderboardDungeons, playerLeaderboardPlaytime, playerLeaderboardPVPKills
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
        conn = sqlite3.connect('database/player_activity.db')
        cursor = conn.cursor()
        #logger.info("Tables:", cursor.fetchall())
        cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
        result = cursor.fetchone()
        if not result:
            cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
            result = cursor.fetchone()
    
        if not result:
            await interaction.followup.send(f"No data found for username: {name}. Please make sure you have logged in recently, and in a supported guild.", ephemeral=True)
            conn.close()
            return
            
        file, embed = await asyncio.to_thread(playerActivityPlaytime, result[0], name)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 14 days.")
    
    @activityCommands.command(name="contribution", description="Shows a graph displaying the amount of contributiond xp every day over the past two weeks.")
    @app_commands.describe(name='Username of the player search Ex: BadPingHere, Salted.',)
    async def activityContributions(self, interaction: discord.Interaction, name: str):
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
            
        file, embed = await asyncio.to_thread(playerActivityContributions, result[0], name)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 14 days.")

    @activityCommands.command(name="dungeons", description="Shows a graph displaying the amount of dungeons completed total every day for the past week.")
    @app_commands.describe(name='Username of the player search Ex: BadPingHere, Salted.',)
    async def activityDungeons(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /player activity dungeons was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter username is: {name}.")
        current_time = time.time()
        elapsed = current_time - self.guildLookupCooldown

        if elapsed <= 5:
            remaining = 5 - elapsed
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {remaining:.1f} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        self.guildLookupCooldown = current_time
        conn = sqlite3.connect('database/player_activity.db')
        cursor = conn.cursor()
        
        cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
        result = cursor.fetchone()
        if not result:
            cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
            result = cursor.fetchone()
    
        if not result:
            await interaction.followup.send(f"No data found for username: {name}. Please make sure you have logged in recently, and in a supported guild.", ephemeral=True)
            conn.close()
            return
            
        file, embed = await asyncio.to_thread(playerActivityDungeons, result[0], name)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 7 days.")

    @activityCommands.command(name="dungeons_pie", description="Shows a pie chart displaying the different dungeons's you have done.")
    @app_commands.describe(name='Username of the player search Ex: BadPingHere, Salted.',)
    async def activityDungeonsPie(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /player activity dungeons_pie was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter username is: {name}.")
        current_time = time.time()
        elapsed = current_time - self.guildLookupCooldown

        if elapsed <= 5:
            remaining = 5 - elapsed
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {remaining:.1f} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        self.guildLookupCooldown = current_time
        conn = sqlite3.connect('database/player_activity.db')
        cursor = conn.cursor()
        
        cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
        result = cursor.fetchone()
        if not result:
            cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
            result = cursor.fetchone()
    
        if not result:
            await interaction.followup.send(f"No data found for username: {name}. Please make sure you have logged in recently, and in a supported guild.", ephemeral=True)
            conn.close()
            return
            
        file, embed = await asyncio.to_thread(playerActivityTotalDungeons, result[0], name)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 7 days.")

    @activityCommands.command(name="raids", description="Shows a graph displaying the amount of raids completed total every day for the past week.")
    @app_commands.describe(name='Username of the player search Ex: BadPingHere, Salted.',)
    async def activityRaids(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /player activity raids was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter username is: {name}.")
        current_time = time.time()
        elapsed = current_time - self.guildLookupCooldown

        if elapsed <= 5:
            remaining = 5 - elapsed
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {remaining:.1f} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        self.guildLookupCooldown = current_time
        conn = sqlite3.connect('database/player_activity.db')
        cursor = conn.cursor()
        
        cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
        result = cursor.fetchone()
        if not result:
            cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
            result = cursor.fetchone()
    
        if not result:
            await interaction.followup.send(f"No data found for username: {name}. Please make sure you have logged in recently, and in a supported guild.", ephemeral=True)
            conn.close()
            return
            
        file, embed = await asyncio.to_thread(playerActivityRaids, result[0], name)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 7 days.")

    @activityCommands.command(name="raids_pie", description="Shows a pie chart displaying the different raid's you have done.")
    @app_commands.describe(name='Username of the player search Ex: BadPingHere, Salted.',)
    async def activityRaidsPie(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /player activity raids_pie was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter username is: {name}.")
        current_time = time.time()
        elapsed = current_time - self.guildLookupCooldown

        if elapsed <= 5:
            remaining = 5 - elapsed
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {remaining:.1f} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        self.guildLookupCooldown = current_time
        conn = sqlite3.connect('database/player_activity.db')
        cursor = conn.cursor()
        
        cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
        result = cursor.fetchone()
        if not result:
            cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
            result = cursor.fetchone()
    
        if not result:
            await interaction.followup.send(f"No data found for username: {name}. Please make sure you have logged in recently, and in a supported guild.", ephemeral=True)
            conn.close()
            return
            
        file, embed = await asyncio.to_thread(playerActivityTotalRaids, result[0], name)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 7 days.")

    @activityCommands.command(name="mobs_killed", description="Shows a graph displaying the amount of total mobs killed every day for the past week.")
    @app_commands.describe(name='Username of the player search Ex: BadPingHere, Salted.',)
    async def activityMobsKilled(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /player activity mobs_killed was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter username is: {name}.")
        current_time = time.time()
        elapsed = current_time - self.guildLookupCooldown

        if elapsed <= 5:
            remaining = 5 - elapsed
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {remaining:.1f} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        self.guildLookupCooldown = current_time
        conn = sqlite3.connect('database/player_activity.db')
        cursor = conn.cursor()
        
        cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
        result = cursor.fetchone()
        if not result:
            cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
            result = cursor.fetchone()
    
        if not result:
            await interaction.followup.send(f"No data found for username: {name}. Please make sure you have logged in recently, and in a supported guild.", ephemeral=True)
            conn.close()
            return
            
        file, embed = await asyncio.to_thread(playerActivityMobsKilled, result[0], name)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 7 days.")

    @activityCommands.command(name="wars", description="Shows a graph displaying the amount of total wars every day for the past week.")
    @app_commands.describe(name='Username of the player search Ex: BadPingHere, Salted.',)
    async def activityWars(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /player activity wars was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter username is: {name}.")
        current_time = time.time()
        elapsed = current_time - self.guildLookupCooldown

        if elapsed <= 5:
            remaining = 5 - elapsed
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {remaining:.1f} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        self.guildLookupCooldown = current_time
        conn = sqlite3.connect('database/player_activity.db')
        cursor = conn.cursor()
        
        cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
        result = cursor.fetchone()
        if not result:
            cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
            result = cursor.fetchone()
    
        if not result:
            await interaction.followup.send(f"No data found for username: {name}. Please make sure you have logged in recently, and in a supported guild.", ephemeral=True)
            conn.close()
            return
            
        file, embed = await asyncio.to_thread(playerActivityWars, result[0], name)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 7 days.")
    
    leaderboardCommands = app_commands.Group(name="leaderboard",description="this is never seen, yet discord flips the fuck out if its not here.",)
    @leaderboardCommands.command(name="raids", description="Shows the leaderboard of the top 10 players with the highest total raids completed.")
    async def leaderboardRaids(self, interaction: discord.Interaction):
        logger.info(f"Command /player leaderboard raids was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}).")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        
        embed = await asyncio.to_thread(playerLeaderboardRaids)
        if embed:
            await interaction.followup.send(embed=embed)
        else:
            await interaction.followup.send("No data available.")

    @leaderboardCommands.command(name="total_level", description="Shows the leaderboard of the top 10 players with the highest total level.")
    async def leaderboardTotalLevel(self, interaction: discord.Interaction):
        logger.info(f"Command /player leaderboard total_level was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}).")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        
        embed = await asyncio.to_thread(playerLeaderboardTotalLevel)
        if embed:
            await interaction.followup.send(embed=embed)
        else:
            await interaction.followup.send("No data available.")

    @leaderboardCommands.command(name="dungeons", description="Shows the leaderboard of the top 10 players with the highest dungeons completed.")
    async def leaderboardDungeons(self, interaction: discord.Interaction):
        logger.info(f"Command /player leaderboard dungeons was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}).")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        
        embed = await asyncio.to_thread(playerLeaderboardDungeons)
        if embed:
            await interaction.followup.send(embed=embed)
        else:
            await interaction.followup.send("No data available.")

    @leaderboardCommands.command(name="playtime", description="Shows the leaderboard of the top 10 players with the highest playtime in hours.")
    async def leaderboardPlaytime(self, interaction: discord.Interaction):
        logger.info(f"Command /player leaderboard playtime was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}).")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        
        embed = await asyncio.to_thread(playerLeaderboardPlaytime)
        if embed:
            await interaction.followup.send(embed=embed)
        else:
            await interaction.followup.send("No data available.")

    @leaderboardCommands.command(name="pvp_kills", description="Shows the leaderboard of the top 10 players with the highest PvP Kills.")
    async def leaderboardPVPKills(self, interaction: discord.Interaction):
        logger.info(f"Command /player leaderboard pvp_kills was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}).")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        
        embed = await asyncio.to_thread(playerLeaderboardPVPKills)
        if embed:
            await interaction.followup.send(embed=embed)
        else:
            await interaction.followup.send("No data available.")
    
  
async def setup(bot):
    await bot.add_cog(Player(bot))