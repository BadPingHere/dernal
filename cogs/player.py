import discord
from discord.ext import commands
from discord import app_commands
import time
from lib.utils import checkCooldown, playerActivityPlaytime, playerActivityContributions, playerActivityDungeons, playerActivityTotalDungeons, playerActivityRaids, playerActivityTotalRaids, playerActivityMobsKilled, playerActivityWars, playerLeaderboardRaids, playerLeaderboardTotalLevel, playerLeaderboardDungeons, playerLeaderboardPlaytime, playerLeaderboardPVPKills, playerLeaderboardGraids, playerActivityGraids
import sqlite3
import logging
import asyncio
from discord.ui import View, Button
from datetime import datetime, timezone
import os
import shelve

logger = logging.getLogger('discord')

class LeaderboardPaginator(View):
    def __init__(self, data, title, shorthandTitle):
        super().__init__(timeout=None)
        self.data = data
        self.title = title
        self.per_page = 10
        self.page = 0
        self.shorthandTitle = shorthandTitle
        self.total_pages = (len(data) - 1) // 10 + 1 # page +1 ts
        self.update_buttons()

    def get_embed(self):
        start = self.page * self.per_page
        end = start + self.per_page
        page_data = self.data[start:end]

        leaderboardDesc = "```\n{:<3} {:<30} {:<10}\n".format("#", "Player", f"{self.shorthandTitle}")
        separator = "-" * 45 + "\n"
        leaderboardDesc += separator
        for i, (name, value) in enumerate(page_data, start=start + 1):
            if value % 1 == 0: # Checks if int
                formattedValue = f"{int(value):,}"
            else:
                formattedValue = f"{value:,.2f}"
            leaderboardDesc += "{:<3} {:<30} {:<10}\n".format(i, name, formattedValue)
        leaderboardDesc += "```"

        embed = discord.Embed(
            title=self.title,
            description=leaderboardDesc,
            color=discord.Color.blue()
        )
        embed.set_footer(text=f"Page {self.page + 1}/{self.total_pages} • https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
        return embed

    def update_buttons(self): # disables when shits l;ike that
        self.first.disabled = self.page == 0
        self.prev.disabled = self.page == 0
        self.next.disabled = self.page >= self.total_pages - 1
        self.last.disabled = self.page >= self.total_pages - 1

    @discord.ui.button(label="<<", style=discord.ButtonStyle.secondary) # go to front
    async def first(self, interaction: discord.Interaction, button: Button):
        self.page = 0
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

    @discord.ui.button(label="<", style=discord.ButtonStyle.primary) # up one
    async def prev(self, interaction: discord.Interaction, button: Button):
        self.page -= 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

    @discord.ui.button(label=">", style=discord.ButtonStyle.primary) #back one
    async def next(self, interaction: discord.Interaction, button: Button):
        self.page += 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

    @discord.ui.button(label=">>", style=discord.ButtonStyle.secondary) # goto back
    async def last(self, interaction: discord.Interaction, button: Button):
        self.page = self.total_pages - 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)


@app_commands.allowed_installs(guilds=True, users=True)
@app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
class Player(commands.GroupCog, name="player"):
    def __init__(self, bot):
        self.bot = bot
        self.guildLookupCooldown = 0
        rootDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.graidFilePath = os.path.join(rootDir, 'database', 'graid')
    activityCommands = app_commands.Group(name="activity", description="this is never seen, yet discord flips the x out if its not here.")
    
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

    @activityCommands.command(name="guild_raids", description="Shows a graph with the amount of guild raids done over the past 2 weeks for supported players.")
    @app_commands.describe(name='Username of the player search Ex: BadPingHere, Salted.',)
    async def activityGraids(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /player activity guild_raids was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter username is: {name}.")
        current_time = time.time()
        elapsed = current_time - self.guildLookupCooldown

        if elapsed <= 5:
            remaining = 5 - elapsed
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {remaining:.1f} more seconds.",ephemeral=True)
            return 
        with shelve.open(self.graidFilePath) as db:
            confirmedGRaid = db['guild_raids']
        if name not in str(confirmedGRaid):
            await interaction.response.send_message(f"No data found for username: {name}. Please make you are in a level 100+ guild, and have done guild raids in the past 14 days.", ephemeral=True)
            return
        await interaction.response.defer()
        file, embed = await asyncio.to_thread(playerActivityGraids, name)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 7 days.")
    
    leaderboardCommands = app_commands.Group(name="leaderboard",description="this is never seen, yet discord flips the x out if its not here.",)
    @leaderboardCommands.command(name="raids", description="Shows the leaderboard of the top 100 players with the highest total raids completed.")
    async def leaderboardRaids(self, interaction: discord.Interaction):
        logger.info(f"Command /player leaderboard raids was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}).")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        
        data = await asyncio.to_thread(playerLeaderboardRaids)
        if data:
            view = LeaderboardPaginator(data, "Top 100 Players by Raids Completed", "Raids")
            await interaction.followup.send(embed=view.get_embed(), view=view)
        else:
            await interaction.followup.send("No data available.")

    @leaderboardCommands.command(name="guild_raids", description="Shows the leaderboard of the top 100 players with the highest guild raids in the past 2 weeks.")
    async def leaderboardGRaids(self, interaction: discord.Interaction):
        logger.info(f"Command /player leaderboard guild_raids was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}).")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        
        data = await asyncio.to_thread(playerLeaderboardGraids)
        if data:
            num = len(data)
            view = LeaderboardPaginator(data, f"Top {num} Players by Guild Raids", "Guild Raids")
            await interaction.followup.send(embed=view.get_embed(), view=view)
        else:
            await interaction.followup.send("No data available.")


    @leaderboardCommands.command(name="total_level", description="Shows the leaderboard of the top 100 players with the highest total level.")
    async def leaderboardTotalLevel(self, interaction: discord.Interaction):
        logger.info(f"Command /player leaderboard total_level was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}).")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        
        data = await asyncio.to_thread(playerLeaderboardTotalLevel)
        if data:
            view = LeaderboardPaginator(data, "Top 100 Players by Total Level", "Levels")
            await interaction.followup.send(embed=view.get_embed(), view=view)
        else:
            await interaction.followup.send("No data available.")

    @leaderboardCommands.command(name="dungeons", description="Shows the leaderboard of the top 100 players with the highest dungeons completed.")
    async def leaderboardDungeons(self, interaction: discord.Interaction):
        logger.info(f"Command /player leaderboard dungeons was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}).")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        
        data = await asyncio.to_thread(playerLeaderboardDungeons)
        if data:
            view = LeaderboardPaginator(data, "Top 100 Players by Dungeons Completed", "Dungeons")
            await interaction.followup.send(embed=view.get_embed(), view=view)
        else:
            await interaction.followup.send("No data available.")

    @leaderboardCommands.command(name="playtime", description="Shows the leaderboard of the top 100 players with the highest playtime in hours.")
    async def leaderboardPlaytime(self, interaction: discord.Interaction):
        logger.info(f"Command /player leaderboard playtime was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}).")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        
        data = await asyncio.to_thread(playerLeaderboardPlaytime)
        if data:
            view = LeaderboardPaginator(data, "Top 100 Players by Playtime", "Hours")
            await interaction.followup.send(embed=view.get_embed(), view=view)
        else:
            await interaction.followup.send("No data available.")

    @leaderboardCommands.command(name="pvp_kills", description="Shows the leaderboard of the top 100 players with the highest PvP Kills.")
    async def leaderboardPVPKills(self, interaction: discord.Interaction):
        logger.info(f"Command /player leaderboard pvp_kills was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}).")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        
        data = await asyncio.to_thread(playerLeaderboardPVPKills)
        if data:
            view = LeaderboardPaginator(data, "Top 100 Players by PVP Kills", "Kills")
            await interaction.followup.send(embed=view.get_embed(), view=view)
        else:
            await interaction.followup.send("No data available.")
    
  
async def setup(bot):
    await bot.add_cog(Player(bot))