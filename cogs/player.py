import discord
from discord.ext import commands
from discord import app_commands
import time
from lib.utils import getGraidDatabaseData, checkCooldown, timeframeMap3, timeframeMap2, playerGuildHistory, leaderboardBuilder, activityBuilder
import sqlite3
import logging
import asyncio
from discord.ui import View, Button
from datetime import datetime, timezone
import os
from typing import Optional

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
        rootDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    async def timeframeAutocomplete3(self, interaction: discord.Interaction, current: str):
        keys = list(timeframeMap3.keys())
        return [app_commands.Choice(name=k, value=k)for k in keys if current.lower() in k.lower()][:25]
    
    async def timeframeAutocomplete2(self, interaction: discord.Interaction, current: str):
        keys = list(timeframeMap2.keys())
        return [app_commands.Choice(name=k, value=k)for k in keys if current.lower() in k.lower()][:25]
    
    async def autocompleteTheme(self, interaction: discord.Interaction, current: str):
        # Ideal world the themes should be stored elsewhere like utils and imported
        values = ["light", "dark", "discord"]
        return [app_commands.Choice(name=k, value=k)for k in values if current.lower() in k.lower()][:25]
    
    async def autocompleteActivityTimeframe(self, interaction: discord.Interaction, current: str):
        # Ideal world the themes should be stored elsewhere like utils and imported
        values = ["Last 14 Days", "Last 7 Days", "Last 3 Days", "Last 24 Hours", "Last 30 Days"]
        return [app_commands.Choice(name=k, value=k)for k in values if current.lower() in k.lower()][:25]
    
    activityCommands = app_commands.Group(name="activity", description="this is never seen, yet discord flips the x out if its not here.")
    
    @activityCommands.command(name="playtime", description="Shows the graph displaying the average amount of playtime every day.")
    @app_commands.describe(name='Username of the player search Ex: BadPingHere, Salted.',)
    @app_commands.describe(theme='The theme of the graphic (defaults to light mode)',)
    async def activityPlaytime(self, interaction: discord.Interaction, name: str, timeframe: str,  theme: Optional[str]):
        logger.info(f"Command /player activity playtime was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter username is: {name}.")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        
        conn = sqlite3.connect('database/player_activity.db')
        cursor = conn.cursor()
        #logger.info("Tables:", cursor.fetchall())
        cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
        result = cursor.fetchone()
        if not result:
            cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
            result = cursor.fetchone()
    
        if not result:
            await interaction.followup.send(f"No data found for username: {name}. Please make sure you have logged in recently..", ephemeral=True)
            conn.close()
            return
            
        file, embed = await asyncio.to_thread(activityBuilder,"playerActivityPlaytime", uuid=result[0], name=name, theme=theme or "light", timeframe=timeframe)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 14 days.")
    
    @activityCommands.command(name="contribution", description="Shows a graph displaying the amount of contributiond xp every day.")
    @app_commands.describe(name='Username of the player search Ex: BadPingHere, Salted.',)
    @app_commands.describe(theme='The theme of the graphic (defaults to light mode)',)
    async def activityContributions(self, interaction: discord.Interaction, name: str, timeframe: str,  theme: Optional[str]):
        logger.info(f"Command /player activity xp was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter username is: {name}.")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        
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
            
        file, embed = await asyncio.to_thread(activityBuilder,"playerActivityContributions", uuid=result[0], name=name, theme=theme or "light", timeframe=timeframe)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 14 days.")

    @activityCommands.command(name="dungeons", description="Shows a graph displaying the amount of dungeons completed total every day.")
    @app_commands.describe(name='Username of the player search Ex: BadPingHere, Salted.',)
    @app_commands.describe(theme='The theme of the graphic (defaults to light mode)',)
    async def activityDungeons(self, interaction: discord.Interaction, name: str, timeframe: str,  theme: Optional[str]):
        logger.info(f"Command /player activity dungeons was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter username is: {name}.")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        
        conn = sqlite3.connect('database/player_activity.db')
        cursor = conn.cursor()
        
        cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
        result = cursor.fetchone()
        if not result:
            cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
            result = cursor.fetchone()
    
        if not result:
            await interaction.followup.send(f"No data found for username: {name}. Please make sure you have logged in recently..", ephemeral=True)
            conn.close()
            return
            
        file, embed = await asyncio.to_thread(activityBuilder,"playerActivityDungeons", uuid=result[0], name=name, theme=theme or "light", timeframe=timeframe)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 7 days.")

    @activityCommands.command(name="dungeons_pie", description="Shows a pie chart displaying the different dungeons's you have done.")
    @app_commands.describe(name='Username of the player search Ex: BadPingHere, Salted.',)
    @app_commands.describe(theme='The theme of the graphic (defaults to light mode)',)
    async def activityDungeonsPie(self, interaction: discord.Interaction, name: str, theme: Optional[str]):
        logger.info(f"Command /player activity dungeons_pie was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter username is: {name}.")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        
        conn = sqlite3.connect('database/player_activity.db')
        cursor = conn.cursor()
        
        cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
        result = cursor.fetchone()
        if not result:
            cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
            result = cursor.fetchone()
    
        if not result:
            await interaction.followup.send(f"No data found for username: {name}. Please make sure you have logged in recently..", ephemeral=True)
            conn.close()
            return
            
        file, embed = await asyncio.to_thread(activityBuilder,"playerActivityTotalDungeons", uuid=result[0], name=name, theme=theme or "light")
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 7 days.")

    @activityCommands.command(name="raids", description="Shows a graph displaying the amount of raids completed total every day.")
    @app_commands.describe(name='Username of the player search Ex: BadPingHere, Salted.',)
    @app_commands.describe(theme='The theme of the graphic (defaults to light mode)',)
    async def activityRaids(self, interaction: discord.Interaction, name: str, timeframe: str,  theme: Optional[str]):
        logger.info(f"Command /player activity raids was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter username is: {name}.")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        
        conn = sqlite3.connect('database/player_activity.db')
        cursor = conn.cursor()
        
        cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
        result = cursor.fetchone()
        if not result:
            cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
            result = cursor.fetchone()
    
        if not result:
            await interaction.followup.send(f"No data found for username: {name}. Please make sure you have logged in recently..", ephemeral=True)
            conn.close()
            return
            
        file, embed = await asyncio.to_thread(activityBuilder,"playerActivityRaids", uuid=result[0], name=name, theme=theme or "light", timeframe=timeframe)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 7 days.")

    @activityCommands.command(name="raids_pie", description="Shows a pie chart displaying the different raid's you have done.")
    @app_commands.describe(name='Username of the player search Ex: BadPingHere, Salted.',)
    @app_commands.describe(theme='The theme of the graphic (defaults to light mode)',)
    async def activityRaidsPie(self, interaction: discord.Interaction, name: str, theme: Optional[str]):
        logger.info(f"Command /player activity raids_pie was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter username is: {name}.")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        
        conn = sqlite3.connect('database/player_activity.db')
        cursor = conn.cursor()
        
        cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
        result = cursor.fetchone()
        if not result:
            cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
            result = cursor.fetchone()
    
        if not result:
            await interaction.followup.send(f"No data found for username: {name}. Please make sure you have logged in recently..", ephemeral=True)
            conn.close()
            return
            
        file, embed = await asyncio.to_thread(activityBuilder,"playerActivityTotalRaids", uuid=result[0], name=name, theme=theme or "light")
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 7 days.")

    @activityCommands.command(name="mobs_killed", description="Shows a graph displaying the amount of total mobs killed every day.")
    @app_commands.describe(name='Username of the player search Ex: BadPingHere, Salted.',)
    @app_commands.describe(theme='The theme of the graphic (defaults to light mode)',)
    async def activityMobsKilled(self, interaction: discord.Interaction, name: str, timeframe: str,  theme: Optional[str]):
        logger.info(f"Command /player activity mobs_killed was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter username is: {name}.")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        
        conn = sqlite3.connect('database/player_activity.db')
        cursor = conn.cursor()
        
        cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
        result = cursor.fetchone()
        if not result:
            cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
            result = cursor.fetchone()
    
        if not result:
            await interaction.followup.send(f"No data found for username: {name}. Please make sure you have logged in recently..", ephemeral=True)
            conn.close()
            return
            
        file, embed = await asyncio.to_thread(activityBuilder,"playerActivityMobsKilled", uuid=result[0], name=name, theme=theme or "light", timeframe=timeframe)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 7 days.")

    @activityCommands.command(name="wars", description="Shows a graph displaying the amount of total wars every day.")
    @app_commands.describe(name='Username of the player search Ex: BadPingHere, Salted.',)
    @app_commands.describe(theme='The theme of the graphic (defaults to light mode)',)
    async def activityWars(self, interaction: discord.Interaction, name: str, timeframe: str,  theme: Optional[str]):
        logger.info(f"Command /player activity wars was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter username is: {name}.")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        conn = sqlite3.connect('database/player_activity.db')
        cursor = conn.cursor()
        
        cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
        result = cursor.fetchone()
        if not result:
            cursor.execute("SELECT uuid FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (name,))
            result = cursor.fetchone()
    
        if not result:
            await interaction.followup.send(f"No data found for username: {name}. Please make sure you have logged in recently..", ephemeral=True)
            conn.close()
            return
            
        file, embed = await asyncio.to_thread(activityBuilder,"playerActivityWars", uuid=result[0], name=name, theme=theme or "light", timeframe=timeframe)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 7 days.")

    @activityCommands.command(name="guild_raids", description="Shows a graph with the amount of guild raids done for supported players.")
    @app_commands.describe(name='Username of the player search Ex: BadPingHere, Salted.',)
    @app_commands.describe(theme='The theme of the graphic (defaults to light mode)',)
    async def activityGraids(self, interaction: discord.Interaction, name: str, timeframe: str,  theme: Optional[str]):
        logger.info(f"Command /player activity guild_raids was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter username is: {name}.")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return 
        confirmedGRaid = getGraidDatabaseData("guild_raids")
        if name not in str(confirmedGRaid):
            await interaction.response.send_message(f"No data found for username: {name}. Please make you are in a level 100+ guild, and have done guild raids in the past 14 days.", ephemeral=True)
            return
        await interaction.response.defer()
        file, embed = await asyncio.to_thread(activityBuilder,"playerActivityGraids", name=name, theme=theme or "light", timeframe=timeframe)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 14 days.")
    
    leaderboardCommands = app_commands.Group(name="leaderboard",description="this is never seen, yet discord flips the x out if its not here.",)
    @leaderboardCommands.command(name="raids", description="Shows the leaderboard of the top 100 players with the highest total raids completed.")
    @app_commands.describe(timeframe='The timeframe you want to see. ',)
    async def leaderboardRaids(self, interaction: discord.Interaction, timeframe: str):
        logger.info(f"Command /player leaderboard raids was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). The timeframe is {timeframe}.")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        
        data = await asyncio.to_thread(leaderboardBuilder, "playerLeaderboardRaids", timeframe=timeframe)
        if data:
            view = LeaderboardPaginator(data, f"Top 100 Players by Raids Completed - {timeframe}", "Raids")
            await interaction.followup.send(embed=view.get_embed(), view=view)
        else:
            await interaction.followup.send("No data available.")

    @leaderboardCommands.command(name="guild_raids", description="Shows the leaderboard of the top 100 players with the highest guild raids in the past 2 weeks.")
    @app_commands.describe(timeframe='The timeframe you want to see. ',)
    async def leaderboardGRaids(self, interaction: discord.Interaction, timeframe: str):
        logger.info(f"Command /player leaderboard guild_raids was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). The timeframe is {timeframe}.")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        
        data = await asyncio.to_thread(leaderboardBuilder, "playerLeaderboardGraids", timeframe=timeframe)
        if data:
            num = len(data)
            view = LeaderboardPaginator(data, f"Top {num} Players by Guild Raids - {timeframe}", "Guild Raids")
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
        
        data = await asyncio.to_thread(leaderboardBuilder, "playerLeaderboardTotalLevel")
        if data:
            view = LeaderboardPaginator(data, "Top 100 Players by Total Level", "Levels")
            await interaction.followup.send(embed=view.get_embed(), view=view)
        else:
            await interaction.followup.send("No data available.")

    @leaderboardCommands.command(name="dungeons", description="Shows the leaderboard of the top 100 players with the highest dungeons completed.")
    @app_commands.describe(timeframe='The timeframe you want to see. ',)
    async def leaderboardDungeons(self, interaction: discord.Interaction, timeframe: str):
        logger.info(f"Command /player leaderboard dungeons was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). The timeframe is {timeframe}.")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        
        data = await asyncio.to_thread(leaderboardBuilder, "playerLeaderboardDungeons", timeframe=timeframe)
        if data:
            view = LeaderboardPaginator(data, f"Top 100 Players by Dungeons Completed - {timeframe}", "Dungeons")
            await interaction.followup.send(embed=view.get_embed(), view=view)
        else:
            await interaction.followup.send("No data available.")

    @leaderboardCommands.command(name="playtime", description="Shows the leaderboard of the top 100 players with the highest playtime in hours.")
    @app_commands.describe(timeframe='The timeframe you want to see. ',)
    async def leaderboardPlaytime(self, interaction: discord.Interaction, timeframe: str):
        logger.info(f"Command /player leaderboard playtime was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). The timeframe is {timeframe}.")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        
        data = await asyncio.to_thread(leaderboardBuilder, "playerLeaderboardPlaytime", timeframe=timeframe)
        if data:
            view = LeaderboardPaginator(data, f"Top 100 Players by Playtime - {timeframe}", "Hours")
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
        
        data = await asyncio.to_thread(leaderboardBuilder, "playerLeaderboardPVPKills")
        if data:
            view = LeaderboardPaginator(data, "Top 100 Players by PVP Kills", "Kills")
            await interaction.followup.send(embed=view.get_embed(), view=view)
        else:
            await interaction.followup.send("No data available.")

    @app_commands.command(description="Shows the guild history of a user until Nov. 2024.")
    @app_commands.describe(name='Username of the player search Ex: BadPingHere, Salted.',)
    async def guild_history(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /player guild_history was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter username is: {name}.")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        conn = sqlite3.connect('database/guild_activity.db')
        cursor = conn.cursor()
        
        cursor.execute("SELECT uuid FROM members WHERE name = ? COLLATE NOCASE LIMIT 1", (name,))
        result = cursor.fetchone()
    
        if not result:
            await interaction.followup.send(f"No data found for username: {name}.", ephemeral=True)
            conn.close()
            return
        
        embed = await asyncio.to_thread(playerGuildHistory, result[0], name)
        if embed:
            await interaction.followup.send(embed=embed)
        else:
            await interaction.followup.send("No data available.")

    # Add autocomplete for timeframe parameters
    leaderboardRaids.autocomplete("timeframe")(timeframeAutocomplete3)
    leaderboardGRaids.autocomplete("timeframe")(timeframeAutocomplete2)
    leaderboardDungeons.autocomplete("timeframe")(timeframeAutocomplete3)
    leaderboardPlaytime.autocomplete("timeframe")(timeframeAutocomplete3)
    
    activityPlaytime.autocomplete("theme")(autocompleteTheme)
    activityContributions.autocomplete("theme")(autocompleteTheme)
    activityDungeons.autocomplete("theme")(autocompleteTheme)
    activityDungeonsPie.autocomplete("theme")(autocompleteTheme)
    activityRaids.autocomplete("theme")(autocompleteTheme)
    activityRaidsPie.autocomplete("theme")(autocompleteTheme)
    activityMobsKilled.autocomplete("theme")(autocompleteTheme)
    activityWars.autocomplete("theme")(autocompleteTheme)
    activityGraids.autocomplete("theme")(autocompleteTheme)

    activityPlaytime.autocomplete("timeframe")(autocompleteActivityTimeframe)
    activityContributions.autocomplete("timeframe")(autocompleteActivityTimeframe)
    activityDungeons.autocomplete("timeframe")(autocompleteActivityTimeframe)
    activityRaids.autocomplete("timeframe")(autocompleteActivityTimeframe)
    activityMobsKilled.autocomplete("timeframe")(autocompleteActivityTimeframe)
    activityWars.autocomplete("timeframe")(autocompleteActivityTimeframe)
    activityGraids.autocomplete("timeframe")(autocompleteActivityTimeframe)
  
async def setup(bot):
    await bot.add_cog(Player(bot))