import discord
from discord.ext import commands
from discord import app_commands
from discord.ui import View, Button
import time
from lib.utils import makeRequest, guildLookup, lookupGuild, guildActivityPlaytime, guildActivityXP, guildActivityTerritories, guildActivityWars, guildActivityOnlineMembers, guildActivityTotalMembers, guildLeaderboardOnlineMembers, guildLeaderboardTotalMembers, guildLeaderboardWars, guildLeaderboardXP, guildLeaderboardPlaytime
import sqlite3
import logging

logger = logging.getLogger('discord')
class InactivityView(View):
    def __init__(self, inactivityDict):
        super().__init__(timeout=None)
        self.inactivityDict = inactivityDict
        self.category_keys = list(inactivityDict.keys())
        self.current_category_index = 0
        self.update_buttons()

    def update_buttons(self):
        # Update the button states based on the current category index
        self.children[0].disabled = self.current_category_index == 0  # Disable back button if at the start
        self.children[1].disabled = self.current_category_index == len(self.category_keys) - 1  # Disable next button if at the end

    async def update_embed(self, interaction: discord.Interaction):
        # Create and send the updated embed based on the current category
        current_category = self.category_keys[self.current_category_index]
        embed = discord.Embed(
            title=f"{current_category}",
            description=self.get_user_list(current_category),
            color=discord.Color.blue()
        )
        await interaction.response.edit_message(embed=embed, view=self)

    def get_user_list(self, category):
        # Retrieve the list of users in the current category
        users = self.inactivityDict[category]
        if users:
            backslashChar = "\\" #because linux is awesome!
            return "\n".join([f"**{username.replace('_', f'{backslashChar}_')}** - Last online <t:{timestamp}:F>." for username, timestamp in users])
        else:
            return "No users found."

    @discord.ui.button(label="Back", style=discord.ButtonStyle.primary)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        self.current_category_index -= 1
        self.update_buttons()
        await self.update_embed(interaction)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.primary)
    async def next_button(self, interaction: discord.Interaction, button: Button):
        self.current_category_index += 1
        self.update_buttons()
        await self.update_embed(interaction)


class Guild(commands.GroupCog, name="guild"):
    def __init__(self, bot):
        self.bot = bot
        self.guildLookupCooldown = 0

    activityCommands = app_commands.Group(name="activity", description="this is never seen, yet discord flips the fuck out if its not here.")
    
    @activityCommands.command(name="playtime", description="Shows the graph displaying the average amount of players online over the past day.")
    @app_commands.describe(name='Prefix or Name of the guild search Ex: TAq, Calvish.',)
    async def activityPlaytime(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /guild activity playtime was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")
        current_time = time.time()
        if int(current_time - self.guildLookupCooldown) <= 10:
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {current_time - self.guildLookupCooldown} seconds.", ephemeral=True)
            return
        await interaction.response.defer()
        self.guildLookupCooldown = current_time
        conn = sqlite3.connect('database/guild_activity.db')
        cursor = conn.cursor()
        
        if len(name) <= 4:
            cursor.execute("SELECT uuid FROM guilds WHERE prefix = ?", (name,))
            result = cursor.fetchone()
            if not result:
                cursor.execute("SELECT uuid FROM guilds WHERE name = ?", (name,))
                result = cursor.fetchone()
        else:
            cursor.execute("SELECT uuid FROM guilds WHERE name = ?", (name,))
            result = cursor.fetchone()
            if not result:
                cursor.execute("SELECT uuid FROM guilds WHERE prefix = ?", (name,))
                result = cursor.fetchone()
        
        if not result:
            await interaction.followup.send(f"No data found for guild: {name}", ephemeral=True)
            conn.close()
            return
            
        file, embed = await guildActivityPlaytime(result[0], name)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 24 hours")
    
    @activityCommands.command(name="xp", description="Shows a bar graph displaying the total xp a guild has every day, for the past 2 weeks.")
    @app_commands.describe(name='Prefix or Name of the guild search Ex: TAq, Calvish.',)
    async def activityXP(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /guild activity xp was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")
        current_time = time.time()
        if int(current_time - self.guildLookupCooldown) <= 10:
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {current_time - self.guildLookupCooldown} seconds.", ephemeral=True)
            return
        self.guildLookupCooldown = current_time
        await interaction.response.defer()
            
        conn = sqlite3.connect('database/guild_activity.db')
        cursor = conn.cursor()
        
        if len(name) <= 4:
            cursor.execute("SELECT uuid FROM guilds WHERE prefix = ?", (name,))
            result = cursor.fetchone()
            if not result:
                cursor.execute("SELECT uuid FROM guilds WHERE name = ?", (name,))
                result = cursor.fetchone()
        else:
            cursor.execute("SELECT uuid FROM guilds WHERE name = ?", (name,))
            result = cursor.fetchone()
            if not result:
                cursor.execute("SELECT uuid FROM guilds WHERE prefix = ?", (name,))
                result = cursor.fetchone()
        
        if not result:
            await interaction.followup.send(f"No data found for guild: {name}", ephemeral=True)
            conn.close()
            return
            
        file, embed = await guildActivityXP(result[0], name)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 14 days")
    
    @activityCommands.command(name="territories", description="Shows a graph displaying the amount of territories a guild has for the past 3 days.")
    @app_commands.describe(name='Prefix or Name of the guild search Ex: TAq, Calvish.',)
    async def activityTerritories(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /guild activity territories was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")
        current_time = time.time()
        if int(current_time - self.guildLookupCooldown) <= 10:
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {current_time - self.guildLookupCooldown} seconds.", ephemeral=True)
            return
        self.guildLookupCooldown = current_time
        await interaction.response.defer()
            
        conn = sqlite3.connect('database/guild_activity.db')
        cursor = conn.cursor()
        
        if len(name) <= 4:
            cursor.execute("SELECT uuid FROM guilds WHERE prefix = ?", (name,))
            result = cursor.fetchone()
            if not result:
                cursor.execute("SELECT uuid FROM guilds WHERE name = ?", (name,))
                result = cursor.fetchone()
        else:
            cursor.execute("SELECT uuid FROM guilds WHERE name = ?", (name,))
            result = cursor.fetchone()
            if not result:
                cursor.execute("SELECT uuid FROM guilds WHERE prefix = ?", (name,))
                result = cursor.fetchone()
        
        if not result:
            await interaction.followup.send(f"No data found for guild: {name}", ephemeral=True)
            conn.close()
            return
        file, embed = await guildActivityTerritories(result[0], name)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 3 days")

    @activityCommands.command(name="wars", description="Shows a graph displaying the total amount of wars a guild has done over the past 3 days.")
    @app_commands.describe(name='Prefix or Name of the guild search Ex: TAq, Calvish.',)
    async def activityWars(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /guild activity wars was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")
        current_time = time.time()
        if int(current_time - self.guildLookupCooldown) <= 10:
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {current_time - self.guildLookupCooldown} seconds.", ephemeral=True)
            return
        self.guildLookupCooldown = current_time
        await interaction.response.defer()
            
        conn = sqlite3.connect('database/guild_activity.db')
        cursor = conn.cursor()
        
        if len(name) <= 4:
            cursor.execute("SELECT uuid FROM guilds WHERE prefix = ?", (name,))
            result = cursor.fetchone()
            if not result:
                cursor.execute("SELECT uuid FROM guilds WHERE name = ?", (name,))
                result = cursor.fetchone()
        else:
            cursor.execute("SELECT uuid FROM guilds WHERE name = ?", (name,))
            result = cursor.fetchone()
            if not result:
                cursor.execute("SELECT uuid FROM guilds WHERE prefix = ?", (name,))
                result = cursor.fetchone()
        
        if not result:
            await interaction.followup.send(f"No data found for guild: {name}", ephemeral=True)
            conn.close()
            return
        file, embed = await guildActivityWars(result[0], name)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 3 days")

    @activityCommands.command(name="total_members", description="Shows a graph displaying the total members a guild has for the past day.")
    @app_commands.describe(name='Prefix or Name of the guild search Ex: TAq, Calvish.',)
    async def activityTotal_members(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /guild activity total_members was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")
        current_time = time.time()
        if int(current_time - self.guildLookupCooldown) <= 10:
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {current_time - self.guildLookupCooldown} seconds.", ephemeral=True)
            return
        await interaction.response.defer()
        self.guildLookupCooldown = current_time
        conn = sqlite3.connect('database/guild_activity.db')
        cursor = conn.cursor()
        
        if len(name) <= 4:
            cursor.execute("SELECT uuid FROM guilds WHERE prefix = ?", (name,))
            result = cursor.fetchone()
            if not result:
                cursor.execute("SELECT uuid FROM guilds WHERE name = ?", (name,))
                result = cursor.fetchone()
        else:
            cursor.execute("SELECT uuid FROM guilds WHERE name = ?", (name,))
            result = cursor.fetchone()
            if not result:
                cursor.execute("SELECT uuid FROM guilds WHERE prefix = ?", (name,))
                result = cursor.fetchone()
        
        if not result:
            await interaction.followup.send(f"No data found for guild: {name}", ephemeral=True)
            conn.close()
            return
            
        file, embed = await guildActivityTotalMembers(result[0], name)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 24 hours")
    
    @activityCommands.command(name="online_members", description="Shows a graph displaying the average amount of online members a guild has for the past day.")
    @app_commands.describe(name='Prefix or Name of the guild search Ex: TAq, Calvish.',)
    async def activityOnline_members(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /guild activity online_members was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")
        current_time = time.time()
        if int(current_time - self.guildLookupCooldown) <= 10:
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {current_time - self.guildLookupCooldown} seconds.", ephemeral=True)
            return
        await interaction.response.defer()
        self.guildLookupCooldown = current_time
        conn = sqlite3.connect('database/guild_activity.db')
        cursor = conn.cursor()
        
        if len(name) <= 4:
            cursor.execute("SELECT uuid FROM guilds WHERE prefix = ?", (name,))
            result = cursor.fetchone()
            if not result:
                cursor.execute("SELECT uuid FROM guilds WHERE name = ?", (name,))
                result = cursor.fetchone()
        else:
            cursor.execute("SELECT uuid FROM guilds WHERE name = ?", (name,))
            result = cursor.fetchone()
            if not result:
                cursor.execute("SELECT uuid FROM guilds WHERE prefix = ?", (name,))
                result = cursor.fetchone()
        
        if not result:
            await interaction.followup.send(f"No data found for guild: {name}", ephemeral=True)
            conn.close()
            return
            
        file, embed = await guildActivityOnlineMembers(result[0], name)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 24 hours")
    
    leaderboardCommands = app_commands.Group(name="leaderboard", description="this is never seen, yet discord flips the fuck out if its not here.")

    @leaderboardCommands.command(name="online_members", description="Shows a leaderboard of the top 10 guild's average amount of online players.")
    async def leaderboardOnline_members(self, interaction: discord.Interaction):
        logger.info(f"Command /guild leaderboard online_members was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}).")
        current_time = time.time()
        if int(current_time - self.guildLookupCooldown) <= 10:
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {current_time - self.guildLookupCooldown} seconds.", ephemeral=True)
            return
        self.guildLookupCooldown = current_time
        await interaction.response.defer()
        
        embed = await guildLeaderboardOnlineMembers()
        if embed:
            await interaction.followup.send(embed=embed)
        else:
            await interaction.followup.send("No data available.")
    
    @leaderboardCommands.command(name="total_members", description="Shows a leaderboard of the top 10 guild's total members.")
    async def leaderboardTotal_members(self, interaction: discord.Interaction):
        logger.info(f"Command /guild leaderboard total_members was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}).")
        current_time = time.time()
        if int(current_time - self.guildLookupCooldown) <= 10:
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {current_time - self.guildLookupCooldown} seconds.", ephemeral=True)
            return
        self.guildLookupCooldown = current_time
        await interaction.response.defer()
        
        embed = await guildLeaderboardTotalMembers()
        if embed:
            await interaction.followup.send(embed=embed)
        else:
            await interaction.followup.send("No data available.")
    
    @leaderboardCommands.command(name="wars", description="Shows a leaderboard of the top 10 guild's war amount.")
    async def leaderboardWars(self, interaction: discord.Interaction):
        logger.info(f"Command /guild leaderboard wars was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}).")
        current_time = time.time()
        if int(current_time - self.guildLookupCooldown) <= 10:
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {current_time - self.guildLookupCooldown} seconds.", ephemeral=True)
            return
        self.guildLookupCooldown = current_time
        await interaction.response.defer()
        
        embed = await guildLeaderboardWars()
        if embed:
            await interaction.followup.send(embed=embed)
        else:
            await interaction.followup.send("No data available.")

    @leaderboardCommands.command(name="xp", description="Shows a leaderboard of the top 10 guild's xp gained over the past 24 hours.")
    async def leaderboardXP(self, interaction: discord.Interaction):
        logger.info(f"Command /guild leaderboard xp was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}).")
        current_time = time.time()
        if int(current_time - self.guildLookupCooldown) <= 10:
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {current_time - self.guildLookupCooldown} seconds.", ephemeral=True)
            return
        self.guildLookupCooldown = current_time
        await interaction.response.defer()
        
        embed = await guildLeaderboardXP()
        if embed:
            await interaction.followup.send(embed=embed)
        else:
            await interaction.followup.send("No data available.")

    # This works, but the server im using to host this bot takes way too long to perform this. So uncomment if you want.
    @leaderboardCommands.command(name="playtime", description="Shows a leaderboard of the top 10 guild's playtime percentage.")
    async def leaderboardPlaytime(self, interaction: discord.Interaction):
        logger.info(f"Command /guild leaderboard playtime was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}).")
        current_time = time.time()
        if int(current_time - self.guildLookupCooldown) <= 10:
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {current_time - self.guildLookupCooldown} seconds.", ephemeral=True)
            return
        self.guildLookupCooldown = current_time
        await interaction.response.defer()
        
        embed = await guildLeaderboardPlaytime()
        if embed:
            await interaction.followup.send(embed=embed)
        else:
            await interaction.followup.send("No data available.")

    @app_commands.command(description="Shows you to get a quick overview of a guild, like level, online members, etc.")
    @app_commands.describe(name='Prefix or Name of the guild search Ex: TAq, Calvish.',)
    async def overview(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /guild overview was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")
        current_time = time.time()
        if int(current_time - self.guildLookupCooldown) <= 5:
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {current_time - self.guildLookupCooldown} seconds.", ephemeral=True)
            return
        self.guildLookupCooldown = current_time
        if len(name) >= 5:
            URL = f"https://api.wynncraft.com/v3/guild/{name}"
        else:
            URL = f"https://api.wynncraft.com/v3/guild/prefix/{name}"

        r = await makeRequest(URL)
        if r.ok:
            embed = await guildLookup(name, r)
            await interaction.response.send_message(embed=embed)
        else:
            await interaction.response.send_message(f"'{name}' is an unknown prefix or guild name.", ephemeral=True)
  
    @app_commands.command(description="Shows and sorts the player inactivity of a selected guild")
    @app_commands.describe(name='Prefix or Name of the guild search Ex: TAq, Calvish.',)
    async def inactivity(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /guild inactivity was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")
        current_time = time.time()
        if int(current_time - self.guildLookupCooldown) <= 5:
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {current_time - self.guildLookupCooldown} seconds.", ephemeral=True)
            return
        self.guildLookupCooldown = current_time
        if len(name) >= 5:
            URL = f"https://api.wynncraft.com/v3/guild/{name}"
        else:
            URL = f"https://api.wynncraft.com/v3/guild/prefix/{name}"

        r = await makeRequest(URL)
        if r.ok:
            await interaction.response.defer()
            inactivityDict = await lookupGuild(name)
            view = InactivityView(inactivityDict)
            embed = discord.Embed(
                title=f"{view.category_keys[view.current_category_index]}",
                description=view.get_user_list(view.category_keys[view.current_category_index]),
                color=discord.Color.blue()
            )
            await interaction.followup.send(embed=embed, view=view)
        else:
            await interaction.response.send_message(f"'{name}' is an unknown prefix or guild name.", ephemeral=True)
        
async def setup(bot):
    await bot.add_cog(Guild(bot))