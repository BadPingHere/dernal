import discord
from discord.ext import commands
from discord import app_commands
from discord.ui import View, Button
from typing import Optional
import time
from lib.utils import timeframeMap2, timeframeMap3, getGraidDatabaseData, checkCooldown, guildLookup, lookupGuild, guildLeaderboardXPButGuildSpecific, guildActivityXP, guildActivityTerritories, guildActivityWars, guildActivityOnlineMembers, guildActivityTotalMembers, guildLeaderboardOnlineMembers, guildLeaderboardTotalMembers, guildLeaderboardWars, guildLeaderboardXP, guildLeaderboardOnlineButGuildSpecific, guildLeaderboardWarsButGuildSpecific, guildLeaderboardGraids, guildLeaderboardGraidsButGuildSpecific, guildActivityGraids, guildOnline
from lib.makeRequest import makeRequest
import sqlite3
import logging
import asyncio
from datetime import datetime, timezone

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

async def progressEmbed(current, total, guild_name):
    percentage = (current / total) * 100
    filled_blocks = int(percentage // 5) # 5% per block
    empty_blocks = 20 - filled_blocks
    
    progress_bar = "█" * filled_blocks + "░" * empty_blocks
    
    embed = discord.Embed(
        title=f"🔍 Analyzing {guild_name} Inactivity",
        description=f"**Progress: {current}/{total} members processed**\n"
                   f"`{progress_bar}` {percentage:.1f}%\n\n"
                   f"*This may take several minutes due to API limitations. Blame wynncraft, not me.*",
        color=discord.Color.blue()
    )
    
    if current > 0:
        estimated_total_time = total * 0.6  # Rough estimate: 0.6 seconds per member
        elapsed_time = current * 0.6
        remaining_time = estimated_total_time - elapsed_time
        if remaining_time > 60:
            time_str = f"~{int(remaining_time//60)}m {int(remaining_time%60)}s remaining"
        else:
            time_str = f"~{int(remaining_time)}s remaining"
        embed.add_field(name="Estimated Time", value=time_str, inline=False)
        embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
    
    return embed
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

        leaderboardDesc = "```\n{:<3} {:<30} {:<10}\n".format("#", "Name", f"{self.shorthandTitle}")
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
class Guild(commands.GroupCog, name="guild"):
    def __init__(self, bot):
        self.bot = bot
        self.EligibleGuilds = getGraidDatabaseData("EligibleGuilds")
    
    async def timeframeAutocomplete2(self, interaction: discord.Interaction, current: str):
        keys = list(timeframeMap2.keys())
        return [app_commands.Choice(name=k, value=k)for k in keys if current.lower() in k.lower()][:25]
    
    async def timeframeAutocomplete3(self, interaction: discord.Interaction, current: str):
        keys = list(timeframeMap3.keys())
        return [app_commands.Choice(name=k, value=k)for k in keys if current.lower() in k.lower()][:25]
    
    activityCommands = app_commands.Group(name="activity", description="this is never seen, yet discord flips the x out if its not here.",)
    @activityCommands.command(name="xp", description="Shows a bar graph displaying the total xp a guild has every day, for the past 2 weeks.")
    @app_commands.describe(name='Prefix or Name of the guild search Ex: TAq, Calvish.',)   
    async def activityXP(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /guild activity xp was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
            
        conn = sqlite3.connect('database/guild_activity.db')
        cursor = conn.cursor()
        
        if len(name) <= 4:
            cursor.execute("SELECT uuid FROM guilds WHERE prefix = ? COLLATE NOCASE", (name,))
            result = cursor.fetchone()
            if not result:
                cursor.execute("SELECT uuid FROM guilds WHERE name = ? COLLATE NOCASE", (name,))
                result = cursor.fetchone()
        else:
            cursor.execute("SELECT uuid FROM guilds WHERE name = ? COLLATE NOCASE", (name,))
            result = cursor.fetchone()
            if not result:
                cursor.execute("SELECT uuid FROM guilds WHERE prefix = ? COLLATE NOCASE", (name,))
                result = cursor.fetchone()
        
        if not result:
            await interaction.followup.send(f"No data found for guild: {name}", ephemeral=True)
            conn.close()
            return
            
        file, embed = await asyncio.to_thread(guildActivityXP,result[0], name)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 14 days")
    
    @activityCommands.command(name="territories", description="Shows a graph displaying the amount of territories a guild has for the past 7 days.")
    @app_commands.describe(name='Prefix or Name of the guild search Ex: TAq, Calvish.',)
    async def activityTerritories(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /guild activity territories was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
            
        conn = sqlite3.connect('database/guild_activity.db')
        cursor = conn.cursor()
        
        if len(name) <= 4:
            cursor.execute("SELECT uuid FROM guilds WHERE prefix = ? COLLATE NOCASE", (name,))
            result = cursor.fetchone()
            if not result:
                cursor.execute("SELECT uuid FROM guilds WHERE name = ? COLLATE NOCASE", (name,))
                result = cursor.fetchone()
        else:
            cursor.execute("SELECT uuid FROM guilds WHERE name = ? COLLATE NOCASE", (name,))
            result = cursor.fetchone()
            if not result:
                cursor.execute("SELECT uuid FROM guilds WHERE prefix = ? COLLATE NOCASE", (name,))
                result = cursor.fetchone()
        
        if not result:
            await interaction.followup.send(f"No data found for guild: {name}", ephemeral=True)
            conn.close()
            return
        file, embed = await asyncio.to_thread(guildActivityTerritories, result[0], name)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 3 days")

    @activityCommands.command(name="wars", description="Shows a graph displaying the total amount of wars a guild has done over the past 7 days.")
    @app_commands.describe(name='Prefix or Name of the guild search Ex: TAq, Calvish.',) 
    async def activityWars(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /guild activity wars was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")
        
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        
        await interaction.response.defer()
            
        conn = sqlite3.connect('database/guild_activity.db')
        cursor = conn.cursor()
        
        if len(name) <= 4:
            cursor.execute("SELECT uuid FROM guilds WHERE prefix = ? COLLATE NOCASE", (name,))
            result = cursor.fetchone()
            if not result:
                cursor.execute("SELECT uuid FROM guilds WHERE name = ? COLLATE NOCASE", (name,))
                result = cursor.fetchone()
        else:
            cursor.execute("SELECT uuid FROM guilds WHERE name = ? COLLATE NOCASE", (name,))
            result = cursor.fetchone()
            if not result:
                cursor.execute("SELECT uuid FROM guilds WHERE prefix = ? COLLATE NOCASE", (name,))
                result = cursor.fetchone()
        
        if not result:
            await interaction.followup.send(f"No data found for guild: {name}", ephemeral=True)
            conn.close()
            return
        file, embed = await asyncio.to_thread(guildActivityWars, result[0], name)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 3 days")

    @activityCommands.command(name="members", description="Shows a graph displaying the amount of members a guild has for the past 7 days.")
    @app_commands.describe(name='Prefix or Name of the guild search Ex: TAq, Calvish.',) 
    async def activityTotal_members(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /guild activity members was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        conn = sqlite3.connect('database/guild_activity.db')
        cursor = conn.cursor()
        
        if len(name) <= 4:
            cursor.execute("SELECT uuid FROM guilds WHERE prefix = ? COLLATE NOCASE", (name,))
            result = cursor.fetchone()
            if not result:
                cursor.execute("SELECT uuid FROM guilds WHERE name = ? COLLATE NOCASE", (name,))
                result = cursor.fetchone()
        else:
            cursor.execute("SELECT uuid FROM guilds WHERE name = ? COLLATE NOCASE", (name,))
            result = cursor.fetchone()
            if not result:
                cursor.execute("SELECT uuid FROM guilds WHERE prefix = ? COLLATE NOCASE", (name,))
                result = cursor.fetchone()
        
        if not result:
            await interaction.followup.send(f"No data found for guild: {name}", ephemeral=True)
            conn.close()
            return
            
        file, embed = await asyncio.to_thread(guildActivityTotalMembers, result[0], name)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 24 hours")
    
    @activityCommands.command(name="online_members", description="Shows a graph displaying the average amount of online members a guild has for the past 3 days.")
    @app_commands.describe(name='Prefix or Name of the guild search Ex: TAq, Calvish.',)
    async def activityOnline_members(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /guild activity online_members was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        conn = sqlite3.connect('database/guild_activity.db')
        cursor = conn.cursor()
        
        if len(name) <= 4:
            cursor.execute("SELECT uuid FROM guilds WHERE prefix = ? COLLATE NOCASE", (name,))
            result = cursor.fetchone()
            if not result:
                cursor.execute("SELECT uuid FROM guilds WHERE name = ? COLLATE NOCASE", (name,))
                result = cursor.fetchone()
        else:
            cursor.execute("SELECT uuid FROM guilds WHERE name = ? COLLATE NOCASE", (name,))
            result = cursor.fetchone()
            if not result:
                cursor.execute("SELECT uuid FROM guilds WHERE prefix = ? COLLATE NOCASE", (name,))
                result = cursor.fetchone()
        
        if not result:
            await interaction.followup.send(f"No data found for guild: {name}", ephemeral=True)
            conn.close()
            return
            
        file, embed = await asyncio.to_thread(guildActivityOnlineMembers, result[0], name)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 3 days")

    @activityCommands.command(name="guild_raids", description="Shows a graph displaying the amount of guild raids completed in the past 14 days.")
    @app_commands.describe(name='Prefix of the guild search Ex: TAq, Calvish.',)
    async def activityGRaids(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /guild activity guild_raids was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        if name not in self.EligibleGuilds :# Guilds above lvl 80
            await interaction.response.send_message(f"The guild provided is not at or above level 80. If this is a mistake, please report this bug on github.",ephemeral=True)
            return  
        await interaction.response.defer()
        file, embed = await asyncio.to_thread(guildActivityGraids, name)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available for the last 14 days")
    
    leaderboardCommands = app_commands.Group(name="leaderboard",description="this is never seen, yet discord flips the x out if its not here.",)
    @leaderboardCommands.command(name="online_members", description="Shows a leaderboard of the top 100 guild's average amount of online players.")
    @app_commands.describe(name='Prefix or Name of the guild Ex: TAq, Calvish.',)
    @app_commands.describe(timeframe='The timeframe you want to see. ',)
    async def leaderboardOnline_members(self, interaction: discord.Interaction, timeframe: str, name: Optional[str]):
        logger.info(f"Command /guild leaderboard online_members was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). The name is {name}. The timeframe is {timeframe}.")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        
        if not name: # Normal guild shit
            data = await asyncio.to_thread(guildLeaderboardOnlineMembers, timeframe)
            view = LeaderboardPaginator(data, f"Top 100 Guilds by Online Member Average - {timeframe}", "Online Average")
        else:
            conn = sqlite3.connect('database/guild_activity.db')
            cursor = conn.cursor()
            
            if len(name) <= 4:
                cursor.execute("SELECT uuid FROM guilds WHERE prefix = ? COLLATE NOCASE", (name,))
                result = cursor.fetchone()
                if not result:
                    cursor.execute("SELECT uuid FROM guilds WHERE name = ? COLLATE NOCASE", (name,))
                    result = cursor.fetchone()
            else:
                cursor.execute("SELECT uuid FROM guilds WHERE name = ? COLLATE NOCASE", (name,))
                result = cursor.fetchone()
                if not result:
                    cursor.execute("SELECT uuid FROM guilds WHERE prefix = ? COLLATE NOCASE", (name,))
                    result = cursor.fetchone()
            
            if not result:
                await interaction.followup.send(f"No data found for guild: {name}", ephemeral=True)
                conn.close()
                return
            data = await asyncio.to_thread(guildLeaderboardOnlineButGuildSpecific, result[0], timeframe)
            view = LeaderboardPaginator(data, f"Top 100 Players in {name} by Playtime Average- {timeframe}", "Hours/day")

        if data:
            await interaction.followup.send(embed=view.get_embed(), view=view)
        else:
            await interaction.followup.send("No data available.")
    
    @leaderboardCommands.command(name="guild_raids", description="Shows a leaderboard of the level 80+ guild's guild raids.")
    @app_commands.describe(name='Prefix of the guild Ex: TAq, SEQ.',)
    @app_commands.describe(timeframe='The timeframe you want to see. ',)
    async def leaderboardGraids(self, interaction: discord.Interaction, timeframe: str, name: Optional[str]):
        logger.info(f"Command /guild leaderboard guild_raids was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). The name is {name}. The timeframe is {timeframe}.")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        
        if name:
            if name not in self.EligibleGuilds:# Guilds above lvl 80
                await interaction.response.send_message(f"The guild provided is not at or above level 80. If this is a mistake, please report this bug on github.",ephemeral=True)
                return  
            else:
                await interaction.response.defer()
                data = await asyncio.to_thread(guildLeaderboardGraidsButGuildSpecific, name, timeframe)
                num = len(data)
                view = LeaderboardPaginator(data, f"Top {num} Players in {name} by Guild Raids - {timeframe}", "Guild Raids")
        else:
            await interaction.response.defer()
            data = await asyncio.to_thread(guildLeaderboardGraids, timeframe)
            num = len(data)
            view = LeaderboardPaginator(data, f"Top {num} Guilds by Guild Raids - {timeframe}", "Guild Raids")

        if data:
            await interaction.followup.send(embed=view.get_embed(), view=view)
        else:
            await interaction.followup.send("No data available.")

    @leaderboardCommands.command(name="members", description="Shows a leaderboard of the top 100 guild's member count.")
    async def leaderboardTotal_members(self, interaction: discord.Interaction):
        logger.info(f"Command /guild leaderboard members was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id})")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()
        
        data = await asyncio.to_thread(guildLeaderboardTotalMembers)
        if data:
            view = LeaderboardPaginator(data, f"Top 100 Guilds by Member Count", "Members")
            await interaction.followup.send(embed=view.get_embed(), view=view)
        else:
            await interaction.followup.send("No data available.")
    
    @leaderboardCommands.command(name="wars", description="Shows a leaderboard of the top 100 guild's war amount.")
    @app_commands.describe(name='Prefix or Name of the guild Ex: TAq, Calvish.',)
    @app_commands.describe(timeframe='The timeframe you want to see. ',)
    async def leaderboardWars(self, interaction: discord.Interaction, timeframe: str,  name: Optional[str]):
        logger.info(f"Command /guild leaderboard wars was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). The name is {name}. The timeframe is {timeframe}.")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()

        if not name: # Normal guild shit
            data = await asyncio.to_thread(guildLeaderboardWars, timeframe)
            view = LeaderboardPaginator(data, f"Top 100 Guilds by Wars Won - {timeframe}", "Wars Won")
        else:
            conn = sqlite3.connect('database/guild_activity.db')
            cursor = conn.cursor()
            
            if len(name) <= 4:
                cursor.execute("SELECT uuid FROM guilds WHERE prefix = ? COLLATE NOCASE", (name,))
                result = cursor.fetchone()
                if not result:
                    cursor.execute("SELECT uuid FROM guilds WHERE name = ? COLLATE NOCASE", (name,))
                    result = cursor.fetchone()
            else:
                cursor.execute("SELECT uuid FROM guilds WHERE name = ? COLLATE NOCASE", (name,))
                result = cursor.fetchone()
                if not result:
                    cursor.execute("SELECT uuid FROM guilds WHERE prefix = ? COLLATE NOCASE", (name,))
                    result = cursor.fetchone()
            
            if not result:
                await interaction.followup.send(f"No data found for guild: {name}", ephemeral=True)
                conn.close()
                return
            data = await asyncio.to_thread(guildLeaderboardWarsButGuildSpecific, result[0], timeframe)
            view = LeaderboardPaginator(data, f"Top 100 Players in {name} by Wars Won - {timeframe}", "Wars Won")

        if data:
            await interaction.followup.send(embed=view.get_embed(), view=view)
        else:
            await interaction.followup.send("No data available.")

    @leaderboardCommands.command(name="xp", description="Shows a leaderboard of the top 100 guild's xp gained over the past 24 hours.")
    @app_commands.describe(name='Prefix or Name of the guild Ex: TAq, Calvish. Shows data for the past 7 days.',)
    async def leaderboardXP(self, interaction: discord.Interaction, timeframe: str, name: Optional[str]):
        logger.info(f"Command /guild leaderboard xp was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). The name is {name}. The timeframe is {timeframe}.")
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        await interaction.response.defer()

        if not name: # Normal guild shit
            data = await asyncio.to_thread(guildLeaderboardXP, timeframe)
            view = LeaderboardPaginator(data, f"Top 100 Guilds by XP Gain - {timeframe}", "XP")
        else:
            conn = sqlite3.connect('database/guild_activity.db')
            cursor = conn.cursor()
            
            if len(name) <= 4:
                cursor.execute("SELECT uuid FROM guilds WHERE prefix = ? COLLATE NOCASE", (name,))
                result = cursor.fetchone()
                if not result:
                    cursor.execute("SELECT uuid FROM guilds WHERE name = ? COLLATE NOCASE", (name,))
                    result = cursor.fetchone()
            else:
                cursor.execute("SELECT uuid FROM guilds WHERE name = ? COLLATE NOCASE", (name,))
                result = cursor.fetchone()
                if not result:
                    cursor.execute("SELECT uuid FROM guilds WHERE prefix = ? COLLATE NOCASE", (name,))
                    result = cursor.fetchone()
            
            if not result:
                await interaction.followup.send(f"No data found for guild: {name}", ephemeral=True)
                conn.close()
                return
            data = await asyncio.to_thread(guildLeaderboardXPButGuildSpecific, result[0], timeframe)
            view = LeaderboardPaginator(data, f"Top 100 Players in {name} by XP Gain - {timeframe}", "XP")

        if data:
            await interaction.followup.send(embed=view.get_embed(), view=view)
        else:
            await interaction.followup.send("No data available.")
            
    @app_commands.command(description="Shows you to get a quick overview of a guild, like level, online members, etc.")
    @app_commands.describe(name='Prefix or Name of the guild search Ex: TAq, Calvish. (Case Sensitive)',)
    async def overview(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /guild overview was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")
        response = await asyncio.to_thread(checkCooldown, interaction.guild.id, 10)

        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        if len(name) >= 5:
            URL = f"https://api.wynncraft.com/v3/guild/{name}"
        else:
            URL = f"https://api.wynncraft.com/v3/guild/prefix/{name}"

        success, r = await asyncio.to_thread(makeRequest, URL)
        if not success:
            logger.error("Error while getting request in /guild overview")
            await interaction.response.send_message("There was an error while getting data from the API. If this issue is persistent, please report it on my github.", ephemeral=True)
            return
        if r.ok:
            embed = await asyncio.to_thread(guildLookup, name, r)
            await interaction.response.send_message(embed=embed)
        else:
            await interaction.response.send_message(f"'{name}' is an unknown prefix or guild name.", ephemeral=True)

    @app_commands.command(description="Shows and sorts the player inactivity of a selected guild")
    @app_commands.describe(name='Prefix or Name of the guild search Ex: TAq, Calvish. (Case Sensitive)',) 
    async def inactivity(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /guild inactivity was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")
        response = await asyncio.to_thread(checkCooldown, interaction.guild.id, 10)

        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        if len(name) >= 5:
            URL = f"https://api.wynncraft.com/v3/guild/{name}"
        else:
            URL = f"https://api.wynncraft.com/v3/guild/prefix/{name}"

        success, r = await asyncio.to_thread(makeRequest, URL)
        if not success:
            logger.error("Error while getting request in /guild inactivity")
            await interaction.response.send_message("There was an error while getting data from the API. If this issue is persistent, please report it on my github.", ephemeral=True)
            return
        if not r.ok:
            await interaction.response.send_message(f"'{name}' is an unknown prefix or guild name.", ephemeral=True)
            return
        guildData = r.json()
        guildPrefix = guildData.get("prefix")

        total_members = guildData["members"]["total"]
        
        # Send initial progress message
        await interaction.response.defer()
        progress_embed = await progressEmbed(0, total_members, guildPrefix)
        message = await interaction.followup.send(embed=progress_embed)
        
        # Create a shared progress state
        progress_state = {"current": 0, "last_update": 0}
        
        # Create a separate async task to handle progress updates
        async def progress_updater():
            while progress_state["current"] < total_members:
                await asyncio.sleep(1)  # Check every 2 seconds
                current = progress_state["current"]
                if current > progress_state["last_update"]:
                    try:
                        embed = await progressEmbed(current, total_members, guildPrefix)
                        await message.edit(embed=embed)
                        progress_state["last_update"] = current
                    except discord.HTTPException as e:
                        logger.warning(f"Failed to update progress message: {e}")
        
        # Start the progress updater task
        updater_task = asyncio.create_task(progress_updater())
        
        # Simple progress callback that just updates the shared state
        def sync_progress_callback(current, total):
            progress_state["current"] = current
        
        # Run the lookup with progress updates
        try:
            inactivityDict = await asyncio.to_thread(lookupGuild, r, sync_progress_callback)
            
            # Cancel the updater task
            updater_task.cancel()
            try:
                await updater_task
            except asyncio.CancelledError:
                pass
            
            # Send final result
            view = InactivityView(inactivityDict)
            embed = discord.Embed(
                title=f"{view.category_keys[view.current_category_index]} - {guildPrefix}",
                description=view.get_user_list(view.category_keys[view.current_category_index]),
                color=discord.Color.blue()
            )
            embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
            await message.edit(embed=embed, view=view)
            
        except Exception as e:
            # Cancel the updater task if there's an error
            updater_task.cancel()
            try:
                await updater_task
            except asyncio.CancelledError:
                pass
                
            logger.error(f"Error during inactivity lookup: {e}")
            embed = discord.Embed(
                title="❌ Error",
                description="An error occurred while processing the guild members. Please try again later.",
                color=discord.Color.red()
            )
            embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
            await message.edit(embed=embed)
        
    @app_commands.command(description="Displays the current online members of a guild.")
    @app_commands.describe(name='Prefix or Name of the guild search Ex: TAq, Calvish. (Case Sensitive)',)
    async def online(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /guild online was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")
        response = await asyncio.to_thread(checkCooldown, interaction.guild.id, 10)

        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        
        if len(name) >= 5:
            URL = f"https://api.wynncraft.com/v3/guild/{name}"
        else:
            URL = f"https://api.wynncraft.com/v3/guild/prefix/{name}"

        success, r = await asyncio.to_thread(makeRequest, URL)
        if not success:
            logger.error("Error while getting request in /guild online")
            await interaction.response.send_message("There was an error while getting data from the API. If this issue is persistent, please report it on my github.", ephemeral=True)
            return
        if r.ok:
            embed = await asyncio.to_thread(guildOnline, name, r)
            await interaction.response.send_message(embed=embed)
        else:
            await interaction.response.send_message(f"'{name}' is an unknown prefix or guild name.", ephemeral=True)
    
    leaderboardOnline_members.autocomplete("timeframe")(timeframeAutocomplete3)
    leaderboardGraids.autocomplete("timeframe")(timeframeAutocomplete2)
    leaderboardWars.autocomplete("timeframe")(timeframeAutocomplete3)
    leaderboardXP.autocomplete("timeframe")(timeframeAutocomplete3)

async def setup(bot):
    await bot.add_cog(Guild(bot))