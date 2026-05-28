import discord
from discord.ext import commands
from discord import app_commands
from discord.ui import View, Button
from typing import Optional
import time
from lib.utils import activityBuilder, leaderboardBuilder, guildLookup, inactivityCheck, guildOnline, checkNameValidity, SRleaderboardBuilder, leaderboardTimeframeMap, activityTimeframeMap, themeMap
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

    def get_description(self, category):
        desc = self.get_user_list(category)
        if category == "Four Week Inactive Users":
            desc = "-# Note: Players with 'lastJoin' hidden on api will show as last logged in 1960.\n" + desc
        return desc

    async def update_embed(self, interaction: discord.Interaction):
        # Create and send the updated embed based on the current category
        current_category = self.category_keys[self.current_category_index]
        embed = discord.Embed(
            title=f"{current_category}",
            description=self.get_description(current_category),
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
        leaderboardDesc = ""

        if self.shorthandTitle == "SR": #coming from guild leaderboard sr, add a note
            leaderboardDesc = "Note: This is not a 100% accurate list, as small guilds not being\n tracked won't show up."

        leaderboardDesc += "```\n{:<3} {:<24} {:<10}\n".format("#", "Name", f"{self.shorthandTitle}")
        separator = "-" * 39 + "\n"
        leaderboardDesc += separator
        for i, (name, value) in enumerate(page_data, start=start + 1):
            if value % 1 == 0: # Checks if int
                formattedValue = f"{int(value):,}"
            else:
                formattedValue = f"{value:,.2f}"
            leaderboardDesc += "{:<3} {:<24} {:<10}\n".format(i, name, formattedValue)
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
    
    async def autocompletLeaderboardTimeframe(self, interaction: discord.Interaction, current: str):
        return [app_commands.Choice(name=k, value=k)for k in leaderboardTimeframeMap if current.lower() in k.lower()][:25]
    
    async def autocompleteTheme(self, interaction: discord.Interaction, current: str):
        return [app_commands.Choice(name=k, value=k)for k in themeMap if current.lower() in k.lower()][:25]
    
    async def autocompleteActivityTimeframe(self, interaction: discord.Interaction, current: str):
        return [app_commands.Choice(name=k, value=k)for k in activityTimeframeMap if current.lower() in k.lower()][:25]
    
    activityCommands = app_commands.Group(name="activity", description="this is never seen, yet discord flips the x out if its not here.",)
    @activityCommands.command(name="xp", description="Shows a bar graph displaying the total xp a guild has every day, for the past 2 weeks.")
    @app_commands.describe(name='Prefix or Name of the guild search Ex: TAq, Calvish.',)   
    @app_commands.describe(theme='The theme of the graphic (defaults to light mode)',)
    async def activityXP(self, interaction: discord.Interaction, name: str, timeframe: str,  theme: Optional[str]):
        logger.info(f"Command /guild activity xp was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")

        await interaction.response.defer()
        
        success, jsonData = await asyncio.to_thread(checkNameValidity, name, "guild")
        if not success:
            await interaction.followup.send(f"No data found for guild: {name}. Is this a valid prefix or guild name?", ephemeral=True)
            return
        guildUUID = jsonData["guild_uuid"]

        file, embed = await asyncio.to_thread(activityBuilder,"guildActivityXP", uuid=guildUUID[0], name=name, theme=theme or "light", timeframe=timeframe)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send(f"No data available for the {timeframe}")
    
    @activityCommands.command(name="territories", description="Shows a graph displaying the amount of territories a guild has.")
    @app_commands.describe(name='Prefix or Name of the guild search Ex: TAq, Calvish.',)
    @app_commands.describe(theme='The theme of the graphic (defaults to light mode)',)
    async def activityTerritories(self, interaction: discord.Interaction, name: str, timeframe: str,  theme: Optional[str]):
        logger.info(f"Command /guild activity territories was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")

        await interaction.response.defer()
            
        success, jsonData = await asyncio.to_thread(checkNameValidity, name, "guild")
        if not success:
            await interaction.followup.send(f"No data found for guild: {name}. Is this a valid prefix or guild name?", ephemeral=True)
            return
        guildUUID = jsonData["guild_uuid"]

        file, embed = await asyncio.to_thread(activityBuilder,"guildActivityTerritories", uuid=guildUUID, name=name, theme=theme or "light", timeframe=timeframe)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send(f"No data available for the {timeframe}")

    @activityCommands.command(name="wars", description="Shows a graph displaying the total amount of wars a guild has done.")
    @app_commands.describe(name='Prefix or Name of the guild search Ex: TAq, Calvish.',) 
    @app_commands.describe(theme='The theme of the graphic (defaults to light mode)',)
    async def activityWars(self, interaction: discord.Interaction, name: str, timeframe: str,  theme: Optional[str]):
        logger.info(f"Command /guild activity wars was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")
        

        
        await interaction.response.defer()
            
        success, jsonData = await asyncio.to_thread(checkNameValidity, name, "guild")
        if not success:
            await interaction.followup.send(f"No data found for guild: {name}. Is this a valid prefix or guild name?", ephemeral=True)
            return
        guildUUID = jsonData["guild_uuid"]

        file, embed = await asyncio.to_thread(activityBuilder,"guildActivityWars", uuid=guildUUID, name=name, theme=theme or "light", timeframe=timeframe)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send(f"No data available for the {timeframe}")

    @activityCommands.command(name="members", description="Shows a graph displaying the amount of members a guild has.")
    @app_commands.describe(name='Prefix or Name of the guild search Ex: TAq, Calvish.',) 
    @app_commands.describe(theme='The theme of the graphic (defaults to light mode)',)
    async def activityTotal_members(self, interaction: discord.Interaction, name: str, timeframe: str,  theme: Optional[str]):
        logger.info(f"Command /guild activity members was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")

        await interaction.response.defer()
        
        success, jsonData = await asyncio.to_thread(checkNameValidity, name, "guild")
        if not success:
            await interaction.followup.send(f"No data found for guild: {name}. Is this a valid prefix or guild name?", ephemeral=True)
            return
        guildUUID = jsonData["guild_uuid"]

        file, embed = await asyncio.to_thread(activityBuilder,"guildActivityTotalMembers", uuid=guildUUID, name=name, theme=theme or "light", timeframe=timeframe)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send(f"No data available for the {timeframe}")
    
    @activityCommands.command(name="online_members", description="Shows a graph displaying the average amount of online members a guild has.")
    @app_commands.describe(name='Prefix or Name of the guild search Ex: TAq, Calvish.',)
    @app_commands.describe(theme='The theme of the graphic (defaults to light mode)',)
    async def activityOnline_members(self, interaction: discord.Interaction, name: str, timeframe: str,  theme: Optional[str]):
        logger.info(f"Command /guild activity online_members was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")

        await interaction.response.defer()
        
        success, jsonData = await asyncio.to_thread(checkNameValidity, name, "guild")
        if not success:
            await interaction.followup.send(f"No data found for guild: {name}. Is this a valid prefix or guild name?", ephemeral=True)
            return
        guildUUID = jsonData["guild_uuid"]

        file, embed = await asyncio.to_thread(activityBuilder,"guildActivityOnlineMembers", uuid=guildUUID, name=name, theme=theme or "light", timeframe=timeframe)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send(f"No data available for the {timeframe}")

    @activityCommands.command(name="guild_raids", description="Shows a graph displaying the amount of guild raids completed.")
    @app_commands.describe(name='Prefix of the guild search Ex: TAq, Calvish.',)
    @app_commands.describe(theme='The theme of the graphic (defaults to light mode)',)
    async def activityGRaids(self, interaction: discord.Interaction, name: str, timeframe: str,  theme: Optional[str]):
        logger.info(f"Command /guild activity guild_raids was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")

        await interaction.response.defer()

        success, jsonData = await asyncio.to_thread(checkNameValidity, name, "guild")
        if not success:
            await interaction.followup.send(f"No data found for guild: {name}. Is this a valid prefix or guild name?", ephemeral=True)
            return
        guildUUID = jsonData["guild_uuid"]

        file, embed = await asyncio.to_thread(activityBuilder,"guildActivityGraids", uuid=guildUUID, name=name, theme=theme or "light", timeframe=timeframe)
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send(f"No data available for the {timeframe}")

    @activityCommands.command(name="graids_pie", description="Shows a pie chart displaying the different graid's a guild has done.")
    @app_commands.describe(name='Prefix of the guild search Ex: TAq, Calvish.',)
    @app_commands.describe(theme='The theme of the graphic (defaults to light mode)',)
    async def activityGraidsPie(self, interaction: discord.Interaction, name: str, theme: Optional[str]):
        logger.info(f"Command /guild activity graids_pie was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter username is: {name}.")

        await interaction.response.defer()
        
        success, jsonData = await asyncio.to_thread(checkNameValidity, name, "guild")
        if not success:
            await interaction.followup.send(f"No data found for guild: {name}. Is this a valid prefix or guild name?", ephemeral=True)
            return
        guildUUID = jsonData["guild_uuid"]

        file, embed = await asyncio.to_thread(activityBuilder,"guildActivityGraidPie", uuid=guildUUID, name=name, theme=theme or "light")
        
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("No data available.")
    
    leaderboardCommands = app_commands.Group(name="leaderboard",description="this is never seen, yet discord flips the x out if its not here.",)
    @leaderboardCommands.command(name="online_members", description="Shows a leaderboard of the top 100 guild's average amount of online players.")
    @app_commands.describe(name='Prefix or Name of the guild Ex: TAq, Calvish.',)
    @app_commands.describe(timeframe='The timeframe you want to see. ',)
    async def leaderboardOnline_members(self, interaction: discord.Interaction, timeframe: str, name: Optional[str]):
        logger.info(f"Command /guild leaderboard online_members was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). The name is {name}. The timeframe is {timeframe}.")

        await interaction.response.defer()
        
        if not name: # Normal guild shit
            data = await asyncio.to_thread(leaderboardBuilder, "guildLeaderboardOnlineMembers", timeframe=timeframe)
            view = LeaderboardPaginator(data, f"Top 100 Guilds by Online Member Average - {timeframe}", "Online Average")
        else:
            success, jsonData = await asyncio.to_thread(checkNameValidity, name, "guild")
            if not success:
                await interaction.followup.send(f"No data found for guild: {name}. Is this a valid prefix or guild name?", ephemeral=True)
                return
            guildUUID = jsonData["guild_uuid"]

            data = await asyncio.to_thread(leaderboardBuilder, "guildLeaderboardOnlineButGuildSpecific", timeframe=timeframe, uuid=guildUUID)
            view = LeaderboardPaginator(data, f"Top 100 Players in {name} by Playtime Average- {timeframe}", "Hours")

        if data:
            await interaction.followup.send(embed=view.get_embed(), view=view)
        else:
            await interaction.followup.send("No data available.")
    
    @leaderboardCommands.command(name="guild_raids", description="Shows a leaderboard of the level 80+ guild's guild raids.")
    @app_commands.describe(name='Prefix of the guild Ex: TAq, SEQ.',)
    @app_commands.describe(timeframe='The timeframe you want to see. ',)
    async def leaderboardGraids(self, interaction: discord.Interaction, timeframe: str, name: Optional[str]):
        logger.info(f"Command /guild leaderboard guild_raids was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). The name is {name}. The timeframe is {timeframe}.")
        
        if name:
            await interaction.response.defer()
            success, jsonData = await asyncio.to_thread(checkNameValidity, name, "guild")
            if not success:
                await interaction.followup.send(f"No data found for guild: {name}. Is this a valid prefix or guild name?", ephemeral=True)
                return
            guildUUID = jsonData["guild_uuid"]

            data = await asyncio.to_thread(leaderboardBuilder, "guildLeaderboardGraidsButGuildSpecific", timeframe=timeframe, uuid=guildUUID)
            num = len(data)
            view = LeaderboardPaginator(data, f"Top {num} Players in {name} by Guild Raids - {timeframe}", "Guild Raids")
        else:
            await interaction.response.defer()
            data = await asyncio.to_thread(leaderboardBuilder, "guildLeaderboardGraids", timeframe=timeframe)
            num = len(data)
            view = LeaderboardPaginator(data, f"Top {num} Guilds by Guild Raids - {timeframe}", "Guild Raids")

        if data:
            await interaction.followup.send(embed=view.get_embed(), view=view)
        else:
            await interaction.followup.send("No data available.")

    @leaderboardCommands.command(name="wars", description="Shows a leaderboard of the top 100 guild's war amount.")
    @app_commands.describe(name='Prefix or Name of the guild Ex: TAq, Calvish.',)
    @app_commands.describe(timeframe='The timeframe you want to see. ',)
    async def leaderboardWars(self, interaction: discord.Interaction, timeframe: str,  name: Optional[str]):
        logger.info(f"Command /guild leaderboard wars was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). The name is {name}. The timeframe is {timeframe}.")

        await interaction.response.defer()

        if not name: # Normal guild shit
            data = await asyncio.to_thread(leaderboardBuilder, "guildLeaderboardWars", timeframe=timeframe)
            view = LeaderboardPaginator(data, f"Top 100 Guilds by Wars Won - {timeframe}", "Wars Won")
        else:
            success, jsonData = await asyncio.to_thread(checkNameValidity, name, "guild")
            if not success:
                await interaction.followup.send(f"No data found for guild: {name}. Is this a valid prefix or guild name?", ephemeral=True)
                return
            guildUUID = jsonData["guild_uuid"]

            data = await asyncio.to_thread(leaderboardBuilder, "guildLeaderboardWarsButGuildSpecific", timeframe=timeframe, uuid=guildUUID)
            view = LeaderboardPaginator(data, f"Top 100 Players in {name} by Wars Won - {timeframe}", "Wars Won")

        if data:
            await interaction.followup.send(embed=view.get_embed(), view=view)
        else:
            await interaction.followup.send("No data available.")

    @leaderboardCommands.command(name="xp", description="Shows a leaderboard of the top 100 guild's xp gained over the past 24 hours.")
    @app_commands.describe(name='Prefix or Name of the guild Ex: TAq, Calvish. Shows data for the past 7 days.',)
    async def leaderboardXP(self, interaction: discord.Interaction, timeframe: str, name: Optional[str]):
        logger.info(f"Command /guild leaderboard xp was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). The name is {name}. The timeframe is {timeframe}.")

        await interaction.response.defer()

        if not name: # Normal guild shit
            data = await asyncio.to_thread(leaderboardBuilder, "guildLeaderboardXP", timeframe=timeframe)
            view = LeaderboardPaginator(data, f"Top 100 Guilds by XP Gain - {timeframe}", "XP")
        else:
            success, jsonData = await asyncio.to_thread(checkNameValidity, name, "guild")
            if not success:
                await interaction.followup.send(f"No data found for guild: {name}. Is this a valid prefix or guild name?", ephemeral=True)
                return
            guildUUID = jsonData["guild_uuid"]

            data = await asyncio.to_thread(leaderboardBuilder, "guildLeaderboardXPButGuildSpecific", timeframe=timeframe, uuid=guildUUID)
            view = LeaderboardPaginator(data, f"Top 100 Players in {name} by XP Gain - {timeframe}", "XP")

        if data:
            await interaction.followup.send(embed=view.get_embed(), view=view)
        else:
            await interaction.followup.send("No data available.")

    @leaderboardCommands.command(name="season_rating", description="Shows a leaderboard of the top 100 season ratings for a given season")
    @app_commands.describe(season='Season number you want the leaderboard for Ex. 8, 30.',)
    @app_commands.describe(name='Prefix or Name of the guild Ex: TAq, Calvish. Shows all partipated seasons and their rating.',)
    async def leaderboardSR(self, interaction: discord.Interaction, season: Optional[int], name: Optional[str]):
        await interaction.response.defer()

        if not season and not name:
            await interaction.send(f"You must input a season or guild prefix/name.", ephemeral=True)

        logger.info(f"Command /guild leaderboard season_rating was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). The name is {season}. The timeframe is {name}.")

        if season: # We get the season's data
            data = await asyncio.to_thread(SRleaderboardBuilder, season=season)
            view = LeaderboardPaginator(data, f"Season {season} Ranking", "SR")

            if data:
                await interaction.followup.send(embed=view.get_embed(), view=view)
            else:
                await interaction.followup.send("No data available.")
        else: # we get a specific guild's rankings
            success, jsonData = await asyncio.to_thread(checkNameValidity, name, "guild")
            if not success:
                await interaction.followup.send(f"No data found for guild: {name}. Is this a valid prefix or guild name?", ephemeral=True)
                return
            guildUUID = jsonData["guild_uuid"]

            embed = await asyncio.to_thread(SRleaderboardBuilder, uuid=guildUUID, name=name)
            await interaction.followup.send(embed=embed)
              
    @app_commands.command(description="Shows you to get a quick overview of a guild, like level, online members, etc.")
    @app_commands.describe(name='Prefix or Name of the guild search Ex: TAq, Calvish. (Case Sensitive)',)
    async def overview(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /guild overview was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")
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

        await interaction.response.defer()

        if len(name) >= 5:
            URL = f"https://api.wynncraft.com/v3/guild/{name}"
        else:
            URL = f"https://api.wynncraft.com/v3/guild/prefix/{name}"

        success, r = await asyncio.to_thread(makeRequest, URL)
        if not success:
            logger.error("Error while getting request in /guild inactivity")
            await interaction.followup.send("There was an error while getting data from the API. If this issue is persistent, please report it on my github.", ephemeral=True)
            return
        if not r.ok:
            await interaction.followup.send(f"'{name}' is an unknown prefix or guild name.", ephemeral=True)
            return
        guildData = r.json()
        guildPrefix = guildData.get("prefix")

        try:
            inactivityDict = await asyncio.to_thread(inactivityCheck, r)
            
            view = InactivityView(inactivityDict)
            embed = discord.Embed(
                title=f"{view.category_keys[view.current_category_index]} - {guildPrefix}",
                description=view.get_description(view.category_keys[view.current_category_index]),
                color=discord.Color.blue()
            )
            embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
            logger.info(view)
            logger.info(embed)
            await interaction.followup.send(embed=embed, view=view)
            
        except Exception as e:
            logger.error(f"Error during inactivity lookup: {e}")
            embed = discord.Embed(
                title="❌ Error",
                description="An error occurred while processing the guild members. Please try again later.",
                color=discord.Color.red()
            )
            embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")
            await interaction.followup.send(embed=embed)
        
    @app_commands.command(description="Displays the current online members of a guild.")
    @app_commands.describe(name='Prefix or Name of the guild search Ex: TAq, Calvish. (Case Sensitive)',)
    async def online(self, interaction: discord.Interaction, name: str):
        logger.info(f"Command /guild online was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {name}.")
        
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
    
    # Autocomplete Theme
    activityXP.autocomplete("theme")(autocompleteTheme)
    activityTerritories.autocomplete("theme")(autocompleteTheme)
    activityWars.autocomplete("theme")(autocompleteTheme)
    activityTotal_members.autocomplete("theme")(autocompleteTheme)
    activityOnline_members.autocomplete("theme")(autocompleteTheme)
    activityGRaids.autocomplete("theme")(autocompleteTheme)

    # Autocomplete timeframe
    activityXP.autocomplete("timeframe")(autocompleteActivityTimeframe)
    activityTerritories.autocomplete("timeframe")(autocompleteActivityTimeframe)
    activityWars.autocomplete("timeframe")(autocompleteActivityTimeframe)
    activityTotal_members.autocomplete("timeframe")(autocompleteActivityTimeframe)
    activityOnline_members.autocomplete("timeframe")(autocompleteActivityTimeframe)
    activityGRaids.autocomplete("timeframe")(autocompleteActivityTimeframe)
    leaderboardOnline_members.autocomplete("timeframe")(autocompletLeaderboardTimeframe)
    leaderboardGraids.autocomplete("timeframe")(autocompletLeaderboardTimeframe)
    leaderboardWars.autocomplete("timeframe")(autocompletLeaderboardTimeframe)
    leaderboardXP.autocomplete("timeframe")(autocompletLeaderboardTimeframe)
    

async def setup(bot):
    await bot.add_cog(Guild(bot))