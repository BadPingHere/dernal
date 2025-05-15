import discord
from discord.ext import commands
from discord import app_commands
from typing import Optional
import os
import time
from lib.utils import rollGiveaway, makeRequest, checkCooldown
import sqlite3
import logging
import math
import shelve
from datetime import datetime, timezone
import asyncio

logger = logging.getLogger('discord')

class GuildSelect(discord.ui.Select):
    def __init__(self, members, page=0, items_per_page=25):
        start_idx = page * items_per_page
        end_idx = start_idx + items_per_page
        page_members = members[start_idx:end_idx]
        
        options = [
            discord.SelectOption(label=member[1], value=member[0])  # value=UUID, label=username
            for member in page_members
        ]
        super().__init__(
            placeholder="Select players...",
            min_values=1,
            max_values=len(options),  # Allow selecting all options on the page
            options=options
        )

    async def callback(self, interaction: discord.Interaction):
        try:
            # Update the parent view's selected users
            selected_users = [option.label for option in self.options if option.value in self.values]
            self.view.updateWeelyUsers(self.values, selected_users)
            
            # Update the message content immediately after selection
            await interaction.response.edit_message(
                content=f"Select players who have done their weekly (Page {self.view.current_page + 1}/{self.view.total_pages})\nCurrently selected: {', '.join(sorted(self.view.weeklyNames))}\nNote: Only press submit once you are sure of every name. To remove a name, simply reselect them.",
                view=self.view
            )
        except discord.errors.InteractionResponded:
            # If the interaction has already been responded to, use followup
            await interaction.followup.edit_message(
                message_id=interaction.message.id,
                content=f"Select players who have done their weekly (Page {self.view.current_page + 1}/{self.view.total_pages})\nCurrently selected: {', '.join(sorted(self.view.weeklyNames))}\nNote: Only press submit once you are sure of every name. To remove a name, simply reselect them.",
                view=self.view
            )
        except Exception as e:
            logger.error(f"Error in GuildSelect callback: {str(e)}")
            try:
                # Attempt to send an error message if we haven't responded yet
                await interaction.response.send_message("An error occurred while processing your selection. Please try again.", ephemeral=True)
            except:
                # If we can't send a new message, try to edit the existing one
                await interaction.followup.edit_message(
                    message_id=interaction.message.id,
                    content=f"Select players who have done their weekly (Page {self.view.current_page + 1}/{self.view.total_pages})\nCurrently selected: {', '.join(sorted(self.view.weeklyNames))}\nNote: Only press submit once you are sure of every name. To remove a name, simply reselect them.",
                    view=self.view
                )

class GuildSelectView(discord.ui.View):
    def __init__(self, members, rollCount):
        super().__init__()
        self.all_members = members
        self.members = members
        self.rollCount = rollCount
        self.current_page = 0
        self.items_per_page = 25
        self.total_pages = math.ceil(len(members) / self.items_per_page)
        self.weeklyUUIDs = set() 
        self.weeklyNames = set() 
        self.updateSelection()

    def updateWeelyUsers(self, new_uuids, new_names):
        for uuid, name in zip(new_uuids, new_names): # Either adds or removes, depending if theyre already there.
            if uuid in self.weeklyUUIDs:
                self.weeklyUUIDs.remove(uuid)
                self.weeklyNames.remove(name)
            else:
                self.weeklyUUIDs.add(uuid)
                self.weeklyNames.add(name)

    def updateSelection(self): # Whole lotta yapping
        for item in self.children[:]:
            if isinstance(item, GuildSelect):
                self.remove_item(item)
        select = GuildSelect(self.members, self.current_page)
        start_idx = self.current_page * self.items_per_page
        page_members = self.members[start_idx:start_idx + self.items_per_page]
        select.default_values = [member[0] for member in page_members if member[0] in self.weeklyUUIDs]
        self.add_item(select)
        self.prevButton.disabled = self.current_page == 0
        self.nextButton.disabled = self.current_page >= self.total_pages - 1

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.primary)
    async def prevButton(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page > 0:
            self.current_page -= 1
            self.updateSelection()
            await interaction.response.edit_message(
                content=f"Select players who have done their weekly (Page {self.current_page + 1}/{self.total_pages})\nCurrently selected: {', '.join(sorted(self.weeklyNames))}\nNote: Only press submit once you are sure of every name. To remove a name, simply reselect them.",
                view=self
            )

    @discord.ui.button(label="Next", style=discord.ButtonStyle.primary)
    async def nextButton(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page < self.total_pages - 1:
            self.current_page += 1
            self.updateSelection()
            await interaction.response.edit_message(
                content=f"Select players who have done their weekly (Page {self.current_page + 1}/{self.total_pages})\nCurrently selected: {', '.join(sorted(self.weeklyNames))}\nNote: Only press submit once you are sure of every name. To remove a name, simply reselect them.",
                view=self
            )

    @discord.ui.button(label="Search", style=discord.ButtonStyle.secondary)
    async def searchButton(self, interaction: discord.Interaction, button: discord.ui.Button):
        class SearchModal(discord.ui.Modal, title="Search Username"):
            search_input = discord.ui.TextInput(label="Enter part of the username:", required=True)

            async def on_submit(inner_self, modal_interaction: discord.Interaction):
                search_text = inner_self.search_input.value.lower()
                filtered_members = [m for m in self.all_members if search_text in m[1].lower()]
                if not filtered_members:
                    await modal_interaction.response.send_message(f"No users found matching '{search_text}'.", ephemeral=True)
                    return
                self.members = filtered_members
                self.total_pages = max(1, math.ceil(len(self.members) / self.items_per_page))
                self.current_page = 0
                self.updateSelection()
                await modal_interaction.response.edit_message(
                    content=f"Select players who have done their weekly (Page {self.current_page + 1}/{self.total_pages})\nCurrently selected: {', '.join(sorted(self.weeklyNames))}\nNote: Only press submit once you are sure of every name. To remove a name, simply reselect them.",
                    view=self
                )

        await interaction.response.send_modal(SearchModal())

    @discord.ui.button(label="Reset Search", style=discord.ButtonStyle.danger)
    async def resetButton(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.members = self.all_members
        self.total_pages = max(1, math.ceil(len(self.members) / self.items_per_page))
        self.current_page = 0
        self.updateSelection()
        await interaction.response.edit_message(
            content=f"Select players who have done their weekly (Page {self.current_page + 1}/{self.total_pages})\nCurrently selected: {', '.join(sorted(self.weeklyNames))}\nNote: Only press submit once you are sure of every name. To remove a name, simply reselect them.",
            view=self
        )

    @discord.ui.button(label="Submit", style=discord.ButtonStyle.green)
    async def submitButton(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(view=self)
        # This is where we would roll the giveaway n shit but like its not done rn.
        #logger.info(f"Weekly UUID's: {self.weeklyUUIDs}")
        chances, winners = await asyncio.to_thread(rollGiveaway, self.weeklyNames, self.rollCount)
        sortedChances = sorted(chances.items(), key=lambda x: x[1], reverse=True)
        topChances = sortedChances[:5] # list of top 5
        topChancesButFormatted = "\n".join(f"`{name}`: {percentage:.1f}%" for name, percentage in topChances)

        description = f"""**Top Chances**\n{topChancesButFormatted}\n
                    **Players with weeklys done** ({len(self.weeklyNames)})\n{", ".join(sorted(self.weeklyNames)) if self.weeklyNames else "None"}\n
                    **Number of Rolls**\n{str(self.rollCount)}\n
                    {"**Winners**" if self.rollCount > 1 else "**Winner**"}\n{", ".join(f"{winner}" for winner in winners) if winners else "None"}
                    """
        embed = discord.Embed(
            title="🎉 Giveaway Results 🎉",
            description=description,
            color=discord.Color.gold()
        )
        embed.set_footer(text=f"https://github.com/badpinghere/dernal • {datetime.now(timezone.utc).strftime('%m/%d/%Y, %I:%M %p')}")

        await interaction.channel.send(embed=embed)

@app_commands.allowed_installs(guilds=True, users=False)
@app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)  
class giveaway(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        rootDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.giveawayFilePath = os.path.join(rootDir, 'database', 'giveaway')
        with shelve.open(self.giveawayFilePath) as db:
            self.giveawayGuild = dict(db)
    
    giveawayCommands = app_commands.Group(name="giveaway", description="this is never seen, yet discord flips the x out if its not here.",)
    
    @giveawayCommands.command(name="configure", description="Configure the giveaway system with a guild prefix")
    @app_commands.describe(prefix='The guild prefix to configure for giveaways')
    async def configure(self, interaction: discord.Interaction, prefix: str):
        requiredRoleName = "Giveaway Permission"
        #logger.info(f"interaction.user.roles: {interaction.user.roles}")
        permission = 0
        for role in interaction.user.roles:
            #logger.info(f"role.name: {role.name}")
            if role.name.lower() == requiredRoleName.lower():
                permission = 1
        if permission != 1:
            await interaction.response.send_message(f"You do not have the required role to use this command! If you are a server owner, create a role named '{requiredRoleName}', and give it to people who need to run this command.", ephemeral=True)
            return
        
        logger.info(f"Command /giveaway configure was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter prefix is: {prefix}.")
        response = await asyncio.to_thread(checkCooldown, interaction.guild.id, 10)

        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        
        # Check if guild exists in database
        conn = sqlite3.connect('database/guild_activity.db')
        cursor = conn.cursor()
        
        cursor.execute("SELECT uuid FROM guilds WHERE prefix = ? COLLATE NOCASE", (prefix,))
        result = cursor.fetchone()
        
        if not result:
            await interaction.response.send_message(f"No guild found with prefix: {prefix}", ephemeral=True)
            conn.close()
            return
        
        with shelve.open(self.giveawayFilePath) as db:
            key = str(interaction.guild_id) # Server ID
            db[key] = {
                'guildUUID': result[0], # Guild UUID
                'prefix': prefix, # Guild Prefix
                'configuredUserID': interaction.user.id,  # User who configured it's ID
                'configuredAt': time.time()  # Time submitted
            }

        await interaction.response.send_message(f"Successfully configured giveaway system for guild with prefix: {prefix}", ephemeral=True)
        conn.close()

    @giveawayCommands.command(name="roll", description="Roll for winners from the configured guild")
    @app_commands.describe(winners='The amount of winners for the giveaway.')
    async def roll(self, interaction: discord.Interaction, winners: int):
        requiredRoleName = "Giveaway Permission"
        #logger.info(f"interaction.user.roles: {interaction.user.roles}")
        permission = 0
        for role in interaction.user.roles:
            if role.name.lower() == requiredRoleName.lower():
                permission = 1
        if permission != 1:
            await interaction.response.send_message(f"You do not have the required role to use this command! If you are a server owner, create a role named '{requiredRoleName}', and give it to people who need to run this command.", ephemeral=True)
            return

        with shelve.open(self.giveawayFilePath) as db:
            key = str(interaction.guild_id)
            if key in db: # If user is in there
                config = db[key]
                guildUUID = config['guildUUID']
                
        logger.info(f"Command /giveaway roll was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}).")
        
        if not guildUUID:
            await interaction.response.send_message("Please configure a guild first using /giveaway configure", ephemeral=True)
            return
        
        success, r = makeRequest("https://api.wynncraft.com/v3/guild/uuid/"+str(guildUUID))
        if not success:
            logger.error("Error while getting request in /giveaway roll")
            await interaction.response.send_message("There was an error while getting data from the API. If this issue is persistent, please report it on my github.", ephemeral=True)
            return

        if r is None:
            logger.error(f"R is None in roll. Here is r: {r}.")
            return [["Unknown", "Unknown", 1738]]  # failed request, so just give a unknown. also ay.

        jsonData = r.json()
        members = []

        for rank in jsonData["members"]: # this iterates through every rank like chief, owner, etc
            if isinstance(jsonData["members"][rank], dict):
                for username, member_data in jsonData["members"][rank].items():
                    members.append([member_data['uuid'], username])
        members.sort(key=lambda x: x[1].lower()) # Sorts alphabetically by username, for easier searching.

        if not members:
            await interaction.response.send_message("No members found in the configured guild. This may be due to your guild not being included in the guild list.", ephemeral=True)
            return

        view = GuildSelectView(members, winners)
        total_pages = math.ceil(len(members) / 25)
        await interaction.response.send_message(
            f"Select players who have done their weekly (Page 1/{total_pages})\nCurrently selected: None\nNote: Only press submit once you are sure of every name. To remove a name, simply reselect them.", 
            view=view,
            ephemeral=True
        )

async def setup(bot):
    await bot.add_cog(giveaway(bot))