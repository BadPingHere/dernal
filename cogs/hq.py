import discord
from discord.ext import commands
from discord import app_commands
from discord.ui import View, Button
from typing import Optional
from lib.utils import getTerritoryNames
from lib.makeRequest import makeRequest
import logging
import asyncio
from datetime import datetime, timezone

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

        leaderboardDesc = "```\n{:<3} {:<20} {:<25}\n".format("#", "Territory", f"{self.shorthandTitle}")
        separator = "-" * 56 + "\n"
        leaderboardDesc += separator
        for i, (name, value) in enumerate(page_data, start=start + 1):
            #logger.info(f"value: {value}")
            leaderboardDesc += "{:<3} {:<20} {:<25}\n".format(i, name, value)
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


class HQ(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.allowed_installs(guilds=True, users=True)
    @app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)  
    @app_commands.command(description="Outputs the top hq locations.")
    @app_commands.describe(
        guild='Prefix of the guild Ex: TAq, ICo.',
    )
    async def hq(self, interaction: discord.Interaction, guild: Optional[str]):
        logger.info(f"Command /hq was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Parameter guild is: {guild}.")
        URL = "https://api.wynncraft.com/v3/guild/list/territory"
        success, r = await asyncio.to_thread(makeRequest, URL)
        if not success:
            logger.error("Error while getting request in /hq")
            await interaction.response.send_message("There was an error while getting data from the API. If this issue is persistent, please report it on my github.", ephemeral=True)
            return
        if guild: 
            # Check if they own territory before even running it
            if guild.lower() not in str(r.json()).lower():
                await interaction.response.send_message(f"The guild you inputted does not own any territories. If this is incorrect, check if the prefix is exactly right, including capitals, and if it is, report this bug.", ephemeral=True)
            else:
                untainteddata = r.json()
                data = await asyncio.to_thread(getTerritoryNames, untainteddata, guild)
        else:
            untainteddata = r.json()
            data = await asyncio.to_thread(getTerritoryNames, untainteddata, None)

        if data:
            view = LeaderboardPaginator(data, f"Top {len(data)} Territories by HQ Strength - {guild if guild else 'Global'}", "Strength")
            await interaction.response.send_message(embed=view.get_embed(), view=view)
        else:
            await interaction.response.send_message("No data available.")

async def setup(bot):
    await bot.add_cog(HQ(bot))