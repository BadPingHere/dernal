import discord
from discord.ext import commands
from discord import app_commands
from discord.ui import View, Button
import time
from lib.utils import makeRequest, guildLookup, lookupGuild

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
            return "\n".join([f"**{username.replace('_', '\\_')}** - Last online <t:{timestamp}:F>." for username, timestamp in users])
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

    @app_commands.command(description="Shows overall stats and information about the selected guild")
    @app_commands.describe(
        name='Prefix or Name of the guild search Ex: TAq, Calvish.',
    )
    async def lookups(self, interaction: discord.Interaction, name: str):
        current_time = time.time()
        if int(current_time - self.guildLookupCooldown) <= 5:
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {current_time - self.guildLookupCooldown} seconds.", ephemeral=True)
            return

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

        self.guildLookupCooldown = current_time
    
    @app_commands.command(description="Shows and sorts the player inactivity of a selected guild")
    @app_commands.describe(
        name='Prefix or Name of the guild search Ex: TAq, Calvish.',
    )
    async def inactivity(self, interaction: discord.Interaction, name: str):
        current_time = time.time()
        if int(current_time - self.guildLookupCooldown) <= 5:
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {current_time - self.guildLookupCooldown} seconds.", ephemeral=True)
            return

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
        self.guildLookupCooldown = current_time

async def setup(bot):
    await bot.add_cog(Guild(bot))