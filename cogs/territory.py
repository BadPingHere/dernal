import discord
from discord.ext import commands
from discord import app_commands
from lib.utils import checkCooldown, mapCreator, heatmapCreator, timeframeMap1
import logging
import asyncio

logger = logging.getLogger('discord')

@app_commands.allowed_installs(guilds=True, users=True)
@app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
class Territory(commands.GroupCog, name="territory"):
    def __init__(self, bot):
        self.bot = bot
        
    async def timeframeAutocomplete(self, interaction: discord.Interaction, current: str):
        keys = list(timeframeMap1.keys())
        return [app_commands.Choice(name=k, value=k)for k in keys if current.lower() in k.lower()][:25]
    
    @app_commands.command(description="Generates the current Wynncraft Territory Map.")
    async def map(self, interaction: discord.Interaction):
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 30)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        logger.info(f"Command /territory map was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}).")

        await interaction.response.defer()

        file, embed = await asyncio.to_thread(mapCreator)
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("An error occured while getting the territory map.")

    @app_commands.command(description="Generates the current Wynncraft Territory Heatmap.")
    @app_commands.describe(timeframe='The timeframe you wish to create a heatmap for.',)
    async def heatmap(self, interaction: discord.Interaction, timeframe: str):
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 30)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        logger.info(f"Command /territory heatmap was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}).")

        await interaction.response.defer()

        file, embed = await asyncio.to_thread(heatmapCreator, timeframe)
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("An error occured while getting the territory heatmap.")

    heatmap.autocomplete("timeframe")(timeframeAutocomplete)
async def setup(bot):
    await bot.add_cog(Territory(bot))