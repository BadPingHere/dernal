import discord
from discord.ext import commands
from discord import app_commands
from lib.utils import mapCreator, heatmapCreator, timeframeMap
import logging
import asyncio

logger = logging.getLogger('discord')

@app_commands.allowed_installs(guilds=True, users=True)
@app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
class Territory(commands.GroupCog, name="territory"):
    def __init__(self, bot):
        self.bot = bot
        
    async def timeframeAutocomplete(self, interaction: discord.Interaction, current: str):
        return [app_commands.Choice(name=k, value=k)for k in timeframeMap() if current.lower() in k.lower()][:25]
    
    @app_commands.command(description="Generates the current Wynncraft Territory Map.")
    @app_commands.describe(map_type='The type of map you wish to see:',)
    @app_commands.choices(map_type=[
        app_commands.Choice(name="Normal Map", value="map"),
        app_commands.Choice(name="Defenses", value="defense"),
        app_commands.Choice(name="Treasury", value="treasury"),
    ])
    async def map(self, interaction: discord.Interaction, map_type: app_commands.Choice[str]):
        logger.info(f"Command /territory map was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Map_type is {map_type.value}.")

        await interaction.response.defer()

        file, view = await asyncio.to_thread(mapCreator, map_type.value)
        if file and view:
            await interaction.followup.send(file=file, view=view)
        else:
            await interaction.followup.send("An error occured while getting the territory map.")

    @app_commands.command(description="Generates the current Wynncraft Territory Heatmap.")
    @app_commands.describe(timeframe='The timeframe you wish to create a heatmap for.',)
    async def heatmap(self, interaction: discord.Interaction, timeframe: str): #TODO: Make heatmap use new database
        logger.info(f"Command /territory heatmap was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). Timeframe is {timeframe}.")

        await interaction.response.defer()

        file, view = await asyncio.to_thread(heatmapCreator, timeframe)
        if file and view:
            await interaction.followup.send(file=file, view=view)
        else:
            await interaction.followup.send("An error occured while getting the territory heatmap.")

    heatmap.autocomplete("timeframe")(timeframeAutocomplete)
async def setup(bot):
    await bot.add_cog(Territory(bot))