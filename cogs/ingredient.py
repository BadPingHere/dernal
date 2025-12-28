import discord
from discord.ext import commands
from discord import app_commands
from lib.utils import checkCooldown, ingredientMapCreator
import logging
import asyncio
from typing import Optional, Annotated

logger = logging.getLogger('discord')

@app_commands.allowed_installs(guilds=True, users=True)
@app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
class Territory(commands.GroupCog, name="ingredient"):
    def __init__(self, bot):
        self.bot = bot
    async def tierAutocomplete(self, interaction: discord.Interaction, current: str):
        keys = ["0", "1", "2", "3"]
        return [app_commands.Choice(name=k, value=k)for k in keys if current.lower() in k.lower()][:25]
    @app_commands.command(description="Generates a map for mob spawns that drop ingredients.")
    @app_commands.describe(ingredient='The ingredient you wish to search for.',)
    @app_commands.describe(price='The price floor of ingredients you wish to search for in EB.',)
    @app_commands.describe(tier='The tier of ingredient to search for.',)
    async def map(self, interaction: discord.Interaction, ingredient: Optional[str], price: Optional[int], tier: Optional[int]):
        response = await asyncio.to_thread(checkCooldown, interaction.user.id, 10)
        #logger.info(response)
        if response != True: # If not true, there is cooldown, we dont run it!!!
            await interaction.response.send_message(f"Due to a cooldown, we cannot process this request. Please try again after {response} more seconds.",ephemeral=True)
            return
        logger.info(f"Command /ingredient map was ran in server {interaction.guild_id} by user {interaction.user.name}({interaction.user.id}). ingredient is {ingredient}, price is {price}, tier is {tier}.")

        await interaction.response.defer()

        file, embed = await asyncio.to_thread(ingredientMapCreator, ingredient, price, tier)
        if file and embed:
            await interaction.followup.send(file=file, embed=embed)
        else:
            await interaction.followup.send("An error occured while getting the ingredient map.")
            
    map.autocomplete("tier")(tierAutocomplete)

async def setup(bot):
    await bot.add_cog(Territory(bot))