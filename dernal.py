import discord
from discord.ext import commands
import logging
import logging.handlers
import asyncio
import os
from dotenv import load_dotenv
import platform
import requests
import json

# Set up logging
logger = logging.getLogger('discord')
logger.setLevel(logging.INFO)
handler = logging.handlers.RotatingFileHandler(
    filename='discord.log',
    encoding='utf-8',
    maxBytes=128 * 1024 * 1024,  # 128 MiB
)
dt_fmt = '%Y-%m-%d %H:%M:%S'
formatter = logging.Formatter('{asctime} - {levelname:<8} - {name}: {message}', dt_fmt, style='{')
handler.setFormatter(formatter)
logger.addHandler(handler)

intents = discord.Intents.default()
bot = commands.Bot(command_prefix=commands.when_mentioned, intents=intents) # i learned recently that this means people can run commands with @dernal command, and you can send me errors with @dernal message!

# TODO:
#? 1. Improve performance on database commands.
#! 2. Add front-facing errors to ALL commands, no command should time out because of either a user or bot error.
#! 3. Allow guilds to change the hardcoded weightings of giveaways.
#! 4. Fix Inactivity and other long commands that can have too much chars

def checkUpdates(localVersionFile='version.json'):
    try:
        releasesUrl = f"https://api.github.com/repos/badpinghere/dernal/releases/latest"
        commitsUrl = f"https://api.github.com/repos/badpinghere/dernal/commits/main"
        try:
            release_response = requests.get(releasesUrl)
            release_data = release_response.json()
            latestVersion = release_data.get('tag_name', 'unknown')
            releaseUrl = release_data.get('html_url', '')
        except:
            commits_response = requests.get(commitsUrl)
            commits_data = commits_response.json()
            latestVersion = commits_data.get('sha', 'unknown')[:7] 
            releaseUrl = f"https://github.com/badpinghere/dernal/commits/main"
        
        # Check local version
        if not os.path.exists(localVersionFile):
            with open(localVersionFile, 'w') as f:
                json.dump({"version": latestVersion}, f)
            return {
                "is_up_to_date": False,
                "current_version": "Unknown",
                "latest_version": latestVersion,
                "update_available": True,
                "update_url": releaseUrl
            }
        with open(localVersionFile, 'r') as f:
            localData = json.load(f)
        
        currentVersion = localData.get('version', 'unknown')
        updateAvailable = currentVersion != latestVersion
        
        return {
            "is_up_to_date": not updateAvailable,
            "current_version": currentVersion,
            "latest_version": latestVersion,
            "update_available": updateAvailable,
            "update_url": releaseUrl
        }
    
    except Exception as e:
        return {
            "is_up_to_date": True,
            "error": str(e),
            "message": "Could not check for updates"
        }

@bot.event
async def on_ready():
    logger.info(f'Logged in as {bot.user} (ID: {bot.user.id})')
    logger.info('Syncing bot commands, this may take some time.')

    #bot.tree.clear_commands(guild=None) 
    await bot.tree.sync(guild=None)
    logger.info("Command Tree is synced to global!")
    guild_id = os.getenv("SERVER_ID")
    if guild_id: # if server_id is present, faster syncs. mainly for development, but end user can use it too ig.
        guild = discord.Object(id=guild_id) 
        #bot.tree.clear_commands(guild=guild) # this is legit only for one reason. because discord bots suck.
        await bot.tree.sync(guild=guild)
        logger.info(f"Command Tree is synced to your server with id {os.getenv('SERVER_ID')}!")

    logger.info('------')
    update_info = checkUpdates()
    if update_info.get('update_available', False):
        logger.info(f"Update available! Current version: {update_info['current_version']}, "
              f"Latest version: {update_info['latest_version']}")
        logger.info(f"Update URL: {update_info['update_url']}")
    else:
        logger.info("Dernal is up to date!")
    logger.info(f"Logged in as {bot.user.name}")
    logger.info(f"Discord.py version: {discord.__version__}")
    logger.info(f"Python version: {platform.python_version()}")
    logger.info(f"Running on: {platform.system()} {platform.release()} ({os.name})")
    logger.info("-------------------")

async def load_cogs():
    logger.info("Loading bot cogs:")
    for filename in os.listdir('./cogs'): # gets commands (cogs(why is it called cogs))
        if filename.endswith('.py'):
            try:
                await bot.load_extension(f'cogs.{filename[:-3]}')
                logger.info(f'Loaded Cog: {filename[:-3]}')
            except Exception as e:
                logger.error(f'Failed to load Cog {filename[:-3]}')
                logger.error(f'Error: {str(e)}')

async def main(): # idk why i couldnt explain it? maybe it was my past code but like this is not unexplainable
    async with bot:
        await load_cogs()
        load_dotenv()
        await bot.start(os.getenv("TOKEN"))

if __name__ == "__main__":
    asyncio.run(main())