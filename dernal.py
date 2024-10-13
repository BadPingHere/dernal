import discord
from discord.ext import commands
import logging
import logging.handlers
import asyncio
import os
from dotenv import load_dotenv
import platform

# Set up logging
logger = logging.getLogger('discord')
logger.setLevel(logging.INFO)
handler = logging.handlers.RotatingFileHandler(
    filename='discord.log',
    encoding='utf-8',
    maxBytes=32 * 1024 * 1024,  # 32 MiB
    backupCount=5,  # Rotate through 5 files
)
dt_fmt = '%Y-%m-%d %H:%M:%S'
formatter = logging.Formatter('[{asctime}] [{levelname:<8}] {name}: {message}', dt_fmt, style='{')
handler.setFormatter(formatter)
logger.addHandler(handler)

intents = discord.Intents.default()
bot = commands.Bot(command_prefix=None, intents=intents)

# TODO: slash command that shows the territory history of your guild, sum like 'August 9th: 🔴 X was taken by SEQ newline here  🟢 X was taken from SEQ'
# TODO: store data on all guilds, and like have stats on them (Daily active users, Wars won, etc) available from a slash command 

@bot.event
async def on_ready():
    print("\n")
    print(f'Logged in as {bot.user} (ID: {bot.user.id})')
    print('Syncing bot commands, this may take some time.')

    #bot.tree.clear_commands(guild=None) 
    await bot.tree.sync(guild=None)
    print("Command Tree is synced to global!")
    guild_id = os.getenv("SERVER_ID")
    if guild_id: # if server_id is present, faster syncs. mainly for development, but end user can use it too ig.
        guild = discord.Object(id=guild_id) 
        #bot.tree.clear_commands(guild=guild) # this is legit only for one reason. because discord bots suck.
        await bot.tree.sync(guild=guild)
        print(f"Command Tree is synced to your server with id {os.getenv("SERVER_ID")}!")

    print('------')
    logger.info(f"Logged in as {bot.user.name}")
    logger.info(f"Discord.py version: {discord.__version__}")
    logger.info(f"Python version: {platform.python_version()}")
    logger.info(
        f"Running on: {platform.system()} {platform.release()} ({os.name})"
    )
    logger.info("-------------------")

async def load_cogs():
    print("Loading bot cogs:")
    for filename in os.listdir('./cogs'): # gets commands (cogs(why is it called cogs))
        if filename.endswith('.py'):
            try:
                await bot.load_extension(f'cogs.{filename[:-3]}')
                print(f'Loaded Cog: {filename[:-3]}')
            except Exception as e:
                print(f'Failed to load Cog {filename[:-3]}')
                print(f'Error: {str(e)}')

async def main(): # couldnt explain whats happening around here. at all.
    async with bot:
        await load_cogs()
        load_dotenv()
        await bot.start(os.getenv("TOKEN"))

if __name__ == "__main__":
    asyncio.run(main())