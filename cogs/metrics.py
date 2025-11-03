from discord.ext import commands
from discord.ext.prometheus import PrometheusCog
from discord import Interaction
from prometheus_client import start_http_server, Gauge, Counter, Histogram
import psutil
import sqlite3
import time
import logging
import asyncio
import os
from collections import deque
logger = logging.getLogger('discord')


cpuUsagePercent = Gauge("cpu_usage_percent", "CPU usage percentage")
disk_usage_percent = Gauge("disk_usage_percent", "Disk usage percentage", ["mount"])
disk_total_bytes = Gauge("disk_total_bytes", "Total disk size in bytes", ["mount"])
disk_used_bytes = Gauge("disk_used_bytes", "Used disk space in bytes", ["mount"])
memory_usage_percent = Gauge("memory_usage_percent", "Memory usage percentage")
memory_total_bytes = Gauge("memory_total_bytes", "Total memory in bytes")
memory_used_bytes = Gauge("memory_used_bytes", "Used memory in bytes")
api_calls_count = Gauge("api_calls_count","Number of API calls in the previous full minute",["route"])


unique_users_total = Gauge("unique_users_total", "Total number of unique users who have used the bot")
unique_users_recent = Gauge("unique_users_recent", "Unique users in the last 30 days")
guild_changes_gauge = Gauge("guild_changes", "Recent guild joins/leaves", ["event", "guild_name"])
command_executions = Counter("command_executions_total", "Total commands executed", ["command_name"])
command_executions_guild = Counter("command_executions_by_guild_total", "Commands executed per guild", ["guild_id", "guild_name"])
command_failures = Counter("command_failures_total", "Total command failures", ["command_name"])
command_successes = Counter("command_successes_total", "Total successful commands", ["command_name"])
command_latency = Histogram("command_duration_seconds", "Command execution duration in seconds", ["command_name"])

recent_guild_changes = deque(maxlen=10)

# Sections:

# General:
#* Total unique users
#* total guilds
#* total commands
#* uptime 
#* online status
#* discord latency
#* scrape duration
#* total users over time
#* cpu usage %
#* disk usage %
#* guilds over time
#? Guilds who add/remove bot, and itll be a list of the last 10 in grafana

# Commands Info:
#! Command Count (Per the timeframe)
#* Api Usage
#! Command failure rates (a list of the most recent commands that failed)
#! Command failure rates (for the amount of times its been ran, how many times has it failed %)
#! Time it takes for a command to finish (histogram)
#! Guild with most commands ran for given timeframe
#

# Individual server stats:
#! If possible I want to have a like procedually generated kind of visulization of every guild with general stats on all of them, like server icon, commands usage stats, total members, etc in grafana.



class Metrics(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        start_http_server(8000)  # Prometheus endpoint
        self.bot.loop.create_task(self.metric_loop())

    async def metric_loop(self):
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            await self.update_system_metrics()
            await self.update_api_metrics()
            await self.update_unique_users()
            await asyncio.sleep(60)

    async def update_system_metrics(self):
        cpuUsagePercent.set(psutil.cpu_percent())

        mem = psutil.virtual_memory()
        memory_usage_percent.set(mem.percent)
        memory_total_bytes.set(mem.total)
        memory_used_bytes.set(mem.used)

        botPath = os.path.abspath(__file__)
        mountpoint = next((part.mountpoint for part in psutil.disk_partitions() if botPath.startswith(part.mountpoint)),"/")
        usage = psutil.disk_usage(mountpoint)
        disk_usage_percent.labels(mount=mountpoint).set(usage.percent)
        disk_total_bytes.labels(mount=mountpoint).set(usage.total)
        disk_used_bytes.labels(mount=mountpoint).set(usage.used)

    async def update_api_metrics(self):
        try:
            now = int(time.time())
            end = now - (now % 60) - 1
            start = end - 59

            conn = sqlite3.connect("database/metrics.db")
            cursor = conn.cursor()
            cursor.execute("""
                SELECT route, SUM(count) 
                FROM api_usage 
                WHERE timestamp BETWEEN ? AND ? 
                GROUP BY route
            """, (start, end))

            results = dict(cursor.fetchall())

            cursor.execute("SELECT DISTINCT route FROM api_usage")
            all_routes = [r[0] for r in cursor.fetchall()]
            conn.close()
            
            for label in list(api_calls_count._metrics.keys()):
                api_calls_count.remove(*label)
            for route in all_routes:
                api_calls_count.labels(route=route).set(results.get(route, 0))
                
        except Exception as e:
            logger.info(f"Error updating API metrics: {e}")
            
    async def update_unique_users(self):
        try:
            conn = sqlite3.connect("database/metrics.db")
            cursor = conn.cursor()
            # Ensure tables exist
            cursor.execute("CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY)")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_activity (
                    user_id INTEGER,
                    timestamp INTEGER
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_activity_ts ON user_activity (timestamp)")

            # Get total unique users
            count_total = cursor.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            unique_users_total.set(count_total)

            # Get unique users in last 30 days
            now = int(time.time())
            thirty_days_ago = now - (30 * 24 * 60 * 60)
            count_recent = cursor.execute("""
                SELECT COUNT(DISTINCT user_id)
                FROM user_activity
                WHERE timestamp >= ?
            """, (thirty_days_ago,)).fetchone()[0]
            unique_users_recent.set(count_recent)

            conn.close()
        except Exception as e:
            logger.info(f"Error updating unique user metrics: {e}")


    @commands.Cog.listener()
    async def on_app_command_completion(self, interaction: Interaction, command):
        try:
            user_id = interaction.user.id
            guild = interaction.guild
            command_name = command.qualified_name if hasattr(command, "qualified_name") else command.name

            # Measure latency
            duration = time.time() - interaction.created_at.timestamp()
            command_latency.labels(command_name=command_name).observe(duration)

            # Increment counters
            command_executions.labels(command_name=command_name).inc()
            command_successes.labels(command_name=command_name).inc()
            if guild:
                command_executions_guild.labels(guild_id=guild.id, guild_name=guild.name).inc()

            # Record in DB
            conn = sqlite3.connect("database/metrics.db")
            cursor = conn.cursor()
            cursor.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
            cursor.execute("INSERT INTO user_activity (user_id, timestamp) VALUES (?, ?)", (user_id, int(time.time())))
            conn.commit()
            conn.close()

            #logger.info(f"[Metrics] Recorded slash command: {command_name} by {interaction.user}")
        except Exception as e:
            logger.info(f"Error in on_app_command_completion: {e}")

    @commands.Cog.listener()
    async def on_app_command_error(self, interaction: Interaction, command, error):
        try:
            command_name = command.qualified_name if hasattr(command, "qualified_name") else command.name
            command_failures.labels(command_name=command_name).inc()
            #logger.info(f"[Metrics] Error in slash command {command_name}: {error}")
        except Exception as e:
            logger.info(f"Failed to record command error: {e}")

    @commands.Cog.listener()
    async def on_guild_join(self, guild):
        recent_guild_changes.append(("join", guild.name))
        guild_changes_gauge.labels(event="join", guild_name=guild.name).inc()
        #logger.info(f"[Metrics] Guild joined: {guild.name}")

    @commands.Cog.listener()
    async def on_guild_remove(self, guild):
        recent_guild_changes.append(("remove", guild.name))
        guild_changes_gauge.labels(event="remove", guild_name=guild.name).inc()
        #logger.info(f"[Metrics] Guild removed: {guild.name}")

async def setup(bot):
    await bot.add_cog(PrometheusCog(bot))
    await bot.add_cog(Metrics(bot))
