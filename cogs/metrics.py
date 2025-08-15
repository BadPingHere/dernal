from discord.ext import commands
from discord.ext.prometheus import PrometheusCog
from prometheus_client import start_http_server, Gauge
import psutil
import sqlite3
import time
import asyncio
import os

cpuUsagePercent = Gauge("cpu_usage_percent", "CPU usage percentage")
disk_usage_percent = Gauge("disk_usage_percent", "Disk usage percentage", ["mount"])
disk_total_bytes = Gauge("disk_total_bytes", "Total disk size in bytes", ["mount"])
disk_used_bytes = Gauge("disk_used_bytes", "Used disk space in bytes", ["mount"])
memory_usage_percent = Gauge("memory_usage_percent", "Memory usage percentage")
memory_total_bytes = Gauge("memory_total_bytes", "Total memory in bytes")
memory_used_bytes = Gauge("memory_used_bytes", "Used memory in bytes")
api_calls_count = Gauge("api_calls_count","Number of API calls in the previous full minute",["route"])

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

            conn = sqlite3.connect("database/api_usage.db")
            cursor = conn.cursor()
            cursor.execute("""
                SELECT route, SUM(count) 
                FROM api_usage 
                WHERE timestamp BETWEEN ? AND ? 
                GROUP BY route
            """, (start, end))

            results = dict(cursor.fetchall())
            conn.close()

            conn = sqlite3.connect("database/api_usage.db")
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT route FROM api_usage")
            all_routes = [r[0] for r in cursor.fetchall()]
            conn.close()
            for label in list(api_calls_count._metrics.keys()):
                api_calls_count.remove(*label)
            for route in all_routes:
                api_calls_count.labels(route=route).set(results.get(route, 0))
        except Exception as e:
            print(f"Error updating API metrics: {e}")

async def setup(bot):
    await bot.add_cog(PrometheusCog(bot))
    await bot.add_cog(Metrics(bot))
