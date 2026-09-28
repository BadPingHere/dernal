import asyncio
from datetime import date, datetime, timedelta, timezone
import json
import logging
import math
import os
import secrets
import time
from urllib.parse import quote, urlencode, urlsplit
from weakref import WeakSet
import discord
from aiohttp import ClientError, ClientSession, ClientTimeout

MAX_POINTS = 50_000
MAX_CATEGORIES = 200
MAX_VALUE = 2**53 - 1
MAX_EVENTS = 18000
MAX_PAYLOAD_BYTES = 1_800_000

def chart_milliseconds(value):
    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def number(value):
    if value is None:
        return None
    value = float(value)
    if not math.isfinite(value) or value < 0 or value > MAX_VALUE:
        raise ValueError("Activity values must be finite, nonnegative, and exactly representable.")
    return int(value) if value.is_integer() else value


def plot_data(x, y, chart_type, title, metric, *, name, theme,
              start=None, end=None, slice_labels=None):
    if len(x) != len(y) or not len(x) or len(x) > MAX_POINTS:
        raise ValueError("Activity graphs support between 1 and 50,000 data points.")
    result = {
        "name": name, "theme": theme or "light", "chart_type": chart_type,
        "title": title, "metric": metric or "Count",
    }
    values = [number(value) for value in y]
    if chart_type == "pie":
        labels = slice_labels if slice_labels is not None else x
        if len(labels) != len(values) or len(labels) > MAX_CATEGORIES:
            raise ValueError("Activity pie charts support at most 200 categories.")
        result.update({
            "pie_mode": "categories", "points": [], "start": None, "end": None,
            "distribution": [{"name": str(label), "value": value} for label, value in zip(labels, values) if value is not None and value > 0],
        })
        if not result["distribution"]:
            raise ValueError("No values available for the pie chart.")
        return result

    points = []
    for timestamp, value in zip(x, values):
        if chart_type == "bar" and start is not None and end is not None:
            day = timestamp.date() if isinstance(timestamp, datetime) else timestamp
            bucket = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
            left, right = max(start, bucket), min(end, bucket + timedelta(days=1))
            timestamp = left + (right - left) / 2
        points.append({"x": chart_milliseconds(timestamp), "y": value})
    result.update({
        "points": points,
        "start": chart_milliseconds(start) if start is not None else points[0]["x"],
        "end": chart_milliseconds(end) if end is not None else points[-1]["x"] + 1,
    })
    return result

def heat_range(timeframe, timeframes):
    end = datetime.now(timezone.utc)
    if timeframe == "Everything":
        return None, end
    if timeframe == "Last 7 Days":
        return end - timedelta(days=7), end
    if timeframe not in timeframes:
        raise ValueError("Choose a timeframe from autocomplete.")
    start, end = timeframes[timeframe]
    return (datetime.strptime(start, "%m/%d/%y").replace(tzinfo=timezone.utc),
            datetime.strptime(end, "%m/%d/%y").replace(tzinfo=timezone.utc) + timedelta(days=1))


def ingredient_data(ingredients, coordinates, rarities, prices):
    result = []
    for name, mobs in ingredients.items():
        spawns = []
        seen = set()
        for mob in mobs:
            for x, z, radius in coordinates.get(mob, []):
                key = (mob, x, z, radius)
                if key in seen:
                    continue
                seen.add(key)
                spawns.append({"mob": mob, "x": x, "z": z, "radius": max(5, radius)})
        if spawns:
            result.append({"name": name, "tier": rarities.get(name),
                           "price": prices.get(name), "spawns": spawns})
    return result


def utc_time(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def timeline_milliseconds(value):
    return math.ceil(utc_time(value).timestamp() * 1000)


def timeline_range(start, end, first, cutoff):
    first, cutoff = utc_time(first), utc_time(cutoff)
    end = utc_time(end) if end else cutoff
    start = utc_time(start) if start else max(first, end - timedelta(days=1))
    if start < first or end > cutoff or start > end:
        raise ValueError(f"Choose a range between {first.isoformat()} and {cutoff.isoformat()} (UTC), with start <= end.")
    return start, end


def build_timeline(geometry, captures, observations, changes, start, end, cutoff, first):
    guilds, indexes = [], {}

    def guild_index(row):
        key = str(row["guild_uuid"]) if row.get("guild_uuid") else None
        if key not in indexes:
            indexes[key] = len(guilds)
            guilds.append({"name": row.get("guild_name") or (f"Unknown guild ({key[:8]})" if key else None),
                           "prefix": row.get("guild_prefix") or ""})
        return indexes[key]
    territories = {name: {"location": item.get("location", item.get("Location")),
                          "links": item.get("links", item.get("Trading Routes", [])),
                          "guild": None, "acquired": None, "hq": None, "defences": None,
                          "treasury": None, "resources": [], "history_known": False}
                   for name, item in geometry.items()}
    seeds = {}
    for row in captures:
        name = row["territory"]
        if name not in territories:
            continue
        seeds[name] = row
        index = guild_index(row)
        territories[name].update(guild=guilds[index], acquired=utc_time(row["acquired"]).isoformat(),
                                 history_known=True)
    for row in observations:
        seed = seeds.get(row["territory"])
        if seed and seed["acquired"] == row["acquired"] and seed.get("guild_uuid") == row.get("guild_uuid"):
            territories[row["territory"]].update(hq=row.get("is_hq"), defences=row.get("defense"))
    events, missing = [], set()
    start_ms, end_ms = timeline_milliseconds(start), timeline_milliseconds(end)
    for row in changes:
        name, acquired = row["territory"], timeline_milliseconds(row["acquired"])
        if name not in territories:
            missing.add(name)
            continue
        if row["is_capture"] and start_ms < acquired <= end_ms:
            events.append([acquired, name, guild_index(row), acquired, None, None])
        observed = timeline_milliseconds(row["observed_at"])
        if start_ms < observed <= end_ms and observed >= acquired:
            events.append([observed, name, -1, acquired, row.get("is_hq"), row.get("defense")])

    events.sort(key=lambda e: (e[0], e[2] == -1, e[3]))
    if len(events) > MAX_EVENTS:
        raise ValueError("This hour contains too many recorded changes to load. Try another time.")
    payload = {"chart_type": "map", "map_mode": "timeline", "name": "Historical territory map",
               "theme": "discord", "territories": territories, "ingredients": [],
               "timeline": {"start": timeline_milliseconds(first), "end": timeline_milliseconds(cutoff),
                            "window_start": start_ms, "window_end": end_ms,
                            "guilds": guilds, "events": events,
                            "missing_geometry": len(missing)}}
    if len(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")) > MAX_PAYLOAD_BYTES:
        raise ValueError("This hour exceeds the replay upload limit. Try another time.")
    return payload


async def load_timeline(fetch, geometry, at=None, cutoff=None):
    now = datetime.now(timezone.utc)
    cutoff = utc_time(cutoff) if cutoff else now
    cutoff = cutoff.replace(microsecond=(cutoff.microsecond // 1000) * 1000)
    if cutoff > now:
        raise ValueError("The snapshot cutoff cannot be in the future.")
    bounds = await fetch("SELECT min(acquired) AS first FROM ts.territory_ownership WHERE is_capture AND observed_at <= %s", (cutoff,))
    first = bounds[0]["first"] if bounds else None
    if first is None:
        raise ValueError("No historical territory captures are available yet.")

    first = datetime.fromtimestamp(timeline_milliseconds(first) / 1000, timezone.utc)
    at = utc_time(at) if at else cutoff
    timeline_range(at, at, first, cutoff)
    start = max(utc_time(first), at.replace(minute=0, second=0, microsecond=0))
    end = min(start.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1), cutoff)
    columns = "o.territory, o.acquired, o.observed_at, o.guild_uuid, o.is_hq, o.defense, o.is_capture, g.name AS guild_name, g.prefix AS guild_prefix"
    source = "ts.territory_ownership o LEFT JOIN core.guilds g ON g.guild_uuid = o.guild_uuid"
    captures = await fetch("""SELECT t.territory, c.acquired, c.guild_uuid, c.observed_at,
        m.is_hq, m.defense, g.name AS guild_name, g.prefix AS guild_prefix
        FROM unnest(%s::text[]) AS t(territory)
        JOIN LATERAL (
            SELECT acquired, guild_uuid, observed_at FROM ts.territory_ownership o
            WHERE o.territory = t.territory AND o.is_capture
              AND o.acquired <= %s AND o.observed_at <= %s
            ORDER BY o.acquired DESC, o.observed_at DESC LIMIT 1
        ) c ON true
        LEFT JOIN LATERAL (
            SELECT is_hq, defense FROM ts.territory_ownership o
            WHERE o.territory = t.territory AND o.acquired = c.acquired
              AND o.guild_uuid IS NOT DISTINCT FROM c.guild_uuid AND o.observed_at <= %s
            ORDER BY o.observed_at DESC LIMIT 1
        ) m ON true
        LEFT JOIN core.guilds g ON g.guild_uuid = c.guild_uuid
        """, (list(geometry), start, cutoff, start))
    changes = await fetch(f"""SELECT {columns} FROM {source}
        WHERE o.observed_at <= %s AND (
          (o.is_capture AND o.acquired > %s AND o.acquired <= %s)
          OR (o.observed_at > %s AND o.observed_at <= %s))
        ORDER BY o.observed_at, o.acquired LIMIT %s""", (cutoff, start, end, start, end, MAX_EVENTS + 1))
    if len(changes) > MAX_EVENTS:
        raise ValueError("This hour contains too many recorded changes to load. Try another time.")
    payload = build_timeline(geometry, captures, captures, changes, start, end, cutoff, first)
    payload["timeline"]["initial"] = timeline_milliseconds(at)
    return payload

logger = logging.getLogger("discord.activity")
GRAPH_TTL = 86400
MAX_GRAPHS = 1000


class WorkerClient:
    def __init__(self, url, token):
        parsed = urlsplit(url)
        local = parsed.hostname in {"127.0.0.1", "localhost", "::1"}
        if (not parsed.hostname or parsed.username or parsed.password
                or parsed.query or parsed.fragment or parsed.path not in {"", "/"}
                or (parsed.scheme != "https" and not (local and parsed.scheme == "http"))):
            raise ValueError("ACTIVITY_WORKER_URL must be an HTTPS origin, without a path or credentials.")
        if len(token) < 32 or len(token) > 256 or not token.isascii() or any(c.isspace() for c in token):
            raise ValueError("ACTIVITY_UPLOAD_TOKEN must contain 32–256 ASCII characters without spaces.")
        self.url = url.rstrip("/")
        self.token = token
        self._graphs = {}
        self.session = None

    async def start(self):
        self.session = ClientSession(timeout=ClientTimeout(total=12))

    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None
        self._graphs.clear()

    def _prune(self):
        now = time.monotonic()
        self._graphs = {key: expiry for key, expiry in self._graphs.items() if expiry > now}

    def has_graph(self, graph_id):
        self._prune()
        return graph_id in self._graphs

    async def put(self, path, data):
        body = json.dumps(data, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        if len(body) > 1_900_000:
            raise ValueError("This graph exceeds the upload limit. Choose a shorter timeframe.")
        for attempt in range(3):
            try:
                async with self.session.put(
                    self.url + path, data=body, allow_redirects=False,
                    headers={"Authorization": "Bearer " + self.token, "Content-Type": "application/json"},
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    if response.status < 500 and response.status != 429:
                        raise ValueError(
                            f"Activity Worker rejected the upload (HTTP {response.status}). "
                            "Check ACTIVITY_WORKER_URL and ACTIVITY_UPLOAD_TOKEN; try a shorter range."
                        )
            except (ClientError, asyncio.TimeoutError):
                pass
            if attempt < 2:
                await asyncio.sleep(0.5 * (attempt + 1))
        raise ValueError("The Activity Worker is unavailable or at its request limit. Try again later.")

    async def publish_graph(self, data, application_id):
        graph_id = secrets.token_urlsafe(24)
        remote = await self.put("/api/graphs/" + graph_id, {
            **data, "chart_type": data.get("chart_type", "line"),
            "id": graph_id, "application_id": str(application_id),
        })
        self._prune()
        while len(self._graphs) >= MAX_GRAPHS:
            del self._graphs[next(iter(self._graphs))]
        remaining = min(GRAPH_TTL, max(0, remote["expires_at"] - time.time()))
        self._graphs[graph_id] = time.monotonic() + remaining
        return graph_id

    async def attach_instance(self, instance_id, graph_id):
        await self.put("/api/instances/" + quote(instance_id, safe=""), {"graph_id": graph_id})


async def start_activity_worker(bot):
    bot.activity_client = None
    bot.activity_views = WeakSet()
    url = os.getenv("ACTIVITY_WORKER_URL", "").strip()
    token = os.getenv("ACTIVITY_UPLOAD_TOKEN", "").strip()
    if not url or not token:
        logger.info("Interactive Activities disabled. To enable configure both ACTIVITY_WORKER_URL and ACTIVITY_UPLOAD_TOKEN in the .env file.")
        return
    client = None
    try:
        client = WorkerClient(url, token)
        await client.start()
        bot.activity_client = client
        logger.info("Interactive Activities enabled using the hosted Cloudflare Worker.")
    except Exception:
        if client is not None:
            await client.close()
        logger.exception("Could not initialize the Activity Worker; image commands remain available.")


async def close_activity_worker(bot):
    for view in list(getattr(bot, "activity_views", ())):
        view.stop()
    client = getattr(bot, "activity_client", None)
    bot.activity_client = None
    if client is not None:
        await client.close()


def activity_enabled(interaction):
    return getattr(interaction.client, "activity_client", None) is not None


class ExploreButton(discord.ui.Button):
    def __init__(self, server, graph_id, label="Explore graph"):
        super().__init__(label=label, style=discord.ButtonStyle.primary)
        self.server = server
        self.graph_id = graph_id
        self.subject = "map" if label == "Explore map" else "graph"

    async def callback(self, interaction):
        if not self.server.has_graph(self.graph_id):
            await interaction.response.send_message(
                f"This {self.subject} has expired. Please run the command again.",
                ephemeral=True,
            )
            return
        try:
            response = await interaction.response.launch_activity()
            instance_id = response.activity_id or getattr(getattr(response, "resource", None), "id", None)
            if not instance_id:
                raise RuntimeError("Discord did not return an Activity instance ID")
            await self.server.attach_instance(str(instance_id), self.graph_id)
        except Exception:
            logger.exception("Could not launch or attach the graph")
            message = "Could not open the graph."
            if interaction.response.is_done():
                await interaction.followup.send(message, ephemeral=True)
            else:
                await interaction.response.send_message(message, ephemeral=True)


async def send_activity_result(interaction, file, preview, chart_type=None):
    try:
        client = getattr(interaction.client, "activity_client", None)
        graph = getattr(preview, "activity_graph", None)
        if client is not None and graph:
            try:
                graph = dict(graph)
                if chart_type is not None:
                    graph["chart_type"] = chart_type
                    if chart_type == "pie" and graph.get("pie_mode") != "categories":
                        graph["title"] = "Online Member Distribution - " + graph["name"]
                        graph["metric"] = "Online members"
                graph_id = await client.publish_graph(graph, interaction.application_id)
                children = list(preview.children)
                preview.clear_items()
                preview = discord.ui.LayoutView(timeout=GRAPH_TTL)
                for item in children:
                    preview.add_item(item)
                label = "Explore map" if graph.get("chart_type") == "map" else "Explore graph"
                preview.add_item(discord.ui.ActionRow(ExploreButton(client, graph_id, label=label)))
                interaction.client.activity_views.add(preview)
            except Exception:
                logger.exception("Could not publish Activity graph; sending the normal image result.")
        await interaction.followup.send(file=file, view=preview)
    finally:
        file.close()


async def send_map_result(interaction, file, preview, api_url, mode, **options):
    if activity_enabled(interaction):
        try:
            query = {"mode": mode}
            if "timeframe" in options:
                query["timeframe"] = options["timeframe"]
            async with ClientSession(timeout=ClientTimeout(total=90)) as session:
                async with session.get(api_url + "/api/map/interactive?" + urlencode(query)) as response:
                    response.raise_for_status()
                    data = await response.json()
            data["filters"] = options
            preview.activity_graph = data
        except Exception:
            logger.exception("Could not load interactive %s map; sending the normal image result.", mode)
    await send_activity_result(interaction, file, preview)