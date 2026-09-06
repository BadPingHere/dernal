CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS ts; 
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS bot;

CREATE TABLE IF NOT EXISTS core.schema_version (
    version     INTEGER PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    description TEXT        NOT NULL
);

INSERT INTO core.schema_version (version, description)
VALUES (1, 'Initial TimescaleDB schema')
ON CONFLICT (version) DO NOTHING;

CREATE TABLE core.guilds (
    guild_uuid UUID PRIMARY KEY,
    name       TEXT        NOT NULL,
    prefix     TEXT        NOT NULL,
    first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_tracked BOOLEAN     NOT NULL DEFAULT true
);

CREATE INDEX guilds_prefix_lower_idx ON core.guilds (lower(prefix));
CREATE INDEX guilds_name_lower_idx   ON core.guilds (lower(name));
CREATE INDEX guilds_tracked_idx      ON core.guilds (guild_uuid) WHERE is_tracked;

CREATE TABLE core.players (
    player_uuid UUID PRIMARY KEY,
    username    TEXT        NOT NULL,
    guild_uuid  UUID        REFERENCES core.guilds,
    online      BOOLEAN,
    last_join   TIMESTAMPTZ,
    first_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX players_username_lower_idx ON core.players (lower(username));
CREATE INDEX players_guild_idx          ON core.players (guild_uuid);

CREATE TABLE core.player_guild_history (
    player_uuid UUID        NOT NULL REFERENCES core.players,
    guild_uuid  UUID        REFERENCES core.guilds,
    joined_at   TIMESTAMPTZ NOT NULL,
    left_at     TIMESTAMPTZ,
    PRIMARY KEY (player_uuid, joined_at)
);

CREATE INDEX player_guild_history_guild_idx
    ON core.player_guild_history (guild_uuid, joined_at DESC);

CREATE TABLE core.guild_season_ratings (
    guild_uuid UUID     NOT NULL REFERENCES core.guilds,
    season     SMALLINT NOT NULL,
    rating     INTEGER  NOT NULL DEFAULT 0,
    PRIMARY KEY (guild_uuid, season)
);

CREATE INDEX season_ratings_lookup_idx ON core.guild_season_ratings (season, rating DESC);

CREATE TABLE ts.player_snapshots (
    ts               TIMESTAMPTZ NOT NULL,
    player_uuid      UUID        NOT NULL,
    guild_uuid       UUID,
    online           BOOLEAN,
    playtime         REAL,
    d_playtime       REAL,
    contribution     BIGINT,
    d_contribution   BIGINT,
    wars             INTEGER,
    d_wars           INTEGER,
    mobs_killed      INTEGER,
    d_mobs_killed    INTEGER,
    total_dungeons   INTEGER,
    d_total_dungeons INTEGER,
    total_raids      INTEGER,
    d_total_raids    INTEGER,
    total_graids     INTEGER,
    d_total_graids   INTEGER
);

SELECT create_hypertable('ts.player_snapshots', by_range('ts', INTERVAL '7 days'));

CREATE INDEX player_snapshots_player_ts_idx ON ts.player_snapshots (player_uuid, ts DESC);
CREATE INDEX player_snapshots_guild_ts_idx  ON ts.player_snapshots (guild_uuid, ts DESC);
ALTER TABLE ts.player_snapshots SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'guild_uuid',
    timescaledb.compress_orderby   = 'player_uuid, ts'
);

SELECT add_compression_policy('ts.player_snapshots', INTERVAL '7 days');
SELECT add_retention_policy  ('ts.player_snapshots', INTERVAL '90 days');
CREATE TABLE ts.guild_snapshots (
    ts             TIMESTAMPTZ NOT NULL,
    guild_uuid     UUID        NOT NULL,
    level          SMALLINT,
    xp_percent     SMALLINT,
    territories    SMALLINT,
    wars           INTEGER,
    online_members SMALLINT,
    total_members  SMALLINT,
    guild_raids    INTEGER
);

SELECT create_hypertable('ts.guild_snapshots', by_range('ts', INTERVAL '7 days'));

CREATE INDEX guild_snapshots_guild_ts_idx ON ts.guild_snapshots (guild_uuid, ts DESC);

ALTER TABLE ts.guild_snapshots SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'guild_uuid',
    timescaledb.compress_orderby   = 'ts DESC'
);

SELECT add_compression_policy('ts.guild_snapshots', INTERVAL '2 days');
SELECT add_retention_policy  ('ts.guild_snapshots', INTERVAL '90 days');
CREATE TABLE ts.territory_ownership (
    acquired    TIMESTAMPTZ NOT NULL,
    territory   TEXT        NOT NULL,
    guild_uuid  UUID        REFERENCES core.guilds,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable('ts.territory_ownership', by_range('acquired', INTERVAL '30 days'));

CREATE INDEX territory_ownership_territory_idx ON ts.territory_ownership (territory, acquired DESC);
CREATE INDEX territory_ownership_guild_idx     ON ts.territory_ownership (guild_uuid, acquired DESC);

ALTER TABLE ts.territory_ownership SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'territory',
    timescaledb.compress_orderby   = 'acquired DESC'
);

SELECT add_compression_policy('ts.territory_ownership', INTERVAL '30 days');
CREATE TABLE ts.api_usage (
    ts       TIMESTAMPTZ NOT NULL,
    route    TEXT        NOT NULL,
    key_name TEXT,
    count    INTEGER     NOT NULL
);

SELECT create_hypertable('ts.api_usage', by_range('ts', INTERVAL '30 days'));

ALTER TABLE ts.api_usage SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'route',
    timescaledb.compress_orderby   = 'ts DESC'
);

SELECT add_compression_policy('ts.api_usage', INTERVAL '7 days');
SELECT add_retention_policy  ('ts.api_usage', INTERVAL '90 days');


-- TODO: Add raw requests for backfill potential
CREATE TABLE raw.payloads (
    payload_hash       BYTEA PRIMARY KEY,
    body               BYTEA       NOT NULL,
    uncompressed_bytes INTEGER     NOT NULL,
    first_seen         TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen          TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE raw.payloads ALTER COLUMN body SET STORAGE EXTERNAL;
CREATE INDEX payloads_last_seen_idx ON raw.payloads (last_seen);
CREATE TABLE raw.calls (
    ts           TIMESTAMPTZ NOT NULL,
    endpoint     TEXT        NOT NULL, 
    entity_uuid  UUID, 
    payload_hash BYTEA       NOT NULL,
    status       SMALLINT    NOT NULL
);

SELECT create_hypertable('raw.calls', by_range('ts', INTERVAL '1 day'));

CREATE INDEX calls_entity_ts_idx ON raw.calls (entity_uuid, ts DESC);

ALTER TABLE raw.calls SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'endpoint',
    timescaledb.compress_orderby   = 'entity_uuid, ts'
);

SELECT add_compression_policy('raw.calls', INTERVAL '2 days');
SELECT add_retention_policy  ('raw.calls', INTERVAL '30 days');

CREATE PROCEDURE raw.gc_payloads(job_id INTEGER, config JSONB)
LANGUAGE plpgsql AS $$
DECLARE
    removed bigint;
BEGIN
    DELETE FROM raw.payloads WHERE last_seen < now() - INTERVAL '31 days';
    GET DIAGNOSTICS removed = ROW_COUNT;
    RAISE NOTICE 'raw.gc_payloads: removed % orphaned payloads', removed;
END;
$$;

SELECT add_job('raw.gc_payloads', INTERVAL '1 day');

CREATE MATERIALIZED VIEW ts.player_daily
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', ts)  AS day,
       player_uuid,
       last(guild_uuid, ts)      AS guild_uuid,
       max(playtime)             AS playtime,
       max(contribution)         AS contribution,
       max(wars)                 AS wars,
       max(mobs_killed)          AS mobs_killed,
       max(total_dungeons)       AS total_dungeons,
       max(total_raids)          AS total_raids,
       max(total_graids)         AS total_graids,
       sum(d_playtime)           AS gained_playtime,
       sum(d_contribution)       AS gained_contribution,
       sum(d_wars)               AS gained_wars,
       sum(d_mobs_killed)        AS gained_mobs_killed,
       sum(d_total_dungeons)     AS gained_dungeons,
       sum(d_total_raids)        AS gained_raids,
       sum(d_total_graids)       AS gained_graids
FROM ts.player_snapshots
GROUP BY 1, 2;

CREATE MATERIALIZED VIEW ts.guild_daily
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', ts)   AS day,
       guild_uuid,
       max(level)                 AS level,
       max(territories)           AS territories,
       max(wars)                  AS wars,
       max(guild_raids)           AS guild_raids,
       max(total_members)         AS total_members,
       avg(online_members)::REAL  AS avg_online
FROM ts.guild_snapshots
GROUP BY 1, 2;

CREATE MATERIALIZED VIEW ts.territory_captures_daily
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', acquired) AS day,
       territory,
       count(*)                       AS captures
FROM ts.territory_ownership
GROUP BY 1, 2;

SELECT add_continuous_aggregate_policy('ts.player_daily',
    start_offset      => INTERVAL '7 days',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

SELECT add_continuous_aggregate_policy('ts.guild_daily',
    start_offset      => INTERVAL '7 days',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

SELECT add_continuous_aggregate_policy('ts.territory_captures_daily',
    start_offset      => INTERVAL '7 days',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

ALTER MATERIALIZED VIEW ts.player_daily SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'guild_uuid',
    timescaledb.compress_orderby   = 'player_uuid, day');

ALTER MATERIALIZED VIEW ts.guild_daily SET (
    timescaledb.compress,
    timescaledb.compress_orderby   = 'guild_uuid, day');

SELECT add_compression_policy('ts.player_daily', compress_after => INTERVAL '30 days');
SELECT add_compression_policy('ts.guild_daily',  compress_after => INTERVAL '30 days');

CREATE TABLE bot.war_subscriptions (
    server_id     BIGINT NOT NULL,
    guild_prefix  TEXT   NOT NULL,
    channel_id    BIGINT,
    ping_role_id  BIGINT,
    ping_interval INTEGER,
    last_ping     TIMESTAMPTZ,
    PRIMARY KEY (server_id, guild_prefix)
);

CREATE TABLE bot.world_event_subscriptions (
    server_id        BIGINT NOT NULL,
    event            TEXT   NOT NULL,
    channel_id       BIGINT,
    ping_role_id     BIGINT,
    annie_ping_timer INTEGER,
    PRIMARY KEY (server_id, event)
);

CREATE TABLE bot.lootpool_subscriptions (
    server_id    BIGINT NOT NULL,
    lootpool     TEXT   NOT NULL,
    channel_id   BIGINT,
    ping_role_id BIGINT,
    PRIMARY KEY (server_id, lootpool)
);

CREATE INDEX war_subs_prefix_idx        ON bot.war_subscriptions (guild_prefix);
CREATE INDEX world_event_subs_event_idx ON bot.world_event_subscriptions (event);
CREATE INDEX lootpool_subs_pool_idx     ON bot.lootpool_subscriptions (lootpool);

CREATE TABLE core.activities (
    activity_id SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('raid', 'dungeon', 'guild_raid')),
    name TEXT NOT NULL,
    UNIQUE (kind, name)
);

CREATE TABLE core.player_activity_totals (
    player_uuid UUID NOT NULL REFERENCES core.players,
    activity_id SMALLINT NOT NULL REFERENCES core.activities,
    total INTEGER NOT NULL CHECK (total >= 0),
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (player_uuid, activity_id)
);

CREATE TABLE ts.player_activity_changes (
    ts TIMESTAMPTZ NOT NULL,
    player_uuid UUID NOT NULL REFERENCES core.players,
    activity_id SMALLINT NOT NULL REFERENCES core.activities,
    total INTEGER NOT NULL CHECK (total >= 0),
    gained INTEGER CHECK (gained >= 0)
);

SELECT create_hypertable('ts.player_activity_changes', by_range('ts', INTERVAL '7 days'));
CREATE INDEX player_activity_changes_player_ts_idx
    ON ts.player_activity_changes (player_uuid, ts DESC);
ALTER TABLE ts.player_activity_changes SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'activity_id',
    timescaledb.compress_orderby = 'player_uuid, ts'
);
SELECT add_compression_policy('ts.player_activity_changes', INTERVAL '7 days');
SELECT add_retention_policy('ts.player_activity_changes', INTERVAL '90 days');

-- No retention policy on daily totals. Keep refreshes within raw retention.
CREATE MATERIALIZED VIEW ts.player_activity_daily
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', ts) AS day,
       player_uuid,
       activity_id,
       sum(gained) AS gained
FROM ts.player_activity_changes
GROUP BY 1, 2, 3
WITH NO DATA;

CREATE INDEX player_activity_daily_player_day_idx
    ON ts.player_activity_daily (player_uuid, day DESC);
SELECT add_continuous_aggregate_policy('ts.player_activity_daily',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
ALTER MATERIALIZED VIEW ts.player_activity_daily SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'activity_id',
    timescaledb.compress_orderby = 'player_uuid, day'
);
SELECT add_compression_policy('ts.player_activity_daily', compress_after => INTERVAL '30 days');
