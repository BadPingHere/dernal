CREATE TABLE IF NOT EXISTS guilds (
    guild_uuid  TEXT PRIMARY KEY,
    name        TEXT,
    prefix      TEXT,
    timestamp  TEXT
);

CREATE TABLE IF NOT EXISTS guild_snapshots (
    guild_uuid      TEXT REFERENCES guilds(guild_uuid),
    timestamp       TEXT,
    level           INTEGER,
    xp_percent      INTEGER,
    territories     INTEGER DEFAULT 0,
    wars            INTEGER DEFAULT 0,
    online_members  INTEGER DEFAULT 0,
    total_members   INTEGER DEFAULT 0,
    guild_raids     INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS users (
    player_uuid TEXT,
    username    TEXT,
    run_id      INTEGER,
    PRIMARY KEY (player_uuid, run_id)
);

CREATE TABLE IF NOT EXISTS user_history (
    player_uuid  TEXT,
    username     TEXT,
    guild_uuid   TEXT,
    guild_name   TEXT,
    guild_prefix TEXT,
    timestamp    TEXT
);

CREATE TABLE IF NOT EXISTS player_snapshots (
    guild_uuid      TEXT REFERENCES guilds(guild_uuid),
    player_uuid     TEXT,
    timestamp       TEXT,
    online          INTEGER DEFAULT 0,
    last_join       TEXT,
    playtime        REAL,
    contribution    INTEGER,
    wars            INTEGER,
    mobs_killed     INTEGER,
    total_dungeons  INTEGER,
    total_raids     INTEGER,
    total_graids    INTEGER
);

CREATE TABLE IF NOT EXISTS player_current_stats (
    player_uuid  TEXT PRIMARY KEY,
    dungeon_dict TEXT,
    raid_dict    TEXT,
    graid_dict   TEXT,
    restrictions TEXT,
    updated_at   TEXT
);

CREATE TABLE IF NOT EXISTS territory_changes (
    territory    TEXT,
    guild_uuid   TEXT,
    guild_prefix TEXT,
    guild_name   TEXT,
    acquired     TEXT
);

CREATE TABLE IF NOT EXISTS guild_season_ratings (
    guild_uuid TEXT    NOT NULL,
    season     INTEGER NOT NULL,
    rating     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (guild_uuid, season),
    FOREIGN KEY (guild_uuid) REFERENCES guilds(guild_uuid)
);

CREATE INDEX IF NOT EXISTS idx_season_ratings_lookup ON guild_season_ratings(season, rating DESC);
CREATE INDEX IF NOT EXISTS idx_guild_snapshots_guild_uuid_timestamp ON guild_snapshots(guild_uuid, timestamp);
CREATE INDEX IF NOT EXISTS idx_guild_snapshots_timestamp ON guild_snapshots(timestamp);
CREATE INDEX IF NOT EXISTS idx_player_snapshots_player_uuid_timestamp ON player_snapshots(player_uuid, timestamp);
CREATE INDEX IF NOT EXISTS idx_player_snapshots_guild_uuid_timestamp ON player_snapshots(guild_uuid, timestamp);
CREATE INDEX IF NOT EXISTS idx_player_snapshots_timestamp ON player_snapshots(timestamp);
CREATE INDEX IF NOT EXISTS idx_player_snapshots_ts_player ON player_snapshots(timestamp, player_uuid);
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_run_id ON users(run_id);
CREATE INDEX IF NOT EXISTS idx_user_history_player_uuid_timestamp ON user_history(player_uuid, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_territory_acquired ON territory_changes (territory, acquired);