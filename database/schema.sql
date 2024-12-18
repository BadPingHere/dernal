    CREATE TABLE IF NOT EXISTS guilds (
        uuid TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        prefix TEXT,
        created TEXT
    );

    CREATE TABLE IF NOT EXISTS guild_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_uuid TEXT,
        timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
        level INTEGER,
        xp_percent REAL,
        territories INTEGER,
        wars INTEGER,
        online_members INTEGER,
        total_members INTEGER,
        FOREIGN KEY (guild_uuid) REFERENCES guilds(uuid)
    );

    CREATE TABLE IF NOT EXISTS members (
        uuid TEXT PRIMARY KEY,
        name TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS guild_members (
        guild_uuid TEXT,
        member_uuid TEXT,
        joined TEXT,
        PRIMARY KEY (guild_uuid, member_uuid),
        FOREIGN KEY (guild_uuid) REFERENCES guilds(uuid),
        FOREIGN KEY (member_uuid) REFERENCES members(uuid)
    );

    CREATE TABLE IF NOT EXISTS member_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_uuid TEXT,
        member_uuid TEXT,
        timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
        contribution INTEGER,
        contribution_rank INTEGER,
        online INTEGER,
        server TEXT,
        FOREIGN KEY (guild_uuid) REFERENCES guilds(uuid),
        FOREIGN KEY (member_uuid) REFERENCES members(uuid)
    );

    CREATE INDEX IF NOT EXISTS idx_guild_snapshots_timestamp ON guild_snapshots(timestamp);
    CREATE INDEX IF NOT EXISTS idx_member_snapshots_timestamp ON member_snapshots(timestamp);
    CREATE INDEX IF NOT EXISTS idx_guild_members_guild_uuid ON guild_members(guild_uuid);
    CREATE INDEX IF NOT EXISTS idx_guild_members_member_uuid ON guild_members(member_uuid);
    CREATE INDEX IF NOT EXISTS idx_guild_snapshots_guild_uuid ON guild_snapshots(guild_uuid);