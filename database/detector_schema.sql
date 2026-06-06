CREATE TABLE IF NOT EXISTS wars (
    serverID INTEGER NOT NULL,
    channelForMessages INTEGER,
    guildPrefix TEXT NOT NULL,
    pingRoleID INTEGER,
    intervalForPing INTEGER,
    lastPing INTEGER,
    PRIMARY KEY (serverID, guildPrefix)
);

CREATE TABLE IF NOT EXISTS world_events (
    serverID INTEGER NOT NULL,
    channelForMessages INTEGER,
    event TEXT NOT NULL,
    pingRoleID INTEGER,
    anniePingTimer INTEGER,
    PRIMARY KEY (serverID, event)
);

CREATE TABLE IF NOT EXISTS lootpools (
    serverID INTEGER NOT NULL,
    channelForMessages INTEGER,
    lootpool TEXT NOT NULL,
    pingRoleID INTEGER,
    PRIMARY KEY (serverID, lootpool)
);
