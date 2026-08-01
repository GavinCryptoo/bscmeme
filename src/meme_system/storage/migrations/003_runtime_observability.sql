CREATE TABLE IF NOT EXISTS runtime_state (
    mode TEXT NOT NULL,
    state_key TEXT NOT NULL,
    value_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (mode, state_key)
);

CREATE TABLE IF NOT EXISTS health_events (
    health_id INTEGER PRIMARY KEY AUTOINCREMENT,
    mode TEXT NOT NULL,
    component TEXT NOT NULL,
    state TEXT NOT NULL,
    error_class TEXT,
    latency_ms INTEGER,
    details_json TEXT NOT NULL DEFAULT '{}',
    recorded_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS latency_events (
    latency_id INTEGER PRIMARY KEY AUTOINCREMENT,
    mode TEXT NOT NULL,
    stage TEXT NOT NULL,
    latency_ms REAL NOT NULL,
    recorded_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS config_versions (
    config_version TEXT PRIMARY KEY,
    mode TEXT NOT NULL,
    config_json TEXT NOT NULL,
    changed_at TEXT NOT NULL,
    change_reason TEXT
);

CREATE TABLE IF NOT EXISTS archive_manifests (
    archive_id TEXT PRIMARY KEY,
    mode TEXT NOT NULL,
    archive_path TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    checksum TEXT
);

CREATE INDEX IF NOT EXISTS ix_health_events_recorded_at
ON health_events(recorded_at DESC);

CREATE INDEX IF NOT EXISTS ix_latency_events_recorded_at
ON latency_events(recorded_at DESC);
