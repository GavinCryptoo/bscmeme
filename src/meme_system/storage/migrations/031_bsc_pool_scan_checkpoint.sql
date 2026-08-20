CREATE TABLE IF NOT EXISTS bsc_pool_scan_checkpoint (
    token_address TEXT PRIMARY KEY,
    scan_status TEXT NOT NULL,
    scan_start_block INTEGER,
    scan_target_block INTEGER,
    last_scanned_block INTEGER,
    v2_logs_found INTEGER NOT NULL DEFAULT 0,
    v3_logs_found INTEGER NOT NULL DEFAULT 0,
    v2_valid_pools INTEGER NOT NULL DEFAULT 0,
    v3_valid_pools INTEGER NOT NULL DEFAULT 0,
    scan_started_at TEXT,
    scan_completed_at TEXT,
    last_scan_at TEXT,
    last_error TEXT
);
