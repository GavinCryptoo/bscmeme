CREATE TABLE IF NOT EXISTS survivor_candidates (
    mint TEXT PRIMARY KEY,
    symbol TEXT,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    state TEXT NOT NULL,
    market_cap_usd TEXT,
    liquidity_usd TEXT,
    holders INTEGER,
    current_price_native TEXT,
    ath_price_native TEXT,
    local_low_native TEXT,
    low_started_at TEXT,
    stable_since TEXT,
    pair_address TEXT,
    pool_type TEXT,
    last_rejection TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS survivor_flow_samples (
    sample_id INTEGER PRIMARY KEY AUTOINCREMENT,
    mint TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    buy_volume_bnb TEXT NOT NULL,
    sell_volume_bnb TEXT NOT NULL,
    buy_count INTEGER NOT NULL,
    sell_count INTEGER NOT NULL,
    price_native TEXT,
    liquidity_native TEXT,
    source TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_survivor_flow_mint_time
    ON survivor_flow_samples(mint, observed_at);

CREATE TABLE IF NOT EXISTS survivor_positions (
    position_id TEXT PRIMARY KEY,
    mint TEXT NOT NULL,
    symbol TEXT,
    opened_at TEXT NOT NULL,
    closed_at TEXT,
    status TEXT NOT NULL,
    entry_price_native TEXT NOT NULL,
    current_price_native TEXT,
    quantity_token TEXT NOT NULL,
    remaining_quantity_token TEXT NOT NULL,
    invested_bnb TEXT NOT NULL,
    realized_bnb TEXT NOT NULL DEFAULT '0',
    pnl_pct TEXT,
    exit_reason TEXT,
    tp1_at TEXT,
    tp2_at TEXT,
    trailing_active INTEGER NOT NULL DEFAULT 0,
    quote_source TEXT,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_survivor_positions_status
    ON survivor_positions(status, opened_at);
