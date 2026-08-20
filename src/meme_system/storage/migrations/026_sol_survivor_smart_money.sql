CREATE TABLE IF NOT EXISTS survivor_smart_money (
    signal_id TEXT PRIMARY KEY,
    mint TEXT NOT NULL,
    direction TEXT,
    trigger_price_usd TEXT,
    current_price_usd TEXT,
    smart_money_count INTEGER,
    exit_rate INTEGER,
    max_gain TEXT,
    signal_at TEXT,
    fetched_at TEXT NOT NULL,
    raw_response_hash TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_survivor_smart_money_mint_time
ON survivor_smart_money(mint, signal_at DESC);
