CREATE TABLE IF NOT EXISTS sol_survivor_swap_events (
    signature TEXT NOT NULL,
    mint TEXT NOT NULL,
    protocol TEXT NOT NULL,
    quote_mint TEXT,
    pool_address TEXT,
    base_vault TEXT,
    quote_vault TEXT,
    candidate_delta TEXT,
    quote_delta TEXT,
    quote_volume_native TEXT,
    quote_volume_usd TEXT,
    usd_conversion_source TEXT,
    direction TEXT NOT NULL,
    confidence TEXT NOT NULL,
    detected_at TEXT NOT NULL,
    slot INTEGER,
    tx_fetched_at TEXT,
    parse_finished_at TEXT NOT NULL,
    latency_ms INTEGER NOT NULL,
    error_class TEXT,
    PRIMARY KEY(signature, mint)
);
CREATE INDEX IF NOT EXISTS idx_sol_survivor_swap_mint_time
ON sol_survivor_swap_events(mint, detected_at DESC);

ALTER TABLE survivor_flow_samples ADD COLUMN signature TEXT;
ALTER TABLE survivor_flow_samples ADD COLUMN slot INTEGER;
ALTER TABLE survivor_flow_samples ADD COLUMN quote_mint TEXT;
ALTER TABLE survivor_flow_samples ADD COLUMN quote_volume_native TEXT;
ALTER TABLE survivor_flow_samples ADD COLUMN volume_usd TEXT;
ALTER TABLE survivor_flow_samples ADD COLUMN usd_conversion_source TEXT;
ALTER TABLE survivor_flow_samples ADD COLUMN protocol TEXT;
ALTER TABLE survivor_flow_samples ADD COLUMN detected_at TEXT;
ALTER TABLE survivor_flow_samples ADD COLUMN tx_fetched_at TEXT;
ALTER TABLE survivor_flow_samples ADD COLUMN parse_finished_at TEXT;
ALTER TABLE survivor_flow_samples ADD COLUMN latency_ms INTEGER;
