CREATE TABLE IF NOT EXISTS bsc_pool_registry (
    token_address TEXT NOT NULL,
    pool_address TEXT NOT NULL,
    pool_type TEXT NOT NULL,
    token0 TEXT NOT NULL,
    token1 TEXT NOT NULL,
    quote_asset TEXT,
    fee INTEGER,
    factory TEXT NOT NULL,
    created_block INTEGER,
    discovered_at TEXT NOT NULL,
    source TEXT NOT NULL,
    validation_status TEXT NOT NULL,
    reserve0 TEXT,
    reserve1 TEXT,
    liquidity_value TEXT,
    PRIMARY KEY (token_address, pool_address)
);

CREATE INDEX IF NOT EXISTS ix_bsc_pool_registry_token
ON bsc_pool_registry(token_address, discovered_at DESC);
