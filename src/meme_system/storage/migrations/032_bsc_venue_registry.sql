CREATE TABLE IF NOT EXISTS bsc_venue_registry (
    token_address TEXT NOT NULL,
    venue_address TEXT NOT NULL,
    chain TEXT NOT NULL,
    is_contract INTEGER NOT NULL,
    code_size INTEGER,
    bytecode_hash TEXT,
    implementation_address TEXT,
    factory_address TEXT,
    token0 TEXT,
    token1 TEXT,
    reserve0 TEXT,
    reserve1 TEXT,
    slot0_supported INTEGER NOT NULL DEFAULT 0,
    liquidity_value TEXT,
    fee INTEGER,
    selector_bitmap TEXT NOT NULL,
    protocol_fingerprint TEXT,
    protocol_family TEXT NOT NULL,
    capabilities_json TEXT NOT NULL,
    strategy_support TEXT NOT NULL,
    discovery_source TEXT NOT NULL,
    discovered_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (token_address, venue_address)
);

CREATE INDEX IF NOT EXISTS ix_bsc_venue_registry_family
ON bsc_venue_registry(protocol_family, bytecode_hash);

CREATE INDEX IF NOT EXISTS ix_bsc_venue_registry_venue
ON bsc_venue_registry(venue_address, updated_at DESC);
