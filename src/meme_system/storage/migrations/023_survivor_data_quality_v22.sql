ALTER TABLE survivor_candidates ADD COLUMN data_quality_cohort TEXT NOT NULL DEFAULT 'LEGACY';
ALTER TABLE survivor_candidates ADD COLUMN history_source TEXT;
ALTER TABLE survivor_candidates ADD COLUMN history_start_at TEXT;
ALTER TABLE survivor_candidates ADD COLUMN history_end_at TEXT;
ALTER TABLE survivor_candidates ADD COLUMN history_sample_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE survivor_candidates ADD COLUMN history_interval TEXT;
ALTER TABLE survivor_candidates ADD COLUMN max_history_gap_seconds TEXT;

CREATE TABLE survivor_price_snapshots (
    mint TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    price_usd TEXT NOT NULL,
    price_native TEXT,
    price_source TEXT NOT NULL,
    PRIMARY KEY (mint, observed_at, price_source)
);

CREATE INDEX ix_survivor_price_snapshots_time
ON survivor_price_snapshots(mint, observed_at);
