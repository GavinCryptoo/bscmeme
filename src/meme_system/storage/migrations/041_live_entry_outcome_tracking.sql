-- Immutable entry facts and separately mutable, post-entry observations for
-- future GMGN Live fills. JSON keeps absent provider data explicitly NULL
-- without widening the live position schema for an exploratory study.
CREATE TABLE IF NOT EXISTS live_entry_snapshots (
    position_id TEXT PRIMARY KEY,
    mint TEXT NOT NULL,
    entry_at TEXT NOT NULL,
    snapshot_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_live_entry_snapshots_entry_at
    ON live_entry_snapshots(entry_at);

CREATE TABLE IF NOT EXISTS live_entry_outcomes (
    position_id TEXT PRIMARY KEY REFERENCES live_entry_snapshots(position_id),
    outcome_json TEXT NOT NULL,
    closed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_live_entry_outcomes_closed_at
    ON live_entry_outcomes(closed_at);
