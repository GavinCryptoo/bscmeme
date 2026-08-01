ALTER TABLE virtual_positions ADD COLUMN entry_quantity_token TEXT NOT NULL DEFAULT '0';
ALTER TABLE virtual_positions ADD COLUMN remaining_quantity_token TEXT NOT NULL DEFAULT '0';
ALTER TABLE virtual_positions ADD COLUMN entry_quote_id TEXT;
ALTER TABLE virtual_positions ADD COLUMN token_name TEXT;
ALTER TABLE virtual_positions ADD COLUMN mfe_pct TEXT NOT NULL DEFAULT '0';
ALTER TABLE virtual_positions ADD COLUMN mae_pct TEXT NOT NULL DEFAULT '0';
ALTER TABLE virtual_positions ADD COLUMN last_return_pct TEXT;
ALTER TABLE virtual_positions ADD COLUMN last_observed_at TEXT;
ALTER TABLE virtual_positions ADD COLUMN last_quote_id TEXT;
ALTER TABLE virtual_positions ADD COLUMN closed_at TEXT;
ALTER TABLE virtual_positions ADD COLUMN closed_reason TEXT;

ALTER TABLE executions ADD COLUMN recorded_at TEXT NOT NULL DEFAULT '';
ALTER TABLE shadow_outcomes ADD COLUMN recorded_at TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS lifecycle_events (
    event_id TEXT PRIMARY KEY,
    position_id TEXT NOT NULL,
    mode TEXT NOT NULL,
    event_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_active_position_lifecycle
ON virtual_positions(mint, mode, strategy_name, ruleset_version)
WHERE status IN ('ENTRY_PENDING', 'OPEN', 'EXIT_TRIGGERED');

CREATE INDEX IF NOT EXISTS ix_positions_opened_at
ON virtual_positions(opened_at DESC);

CREATE INDEX IF NOT EXISTS ix_executions_recorded_at
ON executions(recorded_at DESC);

CREATE INDEX IF NOT EXISTS ix_lifecycle_events_occurred_at
ON lifecycle_events(occurred_at DESC);
