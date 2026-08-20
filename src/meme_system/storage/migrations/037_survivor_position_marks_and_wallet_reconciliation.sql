-- Position marks belong to an open position, not to its Candidate lifecycle.
ALTER TABLE survivor_positions ADD COLUMN position_mark_price_native TEXT;
ALTER TABLE survivor_positions ADD COLUMN position_mark_price_usd TEXT;
ALTER TABLE survivor_positions ADD COLUMN position_mark_source TEXT;
ALTER TABLE survivor_positions ADD COLUMN position_price_updated_at TEXT;
ALTER TABLE survivor_positions ADD COLUMN position_price_freshness TEXT;

-- External wallet activity can close or partially reduce a live position
-- without passing through this runtime's executor.
ALTER TABLE survivor_positions ADD COLUMN external_exit_status TEXT;
ALTER TABLE survivor_positions ADD COLUMN external_exit_detected_at TEXT;
ALTER TABLE survivor_positions ADD COLUMN external_last_known_balance TEXT;
ALTER TABLE survivor_positions ADD COLUMN external_exit_delta_token TEXT;
ALTER TABLE survivor_positions ADD COLUMN external_exit_unpriced INTEGER NOT NULL DEFAULT 0;
