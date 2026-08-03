ALTER TABLE virtual_positions ADD COLUMN raw_name TEXT;
ALTER TABLE virtual_positions ADD COLUMN display_name TEXT;
ALTER TABLE virtual_positions ADD COLUMN symbol TEXT;
ALTER TABLE virtual_positions ADD COLUMN entry_price_snapshot_json TEXT;
ALTER TABLE virtual_positions ADD COLUMN exit_price_snapshot_json TEXT;

ALTER TABLE executions ADD COLUMN price_snapshot_json TEXT;

