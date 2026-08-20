ALTER TABLE virtual_positions ADD COLUMN current_holders INTEGER;
ALTER TABLE virtual_positions ADD COLUMN holders_observed_at TEXT;
ALTER TABLE virtual_positions ADD COLUMN holders_source TEXT;
ALTER TABLE virtual_positions ADD COLUMN observed_price_native TEXT;
ALTER TABLE virtual_positions ADD COLUMN observed_price_at TEXT;
ALTER TABLE virtual_positions ADD COLUMN observed_price_source TEXT;
