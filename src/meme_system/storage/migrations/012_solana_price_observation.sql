ALTER TABLE virtual_positions ADD COLUMN local_price_sol_per_token TEXT;
ALTER TABLE virtual_positions ADD COLUMN local_price_observed_at TEXT;
ALTER TABLE virtual_positions ADD COLUMN local_price_source TEXT;
ALTER TABLE virtual_positions ADD COLUMN local_return_pct TEXT;
ALTER TABLE virtual_positions ADD COLUMN jupiter_price_sol_per_token TEXT;
ALTER TABLE virtual_positions ADD COLUMN jupiter_price_observed_at TEXT;
ALTER TABLE virtual_positions ADD COLUMN jupiter_return_pct TEXT;
