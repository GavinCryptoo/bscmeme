-- Immutable entry/exit context for SOL Survivor Paper positions.  These
-- values are captured only at the actual Paper buy/close decision. Later
-- Candidate updates must never rewrite historical trade facts.
ALTER TABLE survivor_positions ADD COLUMN entry_price_usd TEXT;
ALTER TABLE survivor_positions ADD COLUMN exit_price_native TEXT;
ALTER TABLE survivor_positions ADD COLUMN exit_price_usd TEXT;
ALTER TABLE survivor_positions ADD COLUMN entry_holders INTEGER;
ALTER TABLE survivor_positions ADD COLUMN exit_holders INTEGER;
ALTER TABLE survivor_positions ADD COLUMN entry_market_cap_usd TEXT;
ALTER TABLE survivor_positions ADD COLUMN exit_market_cap_usd TEXT;
ALTER TABLE survivor_positions ADD COLUMN entry_liquidity_usd TEXT;
ALTER TABLE survivor_positions ADD COLUMN exit_liquidity_usd TEXT;
