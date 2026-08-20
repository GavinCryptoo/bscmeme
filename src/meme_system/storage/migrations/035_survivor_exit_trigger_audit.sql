-- Keep the condition that caused an exit separate from the final executable
-- sell result.  A quote can be delayed or move materially after a trigger;
-- the trigger must remain auditable without being mistaken for fill PnL.
ALTER TABLE survivor_positions ADD COLUMN exit_trigger_reason TEXT;
ALTER TABLE survivor_positions ADD COLUMN exit_trigger_pnl_pct TEXT;
ALTER TABLE survivor_positions ADD COLUMN exit_trigger_price_native TEXT;
ALTER TABLE survivor_positions ADD COLUMN exit_triggered_at TEXT;
