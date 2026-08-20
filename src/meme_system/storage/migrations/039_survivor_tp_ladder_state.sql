-- A TP price crossing is a durable market fact, distinct from a later SELL
-- confirmation.  Keep both facts so a pending first partial cannot erase a
-- later TP crossing when the mark retraces before the provider confirms.
ALTER TABLE survivor_positions ADD COLUMN tp1_triggered INTEGER NOT NULL DEFAULT 0;
ALTER TABLE survivor_positions ADD COLUMN tp1_triggered_at TEXT;
ALTER TABLE survivor_positions ADD COLUMN tp1_trigger_price_native TEXT;
ALTER TABLE survivor_positions ADD COLUMN tp1_filled INTEGER NOT NULL DEFAULT 0;
ALTER TABLE survivor_positions ADD COLUMN tp1_filled_at TEXT;
ALTER TABLE survivor_positions ADD COLUMN tp2_triggered INTEGER NOT NULL DEFAULT 0;
ALTER TABLE survivor_positions ADD COLUMN tp2_triggered_at TEXT;
ALTER TABLE survivor_positions ADD COLUMN tp2_trigger_price_native TEXT;
ALTER TABLE survivor_positions ADD COLUMN tp2_filled INTEGER NOT NULL DEFAULT 0;
ALTER TABLE survivor_positions ADD COLUMN tp2_filled_at TEXT;
ALTER TABLE survivor_positions ADD COLUMN tp3_triggered INTEGER NOT NULL DEFAULT 0;
ALTER TABLE survivor_positions ADD COLUMN tp3_triggered_at TEXT;
ALTER TABLE survivor_positions ADD COLUMN tp3_trigger_price_native TEXT;
ALTER TABLE survivor_positions ADD COLUMN tp3_filled INTEGER NOT NULL DEFAULT 0;
ALTER TABLE survivor_positions ADD COLUMN tp3_filled_at TEXT;

-- The legacy timestamps represented completed partials.  Preserve that
-- meaning while making the old rows compatible with the explicit state.
UPDATE survivor_positions
SET tp1_triggered=CASE WHEN tp1_at IS NOT NULL THEN 1 ELSE tp1_triggered END,
    tp1_filled=CASE WHEN tp1_at IS NOT NULL THEN 1 ELSE tp1_filled END,
    tp1_filled_at=COALESCE(tp1_filled_at,tp1_at),
    tp2_triggered=CASE WHEN tp2_at IS NOT NULL THEN 1 ELSE tp2_triggered END,
    tp2_filled=CASE WHEN tp2_at IS NOT NULL THEN 1 ELSE tp2_filled END,
    tp2_filled_at=COALESCE(tp2_filled_at,tp2_at);
