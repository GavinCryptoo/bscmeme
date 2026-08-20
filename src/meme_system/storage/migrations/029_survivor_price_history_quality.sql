ALTER TABLE survivor_candidates ADD COLUMN ath_before_candidate_price_usd TEXT;
ALTER TABLE survivor_candidates ADD COLUMN ath_before_candidate_at TEXT;
ALTER TABLE survivor_candidates ADD COLUMN price_history_quality TEXT NOT NULL DEFAULT 'INSUFFICIENT';
