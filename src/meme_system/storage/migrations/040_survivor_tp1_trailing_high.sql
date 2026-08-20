-- TP1-to-TP2 trailing protection: retain the highest confirmed-position mark
-- after TP1 has actually filled, independent from TP2's existing trail.
ALTER TABLE survivor_positions ADD COLUMN high_since_tp1_native TEXT;
