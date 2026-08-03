-- Existing rows remain historical and are never repriced or revalued.
-- version 0 is surfaced as legacy/incomplete by the Dashboard query contract.
ALTER TABLE virtual_positions
ADD COLUMN price_snapshot_version INTEGER NOT NULL DEFAULT 0;
