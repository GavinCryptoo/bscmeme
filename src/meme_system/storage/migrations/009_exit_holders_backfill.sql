ALTER TABLE virtual_positions ADD COLUMN exit_holders_observed_at TEXT;
ALTER TABLE virtual_positions ADD COLUMN exit_holders_source TEXT;
ALTER TABLE virtual_positions ADD COLUMN exit_holders_status TEXT;

UPDATE virtual_positions
SET exit_holders_status = 'completed',
    exit_holders_source = 'legacy'
WHERE exit_holders IS NOT NULL AND exit_holders_status IS NULL;
