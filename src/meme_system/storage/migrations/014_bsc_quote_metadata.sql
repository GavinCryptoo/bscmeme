ALTER TABLE executions ADD COLUMN quote_source TEXT;
ALTER TABLE executions ADD COLUMN quote_route TEXT;
ALTER TABLE executions ADD COLUMN legacy_valuation INTEGER NOT NULL DEFAULT 0;

-- Preserve every old number, but make the valuation boundary explicit.  The
-- BSC Paper/Shadow databases previously used Binance indicative prices.
UPDATE executions
SET pricing_mode = 'legacy_binance_indicative',
    executable_quote = 0,
    legacy_valuation = 1,
    pnl_status = COALESCE(pnl_status, 'legacy_estimated')
WHERE pricing_mode = 'binance_indicative';
