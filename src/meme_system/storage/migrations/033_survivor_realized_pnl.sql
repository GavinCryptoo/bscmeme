-- survivor_positions.pnl_pct is stored in percentage points, not a decimal
-- ratio.  For a closed Paper position, the only authoritative PnL is the
-- aggregate executable sell-quote proceeds versus the executable buy cost.
-- This one-time correction repairs legacy rows that previously stored a
-- stale WSS mark instead of the realized Paper result.
UPDATE survivor_positions
SET pnl_pct = CAST(
    ((CAST(realized_bnb AS REAL) / NULLIF(CAST(invested_bnb AS REAL), 0)) - 1) * 100
    AS TEXT
)
WHERE status = 'CLOSED'
  AND CAST(invested_bnb AS REAL) > 0;
