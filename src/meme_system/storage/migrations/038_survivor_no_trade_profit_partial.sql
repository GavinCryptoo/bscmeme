-- A profitable 40-second no-trade event may sell only once.  Keep this
-- position-level fact across ticks and process restarts.
ALTER TABLE survivor_positions ADD COLUMN no_trade_profit_partial_done INTEGER NOT NULL DEFAULT 0;
