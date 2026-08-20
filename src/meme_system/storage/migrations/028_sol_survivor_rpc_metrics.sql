ALTER TABLE sol_survivor_swap_events ADD COLUMN rpc_latency_ms INTEGER;
ALTER TABLE sol_survivor_swap_events ADD COLUMN rpc_failed INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sol_survivor_swap_events ADD COLUMN rpc_http_429 INTEGER NOT NULL DEFAULT 0;
