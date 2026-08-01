CREATE TABLE IF NOT EXISTS signals (
    signal_id TEXT PRIMARY KEY,
    chain TEXT NOT NULL,
    mint TEXT NOT NULL,
    source TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    raw_payload_hash TEXT
);

CREATE TABLE IF NOT EXISTS candidates (
    candidate_id TEXT PRIMARY KEY,
    signal_id TEXT NOT NULL,
    mint TEXT NOT NULL,
    mode TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    ruleset_name TEXT NOT NULL,
    ruleset_version TEXT NOT NULL,
    config_version TEXT NOT NULL,
    status TEXT NOT NULL,
    filter_reason TEXT,
    rule_checks_json TEXT NOT NULL DEFAULT '[]',
    soft_features_json TEXT NOT NULL DEFAULT '{}',
    FOREIGN KEY(signal_id) REFERENCES signals(signal_id)
);

CREATE TABLE IF NOT EXISTS virtual_positions (
    position_id TEXT PRIMARY KEY,
    mint TEXT NOT NULL,
    mode TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    ruleset_name TEXT NOT NULL,
    ruleset_version TEXT NOT NULL,
    config_version TEXT NOT NULL,
    quantity_sol TEXT NOT NULL,
    opened_at TEXT NOT NULL,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS executions (
    execution_id TEXT PRIMARY KEY,
    position_id TEXT,
    mode TEXT NOT NULL,
    action TEXT NOT NULL,
    reason TEXT,
    quote_id TEXT,
    quote_age_ms INTEGER,
    quote_input_quantity TEXT,
    quote_output_quantity TEXT,
    price_impact_pct TEXT,
    quote_quoted_at TEXT,
    route_fee TEXT,
    estimated_network_fee TEXT,
    estimated_priority_fee TEXT,
    gross_pnl_sol TEXT,
    gross_pnl_pct TEXT,
    net_pnl_estimated_sol TEXT,
    net_pnl_is_estimated INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS shadow_outcomes (
    outcome_id TEXT PRIMARY KEY,
    position_id TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    ruleset_name TEXT NOT NULL,
    ruleset_version TEXT NOT NULL,
    config_version TEXT NOT NULL,
    returns_after_exit_json TEXT NOT NULL,
    paper_tp_reached INTEGER NOT NULL,
    avoided_loss_pct TEXT,
    missed_profit_pct TEXT
);

CREATE TABLE IF NOT EXISTS audit_events (
    event_id TEXT PRIMARY KEY,
    mode TEXT NOT NULL,
    event_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    payload_json TEXT NOT NULL
);
