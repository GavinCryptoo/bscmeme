ALTER TABLE virtual_positions ADD COLUMN realized_proceeds_sol TEXT NOT NULL DEFAULT '0';
ALTER TABLE virtual_positions ADD COLUMN realized_route_fee_sol TEXT NOT NULL DEFAULT '0';
ALTER TABLE virtual_positions ADD COLUMN realized_network_fee_sol TEXT NOT NULL DEFAULT '0';
ALTER TABLE virtual_positions ADD COLUMN realized_priority_fee_sol TEXT NOT NULL DEFAULT '0';
ALTER TABLE virtual_positions ADD COLUMN tp1_executed_at TEXT;
ALTER TABLE virtual_positions ADD COLUMN tp2_executed_at TEXT;
