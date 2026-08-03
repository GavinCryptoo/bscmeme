ALTER TABLE executions ADD COLUMN pricing_mode TEXT NOT NULL DEFAULT 'executable_quote';
ALTER TABLE executions ADD COLUMN executable_quote INTEGER NOT NULL DEFAULT 1;
