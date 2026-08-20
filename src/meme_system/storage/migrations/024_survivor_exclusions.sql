CREATE TABLE survivor_exclusions (
    mint TEXT PRIMARY KEY,
    reason TEXT NOT NULL,
    excluded_at TEXT NOT NULL
);

CREATE INDEX ix_survivor_exclusions_time
ON survivor_exclusions(excluded_at);
