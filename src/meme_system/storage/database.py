"""SQLite WAL initialization and versioned migration runner."""

from __future__ import annotations

import sqlite3
from pathlib import Path


SCHEMA_VERSION = 15

MIGRATIONS = (
    (1, "001_initial.sql"),
    (2, "002_lifecycle_recovery.sql"),
    (3, "003_runtime_observability.sql"),
    (4, "004_pricing_metadata.sql"),
    (5, "005_exit_holders.sql"),
    (6, "006_entry_holders.sql"),
    (7, "007_entry_liquidity.sql"),
    (8, "008_timeout_exit_status.sql"),
    (9, "009_exit_holders_backfill.sql"),
    (10, "010_exit_market_snapshot.sql"),
    (11, "011_quote_lifecycle_timestamps.sql"),
    (12, "012_solana_price_observation.sql"),
    (13, "013_price_snapshots_and_token_names.sql"),
    # 014 is an isolated BSC migration. Keep it outside the Solana lineage.
    (15, "015_solana_price_snapshot_semantics.sql"),
)


def initialize_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    # The realtime position scheduler owns mutations through its coordinator
    # lock but runs on a dedicated thread; permit that shared connection.
    connection = sqlite3.connect(path, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=5000")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations "
        "(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
    )
    migration_dir = Path(__file__).with_name("migrations")
    # Serialize migration discovery and application across Dashboard threads,
    # runners, and separate processes. executescript() implicitly commits and
    # can race on ALTER TABLE, so execute each simple migration statement in
    # one exclusive transaction instead.
    connection.execute("BEGIN EXCLUSIVE")
    try:
        applied = {
            row[0]
            for row in connection.execute("SELECT version FROM schema_migrations")
        }
        for version, filename in MIGRATIONS:
            if version in applied:
                continue
            script = (migration_dir / filename).read_text(encoding="utf-8")
            for statement in script.split(";"):
                statement = statement.strip()
                if statement:
                    connection.execute(statement)
            connection.execute(
                "INSERT INTO schema_migrations(version, applied_at) VALUES (?, datetime('now'))",
                (version,),
            )
        connection.commit()
    except Exception:
        connection.rollback()
        connection.close()
        raise
    return connection
