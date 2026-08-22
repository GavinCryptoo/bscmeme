"""SQLite WAL initialization and versioned migration runner."""

from __future__ import annotations

import sqlite3
from pathlib import Path


SCHEMA_VERSION = 41

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
    (14, "014_bsc_quote_metadata.sql"),
    (15, "015_solana_price_snapshot_semantics.sql"),
    (16, "016_realtime_position_monitoring.sql"),
    (17, "017_solana_staged_take_profit.sql"),
    (18, "018_survivor_reversal.sql"),
    (19, "019_survivor_no_trade_exit.sql"),
    (20, "020_survivor_v1_runtime_alignment.sql"),
    (21, "021_survivor_price_coverage.sql"),
    (22, "022_survivor_lifecycle_snapshots.sql"),
    (23, "023_survivor_data_quality_v22.sql"),
    (24, "024_survivor_exclusions.sql"),
    (25, "025_sol_survivor_v1.sql"),
    (26, "026_sol_survivor_smart_money.sql"),
    (27, "027_sol_survivor_swap_flow.sql"),
    (28, "028_sol_survivor_rpc_metrics.sql"),
    (29, "029_survivor_price_history_quality.sql"),
    (30, "030_bsc_pool_registry.sql"),
    (31, "031_bsc_pool_scan_checkpoint.sql"),
    (32, "032_bsc_venue_registry.sql"),
    (33, "033_survivor_realized_pnl.sql"),
    (34, "034_sol_survivor_trade_snapshots.sql"),
    (35, "035_survivor_exit_trigger_audit.sql"),
    (36, "036_survivor_live_execution.sql"),
    (37, "037_survivor_position_marks_and_wallet_reconciliation.sql"),
    (38, "038_survivor_no_trade_profit_partial.sql"),
    (39, "039_survivor_tp_ladder_state.sql"),
    (40, "040_survivor_tp1_trailing_high.sql"),
    (41, "041_live_entry_outcome_tracking.sql"),
)


def initialize_database(path: Path) -> sqlite3.Connection:
    """Open a WAL SQLite runtime database and apply pending migrations."""
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
