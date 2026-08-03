from __future__ import annotations

import tempfile
import unittest
import json
from pathlib import Path

from meme_system.config.runtime import RuntimePaths
from meme_system.storage.database import initialize_database
from meme_system.storage.exit_holders import backfill_historical_exit_holders
from meme_system.storage.exit_market import backfill_historical_exit_market
from meme_system.storage.queries import LedgerQueries


class StorageTests(unittest.TestCase):
    def test_database_uses_wal_and_schema_migration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            connection = initialize_database(Path(directory) / "runtime.db")
            journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            self.assertEqual(journal_mode.lower(), "wal")
            self.assertIn("schema_migrations", tables)
            self.assertIn("signals", tables)
            self.assertIn("virtual_positions", tables)
            self.assertIn("lifecycle_events", tables)
            versions = [
                row[0]
                for row in connection.execute(
                    "SELECT version FROM schema_migrations ORDER BY version"
                )
            ]
            self.assertEqual(versions, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
            columns = {
                row[1]
                for row in connection.execute("PRAGMA table_info(virtual_positions)")
            }
            self.assertIn("entry_liquidity_usd", columns)
            self.assertIn("exit_holders_observed_at", columns)
            self.assertIn("exit_holders_source", columns)
            self.assertIn("exit_holders_status", columns)
            self.assertIn("exit_market_cap_usd", columns)
            self.assertIn("exit_liquidity_usd", columns)
            self.assertIn("exit_market_observed_at", columns)
            self.assertIn("exit_market_source", columns)
            self.assertIn("exit_market_status", columns)
            self.assertIn("signal_observed_at", columns)
            self.assertIn("evaluated_at", columns)
            self.assertIn("entry_quote_at", columns)
            self.assertIn("exit_quote_at", columns)
            execution_columns = {
                row[1]
                for row in connection.execute("PRAGMA table_info(executions)")
            }
            self.assertIn("exit_status", execution_columns)
            self.assertIn("pnl_status", execution_columns)
            connection.close()

    def test_paper_and_shadow_paths_are_distinct(self) -> None:
        RuntimePaths().validate_isolation()

    def test_display_since_hides_older_shadow_rows_without_deleting_them(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            connection = initialize_database(Path(directory) / "runtime.db")
            connection.executemany(
                "INSERT INTO signals(signal_id, chain, mint, source, observed_at) VALUES (?, ?, ?, ?, ?)",
                (
                    ("bsc:s-old", "bsc", "old-mint", "test", "2026-08-01T23:59:00+00:00"),
                    ("bsc:s-new", "bsc", "new-mint", "test", "2026-08-02T00:01:00+00:00"),
                ),
            )
            connection.executemany(
                "INSERT INTO candidates(candidate_id, signal_id, mint, mode, strategy_name, "
                "ruleset_name, ruleset_version, config_version, status, soft_features_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    ("c-old", "bsc:s-old", "old-mint", "shadow", "strategy", "rules", "1", "1", "ACCEPTED", "{}"),
                    ("c-new", "bsc:s-new", "new-mint", "shadow", "strategy", "rules", "1", "1", "ACCEPTED", "{}"),
                ),
            )
            connection.executemany(
                "INSERT INTO virtual_positions(position_id, mint, mode, strategy_name, ruleset_name, "
                "ruleset_version, config_version, quantity_sol, opened_at, status, token_name) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    ("p-old", "old-mint", "shadow", "strategy", "rules", "1", "1", "0.001", "2026-08-01T23:59:00+00:00", "OPEN", "Old"),
                    ("p-new", "new-mint", "shadow", "strategy", "rules", "1", "1", "0.001", "2026-08-02T00:01:00+00:00", "OPEN", "New"),
                ),
            )
            connection.commit()

            queries = LedgerQueries(
                connection,
                "shadow",
                display_since="2026-08-02T00:00:00+00:00",
            )
            self.assertEqual([row["signal_id"] for row in queries.signals()], ["bsc:s-new"])
            self.assertEqual([row["candidate_id"] for row in queries.candidates()], ["c-new"])
            self.assertEqual([row["position_id"] for row in queries.positions()], ["p-new"])
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM virtual_positions").fetchone()[0],
                2,
            )
            connection.close()

    def test_closed_positions_include_quote_prices_and_time_bounded_market_snapshots(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            connection = initialize_database(Path(directory) / "runtime.db")
            connection.execute(
                "INSERT INTO signals(signal_id, chain, mint, source, observed_at) "
                "VALUES (?, ?, ?, ?, ?)",
                ("solana:s1", "solana", "mint1", "test", "2026-08-01T00:00:00+00:00"),
            )
            connection.execute(
                "INSERT INTO candidates(candidate_id, signal_id, mint, mode, strategy_name, "
                "ruleset_name, ruleset_version, config_version, status, soft_features_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "candidate1", "solana:s1", "mint1", "paper", "strategy",
                    "rules", "1", "1", "ACCEPTED",
                    json.dumps({"holders": 12, "market_cap_usd": "1000", "liquidity_usd": "250"}),
                ),
            )
            connection.execute(
                "INSERT INTO signals(signal_id, chain, mint, source, observed_at) "
                "VALUES (?, ?, ?, ?, ?)",
                ("solana:s2", "solana", "mint1", "test", "2026-08-01T00:00:30+00:00"),
            )
            connection.execute(
                "INSERT INTO candidates(candidate_id, signal_id, mint, mode, strategy_name, "
                "ruleset_name, ruleset_version, config_version, status, soft_features_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "candidate2", "solana:s2", "mint1", "paper", "strategy",
                    "rules", "1", "1", "ACCEPTED",
                    json.dumps({"holders": 12, "market_cap_usd": "1200", "liquidity_usd": "300"}),
                ),
            )
            connection.execute(
                "INSERT INTO virtual_positions(position_id, mint, mode, strategy_name, "
                "ruleset_name, ruleset_version, config_version, quantity_sol, opened_at, "
                "status, token_name, closed_at, closed_reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "position1", "mint1", "paper", "strategy", "rules", "1", "1",
                    "0.001", "2026-08-01T00:00:00+00:00", "CLOSED", "Test Token",
                    "2026-08-01T00:01:00+00:00", "test_exit",
                ),
            )
            connection.executemany(
                "INSERT INTO executions(execution_id, position_id, mode, action, reason, "
                "quote_input_quantity, quote_output_quantity, net_pnl_estimated_sol, "
                "quote_quoted_at, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    ("entry1", "position1", "paper", "entry", "entry_accepted", "0.001", "1000", None, "2026-08-01T00:00:05+00:00", "2026-08-01T00:00:00+00:00"),
                    ("exit1", "position1", "paper", "exit", "test_exit", "1000", "0.0008", "-0.0002", "2026-08-01T00:01:05+00:00", "2026-08-01T00:01:00+00:00"),
                ),
            )
            connection.commit()

            connection.execute(
                "UPDATE virtual_positions SET exit_market_cap_usd = ?, "
                "exit_liquidity_usd = ?, exit_market_observed_at = ?, "
                "exit_market_source = ?, exit_market_status = ? WHERE position_id = ?",
                (
                    "1180", "290", "2026-08-01T00:00:55+00:00",
                    "binance_dynamic", "completed", "position1",
                ),
            )
            connection.commit()

            rows = LedgerQueries(connection, "paper").positions(status="CLOSED")

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["token_name"], "Test Token")
            self.assertEqual(rows[0]["buy_time"], "2026-08-01T00:00:05+00:00")
            self.assertEqual(rows[0]["sell_time"], "2026-08-01T00:01:05+00:00")
            self.assertEqual(rows[0]["entry_quote_at"], "2026-08-01T00:00:05+00:00")
            self.assertEqual(rows[0]["exit_quote_at"], "2026-08-01T00:01:05+00:00")
            self.assertEqual(rows[0]["entry_time_status"], "quote_quoted_at")
            self.assertEqual(rows[0]["exit_time_status"], "quote_quoted_at")
            self.assertEqual(rows[0]["buy_price_sol"], "0.000001")
            self.assertEqual(rows[0]["sell_price_sol"], "8E-7")
            self.assertEqual(rows[0]["pnl_sol"], "-0.0002")
            self.assertEqual(rows[0]["pnl_rate_pct"], "-20.0")
            self.assertEqual(rows[0]["holders"], 12)
            self.assertEqual(rows[0]["entry_holders"], 12)
            self.assertIsNone(rows[0]["exit_holders"])
            self.assertEqual(rows[0]["market_cap_usd"], "1000")
            self.assertEqual(rows[0]["liquidity_usd"], "250")
            self.assertEqual(rows[0]["entry_market_cap_usd"], "1000")
            self.assertEqual(rows[0]["entry_liquidity_usd"], "250")
            self.assertEqual(rows[0]["exit_market_cap_usd"], "1180")
            self.assertEqual(rows[0]["exit_liquidity_usd"], "290")
            self.assertEqual(rows[0]["exit_market_status"], "completed")
            connection.execute(
                "UPDATE virtual_positions SET exit_holders = ? WHERE position_id = ?",
                (18, "position1"),
            )
            connection.commit()
            rows = LedgerQueries(connection, "paper").positions(status="CLOSED")
            self.assertEqual(rows[0]["entry_holders"], 12)
            self.assertEqual(rows[0]["exit_holders"], 18)
            connection.execute(
                "UPDATE executions SET quote_quoted_at = NULL WHERE position_id = ?",
                ("position1",),
            )
            connection.commit()
            rows = LedgerQueries(connection, "paper").positions(status="CLOSED")
            self.assertIsNone(rows[0]["buy_time"])
            self.assertIsNone(rows[0]["sell_time"])
            self.assertEqual(rows[0]["entry_time_status"], "unknown")
            self.assertEqual(rows[0]["exit_time_status"], "unknown")
            self.assertEqual(rows[0]["buy_price_sol"], "0.000001")
            self.assertEqual(rows[0]["sell_price_sol"], "8E-7")
            connection.close()

    def test_historical_exit_market_backfill_is_pre_exit_and_time_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            connection = initialize_database(Path(directory) / "runtime.db")
            connection.execute(
                "INSERT INTO signals(signal_id, chain, mint, source, observed_at) "
                "VALUES (?, ?, ?, ?, ?)",
                ("solana:market-signal", "solana", "market-mint", "test", "2026-08-01T00:00:55+00:00"),
            )
            connection.execute(
                "INSERT INTO candidates(candidate_id, signal_id, mint, mode, strategy_name, "
                "ruleset_name, ruleset_version, config_version, status, soft_features_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "market-candidate", "solana:market-signal", "market-mint", "paper",
                    "strategy", "rules", "1", "1", "ACCEPTED",
                    json.dumps({"market_cap_usd": "1800", "liquidity_usd": "210"}),
                ),
            )
            connection.execute(
                "INSERT INTO virtual_positions(position_id, mint, mode, strategy_name, "
                "ruleset_name, ruleset_version, config_version, quantity_sol, opened_at, "
                "status, closed_at, closed_reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "market-position", "market-mint", "paper", "strategy", "rules", "1", "1",
                    "0.001", "2026-08-01T00:00:00+00:00", "CLOSED",
                    "2026-08-01T00:01:00+00:00", "stop_loss",
                ),
            )
            connection.commit()

            self.assertEqual(backfill_historical_exit_market(connection, mode="paper"), 1)
            row = connection.execute(
                "SELECT exit_market_cap_usd, exit_liquidity_usd, exit_market_status, "
                "exit_market_source FROM virtual_positions WHERE position_id = ?",
                ("market-position",),
            ).fetchone()
            self.assertEqual(tuple(row), ("1800", "210", "completed", "database_candidate_snapshot"))
            connection.close()

    def test_historical_exit_holders_only_use_matching_nearby_database_event(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            connection = initialize_database(Path(directory) / "runtime.db")
            connection.execute(
                "INSERT INTO virtual_positions(position_id, mint, mode, strategy_name, "
                "ruleset_name, ruleset_version, config_version, quantity_sol, opened_at, "
                "status, entry_quantity_token, remaining_quantity_token, closed_at, closed_reason) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "position-history", "mint-history", "paper", "strategy", "rules", "1", "1",
                    "0.001", "2026-08-01T00:00:00+00:00", "CLOSED", "1000", "0",
                    "2026-08-01T00:01:00+00:00", "stop_loss",
                ),
            )
            connection.execute(
                "INSERT INTO audit_events(event_id, mode, event_type, occurred_at, payload_json) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    "audit:near", "paper", "EXIT_EVALUATED", "2026-08-01T00:00:55+00:00",
                    json.dumps({"position_id": "position-history", "current_holders": 77}),
                ),
            )
            connection.execute(
                "INSERT INTO audit_events(event_id, mode, event_type, occurred_at, payload_json) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    "audit:other", "paper", "EXIT_EVALUATED", "2026-08-01T00:01:00+00:00",
                    json.dumps({"position_id": "other-position", "current_holders": 999}),
                ),
            )
            connection.commit()

            self.assertEqual(backfill_historical_exit_holders(connection, mode="paper"), 1)
            row = connection.execute(
                "SELECT exit_holders, exit_holders_status, exit_holders_source, "
                "exit_holders_observed_at FROM virtual_positions WHERE position_id = ?",
                ("position-history",),
            ).fetchone()
            self.assertEqual(tuple(row), (77, "completed", "database_snapshot", "2026-08-01T00:00:55+00:00"))
            connection.close()
