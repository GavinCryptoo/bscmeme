from __future__ import annotations

import tempfile
import unittest
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from meme_system.config.dashboard import DashboardConfig
from meme_system.dashboard_server import DashboardService
from meme_system.storage.database import initialize_database


class DashboardConfigTests(unittest.TestCase):
    def test_default_is_local_read_only_port_8788(self) -> None:
        config = DashboardConfig()
        config.validate()
        self.assertEqual(config.host, "127.0.0.1")
        self.assertEqual(config.port, 8788)
        self.assertTrue(config.read_only)

    def test_non_local_binding_requires_authentication(self) -> None:
        with self.assertRaises(ValueError):
            DashboardConfig(host="0.0.0.0").validate()

    def test_display_since_applies_to_paper_and_shadow_on_both_chains(self) -> None:
        since = "2026-08-03T04:30:00+00:00"
        with tempfile.TemporaryDirectory() as directory:
            connection = initialize_database(Path(directory) / "runtime.db")
            connection.executemany(
                "INSERT INTO runtime_state(mode, state_key, value_json, updated_at) VALUES (?, ?, ?, ?)",
                (
                    ("paper", "dashboard_display_since", '{"since":"' + since + '"}', since),
                    ("shadow", "dashboard_display_since", '{"since":"' + since + '"}', since),
                ),
            )
            connection.commit()

            self.assertEqual(DashboardService._display_since(connection, "paper", "bsc"), since)
            self.assertEqual(DashboardService._display_since(connection, "shadow", "bsc"), since)
            self.assertEqual(DashboardService._display_since(connection, "paper", "solana"), since)
            self.assertEqual(DashboardService._display_since(connection, "shadow", "solana"), since)
            self.assertIsNone(DashboardService._display_since(connection, "paper", "unknown"))
            connection.close()

    def test_survivor_returns_only_active_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = initialize_database(root / "runtime.db")
            connection.executemany(
                "INSERT INTO survivor_candidates(mint, symbol, first_seen_at, last_seen_at, state, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    ("active-old", "暴富羊", "2026-08-08T00:00:00+00:00", "2026-08-08T00:01:00+00:00", "ACTIVE_CANDIDATE", "2026-08-08T00:01:00+00:00"),
                    ("history-new", "新币", "2026-08-09T00:00:00+00:00", "2026-08-09T00:01:00+00:00", "EXPIRED", "2026-08-09T00:01:00+00:00"),
                    ("active-new", "当前候选", "2026-08-08T01:00:00+00:00", "2026-08-08T01:01:00+00:00", "ACTIVE_CANDIDATE", "2026-08-08T01:01:00+00:00"),
                    ("history-old", "旧记录", "2026-08-07T00:00:00+00:00", "2026-08-07T00:01:00+00:00", "LIGHT_TRACKING", "2026-08-07T00:01:00+00:00"),
                ),
            )
            connection.commit()
            connection.close()

            paths = type("Paths", (), {
                "control_file": root / "control.json",
                "paper_health_file": root / "paper-health.json",
                "shadow_health_file": root / "shadow-health.json",
                "paper_db": root / "runtime.db",
                "shadow_db": root / "shadow.db",
            })()
            service = DashboardService(paths=paths)
            service._chain_paths["bsc"] = paths
            result = service.survivor("bsc", limit=3)
            self.assertEqual(
                [row["state"] for row in result["candidates"]],
                ["ACTIVE_CANDIDATE", "ACTIVE_CANDIDATE"],
            )
            self.assertEqual([row["symbol"] for row in result["candidates"]], ["当前候选", "暴富羊"])

    def test_solana_status_uses_runner_acknowledged_control_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = type("Paths", (), {
                "control_file": root / "control.json",
                "paper_health_file": root / "paper-health.json",
                "shadow_health_file": root / "shadow-health.json",
                "paper_db": root / "paper.db",
                "shadow_db": root / "shadow.db",
            })()
            for mode in ("paper", "shadow"):
                connection = initialize_database(getattr(paths, f"{mode}_db"))
                connection.close()
            paths.control_file.write_text(json.dumps({
                "paper_new_entries_paused": False,
                "shadow_new_entries_paused": False,
            }), encoding="utf-8")
            paths.paper_health_file.write_text(json.dumps({"items": {"runtime_control": {
                "state": "HEALTHY",
                "updated_at": "2026-08-04T00:00:00+00:00",
                "details": {"new_entries_paused": True},
            }}}), encoding="utf-8")
            paths.shadow_health_file.write_text(json.dumps({"items": {"runtime_control": {
                "state": "HEALTHY",
                "updated_at": "2026-08-04T00:00:00+00:00",
                "details": {"new_entries_paused": False},
            }}}), encoding="utf-8")

            status = DashboardService(paths=paths).status("solana")
            self.assertTrue(status["modes"]["paper"]["new_entries_paused"])
            self.assertFalse(status["modes"]["shadow"]["new_entries_paused"])
            self.assertEqual(status["modes"]["paper"]["runtime_control"]["state"], "APPLIED")

    def test_sol_survivor_summary_uses_only_survivor_positions_and_sol_symbol(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            connection = initialize_database(Path(directory) / "runtime.db")
            connection.executemany(
                "INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,status,entry_price_native,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    ("open", "mint-open", "OPEN", "2026-08-10T00:00:00+00:00", "OPEN", "1", "1", "1", "2", "0", "2026-08-10T00:00:00+00:00"),
                    ("closed", "mint-closed", "CLOSED", "2026-08-10T00:00:00+00:00", "CLOSED", "1", "1", "0", "2", "2.5", "2026-08-10T00:00:00+00:00"),
                ),
            )
            connection.commit()
            summary = DashboardService._survivor_paper_summary(connection, "SOL")
            self.assertEqual(summary["open_positions"], 1)
            self.assertEqual(summary["closed_trade_count"], 1)
            self.assertEqual(summary["amount_native"], "0.5")
            self.assertEqual(summary["native_symbol"], "SOL")
            self.assertTrue(summary["positions_included"])

    def test_balanced_survivor_summary_is_realtime_position_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            connection = initialize_database(Path(directory) / "runtime.db")
            connection.executemany(
                "INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,status,entry_price_native,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    ("open", "mint-open", "OPEN", "2026-08-12T03:32:00+00:00", "OPEN", "1", "1", "1", "0.01", "0", "2026-08-12T03:32:00+00:00"),
                    ("win", "mint-win", "WIN", "2026-08-12T03:32:00+00:00", "CLOSED", "1", "1", "0", "0.01", "0.012", "2026-08-12T03:33:00+00:00"),
                    ("loss", "mint-loss", "LOSS", "2026-08-12T03:32:00+00:00", "CLOSED", "1", "1", "0", "0.01", "0.008", "2026-08-12T03:34:00+00:00"),
                ),
            )
            connection.commit()
            summary = DashboardService._survivor_paper_summary(connection, "BNB")
            self.assertEqual(summary["open_positions"], 1)
            self.assertEqual(summary["profitable_trade_count"], 1)
            self.assertEqual(summary["losing_trade_count"], 1)
            self.assertEqual(summary["closed_trade_count"], 2)
            self.assertAlmostEqual(summary["win_rate_pct"], 50.0)
            self.assertAlmostEqual(float(summary["amount_native"]), 0.0)
            self.assertEqual(summary["native_symbol"], "BNB")

    def test_survivor_position_exposes_percentage_pnl_unit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = initialize_database(root / "runtime.db")
            connection.execute(
                "INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,status,entry_price_native,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,pnl_pct,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                ("closed", "mint", "X", "2026-08-10T00:00:00+00:00", "CLOSED", "1", "1", "0", "0.01", "0.002", "-80", "2026-08-10T00:00:00+00:00"),
            )
            connection.commit()
            connection.close()
            paths = type("Paths", (), {
                "control_file": root / "control.json",
                "paper_health_file": root / "paper-health.json",
                "shadow_health_file": root / "shadow-health.json",
                "paper_db": root / "runtime.db",
                "shadow_db": root / "shadow.db",
            })()
            service = DashboardService(paths=paths)
            service._chain_paths["bsc"] = paths
            row = service.survivor("bsc")["positions"][0]
            self.assertEqual(row["pnl_rate_pct"], "-80")
            self.assertEqual(row["pnl_basis"], "EXECUTABLE_QUOTE")

    def test_survivor_open_position_includes_latest_candidate_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = initialize_database(root / "runtime.db")
            connection.execute(
                "INSERT INTO survivor_candidates(mint,symbol,first_seen_at,last_seen_at,state,holders,market_cap_usd,liquidity_usd,first_holders,first_market_cap_usd,first_liquidity_usd,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                ("mint", "指标币", "2026-08-10T00:00:00+00:00", "2026-08-10T00:01:00+00:00", "ACTIVE_CANDIDATE", 42, "12000", "6000", 30, "10000", "5000", "2026-08-10T00:01:00+00:00"),
            )
            connection.execute(
                "INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,status,entry_price_native,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                ("open", "mint", "指标币", "2026-08-10T00:00:00+00:00", "OPEN", "1", "1", "1", "0.01", "0", "2026-08-10T00:01:00+00:00"),
            )
            connection.commit()
            connection.close()
            paths = type("Paths", (), {
                "control_file": root / "control.json",
                "paper_health_file": root / "paper-health.json",
                "shadow_health_file": root / "shadow-health.json",
                "paper_db": root / "runtime.db",
                "shadow_db": root / "shadow.db",
            })()
            service = DashboardService(paths=paths)
            service._chain_paths["bsc"] = paths
            row = service.survivor("bsc")["positions"][0]
            self.assertEqual(row["holders"], 42)
            self.assertEqual(row["market_cap_usd"], "12000")
            self.assertEqual(row["liquidity_usd"], "6000")

    def test_survivor_closed_position_uses_frozen_entry_and_exit_snapshots(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = initialize_database(root / "runtime.db")
            connection.execute(
                "INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,closed_at,status,entry_price_native,entry_price_usd,exit_price_native,exit_price_usd,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,entry_holders,exit_holders,entry_market_cap_usd,exit_market_cap_usd,entry_liquidity_usd,exit_liquidity_usd,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("closed", "mint", "冻结币", "2026-08-10T00:00:00+00:00", "2026-08-10T00:05:00+00:00", "CLOSED", "0.00001", "0.00002", "0.00003", "0.00004", "100", "0", "0.001", "0.003", 31, 75, "15000", "22000", "5000", "8000", "2026-08-10T00:05:00+00:00"),
            )
            connection.commit()
            connection.close()
            paths = type("Paths", (), {
                "control_file": root / "control.json",
                "paper_health_file": root / "paper-health.json",
                "shadow_health_file": root / "shadow-health.json",
                "paper_db": root / "runtime.db",
                "shadow_db": root / "shadow.db",
            })()
            service = DashboardService(paths=paths)
            service._chain_paths["bsc"] = paths
            row = service.survivor("bsc")["positions"][0]
            self.assertEqual(row["entry_price_usd"], "0.00002")
            self.assertEqual(row["exit_price_usd"], "0.00004")
            self.assertEqual(row["entry_holders"], 31)
            self.assertEqual(row["exit_holders"], 75)
            self.assertEqual(row["entry_liquidity_usd"], "5000")
            self.assertEqual(row["exit_liquidity_usd"], "8000")

    def test_dashboard_translates_balanced_exit_reasons(self) -> None:
        page = (Path(__file__).parents[2] / "src" / "meme_system" / "static" / "index.html").read_text()
        self.assertIn("NO_TRADE_40S:'40 秒内未收到真实交易事件，时间止损退出'", page)
        self.assertIn("HARD_STOP_PNL:'硬止损：收益率跌破止损线'", page)
        self.assertIn("实际成交收益率", page)
        self.assertIn("触发条件；实际成交", page)

    def test_survivor_position_query_tolerates_balanced_schema_without_usd_snapshots(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = initialize_database(root / "runtime.db")
            # Recreate only the current Balanced runtime subset so the query
            # cannot accidentally depend on legacy display-only columns.
            connection.execute("DROP TABLE survivor_positions")
            connection.execute(
                "CREATE TABLE survivor_positions("
                "position_id TEXT PRIMARY KEY,mint TEXT NOT NULL,symbol TEXT,opened_at TEXT NOT NULL,closed_at TEXT,"
                "status TEXT NOT NULL,entry_price_native TEXT NOT NULL,current_price_native TEXT,quantity_token TEXT NOT NULL,"
                "remaining_quantity_token TEXT NOT NULL,invested_bnb TEXT NOT NULL,realized_bnb TEXT NOT NULL DEFAULT '0',"
                "pnl_pct TEXT,exit_reason TEXT,tp1_at TEXT,tp2_at TEXT,trailing_active INTEGER NOT NULL DEFAULT 0,"
                "quote_source TEXT,updated_at TEXT NOT NULL,last_trade_at TEXT)"
            )
            connection.execute(
                "INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,status,entry_price_native,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                ("balanced", "mint", "B", "2026-08-10T00:00:00+00:00", "OPEN", "1", "1", "1", "0.01", "0", "2026-08-10T00:00:00+00:00"),
            )
            connection.commit()
            connection.close()
            paths = type("Paths", (), {
                "control_file": root / "control.json",
                "paper_health_file": root / "paper-health.json",
                "shadow_health_file": root / "shadow-health.json",
                "paper_db": root / "runtime.db",
                "shadow_db": root / "shadow.db",
            })()
            service = DashboardService(paths=paths)
            service._chain_paths["bsc"] = paths
            row = service.survivor("bsc")["positions"][0]
            self.assertIsNone(row["entry_price_usd"])
            self.assertIsNone(row["exit_price_usd"])

    def test_analytics_keeps_strategy_pnl_independent_and_supports_filters(self) -> None:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        with tempfile.TemporaryDirectory() as directory:
            connection = initialize_database(Path(directory) / "runtime.db")
            connection.executemany(
                "INSERT INTO signals(signal_id, chain, mint, source, observed_at) VALUES (?, ?, ?, ?, ?)",
                (
                    ("s-a", "solana", "mint-a", "test", now.isoformat()),
                    ("s-b", "solana", "mint-b", "test", (now - timedelta(minutes=2)).isoformat()),
                ),
            )
            connection.executemany(
                "INSERT INTO candidates(candidate_id, signal_id, mint, mode, strategy_name, ruleset_name, "
                "ruleset_version, config_version, status, filter_reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    ("c-a", "s-a", "mint-a", "paper", "strategy-a", "rules", "1", "1", "REJECTED", "no_route,holders_below_min"),
                    ("c-b", "s-b", "mint-b", "paper", "strategy-b", "rules", "1", "1", "ACCEPTED", None),
                ),
            )
            connection.executemany(
                "INSERT INTO virtual_positions(position_id, mint, mode, strategy_name, ruleset_name, "
                "ruleset_version, config_version, quantity_sol, opened_at, status, closed_at, closed_reason) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    ("p-a", "mint-a", "paper", "strategy-a", "rules", "1", "1", "0.001", now.isoformat(), "CLOSED", now.isoformat(), "stop_loss"),
                    ("p-b", "mint-b", "paper", "strategy-b", "rules", "1", "1", "0.001", now.isoformat(), "CLOSED", now.isoformat(), "take_profit"),
                ),
            )
            connection.executemany(
                "INSERT INTO executions(execution_id, position_id, mode, action, reason, net_pnl_estimated_sol, recorded_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    ("e-a", "p-a", "paper", "exit", "stop_loss", "-0.0002", now.isoformat()),
                    ("e-b", "p-b", "paper", "exit", "take_profit", "0.0001", now.isoformat()),
                ),
            )
            connection.commit()

            result = DashboardService.analytics(connection, "paper", "solana", window="all")
            by_name = {item["strategy_name"]: item for item in result["strategies"]}
            self.assertEqual(set(by_name), {"strategy-a", "strategy-b"})
            self.assertEqual(by_name["strategy-a"]["pnl"]["amount_native"], "-0.0002")
            self.assertEqual(by_name["strategy-b"]["pnl"]["amount_native"], "0.0001")
            self.assertEqual(result["loss_reasons"][0]["reason"], "stop_loss")
            self.assertEqual({item["reason"] for item in result["rejection_reasons"]}, {"no_route", "holders_below_min"})
            self.assertEqual(len(result["pnl_trend"]), 24)
            self.assertEqual(len(DashboardService.analytics(connection, "paper", "solana", window="all", trend_window="3d")["pnl_trend"]), 72)
            self.assertEqual(len(DashboardService.analytics(connection, "paper", "solana", window="all", trend_window="1m")["pnl_trend"]), 30)

            filtered = DashboardService.analytics(
                connection,
                "paper",
                "solana",
                strategy="strategy-a",
                outcome="loss",
                window="all",
            )
            self.assertEqual(filtered["summary"]["amount_native"], "-0.0002")
            self.assertEqual(filtered["summary"]["closed_trade_count"], 1)
            connection.close()

    def test_analytics_separates_optional_unavailable_fields_from_rejections(self) -> None:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        unavailable = "token_age_unavailable,unique_buyers_unavailable,buy_sell_ratio_unavailable"
        with tempfile.TemporaryDirectory() as directory:
            connection = initialize_database(Path(directory) / "runtime.db")
            connection.executemany(
                "INSERT INTO signals(signal_id, chain, mint, source, observed_at) VALUES (?, ?, ?, ?, ?)",
                (
                    ("bsc-s-a", "bsc", "0x1", "test", now.isoformat()),
                    ("bsc-s-b", "bsc", "0x2", "test", now.isoformat()),
                ),
            )
            connection.executemany(
                "INSERT INTO candidates(candidate_id, signal_id, mint, mode, strategy_name, ruleset_name, "
                "ruleset_version, config_version, status, filter_reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    (
                        "bsc-c-a",
                        "bsc-s-a",
                        "0x1",
                        "paper",
                        "bsc_binance_indicative",
                        "ultra_early_selective_bsc",
                        "0.1.5",
                        "0.1.5",
                        "REJECTED",
                        f"{unavailable},holders_below_min",
                    ),
                    (
                        "bsc-c-b",
                        "bsc-s-b",
                        "0x2",
                        "paper",
                        "bsc_binance_indicative",
                        "ultra_early_selective_bsc",
                        "0.1.5",
                        "0.1.5",
                        "ACCEPTED",
                        unavailable,
                    ),
                ),
            )
            connection.commit()

            result = DashboardService.analytics(connection, "paper", "bsc", window="all")
            self.assertEqual({item["reason"] for item in result["rejection_reasons"]}, {"holders_below_min"})
            unavailable_rows = {item["reason"]: item["count"] for item in result["unavailable_reasons"]}
            self.assertEqual(set(unavailable_rows), set(unavailable.split(",")))
            self.assertEqual(set(unavailable_rows.values()), {2})
            connection.close()
