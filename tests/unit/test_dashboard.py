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
