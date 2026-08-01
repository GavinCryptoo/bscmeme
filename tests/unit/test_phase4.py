from __future__ import annotations

import base64
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from meme_system.adapters.binance_web3.normalizer import normalize_meme_row
from meme_system.adapters.jupiter import JupiterReadOnlyQuoteProvider
from meme_system.adapters.pump_readonly import _b58decode, _b58encode
from meme_system.config.service import ConfigService
from meme_system.dashboard_server import DashboardService
from meme_system.engines.ledger import SimulationLedger
from meme_system.engines.simulation import DeterministicSimulation
from meme_system.realtime import BinanceRealtimeFeatureProvider, RealtimeCoordinator
from meme_system.runtime_ops import HealthRegistry, JsonlAuditWriter, LatencyRecorder, RuntimeControl, SingleInstanceLock
from meme_system.storage.database import initialize_database
from meme_system.storage.runtime_store import RuntimeStore
from meme_system.telegram_control import TelegramConfig, TelegramControl


NOW = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)


class _Source:
    def __init__(self, record):
        self.record = record

    def fetch_once(self):
        return (self.record,)


class Phase4Tests(unittest.TestCase):
    def test_jupiter_quote_is_read_only_and_keeps_unknown_impact_unavailable(self) -> None:
        calls = []

        def transport(url, headers, timeout):
            calls.append((url, headers))
            payload = {
                "inputMint": "So11111111111111111111111111111111111111112",
                "inAmount": "1000000",
                "outputMint": "MintA",
                "outAmount": "5000000",
                "otherAmountThreshold": "4900000",
                "swapMode": "ExactIn",
                "slippageBps": 50,
                "priceImpactPct": "0.001",
                "routePlan": [{"swapInfo": {"label": "Test AMM"}}],
                "contextSlot": 123,
            }
            return 200, json.dumps(payload).encode(), {}

        provider = JupiterReadOnlyQuoteProvider(
            api_key="redacted-test-key",
            token_decimals={"MintA": 6},
            transport=transport,
        )
        quote = provider.quote_buy("MintA", __import__("decimal").Decimal("0.001"))
        self.assertEqual(quote.provider, "jupiter")
        self.assertIsNone(quote.price_impact_pct)
        self.assertEqual(quote.route, ("Test AMM",))
        self.assertTrue(provider.safe_status()["credentials_configured"])
        self.assertNotIn("redacted-test-key", json.dumps(provider.safe_status()))

    def test_realtime_bootstrap_is_skipped_and_missing_hard_fields_are_explained(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = type("Paths", (), {
                "control_file": root / "control.json",
                "paper_health_file": root / "paper-health.json",
                "shadow_health_file": root / "shadow-health.json",
                "paper_db": root / "paper.db",
                "shadow_db": root / "shadow.db",
                "paper_audit_log": root / "paper.jsonl",
                "shadow_audit_log": root / "shadow.jsonl",
            })()
            record = normalize_meme_row(
                {"contractAddress": "MintA", "name": "Alpha", "holders": "8"},
                fetched_at=NOW,
                historical_bootstrap=False,
            )
            connections = {mode: initialize_database(getattr(paths, f"{mode}_db")) for mode in ("paper", "shadow")}
            engines = {mode: DeterministicSimulation(mode, ledger=SimulationLedger.recover(mode, connections[mode])) for mode in ("paper", "shadow")}
            control = RuntimeControl(paths.control_file)
            coordinator = RealtimeCoordinator(
                source=_Source(record),
                features=BinanceRealtimeFeatureProvider(),
                engines=engines,
                controls=control,
                health={mode: HealthRegistry(getattr(paths, f"{mode}_health_file")) for mode in engines},
                latency=LatencyRecorder(),
                stores={mode: RuntimeStore(connections[mode], mode) for mode in engines},
                audits={mode: JsonlAuditWriter(getattr(paths, f"{mode}_audit_log")) for mode in engines},
            )
            result = coordinator.run_cycle()
            self.assertEqual(result.candidates, 2)
            self.assertEqual(result.accepted, {"paper": 0, "shadow": 0})
            for mode in engines:
                self.assertEqual(len(engines[mode].ledger.candidates), 1)
                self.assertIn("token_age_unavailable", engines[mode].ledger.candidates[0].filter_reason)
                self.assertTrue(getattr(paths, f"{mode}_health_file").exists())
            for connection in connections.values():
                connection.close()

    def test_dashboard_control_and_config_service_are_whitelisted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            from meme_system.config.runtime import RuntimePaths

            paths = RuntimePaths(
                paper_db=root / "paper.db",
                shadow_db=root / "shadow.db",
                paper_audit_log=root / "paper.jsonl",
                shadow_audit_log=root / "shadow.jsonl",
                control_file=root / "control.json",
                paper_health_file=root / "paper-health.json",
                shadow_health_file=root / "shadow-health.json",
            )
            service = DashboardService(paths=paths)
            status, payload = service.set_control({"mode": "paper", "paused": True})
            self.assertEqual(status, 200)
            self.assertTrue(payload["paper_new_entries_paused"])
            config = ConfigService(root / "config.json", mode="paper")
            version = config.update({"poll_interval_sec": 10}, reason="test")
            self.assertTrue(version.version.startswith("cfg-"))
            with self.assertRaises(ValueError):
                config.update({"position_size_sol": "100"}, reason="unsafe")

    def test_lock_and_telegram_disabled_are_safe(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            lock = SingleInstanceLock(Path(directory) / "runner.lock", name="test")
            with lock:
                with self.assertRaises(Exception):
                    SingleInstanceLock(lock.path, name="other").acquire()
            telegram = TelegramControl(TelegramConfig(enabled=False), RuntimeControl(Path(directory) / "control.json"))
            self.assertEqual(telegram.poll_once(), ())

    def test_base58_round_trip_and_nested_account_encoding_fixture(self) -> None:
        raw = bytes(range(32))
        encoded = _b58encode(raw)
        self.assertEqual(_b58decode(encoded), raw)
        account = {"value": {"data": [base64.b64encode(b"abc").decode(), "base64"]}}
        from meme_system.adapters.pump_readonly import _account_bytes

        self.assertEqual(_account_bytes(account), b"abc")
