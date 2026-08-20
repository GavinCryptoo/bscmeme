from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from meme_system.adapters.protocols import ExecutableQuote
from meme_system.analytics.live_entry_outcomes import build_report, spearman
from meme_system.storage.database import initialize_database
from meme_system.strategies.survivor_reversal import BscBalancedLiveEngine, FlowSample, SurvivorCandidate, SurvivorPosition


class LiveEntryOutcomeAnalyticsTests(unittest.TestCase):
    def test_spearman_handles_ties_and_requires_a_small_minimum_sample(self) -> None:
        self.assertIsNone(spearman(((1, 1), (2, 2))))
        self.assertGreater(spearman(((1, 1), (2, 2), (2, 3), (4, 4), (5, 5))) or 0, 0.8)

    def test_report_is_descriptive_and_preserves_source_groups(self) -> None:
        rows = []
        for index in range(8):
            rows.append((
                {"liquidity": str(100 + index), "unique_buyers_60s": None, "signal_source": "BINANCE+OKX" if index % 2 else "BINANCE_ONLY", "holders_growth_1m": str(index), "roundtrip_recovery_pct": str(90 + index)},
                {"mfe_pct": str(index), "mae_pct": str(-index), "realized_pnl_pct": str(index - 3), "hit_tp1_50": index >= 4, "hit_tp2_100": index >= 6},
            ))
        report = build_report(rows)
        self.assertEqual(report["sample_size"], 8)
        self.assertIn("BINANCE_ONLY", report["signal_source_comparison"])
        self.assertIn("Exploratory", report["note"])

    def test_confirmed_entry_snapshot_is_insert_only_and_keeps_unknowns_null(self) -> None:
        now = datetime(2026, 8, 16, tzinfo=timezone.utc)
        with TemporaryDirectory() as directory:
            engine = object.__new__(BscBalancedLiveEngine)
            engine.connection = initialize_database(Path(directory) / "runtime.db")
            engine._candidate_age_seconds = lambda *_: 42
            candidate = SurvivorCandidate("0xtoken", "Token", now - timedelta(seconds=42), now, market_cap_usd=Decimal("10000"), liquidity_usd=Decimal("2000"), holders=50)
            candidate.source_status.update({"discovery_sources": "BINANCE,OKX", "okx_smart_money_count": "2"})
            candidate.flows.append(FlowSample(observed_at=now, buy_volume_bnb=Decimal("1"), sell_volume_bnb=Decimal("0.25"), buy_count=3, sell_count=1))
            position = SurvivorPosition("p1", "0xtoken", "Token", now, Decimal("0.0001"), Decimal("0.0001"), Decimal("10"), Decimal("10"), Decimal("0.001"))
            buy = ExecutableQuote("b", "0xtoken", "buy", Decimal("0.001"), Decimal("10"), None, None, now, 1, provider="gmgn")
            sell = ExecutableQuote("s", "0xtoken", "sell", Decimal("10"), Decimal("0.0009"), None, None, now, 1, provider="gmgn")
            engine._capture_live_entry_snapshot(position, candidate, {"buy_quote": buy, "sell_quote": sell, "submitted_at": now}, now)
            engine.connection.commit()
            snapshot = engine.connection.execute("SELECT snapshot_json FROM live_entry_snapshots WHERE position_id='p1'").fetchone()[0]
            self.assertIn('"signal_source": "BINANCE+OKX"', snapshot)
            self.assertIn('"unique_buyers_60s": null', snapshot)
            self.assertEqual(engine.connection.execute("SELECT count(*) FROM live_entry_outcomes").fetchone()[0], 1)
            engine.connection.close()
