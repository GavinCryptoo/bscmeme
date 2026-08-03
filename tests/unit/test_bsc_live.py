from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from meme_system.adapters.bsc_live import BscLiveConfig, BscLiveError
from meme_system.adapters.protocols import ExecutableQuote
from meme_system.domain.models import BSC_BASELINE_IDENTITY, EntryFeatures, Signal
from meme_system.engines.ledger import SimulationLedger
from meme_system.engines.simulation import DeterministicSimulation
from meme_system.storage.database import initialize_database
from meme_system.strategies.baseline import BaselineStrategy, bsc_baseline_config


NOW = datetime(2026, 8, 2, tzinfo=timezone.utc)


def _quote(side: str, input_quantity: str, output_quantity: str) -> ExecutableQuote:
    return ExecutableQuote(
        quote_id=f"test:{side}:{input_quantity}:{output_quantity}",
        mint="0x1111111111111111111111111111111111111111",
        side=side,
        input_quantity=Decimal(input_quantity),
        output_quantity=Decimal(output_quantity),
        route_fee=None,
        price_impact_pct=None,
        quoted_at=NOW,
        age_ms=0,
        expires_at=None,
        provider="pancakeswap_smart_router",
    )


class FakeLiveExecutor:
    def __init__(self, *, fail_sell: bool = False) -> None:
        self.buy_calls = 0
        self.sell_calls = 0
        self.fail_sell = fail_sell

    def buy(self, mint, amount, *, expected_quote=None):
        self.buy_calls += 1
        return SimpleNamespace(
            quote=_quote("buy", str(amount), "5.2"),
            actual_received=Decimal("5.2"),
            tx_hash="0xentry",
            gas_fee_native=Decimal("0.00001"),
            settlement_verified=True,
        )

    def sell(self, mint, amount, *, expected_quote=None):
        self.sell_calls += 1
        if self.fail_sell:
            raise RuntimeError("synthetic_send_failure")
        return SimpleNamespace(
            quote=_quote("sell", str(amount), "0.0012"),
            actual_received=Decimal("0.0012"),
            tx_hash="0xexit",
            gas_fee_native=Decimal("0.00001"),
            settlement_verified=True,
        )


class BscLiveConfigTests(unittest.TestCase):
    def _values(self) -> dict[str, str]:
        return {
            "LIVE_TRADING": "true",
            "BSC_LIVE_ENABLED": "true",
            "BSC_RPC_URL": "https://bsc.example.invalid",
            "BSC_PRIVATE_KEY": "0x" + "0" * 64,
            "BSC_TRADE_AMOUNT_BNB": "0.001",
            "BSC_MAX_POSITIONS": "1",
            "BSC_SLIPPAGE_BPS": "50",
        }

    def test_requires_all_trade_controls_without_exposing_private_key(self) -> None:
        values = self._values()
        values.pop("BSC_TRADE_AMOUNT_BNB")
        with self.assertRaises(BscLiveError) as context:
            BscLiveConfig.from_mapping(values)
        self.assertNotIn("0" * 32, str(context.exception))

    def test_parses_isolated_live_configuration(self) -> None:
        config = BscLiveConfig.from_mapping(self._values())
        self.assertEqual(config.trade_amount_bnb, Decimal("0.001"))
        self.assertEqual(config.max_positions, 1)
        self.assertEqual(config.slippage_bps, 50)
        self.assertEqual(config.max_entries, 1)
        self.assertEqual(config.helper_path, Path("scripts/pancakeswap_smart_router.cjs"))


class BscLiveEngineTests(unittest.TestCase):
    def _features(self) -> EntryFeatures:
        buy = _quote("buy", "0.001", "5")
        sell = _quote("sell", "5", "0.0012")
        return EntryFeatures(
            token_age_sec=None,
            unique_buyers_15s=None,
            buy_sell_count_ratio_15s=None,
            net_buy_15s=None,
            flow_windows_non_negative=(None, None),
            creator_confirmed_sold=None,
            buy_quote=buy,
            sell_quote=sell,
            evaluated_at=NOW,
            token_name="TEST",
            holders=101,
            market_cap_usd=Decimal("1000"),
            liquidity_usd=Decimal("100"),
            pricing_mode="pancakeswap_smart_router",
            executable_quote=True,
        )

    def test_live_ledger_records_actual_buy_and_sell_separately(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            connection = initialize_database(Path(directory) / "runtime.db")
            ledger = SimulationLedger.recover("live", connection, identity=BSC_BASELINE_IDENTITY)
            strategy = BaselineStrategy(
                replace(
                    bsc_baseline_config(),
                    position_size_sol=Decimal("0.001"),
                    max_open_positions=1,
                )
            )
            executor = FakeLiveExecutor()
            engine = DeterministicSimulation(
                "live",
                strategy=strategy,
                ledger=ledger,
                pricing_mode="pancakeswap_smart_router",
                live_executor=executor,
                live_max_entries=1,
            )
            signal = Signal("sig-live", "0x1111111111111111111111111111111111111111", NOW, "test", "bsc")
            entry = engine.process_entry(signal, self._features(), "live:candidate", "live:position")
            self.assertIsNotNone(entry.position)
            self.assertEqual(entry.position.entry_quantity_token, Decimal("5.2"))
            self.assertEqual(executor.buy_calls, 1)
            exit_result = engine.process_live_exit(
                "live:position",
                NOW,
                _quote("sell", "5.2", "0.0012"),
            )
            self.assertIsNotNone(exit_result.closed_position)
            self.assertEqual(executor.sell_calls, 1)
            rows = connection.execute(
                "SELECT action, pricing_mode FROM executions WHERE mode = 'live' ORDER BY rowid"
            ).fetchall()
            self.assertEqual([(row[0], row[1]) for row in rows], [
                ("entry", "pancakeswap_smart_router"),
                ("exit_attempt", "pancakeswap_smart_router"),
                ("exit", "pancakeswap_smart_router"),
            ])
            connection.close()

    def test_failed_live_exit_is_not_retried(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            connection = initialize_database(Path(directory) / "runtime.db")
            ledger = SimulationLedger.recover("live", connection, identity=BSC_BASELINE_IDENTITY)
            engine = DeterministicSimulation(
                "live",
                strategy=BaselineStrategy(
                    replace(
                        bsc_baseline_config(),
                        position_size_sol=Decimal("0.001"),
                        max_open_positions=1,
                    )
                ),
                ledger=ledger,
                pricing_mode="pancakeswap_smart_router",
                live_executor=FakeLiveExecutor(fail_sell=True),
                live_max_entries=1,
            )
            signal = Signal("sig-live", "0x1111111111111111111111111111111111111111", NOW, "test", "bsc")
            engine.process_entry(signal, self._features(), "live:candidate", "live:position")
            quote = _quote("sell", "5.2", "0.0012")
            engine.process_live_exit("live:position", NOW, quote)
            engine.process_live_exit("live:position", NOW, quote)
            self.assertEqual(engine.live_executor.sell_calls, 1)
            self.assertEqual(len(ledger.active_positions), 1)
            connection.close()


if __name__ == "__main__":
    unittest.main()
