from __future__ import annotations

import unittest
from datetime import datetime, timezone
from decimal import Decimal

from meme_system.adapters.binance_web3.models import BinanceNormalizedSignal
from meme_system.adapters.protocols import ExecutableQuote
from meme_system.adapters.pump_readonly import (
    PumpCurveState,
    PumpMarketState,
    PumpProtocolReadOnlyQuoteProvider,
)
from meme_system.domain.models import BASELINE_IDENTITY, Signal, VirtualPosition
from meme_system.realtime import BinanceRealtimeFeatureProvider


NOW = datetime(2026, 8, 3, tzinfo=timezone.utc)
MINT = "PumpMint"


class _PumpStateAdapter:
    def __init__(self, state: PumpMarketState) -> None:
        self.state = state
        self.calls = 0

    def inspect(self, _mint: str) -> PumpMarketState:
        self.calls += 1
        return self.state


class _JupiterQuotes:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def quote(self, mint: str, side: str, quantity: Decimal) -> ExecutableQuote:
        self.calls.append(side)
        return ExecutableQuote(
            quote_id=f"jupiter:{side}",
            mint=mint,
            side=side,
            input_quantity=quantity,
            output_quantity=Decimal("100") if side == "buy" else Decimal("0.001"),
            route_fee=None,
            price_impact_pct=None,
            quoted_at=NOW,
            age_ms=0,
            provider="jupiter",
            quote_source="jupiter_quote",
        )


def _curve_state() -> PumpMarketState:
    curve = PumpCurveState(
        mint=MINT,
        account_address="CurveAddress",
        status="PUMP_BONDING_CURVE",
        virtual_token_reserves=1_000_000_000_000,
        virtual_sol_reserves=10_000_000_000,
        real_token_reserves=900_000_000_000,
        real_sol_reserves=0,
        token_total_supply=1_000_000_000_000,
        complete=False,
        observed_slot=42,
    )
    return PumpMarketState(MINT, "PUMP_BONDING_CURVE", curve, None, 42)


class SolanaBondingCurveQuoteTests(unittest.TestCase):
    def test_bonding_curve_quotes_both_sides_from_one_inspected_state(self) -> None:
        adapter = _PumpStateAdapter(_curve_state())
        provider = PumpProtocolReadOnlyQuoteProvider(
            adapter, token_decimals={MINT: 6}
        )
        state = adapter.inspect(MINT)
        buy = provider.quote_state(state, "buy", Decimal("0.001"))
        sell = provider.quote_state(state, "sell", buy.output_quantity)
        self.assertGreater(buy.output_quantity, 0)
        self.assertGreater(sell.output_quantity, 0)
        self.assertEqual(buy.side, "buy")
        self.assertEqual(sell.side, "sell")
        self.assertEqual(buy.quote_source, "pump_bonding_curve_quote")
        self.assertEqual(sell.quote_source, "pump_bonding_curve_quote")
        self.assertEqual(adapter.calls, 1)

    def test_feature_provider_uses_pump_before_migration_and_jupiter_after(self) -> None:
        adapter = _PumpStateAdapter(_curve_state())
        pump = PumpProtocolReadOnlyQuoteProvider(adapter, token_decimals={MINT: 6})
        jupiter = _JupiterQuotes()
        features = BinanceRealtimeFeatureProvider(
            quote_provider=jupiter,
            pump_quote_provider=pump,
        )
        record = BinanceNormalizedSignal(
            signal=Signal("signal", MINT, NOW, "fixture"),
            source_signal_id=None,
            source_timestamp=None,
            fetched_at=NOW,
            historical_bootstrap=False,
            raw_response_hash="fixture",
        )
        curve_features = features.entry_features(record, evaluated_at=NOW)
        self.assertEqual(curve_features.pricing_mode, "pump_bonding_curve_quote")
        self.assertEqual(curve_features.buy_quote.quote_source, "pump_bonding_curve_quote")
        self.assertEqual(jupiter.calls, [])

        adapter.state = PumpMarketState(MINT, "PUMPSWAP_READY", None, None, 43)
        migrated_features = features.entry_features(record, evaluated_at=NOW)
        self.assertEqual(migrated_features.pricing_mode, "jupiter_quote")
        self.assertEqual(migrated_features.buy_quote.quote_source, "jupiter_quote")
        self.assertEqual(jupiter.calls, ["buy", "sell"])

        position = VirtualPosition(
            position_id="position",
            mint=MINT,
            mode="paper",
            identity=BASELINE_IDENTITY,
            quantity_sol=Decimal("0.001"),
            opened_at=NOW,
            entry_quantity_token=Decimal("100"),
        )
        exit_quote = features.quote_for_position(position)
        self.assertEqual(exit_quote.quote_source, "jupiter_quote")

