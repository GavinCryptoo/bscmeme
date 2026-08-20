from __future__ import annotations

import time
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from meme_system.adapters.bsc_quote import FLAP_PORTAL, FlapContext, FourMemeContext
from meme_system.adapters.protocols import ExecutableQuote
from meme_system.adapters.universal_execution_router import (
    FOUR_MEME_TOKEN_MANAGER2,
    LaunchpadDirectRouteProvider,
    RouteResult,
    RoundtripResult,
    UniversalExecutionRouter,
)


TOKEN = "0x1111111111111111111111111111111111111111"
WALLET = "0x2222222222222222222222222222222222222222"
NOW = datetime.now(timezone.utc)


def quote(side: str, input_value: str, output_value: str) -> ExecutableQuote:
    return ExecutableQuote(
        quote_id=f"q:{side}", mint=TOKEN, side=side,
        input_quantity=Decimal(input_value), output_quantity=Decimal(output_value),
        route_fee=None, price_impact_pct=None, quoted_at=NOW, age_ms=0,
        expires_at=None, provider="direct", route=("verified",), quote_source="direct",
    )


class DirectPlanTests(unittest.TestCase):
    def provider(self):
        return LaunchpadDirectRouteProvider.__new__(LaunchpadDirectRouteProvider)

    def test_flap_build_has_slippage_and_sell_approval(self):
        provider = self.provider(); provider.slippage_bps = 100
        context = FlapContext(
            mint=TOKEN, token_proxy=TOKEN,
            token_implementation="0x29e6383f0ce68507b5a72a53c2b118a118332aa8",
            launchpad=FLAP_PORTAL, fundraising_currency=None, fundraising_decimals=18,
            token_decimals=18, status=1, migrated=False, pancake_pair=None,
            native_to_quote_swap_enabled=False, tax_rate_bps=0, progress=0,
        )
        buy, approval = provider._plan(context, quote("buy", "0.001", "100"), "buy")
        self.assertEqual(buy.to.lower(), FLAP_PORTAL)
        self.assertEqual(buy.value, 10**15)
        self.assertIsNone(approval)
        sell, approval = provider._plan(context, quote("sell", "100", "0.0011"), "sell")
        self.assertEqual(sell.value, 0)
        self.assertEqual(approval.to.lower(), TOKEN)
        self.assertNotEqual(approval.data, "0x")

    def test_fourmeme_is_version_and_manager_fail_closed(self):
        provider = self.provider(); provider.slippage_bps = 100
        valid = FourMemeContext(
            mint=TOKEN, token_proxy=TOKEN, token_implementation=None,
            launchpad=FOUR_MEME_TOKEN_MANAGER2, fundraising_currency=None,
            fundraising_decimals=18, token_decimals=18, version=2,
            migrated=False, pancake_pair=None, last_price_raw=1,
        )
        tx, approval = provider._plan(valid, quote("buy", "0.001", "100"), "buy")
        self.assertEqual(tx.to.lower(), FOUR_MEME_TOKEN_MANAGER2)
        self.assertEqual(tx.value, 10**15)
        self.assertIsNone(approval)
        unsupported = SimpleNamespace(**{**valid.__dict__, "version": 1})
        with self.assertRaisesRegex(ValueError, "NOT_VERIFIED"):
            provider._plan(unsupported, quote("buy", "0.001", "100"), "buy")


class RaceTests(unittest.TestCase):
    class Provider:
        def __init__(self, name, delay, output=None, reason=None):
            self.provider = name; self.delay = delay; self.output = output; self.reason = reason

        def roundtrip(self, token, amount, build=True):
            time.sleep(self.delay)
            buy = RouteResult(self.provider, token, "buy", amount, Decimal("10") if not self.reason else None, 1, failure_reason=self.reason)
            sell = None if self.reason else RouteResult(self.provider, token, "sell", Decimal("10"), Decimal(self.output), 1)
            return RoundtripResult(self.provider, token, buy, sell)

    def test_provider_failure_does_not_block_parallel_race(self):
        direct = SimpleNamespace(roundtrip=lambda *_args, **_kwargs: None)
        router = UniversalExecutionRouter(WALLET, [
            self.Provider("slow-fail", .15, reason="NO_ROUTE"),
            self.Provider("fast", .01, output="0.0011"),
            self.Provider("best", .02, output="0.0012"),
        ], direct)
        started = time.monotonic()
        best, results = router.roundtrip(TOKEN, Decimal("0.001"), migrate_status=1)
        self.assertLess(time.monotonic() - started, .25)
        self.assertEqual(len(results), 3)
        self.assertEqual(best.provider, "best")

    def test_race_selects_net_output_after_explicit_costs(self):
        direct = SimpleNamespace(roundtrip=lambda *_args, **_kwargs: None)
        expensive = self.Provider("gross-best", 0, output="0.00120")
        cheaper = self.Provider("net-best", 0, output="0.00115")
        original = expensive.roundtrip

        def with_cost(*args, **kwargs):
            item = original(*args, **kwargs)
            sell = RouteResult(**{**item.sell.__dict__, "gas_fee_usd": Decimal("0.10")})
            return RoundtripResult(item.provider, item.token, item.buy, sell)

        expensive.roundtrip = with_cost
        router = UniversalExecutionRouter(
            WALLET, [expensive, cheaper], direct, bnb_usd=Decimal("600")
        )
        best, _ = router.roundtrip(TOKEN, Decimal("0.001"), migrate_status=1)
        self.assertEqual(best.provider, "net-best")

    def test_known_flap_failure_does_not_fall_through_to_aggregators(self):
        failed_buy = RouteResult("FLAP_DIRECT", TOKEN, "buy", Decimal("0.001"), Decimal("10"), 1)
        failed_sell = RouteResult("FLAP_DIRECT", TOKEN, "sell", Decimal("10"), None, 1, failure_reason="NO_ROUTE")
        direct = SimpleNamespace(roundtrip=lambda *_args, **_kwargs: RoundtripResult("LAUNCHPAD_DIRECT", TOKEN, failed_buy, failed_sell))
        aggregator = self.Provider("aggregator", 0, output="0.0011")
        router = UniversalExecutionRouter(WALLET, [aggregator], direct)
        best, results = router.roundtrip(TOKEN, Decimal("0.001"), migrate_status=0, protocol_family="FLAP")
        self.assertIsNone(best)
        self.assertEqual(len(results), 1)


if __name__ == "__main__":
    unittest.main()
