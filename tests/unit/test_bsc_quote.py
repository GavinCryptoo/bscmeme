from __future__ import annotations

import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from eth_abi import encode as abi_encode

from meme_system.adapters.bsc_quote import (
    BscReadOnlyQuoteProvider,
    FLAP_PORTAL,
    FLAP_TOKEN_IMPLEMENTATIONS,
    FOUR_MEME_HELPER3,
    _FLAP_QUOTE_EXACT_INPUT_SELECTOR,
    _FLAP_TOKEN_V6_TYPES,
    _GET_FLAP_TOKEN_V6_SELECTOR,
    _FOUR_INFO_TYPES,
    _GET_PANCAKE_PAIR_SELECTOR,
    _GET_TOKEN_INFO_SELECTOR,
    _TRY_BUY_SELECTOR,
    _TRY_SELL_SELECTOR,
)
from meme_system.domain.models import EntryFeatures
from meme_system.strategies.baseline import BaselineStrategy, bsc_baseline_config


TOKEN = "0x1111111111111111111111111111111111111111"
PAIR = "0x2222222222222222222222222222222222222222"
MANAGER = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
STABLE = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
ROUTER = "0x13f4ea83d0bd40e75c8222255bc855a974568dd4"
ZERO = "0x0000000000000000000000000000000000000000"


class _Rpc:
    urls = ("https://rpc.example",)
    configured = True

    def __init__(self, *, quote: str = ZERO, migrated: bool = False) -> None:
        self.quote = quote
        self.migrated = migrated

    def call_uint(self, to, _data):
        return 6 if to == STABLE else 18

    def call_address(self, _to, data):
        return PAIR if data.startswith(_GET_PANCAKE_PAIR_SELECTOR) and self.migrated else None

    def call(self, method, _params):
        self.assertEqual(method, "eth_getCode")
        return "0x363d3d373d3d3d363d73" + "12" * 20 + "5af43d82803e903d91602b57fd5bf3"

    def call_hex(self, _to, data):
        if data.startswith(_GET_TOKEN_INFO_SELECTOR):
            return "0x" + abi_encode(
                list(_FOUR_INFO_TYPES),
                (2, MANAGER, self.quote, 1, 0, 0, 0, 0, 0, 0, 0, self.migrated),
            ).hex()
        if data.startswith(_TRY_BUY_SELECTOR):
            return "0x" + abi_encode(
                ["address", "address", "uint256", "uint256", "uint256", "uint256", "uint256", "uint256"],
                (MANAGER, self.quote, 100 * 10**18, 0, 0, 0, 0, 0),
            ).hex()
        if data.startswith(_TRY_SELL_SELECTOR):
            return "0x" + abi_encode(
                ["address", "address", "uint256", "uint256"],
                (MANAGER, self.quote, 10**16, 10**12),
            ).hex()
        return None

    def assertEqual(self, first, second):
        if first != second:
            raise AssertionError(f"{first!r} != {second!r}")


class _Router:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def request(self, payload):
        self.calls.append(dict(payload))
        if payload["operation"] == "health":
            return {"status": "ok", "ready": True}
        output_raw = "100000000" if payload["outputNative"] is False else "9000000000000000"
        return {
            "status": "ok",
            "outputRaw": output_raw,
            "routerAddress": ROUTER,
            "route": [{"type": "V2", "pools": [PAIR]}],
        }

    def close(self):
        return None


class _FlapRpc(_Rpc):
    def __init__(self, *, quote: str = ZERO, migrated: bool = False, missing_context: bool = False, native_to_quote: bool = True) -> None:
        super().__init__(quote=quote, migrated=migrated)
        self.missing_context = missing_context
        self.native_to_quote = native_to_quote

    def call(self, method, _params):
        self.assertEqual(method, "eth_getCode")
        implementation = next(iter(FLAP_TOKEN_IMPLEMENTATIONS))[2:]
        return "0x363d3d373d3d3d363d73" + implementation + "5af43d82803e903d91602b57fd5bf3"

    def call_hex(self, to, data):
        if to == FLAP_PORTAL and data.startswith(_GET_FLAP_TOKEN_V6_SELECTOR):
            if self.missing_context:
                return "0x"
            return "0x" + abi_encode(
                list(_FLAP_TOKEN_V6_TYPES),
                (
                    4 if self.migrated else 1,
                    0,
                    0,
                    0,
                    5,
                    0,
                    0,
                    0,
                    0,
                    self.quote,
                    self.native_to_quote,
                    b"\x00" * 32,
                    250,
                    PAIR if self.migrated else ZERO,
                    0,
                ),
            ).hex()
        if to == FLAP_PORTAL and data.startswith(_FLAP_QUOTE_EXACT_INPUT_SELECTOR):
            # The Portal uses a non-view quote function.  Calling it with
            # eth_call remains read-only and returns the protocol result.
            return "0x" + abi_encode(["uint256"], [100 * 10**18 if self.quote == ZERO else 10**8]).hex()
        return super().call_hex(to, data)


class BscReadOnlyQuoteTests(unittest.TestCase):
    def test_native_bonding_curve_uses_onchain_context_and_two_quotes(self) -> None:
        provider = BscReadOnlyQuoteProvider(
            rpc=_Rpc(), helper_path=Path(__file__), router_client=_Router()
        )

        buy, sell, error = provider.quote_candidate(TOKEN, Decimal("0.01"))

        self.assertIsNone(error)
        self.assertIsNotNone(buy)
        self.assertIsNotNone(sell)
        assert buy is not None and sell is not None
        self.assertEqual(buy.quote_source, "bonding_curve_quote")
        self.assertEqual(sell.quote_source, "bonding_curve_quote")
        self.assertIn(FOUR_MEME_HELPER3, buy.route)
        self.assertIn("fundraising:native", buy.route)

    def test_stable_bonding_curve_uses_real_asset_decimals_and_router_conversion(self) -> None:
        router = _Router()
        provider = BscReadOnlyQuoteProvider(
            rpc=_Rpc(quote=STABLE), helper_path=Path(__file__), router_client=router
        )

        buy, sell, error = provider.quote_candidate(TOKEN, Decimal("0.01"))

        self.assertIsNone(error)
        self.assertIsNotNone(buy)
        self.assertIsNotNone(sell)
        self.assertEqual(len(router.calls), 2)
        self.assertEqual(router.calls[0]["outputToken"], STABLE)
        self.assertEqual(router.calls[0]["outputDecimals"], 6)
        self.assertEqual(router.calls[1]["inputToken"], STABLE)
        self.assertEqual(router.calls[1]["inputDecimals"], 6)

    def test_migrated_four_token_uses_pancakeswap_context_not_binance_hint(self) -> None:
        router = _Router()
        provider = BscReadOnlyQuoteProvider(
            rpc=_Rpc(migrated=True), helper_path=Path(__file__), router_client=router
        )

        buy, sell, error = provider.quote_candidate(TOKEN, Decimal("0.01"), {"protocol": 2002})

        self.assertIsNone(error)
        self.assertIsNotNone(buy)
        self.assertIsNotNone(sell)
        assert buy is not None and sell is not None
        self.assertEqual(buy.quote_source, "pancakeswap_quote")
        self.assertEqual(sell.quote_source, "pancakeswap_quote")
        self.assertEqual(len(router.calls), 2)

    def test_unrecognized_venue_has_dedicated_error_and_never_calls_router(self) -> None:
        class MissingContextRpc(_Rpc):
            def call_hex(self, _to, _data):
                return "0x"

        router = _Router()
        provider = BscReadOnlyQuoteProvider(
            rpc=MissingContextRpc(), helper_path=Path(__file__), router_client=router
        )
        buy, sell, error = provider.quote_candidate(TOKEN, Decimal("0.01"))

        self.assertIsNone(buy)
        self.assertIsNone(sell)
        self.assertEqual(error, "venue_unrecognized")
        self.assertEqual(router.calls, [])

    def test_flap_curve_uses_portal_context_and_read_only_quotes(self) -> None:
        provider = BscReadOnlyQuoteProvider(
            rpc=_FlapRpc(), helper_path=Path(__file__), router_client=_Router()
        )

        buy, sell, error = provider.quote_candidate(TOKEN, Decimal("0.01"), {"protocol": 2002})

        self.assertIsNone(error)
        assert buy is not None and sell is not None
        self.assertEqual(buy.quote_source, "flap_bonding_curve_quote")
        self.assertEqual(sell.quote_source, "flap_bonding_curve_quote")
        self.assertIn("venue:flap", buy.route)
        self.assertIn(f"flap_portal:{FLAP_PORTAL}", buy.route)
        self.assertIn("fundraising:native", buy.route)
        self.assertEqual(provider.metrics()["flap_recognized"], 1)
        self.assertEqual(provider.metrics()["flap_quote_success"], 1)

    def test_migrated_flap_uses_portal_pool_then_pancakeswap(self) -> None:
        router = _Router()
        provider = BscReadOnlyQuoteProvider(
            rpc=_FlapRpc(migrated=True), helper_path=Path(__file__), router_client=router
        )

        buy, sell, error = provider.quote_candidate(TOKEN, Decimal("0.01"))

        self.assertIsNone(error)
        assert buy is not None and sell is not None
        self.assertEqual(buy.quote_source, "pancakeswap_quote")
        self.assertIn(f"flap_pool:{PAIR}", buy.route)
        self.assertEqual(len(router.calls), 2)
        self.assertEqual(provider.metrics()["pancakeswap_quote_success"], 1)

    def test_flap_stable_context_uses_portal_asset_decimals(self) -> None:
        router = _Router()
        provider = BscReadOnlyQuoteProvider(
            rpc=_FlapRpc(quote=STABLE), helper_path=Path(__file__), router_client=router
        )

        buy, sell, error = provider.quote_candidate(TOKEN, Decimal("0.01"))

        self.assertIsNone(error)
        assert buy is not None and sell is not None
        self.assertIn(f"fundraising:{STABLE}", buy.route)
        self.assertIn("fundraising_decimals:6", buy.route)
        self.assertEqual(router.calls[0]["inputToken"], STABLE)
        self.assertEqual(router.calls[0]["inputDecimals"], 6)

    def test_flap_context_unavailable_never_falls_back_to_four_or_router(self) -> None:
        router = _Router()
        provider = BscReadOnlyQuoteProvider(
            rpc=_FlapRpc(missing_context=True), helper_path=Path(__file__), router_client=router
        )

        buy, sell, error = provider.quote_candidate(TOKEN, Decimal("0.01"))

        self.assertIsNone(buy)
        self.assertIsNone(sell)
        self.assertEqual(error, "flap_context_unavailable")
        self.assertEqual(router.calls, [])

    def test_bsc_context_errors_are_not_collapsed_to_generic_buy_quote_error(self) -> None:
        decision = BaselineStrategy(bsc_baseline_config()).evaluate_entry(
            EntryFeatures(
                token_age_sec=None,
                unique_buyers_15s=None,
                buy_sell_count_ratio_15s=None,
                net_buy_15s=None,
                flow_windows_non_negative=(None, None),
                creator_confirmed_sold=None,
                buy_quote=None,
                sell_quote=None,
                evaluated_at=datetime.now(timezone.utc),
                holders=200,
                market_cap_usd=Decimal("2000"),
                liquidity_usd=Decimal("200"),
                pricing_mode="bsc_executable_quote",
                pricing_error="fourmeme_context_unavailable",
            )
        )

        reasons = {check.reason_code for check in decision.checks}
        self.assertIn("fourmeme_context_unavailable", reasons)
        self.assertNotIn("buy_quote_unavailable", reasons)

    def test_unquoted_bsc_candidate_has_only_local_rejections(self) -> None:
        decision = BaselineStrategy(bsc_baseline_config()).evaluate_entry(
            EntryFeatures(
                token_age_sec=None,
                unique_buyers_15s=None,
                buy_sell_count_ratio_15s=None,
                net_buy_15s=None,
                flow_windows_non_negative=(None, None),
                creator_confirmed_sold=None,
                buy_quote=None,
                sell_quote=None,
                evaluated_at=datetime.now(timezone.utc),
                holders=1,
                market_cap_usd=Decimal("1"),
                liquidity_usd=Decimal("1"),
                pricing_mode="bsc_executable_quote",
                soft_features={"quote_requested": False},
            )
        )

        reasons = {check.reason_code for check in decision.checks}
        self.assertNotIn("buy_quote_unavailable", reasons)
        self.assertNotIn("sell_quote_unavailable", reasons)
