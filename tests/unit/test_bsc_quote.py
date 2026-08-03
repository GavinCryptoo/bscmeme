from __future__ import annotations

import unittest
from decimal import Decimal
from pathlib import Path

from meme_system.adapters.bsc_quote import (
    BscReadOnlyQuoteProvider,
    _CALC_TRADING_FEE_SELECTOR,
    FOUR_MEME_TOKEN_MANAGER,
)


TOKEN = "0x1111111111111111111111111111111111111111"
PAIR = "0x2222222222222222222222222222222222222222"
ROUTER = "0x13f4ea83d0bd40e75c8222255bc855a974568dd4"
NATIVE = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"


class _Rpc:
    urls = ("https://rpc.example",)
    configured = True

    def call_uint(self, _to, _data):
        return 18


class BscReadOnlyQuoteTests(unittest.TestCase):
    def test_pancakeswap_candidate_requires_and_persists_two_real_quotes(self) -> None:
        provider = BscReadOnlyQuoteProvider(rpc=_Rpc(), helper_path=Path(__file__))

        def router(_mint, side, _amount_raw, _decimals):
            return {
                "outputRaw": "250000000000000000000" if side == "buy" else "9000000000000000",
                "priceImpactPct": "1.25",
                "routerAddress": ROUTER,
                "route": [{"type": "V2", "pools": [PAIR]}],
            }

        provider._router_request = router  # type: ignore[method-assign]
        buy, sell, error = provider.quote_candidate(
            TOKEN,
            Decimal("0.01"),
            {"pair_address": PAIR, "migrate_status": 1, "token_decimals": 18},
        )

        self.assertIsNone(error)
        self.assertIsNotNone(buy)
        self.assertIsNotNone(sell)
        assert buy is not None and sell is not None
        self.assertEqual(buy.quote_source, "pancakeswap_router")
        self.assertEqual(sell.quote_source, "pancakeswap_router")
        self.assertGreater(buy.output_quantity, 0)
        self.assertGreater(sell.output_quantity, 0)
        self.assertTrue(any(item == f"router:{ROUTER}" for item in buy.route))

    def test_four_bonding_curve_candidate_uses_curve_quote_for_both_sides(self) -> None:
        provider = BscReadOnlyQuoteProvider(rpc=_Rpc(), helper_path=Path(__file__))
        info = (TOKEN, NATIVE, 0, 0, 0, 0, 0, 0, 0, 10**18, 0, 0, 0)
        provider._four_token_info = lambda _mint, _curve: info  # type: ignore[method-assign]
        provider._four_fee = lambda _curve, _info, _amount: 1  # type: ignore[method-assign]
        provider._four_call_uint = lambda _curve, selector, _info, _amount: (  # type: ignore[method-assign]
            1 if selector == _CALC_TRADING_FEE_SELECTOR else 1000000000000000000
        )

        buy, sell, error = provider.quote_candidate(
            TOKEN,
            Decimal("0.01"),
            {"protocol": 2002, "migrate_status": 0, "pair_address": NATIVE, "token_decimals": 18},
        )

        self.assertIsNone(error)
        self.assertIsNotNone(buy)
        self.assertIsNotNone(sell)
        assert buy is not None and sell is not None
        self.assertEqual(buy.quote_source, "bonding_curve")
        self.assertEqual(sell.quote_source, "bonding_curve")
        self.assertIn(FOUR_MEME_TOKEN_MANAGER, buy.route)
