from __future__ import annotations

import unittest
from decimal import Decimal
from types import SimpleNamespace

from meme_system.adapters.bsc_wss import BSC_USDT_ADDRESS
from meme_system.adapters.quote_asset_usd import QuoteAssetUsdResolver


class QuoteAssetUsdResolverTests(unittest.TestCase):
    def test_uses_dynamic_stable_mark_not_a_fixed_one_dollar_assumption(self) -> None:
        asset = "0x" + "1" * 40

        class Rpc:
            def call_uint(self, _asset, _selector):
                return 18
            def call_hex(self, _asset, _selector):
                return None

        class Gmgn:
            def quote(self, token_in, token_out, _amount):
                if token_in == asset and token_out == BSC_USDT_ADDRESS:
                    return SimpleNamespace(success=True, output_amount=200 * 10**18)
                if token_in == BSC_USDT_ADDRESS:
                    # 1 USDT -> 1/500 BNB while BNB is $500: dynamically $1.
                    return SimpleNamespace(success=True, output_amount=2 * 10**15)
                return SimpleNamespace(success=False, output_amount=None)

        result = QuoteAssetUsdResolver(Rpc(), gmgn_provider=Gmgn()).resolve(56, asset, native_usd=Decimal("500"))
        self.assertTrue(result.fresh)
        self.assertEqual(result.symbol, None)
        self.assertEqual(result.price_usd, Decimal("200"))
        self.assertEqual(result.source, "GMGN_USDT_USD")


if __name__ == "__main__":
    unittest.main()
