from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from meme_system.adapters.pump_readonly import PumpMarketState, PumpSwapPoolState, SOL_MINT
from meme_system.adapters.solana_price import SolanaPriceBinding
from meme_system.adapters.solana_swap_flow import parse_pumpswap_transaction


NOW = datetime(2026, 8, 9, tzinfo=timezone.utc)
MINT = "Er4q21XgvtaSRq3vpJYRzn2Vy64ezcVZ7XFfeNhZpump"
POOL = "Pool111"
BASE = "BaseVault111"
QUOTE = "QuoteVault111"


def _binding(quote_mint: str = SOL_MINT) -> SolanaPriceBinding:
    pool = PumpSwapPoolState(MINT, POOL, quote_mint, BASE, QUOTE, 1, 0, 100, "PUMPSWAP_READY")
    state = PumpMarketState(MINT, "PUMPSWAP_READY", None, pool, 100)
    return SolanaPriceBinding(MINT, "pumpswap_pool", POOL, (POOL, BASE, QUOTE), state)


def _balance(index: int, mint: str, amount: int, decimals: int) -> dict[str, object]:
    return {"accountIndex": index, "mint": mint, "uiTokenAmount": {"amount": str(amount), "decimals": decimals}}


def _tx(*, base_pre: int, base_post: int, quote_pre: int, quote_post: int) -> dict[str, object]:
    return {
        "slot": 100,
        "transaction": {"message": {"accountKeys": ["User111", BASE, QUOTE]}},
        "meta": {
            "err": None,
            "preTokenBalances": [_balance(1, MINT, base_pre, 6), _balance(2, SOL_MINT, quote_pre, 9)],
            "postTokenBalances": [_balance(1, MINT, base_post, 6), _balance(2, SOL_MINT, quote_post, 9)],
        },
    }


class SolanaSwapFlowTests(unittest.TestCase):
    def parse(self, tx):
        return parse_pumpswap_transaction(
            tx, signature="Sig111", binding=_binding(), detected_at=NOW,
            tx_fetched_at=NOW + timedelta(milliseconds=10),
            parse_finished_at=NOW + timedelta(milliseconds=11),
        )

    def test_buy_is_user_quote_down_candidate_up(self) -> None:
        parsed = self.parse(_tx(base_pre=1_000_000_000, base_post=900_000_000, quote_pre=10_000_000_000, quote_post=11_000_000_000))
        self.assertEqual(parsed.direction, "BUY")
        self.assertGreater(parsed.candidate_delta, 0)
        self.assertLess(parsed.quote_delta, 0)
        self.assertEqual(parsed.confidence, "HIGH")

    def test_sell_is_user_candidate_down_quote_up(self) -> None:
        parsed = self.parse(_tx(base_pre=900_000_000, base_post=1_000_000_000, quote_pre=11_000_000_000, quote_post=10_000_000_000))
        self.assertEqual(parsed.direction, "SELL")
        self.assertLess(parsed.candidate_delta, 0)
        self.assertGreater(parsed.quote_delta, 0)

    def test_non_opposing_deltas_stay_unknown(self) -> None:
        parsed = self.parse(_tx(base_pre=900_000_000, base_post=1_000_000_000, quote_pre=10_000_000_000, quote_post=11_000_000_000))
        self.assertEqual(parsed.direction, "UNKNOWN")
        self.assertEqual(parsed.error_class, "NON_OPPOSING_BALANCE_DELTAS")

    def test_unsupported_quote_stays_unknown(self) -> None:
        parsed = parse_pumpswap_transaction(
            _tx(base_pre=1, base_post=2, quote_pre=2, quote_post=1),
            signature="Sig222", binding=_binding("UnsupportedMint"), detected_at=NOW,
            tx_fetched_at=NOW, parse_finished_at=NOW,
        )
        self.assertEqual(parsed.direction, "UNKNOWN")
        self.assertEqual(parsed.error_class, "QUOTE_MINT_UNSUPPORTED")


if __name__ == "__main__":
    unittest.main()
