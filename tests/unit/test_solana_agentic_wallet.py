from __future__ import annotations

import json
import subprocess
import unittest
from decimal import Decimal

from meme_system.adapters.solana_agentic_wallet import CommandOutput, SolanaAgenticWalletQuoteProvider


class SolanaAgenticWalletTests(unittest.TestCase):
    def test_quote_is_ct501_quote_only(self) -> None:
        commands = []
        def runner(command, _timeout):
            commands.append(tuple(command))
            return CommandOutput(0, json.dumps({"success": True, "data": {
                "fromCoinAmount": "0.001", "toCoinAmount": "1000", "priceImpactPct": "0.2", "route": "fixture"
            }}), "")
        provider = SolanaAgenticWalletQuoteProvider(command_runner=runner)
        quote, failure = provider.quote_result("mint", "buy", Decimal("0.001"))
        self.assertIsNone(failure)
        self.assertEqual(quote.output_quantity, Decimal("1000"))
        command = commands[0]
        self.assertIn("CT_501", command)
        self.assertIn("quote", command)
        self.assertNotIn("swap", command)

    def test_timeout_is_classified(self) -> None:
        def runner(_command, _timeout):
            raise subprocess.TimeoutExpired("baw", 1)
        quote, failure = SolanaAgenticWalletQuoteProvider(command_runner=runner).quote_result("mint", "buy", Decimal("0.001"))
        self.assertIsNone(quote)
        self.assertEqual(failure.reason, "QUOTE_TIMEOUT")


if __name__ == "__main__":
    unittest.main()
