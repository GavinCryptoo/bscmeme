from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from meme_system.adapters.gmgn_openapi import (
    BSC_NATIVE,
    GmgnCliLiveExecutor,
    GmgnCliQuoteProvider,
    GmgnCliRouteProvider,
    classify_gmgn_error,
)


class GmgnOpenApiTests(unittest.TestCase):
    def test_error_classification_is_specific(self):
        self.assertEqual(classify_gmgn_error("HTTP 429"), "RATE_LIMIT")
        self.assertEqual(classify_gmgn_error("no route found"), "NO_ROUTE")
        self.assertEqual(classify_gmgn_error("security risk"), "SECURITY_REJECT")
        self.assertEqual(classify_gmgn_error("HTTP 401 error=40101600"), "OTHER")

    @patch("subprocess.run")
    def test_quote_uses_cli_without_credentials_in_argv(self, run):
        run.return_value = subprocess.CompletedProcess([], 0, json.dumps({
            "output_amount": "100", "min_output_amount": "70", "slippage": 30,
            "tx": {"gas_limit": "1450000", "type": "launchpad", "token_launch_info": {"exchange": "flap"}},
        }), "")
        provider = GmgnCliQuoteProvider("0x2222222222222222222222222222222222222222")
        quote = provider.quote(BSC_NATIVE, "0x1111111111111111111111111111111111111111", 10**15)
        self.assertTrue(quote.success)
        argv = run.call_args.args[0]
        self.assertNotIn("GMGN_API_KEY", " ".join(argv))
        self.assertNotIn("GMGN_PRIVATE_KEY", " ".join(argv))
        self.assertEqual(quote.launch_exchange, "flap")

    @patch("subprocess.run")
    def test_invalid_quote_fails_closed(self, run):
        run.return_value = subprocess.CompletedProcess([], 0, '{"output_amount":"0","min_output_amount":"0"}', "")
        quote = GmgnCliQuoteProvider("0x2222222222222222222222222222222222222222").quote(BSC_NATIVE, "0x1111111111111111111111111111111111111111", 1)
        self.assertFalse(quote.success)
        self.assertEqual(quote.failure_reason, "INVALID_RESPONSE")

    @patch("subprocess.run")
    def test_gmgn_specific_error_code_is_preserved(self, run):
        run.return_value = subprocess.CompletedProcess([], 1, "", "HTTP 401 code=401 error=40101600")
        quote = GmgnCliQuoteProvider("0x2222222222222222222222222222222222222222").quote(BSC_NATIVE, "0x1111111111111111111111111111111111111111", 1)
        self.assertEqual(quote.failure_reason, "OTHER")
        self.assertEqual(quote.error_code, "40101600")

    @patch("subprocess.run")
    def test_route_provider_requires_bidirectional_quote(self, run):
        run.side_effect = [
            subprocess.CompletedProcess([], 0, json.dumps({"output_amount": "1000000000000000000", "min_output_amount": "700000000000000000", "slippage": 30}), ""),
            subprocess.CompletedProcess([], 0, json.dumps({"token": {"decimals": 18}}), ""),
            subprocess.CompletedProcess([], 0, json.dumps({"output_amount": "1000000000000000", "min_output_amount": "700000000000000", "slippage": 30}), ""),
        ]
        provider = GmgnCliRouteProvider("0x2222222222222222222222222222222222222222")
        buy, sell, error = provider.quote_candidate("0x1111111111111111111111111111111111111111", Decimal("0.001"))
        self.assertIsNone(error)
        self.assertIsNotNone(buy)
        self.assertIsNotNone(sell)
        self.assertEqual(buy.provider, "GMGN_CLI")
        token_info_argv = run.call_args_list[1].args[0]
        self.assertIn("--address", token_info_argv)
        self.assertNotIn("--token", token_info_argv)

    @patch("subprocess.run")
    def test_route_provider_prefers_bsc_erc20_decimals_and_caches_it(self, run):
        class Rpc:
            configured = True
            def __init__(self): self.calls = 0
            def call_uint(self, _token, _selector):
                self.calls += 1
                return 9

        run.side_effect = [
            subprocess.CompletedProcess([], 0, json.dumps({"output_amount": "1000000000", "min_output_amount": "700000000", "slippage": 30}), ""),
            subprocess.CompletedProcess([], 0, json.dumps({"output_amount": "1000000000000000", "min_output_amount": "700000000000000", "slippage": 30}), ""),
        ]
        rpc = Rpc()
        provider = GmgnCliRouteProvider("0x2222222222222222222222222222222222222222", rpc=rpc)
        buy, sell, error = provider.quote_candidate("0x1111111111111111111111111111111111111111", Decimal("0.001"))
        self.assertIsNone(error)
        self.assertIsNotNone(buy)
        self.assertIsNotNone(sell)
        self.assertEqual(rpc.calls, 1)
        self.assertEqual(run.call_count, 2)

    def test_live_executor_blocks_swap_without_explicit_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            executor = GmgnCliLiveExecutor(
                "0x2222222222222222222222222222222222222222",
                swaps_enabled=False,
                journal_path=Path(directory) / "gmgn_execution.db",
            )
            result = executor.buy("0x1111111111111111111111111111111111111111", Decimal("0.001"))
            executor.close()
        self.assertEqual(result.stage, "SWAP_BLOCKED")
        self.assertEqual(result.error_code, "REAL_SWAP_DISABLED")


if __name__ == "__main__":
    unittest.main()
