from __future__ import annotations

import json
import subprocess
import time
import unittest
from decimal import Decimal

from meme_system.adapters.binance_agentic_wallet import (
    BINANCE_AGENTIC_WALLET_PROVIDER,
    NATIVE_BNB,
    AsyncRoundTripQuoteProvider,
    BinanceAgenticWalletLiveExecutor,
    BinanceAgenticWalletLiveExecutorError,
    BinanceAgenticWalletRouteProvider,
    BinancePrimaryWithFallbackRouteProvider,
    QuoteFailure,
    _CommandOutput,
    classify_baw_failure,
)
from meme_system.adapters.protocols import ExecutableQuote
from datetime import datetime, timezone


TOKEN = "0x1111111111111111111111111111111111111111"


def _sell_quote(*, provider: str = "DIRECT") -> ExecutableQuote:
    return ExecutableQuote(
        quote_id=f"{provider}:sell",
        mint=TOKEN,
        side="sell",
        input_quantity=Decimal("10"),
        output_quantity=Decimal("0.009"),
        route_fee=None,
        price_impact_pct=None,
        quoted_at=datetime.now(timezone.utc),
        age_ms=0,
        provider=provider,
        executable_style=True,
    )


class BinanceAgenticWalletRouteProviderTests(unittest.TestCase):
    def test_roundtrip_uses_quote_only_and_normalizes_amounts(self) -> None:
        commands: list[tuple[str, ...]] = []

        def runner(command, _timeout):
            commands.append(tuple(command))
            is_buy = "--fromToken" in command and command[command.index("--fromToken") + 1].startswith("0xeeee")
            payload = {"success": True, "data": {"fromCoinAmount": "0.01" if is_buy else "125", "toCoinAmount": "125" if is_buy else "0.009", "slippage": 0.04}}
            return _CommandOutput(0, json.dumps(payload), "")

        provider = BinanceAgenticWalletRouteProvider(command_runner=runner)
        buy, sell, error = provider.quote_candidate(TOKEN, Decimal("0.01"))

        self.assertIsNone(error)
        assert buy is not None and sell is not None
        self.assertEqual(buy.provider, BINANCE_AGENTIC_WALLET_PROVIDER)
        self.assertEqual(buy.output_quantity, Decimal("125"))
        self.assertEqual(sell.output_quantity, Decimal("0.009"))
        self.assertEqual(len(commands), 2)
        self.assertTrue(all("swap" not in command and "approve" not in command for command in commands))

    def test_timeout_is_classified_without_fallback(self) -> None:
        def runner(_command, _timeout):
            raise subprocess.TimeoutExpired("baw", 1)

        provider = BinanceAgenticWalletRouteProvider(command_runner=runner)
        quote, failure = provider.quote_result(TOKEN, "buy", Decimal("0.01"))
        self.assertIsNone(quote)
        assert failure is not None
        self.assertEqual(failure.reason, "REQUEST_TIMEOUT")

    def test_failure_categories_are_specific(self) -> None:
        self.assertEqual(classify_baw_failure(stdout='', stderr='HTTP 429 too many requests'), "RATE_LIMIT")
        self.assertEqual(classify_baw_failure(stdout='', stderr='Not logged in'), "SESSION_ERROR")
        self.assertEqual(classify_baw_failure(stdout='', stderr='security risk blocked'), "SECURITY_BLOCK")

    def test_async_bridge_returns_quote_after_pending_without_blocking(self) -> None:
        def runner(command, _timeout):
            is_buy = command[command.index("--fromToken") + 1].startswith("0xeeee")
            payload = {"success": True, "data": {"fromCoinAmount": "0.01" if is_buy else "5", "toCoinAmount": "5" if is_buy else "0.009", "slippage": 0.04}}
            return _CommandOutput(0, json.dumps(payload), "")

        base = BinanceAgenticWalletRouteProvider(command_runner=runner)
        async_provider = AsyncRoundTripQuoteProvider(base)
        try:
            buy, sell, error = async_provider.quote_candidate(TOKEN, Decimal("0.01"))
            self.assertIsNone(buy)
            self.assertIsNone(sell)
            self.assertEqual(error, "QUOTE_PENDING")
            for _ in range(50):
                time.sleep(0.01)
                buy, sell, error = async_provider.quote_candidate(TOKEN, Decimal("0.01"))
                if buy is not None and sell is not None:
                    break
            self.assertIsNotNone(buy)
            self.assertIsNotNone(sell)
            self.assertIsNone(error)
        finally:
            async_provider.close()


class BinanceAgenticWalletLiveExecutorTests(unittest.TestCase):
    def test_default_executor_blocks_swap_without_invoking_baw(self) -> None:
        commands: list[tuple[str, ...]] = []

        def runner(command, _timeout):
            commands.append(tuple(command))
            return _CommandOutput(0, '{"success": true, "data": {}}', '')

        executor = BinanceAgenticWalletLiveExecutor(command_runner=runner)
        result = executor.buy(TOKEN, Decimal("0.001"))
        self.assertEqual(result.stage, "SWAP_BLOCKED")
        self.assertEqual(result.error_code, "REAL_SWAP_DISABLED")
        self.assertEqual(commands, [])

    def test_preflight_is_read_only_and_detects_bsc(self) -> None:
        def runner(command, _timeout):
            command = tuple(command)
            if "chains" in command:
                payload = {"success": True, "data": [{"binanceChainId": "56", "name": "BSC"}]}
            elif "tx-lock" in command:
                payload = {"success": True, "data": {"status": "UNLOCKED"}}
            elif "settings" in command:
                payload = {"success": True, "data": {"dailyLimit": 50000, "abnormalTxnHandling": "AutoReject"}}
            elif "left-quota" in command:
                payload = {"success": True, "data": {"quotaLeft": 50000}}
            else:
                payload = {"success": True, "data": {"status": "CONNECTED"}}
            return _CommandOutput(0, json.dumps(payload), "")

        checks = BinanceAgenticWalletLiveExecutor(command_runner=runner).preflight()
        self.assertTrue(checks["bsc_supported"])
        self.assertTrue(checks["status"]["ok"])
        self.assertTrue(checks["tx_lock"]["ok"])
        self.assertFalse(checks["swap_commands"]["swap_enabled"])

    def test_submitted_order_is_not_treated_as_confirmed(self) -> None:
        commands: list[tuple[str, ...]] = []

        def runner(command, _timeout):
            command = tuple(command)
            commands.append(command)
            if "status" in command and "market-order" not in command:
                return _CommandOutput(0, '{"success": true, "data": {"status": "CONNECTED"}}', "")
            if "tx-lock" in command:
                return _CommandOutput(0, '{"success": true, "data": {"status": "UNLOCKED"}}', "")
            return _CommandOutput(0, '{"success": true, "data": {"orderId": "123"}}', "")

        executor = BinanceAgenticWalletLiveExecutor(command_runner=runner, swaps_enabled=True, security_precheck=lambda *_: True)
        result = executor.buy(TOKEN, Decimal("0.001"))
        self.assertEqual(result.stage, "SWAP_SUBMITTED")
        self.assertEqual(result.order_id, "123")
        self.assertTrue(result.needs_reconciliation)
        self.assertEqual(sum("market-order" in command and "swap" in command for command in commands), 1)

    def test_finished_order_is_confirmed_and_exposes_tx_hash(self) -> None:
        def runner(command, _timeout):
            self.assertIn("market-order", command)
            return _CommandOutput(
                0,
                json.dumps({"success": True, "data": {"list": [{"orderId": "123", "status": "FINISHED", "txHash": "0xabc"}]}}),
                "",
            )

        result = BinanceAgenticWalletLiveExecutor(command_runner=runner).get_order_status("123")
        self.assertEqual(result.stage, "SWAP_CONFIRMED")
        self.assertEqual(result.tx_hash, "0xabc")

    def test_find_recent_order_reconciles_accepted_pending_sell(self) -> None:
        requested_at = datetime.now(timezone.utc)

        def runner(command, _timeout):
            self.assertEqual(command[1:4], ("market-order", "list", "--json"))
            return _CommandOutput(
                0,
                json.dumps({"success": True, "data": {"list": [{
                    "orderId": "pending-1",
                    "fromToken": TOKEN,
                    "toToken": NATIVE_BNB,
                    "fromTokenQty": "10",
                    "toTokenActualQty": "0.0001",
                    "status": "PENDING",
                    "bookTime": requested_at.isoformat(),
                }]}}),
                "",
            )

        result = BinanceAgenticWalletLiveExecutor(command_runner=runner).find_recent_order(
            TOKEN, Decimal("10"), requested_at=requested_at
        )
        self.assertIsNotNone(result)
        self.assertEqual(result.stage, "SWAP_PENDING")
        self.assertEqual(result.order_id, "pending-1")

    def test_swap_timeout_is_unknown_and_never_retried(self) -> None:
        commands: list[tuple[str, ...]] = []

        def runner(command, _timeout):
            command = tuple(command)
            commands.append(command)
            if "status" in command and "market-order" not in command:
                return _CommandOutput(0, '{"success": true, "data": {"status": "CONNECTED"}}', "")
            if "tx-lock" in command:
                return _CommandOutput(0, '{"success": true, "data": {"status": "UNLOCKED"}}', "")
            if "settings" in command:
                return _CommandOutput(0, '{"success": true, "data": {"abnormalTxnHandling": "AutoReject"}}', "")
            raise subprocess.TimeoutExpired("baw", 1)

        executor = BinanceAgenticWalletLiveExecutor(command_runner=runner, swaps_enabled=True, security_precheck=lambda *_: True)
        result = executor.sell(TOKEN, Decimal("10"))
        self.assertEqual(result.stage, "SWAP_UNKNOWN")
        self.assertTrue(result.needs_reconciliation)
        self.assertFalse(result.retry_allowed)
        self.assertEqual(sum("market-order" in command and "swap" in command for command in commands), 1)

    def test_rejects_non_bsc_executor_configuration(self) -> None:
        with self.assertRaises(BinanceAgenticWalletLiveExecutorError):
            BinanceAgenticWalletLiveExecutor(chain_id="1")

    def test_exit_timeout_and_session_failure_fall_back_to_direct_quote(self) -> None:
        class Primary:
            def __init__(self, reason):
                self.reason = reason

            def quote_result(self, *_args):
                return None, QuoteFailure(self.reason)

        class Direct:
            def quote(self, mint, side, quantity):
                self.request = (mint, side, quantity)
                return _sell_quote()

        for failure in ("REQUEST_TIMEOUT", "SESSION_ERROR"):
            direct = Direct()
            route = BinancePrimaryWithFallbackRouteProvider(Primary(failure), direct)
            quote, error = route.quote_sell(TOKEN, Decimal("10"))
            self.assertIsNone(error)
            self.assertIsNotNone(quote)
            self.assertEqual(direct.request, (TOKEN, "sell", Decimal("10")))

    def test_async_sell_quote_returns_pending_without_holding_owner_loop(self) -> None:
        class SlowRoute:
            def quote_candidate(self, *_args):
                return None, None, "NOT_USED"

            def quote_sell(self, *_args):
                time.sleep(0.15)
                return _sell_quote(provider="BINANCE_AGENTIC_WALLET"), None

        async_provider = AsyncRoundTripQuoteProvider(SlowRoute())
        try:
            started = time.monotonic()
            quote, error = async_provider.sell_quote(TOKEN, Decimal("10"))
            self.assertIsNone(quote)
            self.assertEqual(error, "QUOTE_PENDING")
            self.assertLess(time.monotonic() - started, 0.05)
            for _ in range(50):
                time.sleep(0.01)
                quote, error = async_provider.sell_quote(TOKEN, Decimal("10"))
                if quote is not None:
                    break
            self.assertIsNotNone(quote)
            self.assertIsNone(error)
        finally:
            async_provider.close()
