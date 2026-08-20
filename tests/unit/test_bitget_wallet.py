from __future__ import annotations

import base64
import hashlib
import hmac
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from meme_system.adapters.bitget_wallet import (
    BITGET_WALLET_PROVIDER,
    BitgetApiResponse,
    BitgetExecutionJournal,
    BitgetNonceManager,
    BitgetWalletApiClient,
    BitgetWalletLiveExecutor,
    BitgetWalletRouteProvider,
    classify_bitget_failure,
    BitgetWalletError,
)


class _Response:
    status_code = 200
    headers = {"security-check": "a", "security-request-check": "b", "security-double-check": "00"}

    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class _Session:
    def __init__(self, payload):
        self.payload = payload
        self.last = None

    def post(self, url, *, data, headers, timeout):
        self.last = (url, data.decode(), headers, timeout)
        return _Response(self.payload)

    def close(self):
        pass


class BitgetWalletTests(unittest.TestCase):
    def test_official_request_signature_and_quote_normalization(self):
        payload = {"status": 0, "error_code": 0, "data": {"toAmount": "123", "market": "bgwaggregator", "slippage": "0.03", "priceImpact": "0.01", "fee": {"totalAmountInUsd": "0.02"}}}
        session = _Session(payload)
        client = BitgetWalletApiClient("key", "secret", session=session)
        provider = BitgetWalletRouteProvider(client, "0x1111111111111111111111111111111111111111")
        quote, failure = provider.quote_result("0x2222222222222222222222222222222222222222", "buy", Decimal("0.001"))
        self.assertIsNone(failure)
        self.assertEqual(quote.provider, BITGET_WALLET_PROVIDER)
        self.assertEqual(quote.output_quantity, Decimal("123"))
        _, body, headers, _ = session.last
        content = json.dumps({"apiPath": "/bgw-pro/swapx/order/getSwapPrice", "body": body, "x-api-key": "key", "x-api-timestamp": headers["x-api-timestamp"]}, separators=(",", ":"), sort_keys=True)
        expected = base64.b64encode(hmac.new(b"secret", content.encode(), hashlib.sha256).digest()).decode()
        self.assertEqual(headers["x-api-signature"], expected)

    def test_make_order_requires_response_verification(self):
        verified = []
        client = BitgetWalletApiClient("key", "secret", session=_Session({"status": 0, "error_code": 0, "data": {"orderId": "o"}}), response_verifier=lambda headers: verified.append(dict(headers)) or True)
        self.assertEqual(client.make_order({"x": 1}).payload["data"]["orderId"], "o")
        self.assertEqual(len(verified), 1)

    def test_response_verification_failure_is_fail_closed(self):
        client = BitgetWalletApiClient(
            "key",
            "secret",
            session=_Session({"status": 0, "error_code": 0, "data": {"orderId": "o"}}),
            response_verifier=lambda _headers: False,
        )
        with self.assertRaisesRegex(BitgetWalletError, "BITGET_RESPONSE_VERIFICATION_FAILED"):
            client.make_order({"x": 1})

    def test_error_80012_is_no_route(self):
        self.assertEqual(classify_bitget_failure(BitgetWalletError("BITGET_API_80012", "quote failed")), "NO_ROUTE")
        self.assertEqual(classify_bitget_failure(BitgetWalletError("BITGET_API_80020", "deviation")), "QUOTE_VALUE_DEVIATION")

    def test_execution_journal_blocks_active_duplicate_and_recovers_terminal(self):
        with tempfile.TemporaryDirectory() as temp:
            journal = BitgetExecutionJournal(Path(temp) / "execution.db")
            key = "buy:0x2222222222222222222222222222222222222222"
            self.assertIsNone(journal.claim(key, key[4:], "buy", Decimal("0.001")))
            self.assertEqual(journal.claim(key, key[4:], "buy", Decimal("0.001"))["state"], "CREATING")
            journal.update(logical_key=key, state="FAILED", error_code="NO_ROUTE")
            self.assertIsNone(journal.claim(key, key[4:], "buy", Decimal("0.001")))
            self.assertEqual(journal.connection.execute("PRAGMA quick_check").fetchone()[0], "ok")
            journal.close()

    def test_nonce_manager_uses_pending_baseline(self):
        class Eth:
            @staticmethod
            def get_transaction_count(_address, state):
                self.assertEqual(state, "pending")
                return 7
        class Web3:
            eth = Eth()
        manager = BitgetNonceManager(Web3(), "0xabc")
        manager.validate_and_reserve([7, 8], "order-a")
        with self.assertRaises(BitgetWalletError):
            manager.validate_and_reserve([7], "order-b")
        manager.release("order-a")

    def test_real_swap_is_disabled_by_default(self):
        with tempfile.TemporaryDirectory() as temp:
            executor = BitgetWalletLiveExecutor(
                BitgetWalletApiClient("key", "secret", session=_Session({"status": 0, "error_code": 0, "data": {}})),
                "0x" + "01".zfill(64),
                "http://127.0.0.1:1",
                swaps_enabled=False,
                journal_path=Path(temp) / "execution.db",
            )
            result = executor.buy("0x2222222222222222222222222222222222222222", Decimal("0.001"))
            self.assertEqual(result.stage, "SWAP_BLOCKED")
            executor.close()

    def test_persisted_active_buy_and_sell_block_duplicates(self):
        with tempfile.TemporaryDirectory() as temp:
            executor = BitgetWalletLiveExecutor(
                BitgetWalletApiClient("key", "secret", session=_Session({"status": 0, "error_code": 0, "data": {}})),
                "0x" + "02".zfill(64),
                "http://127.0.0.1:1",
                swaps_enabled=True,
                journal_path=Path(temp) / "execution.db",
            )
            token = "0x2222222222222222222222222222222222222222"
            executor.journal.claim(f"buy:{token}", token, "buy", Decimal("0.001"))
            executor.journal.claim(f"sell:{token}", token, "sell", Decimal("1"))
            buy = executor.buy(token, Decimal("0.001"))
            sell = executor.sell(token, Decimal("1"))
            self.assertEqual((buy.stage, buy.error_code), ("SWAP_UNKNOWN", "DUPLICATE_ORDER_BLOCKED"))
            self.assertEqual((sell.stage, sell.error_code), ("SWAP_UNKNOWN", "DUPLICATE_ORDER_BLOCKED"))
            executor.close()

    def test_order_success_requires_successful_chain_receipt(self):
        with tempfile.TemporaryDirectory() as temp:
            client = BitgetWalletApiClient("key", "secret", session=_Session({"status": 0, "error_code": 0, "data": {}}))
            executor = BitgetWalletLiveExecutor(
                client,
                "0x" + "03".zfill(64),
                "http://127.0.0.1:1",
                journal_path=Path(temp) / "execution.db",
            )
            token = "0x2222222222222222222222222222222222222222"
            executor.journal.claim(f"buy:{token}", token, "buy", Decimal("0.001"))
            executor.journal.bind_order(f"buy:{token}", "order-1", "SUBMITTED")
            tx_hash = "0x" + "12" * 32
            client.get_order = lambda _order_id: BitgetApiResponse(
                payload={"status": 0, "error_code": 0, "data": {"status": "success", "fromAmount": "0.001", "receiveAmount": "100", "txs": [{"stage": "source", "txId": tx_hash}]}},
                headers={},
                latency_ms=1,
            )
            class Eth:
                @staticmethod
                def get_transaction_receipt(_tx_hash):
                    return {"status": 1}
            class FakeWeb3:
                eth = Eth()
            executor.web3 = FakeWeb3()
            result = executor.get_order_status("order-1")
            self.assertEqual(result.stage, "SWAP_CONFIRMED")
            self.assertEqual(result.tx_hash, tx_hash)
            executor.close()


if __name__ == "__main__":
    unittest.main()
