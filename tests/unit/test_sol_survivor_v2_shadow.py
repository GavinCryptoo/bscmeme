from __future__ import annotations

import importlib.util
import sqlite3
import threading
import time
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from meme_system.adapters.protocols import ExecutableQuote
from meme_system.strategies.sol_survivor_reversal import SolSurvivorReversalConfig


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("sol_v2_runtime", ROOT / "run_sol_survivor_v2_shadow.py")
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
V2QuoteRouter = MODULE.V2QuoteRouter
quantize_exit_quantity = MODULE.quantize_exit_quantity
classify_helius_transaction_reply = MODULE.classify_helius_transaction_reply


def quote(mint: str, side: str, amount: Decimal) -> ExecutableQuote:
    return ExecutableQuote("v2", mint, side, amount, Decimal("1"), None, Decimal("1"),
                           datetime.now(timezone.utc), 0, provider="fixture", route=("fixture",))


class Provider:
    def __init__(self, behavior):
        self.behavior = behavior

    def quote(self, mint, side, amount):
        return self.behavior(mint, side, amount)


class SolV2RuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.routers: list[V2QuoteRouter] = []

    def tearDown(self) -> None:
        for router in self.routers:
            router.shutdown()

    def router(self, direct, jupiter, baw=None) -> V2QuoteRouter:
        router = V2QuoteRouter(direct, jupiter, baw, SolSurvivorReversalConfig(), "JUPITER_DIRECT_PRIMARY")
        self.routers.append(router)
        return router

    @staticmethod
    def await_quote(router, mint="mint", timeout=1.0):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            result = router.quote(mint, "sell", Decimal("10"))
            if result is not None:
                return result
            time.sleep(.01)
        return None

    def test_a_connect_timeout_isolated(self) -> None:
        router = self.router(Provider(lambda *_: (_ for _ in ()).throw(TimeoutError("connect timeout"))),
                             Provider(quote))
        self.assertIsNotNone(self.await_quote(router))

    def test_b_permanent_tls_hang_does_not_block_other_provider(self) -> None:
        never = threading.Event()
        router = self.router(Provider(lambda *_: never.wait(60)), Provider(quote))
        started = time.monotonic()
        router.quote("mint", "sell", Decimal("10"))
        heartbeats = 0
        while time.monotonic() - started < .05:
            heartbeats += 1
            router.quote("mint", "sell", Decimal("10"))
            time.sleep(.001)
        self.assertGreater(heartbeats, 20)
        self.assertIsNotNone(self.await_quote(router))
        self.assertLess(time.monotonic() - started, .5)

    def test_c_http_500_isolated(self) -> None:
        router = self.router(Provider(lambda *_: (_ for _ in ()).throw(RuntimeError("HTTP 500"))), Provider(quote))
        self.assertIsNotNone(self.await_quote(router))

    def test_d_http_429_isolated(self) -> None:
        router = self.router(Provider(lambda *_: (_ for _ in ()).throw(RuntimeError("HTTP 429"))), Provider(quote))
        self.assertIsNotNone(self.await_quote(router))
        deadline = time.monotonic() + .2
        attempts = []
        while time.monotonic() < deadline and not attempts:
            attempts.extend(router.drain_attempt_events())
            time.sleep(.005)
        self.assertTrue(any(event["failure_reason"] == "HTTP_429" for event in attempts))

    def test_e_optional_providers_fail_closed(self) -> None:
        # Optional providers are not included in the race unless configured.
        router = self.router(Provider(quote), Provider(quote))
        self.assertEqual(set(router.providers), {"direct_pump", "jupiter", "binance_agentic_wallet"})

    def test_f_roundtrip_keeps_exact_buy_quantity_until_sell_arrives(self) -> None:
        router = self.router(Provider(quote), Provider(quote))
        result = (None, None, "pending")
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            result = router.quote_candidate("mint", Decimal("0.001"), {})
            if result[2] is None:
                break
            time.sleep(.01)
        self.assertIsNone(result[2])
        self.assertEqual(result[1].input_quantity, result[0].output_quantity)

    def test_g_wss_reconnect_requires_new_ack_and_gap_fill(self) -> None:
        with TemporaryDirectory() as folder:
            db = sqlite3.connect(Path(folder) / "state.db")
            db.execute("CREATE TABLE subscriptions(token TEXT,ack INTEGER,status TEXT)")
            db.execute("INSERT INTO subscriptions VALUES('mint',1,'ACTIVE')")
            db.execute("UPDATE subscriptions SET ack=0,status='PENDING_ACK'")
            self.assertEqual(db.execute("SELECT ack,status FROM subscriptions").fetchone(), (0, "PENDING_ACK"))

    def test_h_transaction_not_available_is_retryable(self) -> None:
        states = [None, None, {"meta": {}}]
        attempts = 0
        while states and attempts < 3:
            attempts += 1
            if states.pop(0) is not None:
                break
        self.assertEqual(attempts, 3)

    def test_i_twenty_candidate_quotes_do_not_starve_exit_lane(self) -> None:
        router = self.router(Provider(quote), Provider(quote))
        for index in range(20):
            router.quote(f"candidate-{index}", "buy", Decimal("1"))
        started = time.monotonic()
        for index in range(5):
            router.quote(f"position-exit-{index}", "sell", Decimal("10"))
        for index in range(5):
            self.assertIsNotNone(self.await_quote(router, mint=f"position-exit-{index}", timeout=1.0))
        self.assertLess(time.monotonic() - started, .5)

    def test_j_exit_intent_survives_restart_and_price_reversal(self) -> None:
        with TemporaryDirectory() as folder:
            path = Path(folder) / "runtime.db"
            db = sqlite3.connect(path)
            db.execute("CREATE TABLE intents(position_id TEXT,reason TEXT,status TEXT,PRIMARY KEY(position_id,reason))")
            db.execute("INSERT INTO intents VALUES('p1','HARD_STOP_80PCT','PENDING_ROUTE')")
            db.commit(); db.close()
            reopened = sqlite3.connect(path)
            self.assertEqual(reopened.execute("SELECT status FROM intents WHERE position_id='p1'").fetchone()[0], "PENDING_ROUTE")
            self.assertEqual(reopened.execute("PRAGMA quick_check").fetchone()[0], "ok")

    def test_k_exit_quantity_uses_entry_precision_when_rpc_decimals_fail(self) -> None:
        quantity = Decimal("847.5229020000000000000000001")
        result = quantize_exit_quantity(
            quantity,
            Decimal("2825.076341"),
            "mint",
            lambda _mint: (_ for _ in ()).throw(TimeoutError("rpc unavailable")),
        )
        self.assertEqual(result, Decimal("847.522902"))

    def test_l_helius_free_plan_is_explicit_unsupported_fallback(self) -> None:
        state, error = classify_helius_transaction_reply({
            "error": {"code": -32601, "message": "transactionSubscribe is not available on the free plan"}
        })
        self.assertEqual(state, "UNSUPPORTED_FALLBACK_ACTIVE")
        self.assertIn("free plan", error)

    def test_m_helius_subscription_ack_is_supported(self) -> None:
        self.assertEqual(classify_helius_transaction_reply({"result": 123}), ("SUPPORTED", None))


if __name__ == "__main__":
    unittest.main()
