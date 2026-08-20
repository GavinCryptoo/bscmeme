from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from meme_system.adapters.binance_agentic_wallet import LiveSwapResult
from meme_system.live_dashboard import LiveDashboardError, LiveDashboardService
from meme_system.storage.database import initialize_database


class _FakeExecutor:
    def __init__(self) -> None:
        self.sell_calls = 0

    def preflight(self):
        return {
            "status": {"data": {"status": "CONNECTED"}},
            "chains": {"data": [{"binanceChainId": "56"}]},
            "bsc_supported": True,
            "tx_lock": {"data": {"status": "UNLOCKED"}},
            "left_quota": {"data": {"quotaLeft": 999}},
            "balance": {"data": [{"symbol": "BNB", "balance": "0.01"}]},
        }

    def get_balance(self, token=None):
        if token:
            return {"ok": True, "data": [{"address": token, "balance": "1000"}]}
        return {"ok": True, "data": []}

    def sell(self, token, quantity):
        self.sell_calls += 1
        return LiveSwapResult(stage="SWAP_PENDING", order_id="order-1", requested_at=datetime.now(timezone.utc))

    def get_order_status(self, order_id):
        return LiveSwapResult(stage="SWAP_PENDING", order_id=order_id)


class _FailingPreflightExecutor(_FakeExecutor):
    def preflight(self):
        raise AssertionError("status polling must not call provider preflight")


class _PartialFillExecutor(_FakeExecutor):
    def __init__(self) -> None:
        super().__init__()
        self.token_balance = Decimal("1000")

    def get_balance(self, token=None):
        if token:
            return {"ok": True, "data": [{"address": token, "balance": str(self.token_balance)}]}
        return {"ok": True, "data": []}

    def sell(self, token, quantity):
        self.sell_calls += 1
        self.token_balance -= quantity
        return LiveSwapResult(
            stage="SWAP_CONFIRMED",
            order_id="order-partial",
            requested_at=datetime.now(timezone.utc),
            input_quantity=quantity,
            output_quantity=Decimal("0.00025"),
        )


class _ZeroAfterSellExecutor(_FakeExecutor):
    def __init__(self) -> None:
        super().__init__()
        self.sold = False

    def get_balance(self, token=None):
        if token and self.sold:
            return {"ok": True, "data": []}
        return super().get_balance(token)

    def sell(self, token, quantity):
        self.sell_calls += 1
        self.sold = True
        return LiveSwapResult(
            stage="SWAP_CONFIRMED",
            order_id="order-zero",
            requested_at=datetime.now(timezone.utc),
            input_quantity=quantity,
            output_quantity=Decimal("0.0005"),
        )


class _FakeRoute:
    def quote_result(self, token, side, quantity):
        from meme_system.adapters.protocols import ExecutableQuote

        return ExecutableQuote(
            quote_id="quote-1", mint=token, side=side, input_quantity=quantity,
            output_quantity=Decimal("0.001"), route_fee=None, price_impact_pct=None,
            quoted_at=datetime.now(timezone.utc), age_ms=0, expires_at=None,
            provider="BINANCE_AGENTIC_WALLET",
        ), None


class LiveDashboardTests(unittest.TestCase):
    def test_status_uses_runtime_health_without_synchronous_provider_probe(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "runtime.db"
            health = root / "health.json"
            connection = initialize_database(db)
            connection.close()
            now = datetime.now(timezone.utc).isoformat()
            health.write_text(
                json.dumps(
                    {
                        "updated_at": now,
                        "items": {
                            "coordinator": {"state": "HEALTHY"},
                            "bitget_wallet": {"state": "HEALTHY", "updated_at": now},
                        },
                    }
                ),
                encoding="utf-8",
            )
            service = LiveDashboardService(
                db_path=db,
                health_path=health,
                executor=_FailingPreflightExecutor(),
                route_provider=_FakeRoute(),
                start_worker=False,
            )
            self.assertEqual(service.status()["bitget"], "CONNECTED")
            service.close()

    def test_background_balance_refresh_clears_stale_health_error(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "runtime.db"
            health = root / "health.json"
            connection = initialize_database(db)
            connection.close()
            now = datetime.now(timezone.utc).isoformat()
            health.write_text(
                json.dumps(
                    {
                        "updated_at": now,
                        "items": {
                            "coordinator": {"state": "HEALTHY"},
                            "bitget_wallet": {"state": "DEGRADED", "error_class": "BITGET_BNB_BALANCE_INSUFFICIENT", "updated_at": now},
                        },
                    }
                ),
                encoding="utf-8",
            )
            service = LiveDashboardService(db_path=db, health_path=health, executor=_FakeExecutor(), route_provider=_FakeRoute(), start_worker=False)
            service._wallet_snapshot = {"status": "CONNECTED", "bnb_balance": "0.0015", "error": None}
            self.assertIsNone(service.status()["wallet_error"])
            service.close()

    def test_positions_payload_and_duplicate_sell_protection(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "runtime.db"
            health = root / "health.json"
            connection = initialize_database(db)
            now = datetime.now(timezone.utc).isoformat()
            mint = "0x1111111111111111111111111111111111111111"
            connection.execute(
                "INSERT INTO survivor_candidates(mint,symbol,first_seen_at,last_seen_at,state,current_price_native,current_price_usd,ath_price_native,price_updated_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (mint, "TEST", now, now, "POSITION_OPEN", "0.000002", "0.001", "0.000003", now, now),
            )
            connection.execute(
                "INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,status,entry_price_native,current_price_native,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                ("p-1", mint, "TEST", now, "OPEN", "0.000001", "0.000002", "1000", "1000", "0.001", "0", now),
            )
            connection.commit(); connection.close()
            health.write_text(json.dumps({"updated_at": now, "items": {"coordinator": {"state": "HEALTHY"}, "bitget_wallet": {"state": "HEALTHY", "updated_at": now}}}), encoding="utf-8")
            executor = _FakeExecutor()
            service = LiveDashboardService(db_path=db, health_path=health, executor=executor, route_provider=_FakeRoute(), start_worker=False)
            self.assertEqual(service.positions()["count"], 1)
            status, result = service.request_sell("p-1", 25)
            self.assertEqual(status, 202)
            self.assertEqual(result["status"], "SWAP_PENDING")
            self.assertEqual(executor.sell_calls, 1)
            with self.assertRaisesRegex(LiveDashboardError, "a sell is already in flight"):
                service.request_sell("p-1", 25)
            service.close()

    def test_position_payload_uses_persisted_position_mark_not_candidate_price(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); db = root / "runtime.db"; health = root / "health.json"
            connection = initialize_database(db)
            now = datetime.now(timezone.utc).isoformat()
            mint = "0x7777777777777777777777777777777777777777"
            connection.execute(
                "INSERT INTO survivor_candidates(mint,symbol,first_seen_at,last_seen_at,state,current_price_native,current_price_usd,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                (mint, "TEST", now, now, "POSITION_OPEN", "9", "999", now),
            )
            connection.execute(
                "INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,status,entry_price_native,current_price_native,position_mark_price_native,position_mark_price_usd,position_mark_source,position_price_updated_at,position_price_freshness,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("mark-1", mint, "TEST", now, "OPEN", "1", "1", "2", "600", "GMGN_SELL_QUOTE", now, "FRESH", "10", "10", "10", "0", now),
            )
            connection.commit(); connection.close()
            service = LiveDashboardService(db_path=db, health_path=health, executor=_FakeExecutor(), route_provider=_FakeRoute(), start_worker=False)
            item = service.positions()["items"][0]
            self.assertEqual(item["current_price_native"], "2")
            self.assertEqual(item["current_price_usd"], "600")
            self.assertEqual(item["position_mark_source"], "GMGN_SELL_QUOTE")
            self.assertEqual(item["pnl_pct"], "100")
            service.close()

    def test_closed_positions_returns_persisted_entry_exit_snapshots(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); db = root / "runtime.db"; health = root / "health.json"
            connection = initialize_database(db)
            now = datetime.now(timezone.utc).isoformat()
            mint = "0x9999999999999999999999999999999999999999"
            connection.execute(
                "INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,closed_at,status,entry_price_native,exit_price_native,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,pnl_pct,entry_holders,exit_holders,entry_liquidity_usd,exit_liquidity_usd,entry_confirmed_at,exit_confirmed_at,actual_entry_spend_bnb,actual_exit_proceeds_bnb,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("p-closed", mint, "CLOSED", now, now, "CLOSED", "0.000001", "0.000002", "1000", "0", "0.001", "0.0018", "80", 10, 20, "1200", "2400", now, now, "0.001", "0.0018", now),
            )
            connection.commit(); connection.close()
            service = LiveDashboardService(db_path=db, health_path=health, executor=_FakeExecutor(), route_provider=_FakeRoute(), start_worker=False)
            payload = service.closed_positions()
            self.assertEqual(payload["count"], 1)
            item = payload["items"][0]
            self.assertEqual(item["entry_holders"], 10)
            self.assertEqual(item["exit_holders"], 20)
            self.assertEqual(item["entry_liquidity_usd"], "1200")
            self.assertEqual(item["exit_liquidity_usd"], "2400")
            self.assertEqual(item["pnl_bnb"], "0.0008")
            self.assertEqual(item["pnl_pct"], "80")
            service.close()

    def test_unknown_sell_state_blocks_retry(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); db = root / "runtime.db"; health = root / "health.json"
            connection = initialize_database(db); now = datetime.now(timezone.utc).isoformat(); mint = "0x2222222222222222222222222222222222222222"
            connection.execute("INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,status,entry_price_native,current_price_native,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", ("p-2",mint,"TEST",now,"OPEN","0.000001","0.000002","1000","1000","0.001","0",now)); connection.commit(); connection.close()
            health.write_text(json.dumps({"updated_at": now, "items": {"coordinator": {"state": "HEALTHY"}, "bitget_wallet": {"state": "HEALTHY", "updated_at": now}}}), encoding="utf-8")
            executor = _FakeExecutor(); executor.sell = lambda token, quantity: LiveSwapResult(stage="SWAP_UNKNOWN", needs_reconciliation=True)
            service = LiveDashboardService(db_path=db, health_path=health, executor=executor, route_provider=_FakeRoute(), start_worker=False)
            status, _ = service.request_sell("p-2", 100); self.assertEqual(status, 202)
            with self.assertRaisesRegex(LiveDashboardError, "a sell is already in flight"):
                service.request_sell("p-2", 100)
            self.assertFalse(service.positions()["items"][0]["buttons_enabled"])
            service.close()

    def test_failed_intent_can_be_explicitly_retried(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); db = root / "runtime.db"; health = root / "health.json"
            connection = initialize_database(db); now = datetime.now(timezone.utc).isoformat(); mint = "0x4444444444444444444444444444444444444444"
            connection.execute("INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,status,entry_price_native,current_price_native,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,exit_swap_status,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", ("p-4",mint,"TEST",now,"OPEN","0.000001","0.000002","1000","1000","0.001","0","SWAP_FAILED",now))
            connection.execute("INSERT INTO runtime_state(mode,state_key,value_json,updated_at) VALUES('live','exit_intent:p-4',?,?)", (json.dumps({"state":"EXIT_TRIGGERED_WAITING_ROUTE","reason":"TIME_STOP","quantity":"1000"}), now))
            connection.commit(); connection.close()
            health.write_text(json.dumps({"updated_at": now, "items": {"coordinator": {"state": "HEALTHY"}, "bitget_wallet": {"state": "HEALTHY", "updated_at": now}}}), encoding="utf-8")
            executor = _FakeExecutor()
            service = LiveDashboardService(db_path=db, health_path=health, executor=executor, route_provider=_FakeRoute(), start_worker=False)
            status, result = service.request_sell("p-4", 25)
            self.assertEqual(status, 202)
            self.assertEqual(result["status"], "SWAP_PENDING")
            service.close()

    def test_partial_fill_syncs_actual_remaining_wallet_balance(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = root / "runtime.db"
            health = root / "health.json"
            connection = initialize_database(db)
            now = datetime.now(timezone.utc).isoformat()
            mint = "0x3333333333333333333333333333333333333333"
            connection.execute(
                "INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,status,entry_price_native,current_price_native,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                ("p-3", mint, "TEST", now, "OPEN", "0.000001", "0.000002", "1000", "1000", "0.001", "0", now),
            )
            connection.commit()
            connection.close()
            health.write_text(json.dumps({"updated_at": now, "items": {"coordinator": {"state": "HEALTHY"}, "bitget_wallet": {"state": "HEALTHY", "updated_at": now}}}), encoding="utf-8")
            executor = _PartialFillExecutor()
            service = LiveDashboardService(db_path=db, health_path=health, executor=executor, route_provider=_FakeRoute(), start_worker=False)
            status, result = service.request_sell("p-3", 25)
            self.assertEqual(status, 200)
            self.assertEqual(result["status"], "FINISHED")
            with service._connection(read_only=True) as connection:
                row = connection.execute("SELECT status,remaining_quantity_token,realized_bnb FROM survivor_positions WHERE position_id='p-3'").fetchone()
            self.assertEqual(row["status"], "OPEN")
            self.assertEqual(Decimal(row["remaining_quantity_token"]), Decimal("750"))
            self.assertEqual(Decimal(row["realized_bnb"]), Decimal("0.00025"))
            service.close()

    def test_full_fill_with_missing_token_row_closes_position(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); db = root / "runtime.db"; health = root / "health.json"
            connection = initialize_database(db); now = datetime.now(timezone.utc).isoformat(); mint = "0x5555555555555555555555555555555555555555"
            connection.execute("INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,status,entry_price_native,current_price_native,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", ("p-5",mint,"TEST",now,"OPEN","0.000001","0.000002","1000","1000","0.001","0",now)); connection.commit(); connection.close()
            health.write_text(json.dumps({"updated_at": now, "items": {"coordinator": {"state": "HEALTHY"}, "bitget_wallet": {"state": "HEALTHY", "updated_at": now}}}), encoding="utf-8")
            executor = _ZeroAfterSellExecutor()
            service = LiveDashboardService(db_path=db, health_path=health, executor=executor, route_provider=_FakeRoute(), start_worker=False)
            status, result = service.request_sell("p-5", 100)
            self.assertEqual(status, 200)
            self.assertEqual(result["status"], "FINISHED")
            with service._connection(read_only=True) as connection:
                row = connection.execute("SELECT status,remaining_quantity_token,realized_bnb FROM survivor_positions WHERE position_id='p-5'").fetchone()
            self.assertEqual(row["status"], "CLOSED")
            self.assertEqual(Decimal(row["remaining_quantity_token"]), Decimal("0"))
            self.assertEqual(Decimal(row["realized_bnb"]), Decimal("0.0005"))
            service.close()


if __name__ == "__main__":
    unittest.main()
