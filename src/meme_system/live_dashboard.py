"""Small loopback-only monitor and manual-exit API for BSC Balanced Live.

The page is intentionally separate from the read-only Paper/Shadow dashboard.
It reads only the isolated Live database and routes every manual sell through
the same Bitget Wallet Order Mode executor used by the runtime. The browser
never receives API credentials or the local signing key.
"""

from __future__ import annotations

import json
import mimetypes
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import unquote, urlparse

from meme_system.adapters.binance_agentic_wallet import (
    LiveSwapResult,
)
from meme_system.adapters.bitget_wallet import BitgetWalletLiveExecutor


ACTIVE_SELL_STATES = frozenset(
    {
        "MANUAL_SUBMITTING",
        "SWAP_SUBMITTING",
        "SWAP_SUBMITTED",
        "SWAP_PENDING",
        "WAITING_CONFIRMATION",
        "SWAP_UNKNOWN",
        "BALANCE_SYNC_PENDING",
    }
)
FINAL_SELL_STATES = frozenset({"SWAP_FAILED", "SWAP_CONFIRMED"})
PRICE_STALE_MS = 15_000
PROCESS_HEALTH_STALE_SEC = 15.0


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _decimal(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _json(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _short(value: object, prefix: int = 8, suffix: int = 6) -> str | None:
    raw = str(value or "")
    if not raw:
        return None
    if len(raw) <= prefix + suffix + 3:
        return raw
    return f"{raw[:prefix]}...{raw[-suffix:]}"


class LiveDashboardError(RuntimeError):
    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


class LiveDashboardService:
    """Threaded local API with a deliberately tiny surface area."""

    def __init__(
        self,
        *,
        db_path: Path,
        health_path: Path,
        host: str = "127.0.0.1",
        port: int = 8791,
        baw_binary: str = "baw",
        executor: Any | None = None,
        route_provider: Any | None = None,
        start_worker: bool = True,
    ) -> None:
        if host != "127.0.0.1":
            raise ValueError("Live Dashboard must bind to 127.0.0.1")
        self.db_path = Path(db_path)
        self.health_path = Path(health_path)
        self.host = host
        self.port = int(port)
        del baw_binary  # retained only for compatibility with older launchers
        self.executor = executor or BitgetWalletLiveExecutor.from_env(swaps_enabled=True)
        self.route_provider = route_provider or self.executor
        self._db_lock = threading.RLock()
        self._executor_lock = threading.Lock()
        self._stop = threading.Event()
        self._last_wallet_probe = 0.0
        self._wallet_snapshot: dict[str, Any] = {}
        self._worker: threading.Thread | None = None
        if start_worker:
            self._worker = threading.Thread(target=self._poll_loop, name="live-dashboard-exit-poller", daemon=True)
            self._worker.start()

    # ---------- database helpers ----------

    def _connection(self, *, read_only: bool = False) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path, timeout=5.0, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        if read_only:
            connection.execute("PRAGMA query_only=ON")
        return connection

    @staticmethod
    def _read_health(path: Path) -> dict[str, Any]:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            return {}
        return value if isinstance(value, dict) else {}

    def _read_state(self, connection: sqlite3.Connection, key: str) -> dict[str, Any]:
        row = connection.execute(
            "SELECT value_json FROM runtime_state WHERE mode='live' AND state_key=?",
            (key,),
        ).fetchone()
        return _json(row[0]) if row else {}

    def _write_state(self, connection: sqlite3.Connection, key: str, payload: Mapping[str, Any], now: datetime) -> None:
        connection.execute(
            "INSERT INTO runtime_state(mode,state_key,value_json,updated_at) VALUES('live',?,?,?) "
            "ON CONFLICT(mode,state_key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at",
            (key, json.dumps(dict(payload), ensure_ascii=False, sort_keys=True), now.isoformat()),
        )

    @staticmethod
    def _record_audit(connection: sqlite3.Connection, event_type: str, payload: Mapping[str, Any], now: datetime) -> None:
        event_id = f"live-dashboard:{event_type}:{now.timestamp()}:{threading.get_ident()}"
        connection.execute(
            "INSERT OR IGNORE INTO audit_events(event_id,mode,event_type,occurred_at,payload_json) VALUES(?,?,?,?,?)",
            (event_id, "live", event_type, now.isoformat(), json.dumps(dict(payload), ensure_ascii=False, sort_keys=True)),
        )

    def _active_intent(self, connection: sqlite3.Connection, position_id: str) -> dict[str, Any]:
        intent = self._read_state(connection, f"live_exit_intent:{position_id}")
        if intent.get("state"):
            return intent
        return self._read_state(connection, f"exit_intent:{position_id}")

    def _append_manual_history(self, connection: sqlite3.Connection, position_id: str, item: Mapping[str, Any], now: datetime) -> None:
        current = self._read_state(connection, f"manual_sell_history:{position_id}")
        history = current.get("items") if isinstance(current.get("items"), list) else []
        history = [*history, dict(item)][-100:]
        self._write_state(connection, f"manual_sell_history:{position_id}", {"items": history}, now)

    # ---------- wallet/health ----------

    @staticmethod
    def _balance_items(payload: object) -> list[Mapping[str, Any]]:
        data = payload.get("data") if isinstance(payload, dict) else payload
        if isinstance(data, dict):
            data = data.get("balances") or data.get("list") or [data]
        return [item for item in data if isinstance(item, Mapping)] if isinstance(data, (list, tuple)) else []

    def _token_balance(self, token: str) -> Decimal | None:
        with self._executor_lock:
            response = self.executor.get_balance(token)
        if not response.get("ok"):
            return None
        wanted = token.lower()
        for item in self._balance_items(response):
            address = str(item.get("tokenAddress") or item.get("address") or "").lower()
            if address and address != wanted:
                continue
            value = _decimal(item.get("tokenAmount") or item.get("balance") or item.get("available") or item.get("free"))
            if value is not None:
                return max(Decimal("0"), value)
        # A successful wallet balance response that contains no row for this
        # token means the balance is zero.  Treating it as unavailable leaves
        # a confirmed full sell stuck in BALANCE_SYNC_PENDING forever and
        # prevents the dashboard/runtime from closing the position.
        return Decimal("0")

    def _refresh_wallet_balance_snapshot(self) -> None:
        """Refresh only the BNB balance off the HTTP request path.

        The dashboard must reflect deposits without making every browser
        refresh wait for a full provider preflight. This worker performs a
        read-only balance call periodically and keeps the cached connection
        state supplied by the runtime health file.
        """
        try:
            with self._executor_lock:
                response = self.executor.get_balance()
            if not response.get("ok"):
                return
            bnb = Decimal("0")
            for item in self._balance_items(response):
                symbol = str(item.get("symbol") or item.get("tokenSymbol") or "").upper()
                address = str(item.get("tokenAddress") or item.get("address") or "").lower()
                if symbol not in {"BNB", "WBNB"} and address not in {
                    "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                    "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
                }:
                    continue
                bnb = _decimal(item.get("available") or item.get("balance") or item.get("free") or item.get("amount")) or Decimal("0")
                break
            self._wallet_snapshot = {
                **self._wallet_snapshot,
                "bnb_balance": str(bnb),
                "probed_at": _now().isoformat(),
                "error": None,
            }
        except Exception:
            # Keep the last known balance; the status endpoint remains fast
            # and the sell endpoint still performs its own live preflight.
            return

    def _wallet_probe(self, *, force: bool = False) -> dict[str, Any]:
        now = time.monotonic()
        if not force and now - self._last_wallet_probe < 10 and self._wallet_snapshot:
            return self._wallet_snapshot
        try:
            with self._executor_lock:
                preflight = self.executor.preflight()
            legacy_status = _json(_json(preflight.get("status")).get("data")).get("status")
            status = "CONNECTED" if preflight.get("ok") or legacy_status == "CONNECTED" else "UNCONNECTED"
            bnb = _decimal(preflight.get("bnb_balance")) or Decimal("0")
            if not bnb:
                balance_data = _json(preflight.get("balance")).get("data")
                for item in self._balance_items(balance_data):
                    if str(item.get("symbol") or item.get("tokenSymbol") or "").upper() in {"BNB", "WBNB"}:
                        bnb = _decimal(item.get("available") or item.get("balance") or item.get("free") or item.get("amount")) or Decimal("0")
                        break
            self._wallet_snapshot = {
                "status": status,
                "bsc_supported": bool(preflight.get("bsc_supported")),
                "bnb_balance": str(bnb),
                "wallet_address": preflight.get("wallet_address"),
                "error": preflight.get("error_class"),
                "probed_at": _now().isoformat(),
            }
        except Exception as exc:  # dashboard remains usable when Bitget is down
            self._wallet_snapshot = {"status": "UNCONNECTED", "error": type(exc).__name__}
        self._last_wallet_probe = now
        return self._wallet_snapshot

    def _seed_wallet_from_health(self, wallet_health: Mapping[str, Any]) -> None:
        """Use the runtime's last preflight before making a slow CLI call."""

        if self._wallet_snapshot:
            return
        details = wallet_health.get("details") if isinstance(wallet_health, Mapping) else None
        preflight = details.get("preflight") if isinstance(details, Mapping) else None
        if not isinstance(preflight, Mapping):
            return
        bnb = _decimal(preflight.get("bnb_balance")) or Decimal("0")
        self._wallet_snapshot = {
            "status": "CONNECTED" if preflight.get("ok") else "UNCONNECTED",
            "bsc_supported": bool(preflight.get("bsc_supported")),
            "bnb_balance": str(bnb),
            "wallet_address": preflight.get("wallet_address"),
            "probed_at": wallet_health.get("updated_at") or _now().isoformat(),
        }
        self._last_wallet_probe = time.monotonic()

    # ---------- payloads ----------

    def _runtime_running(self, health: Mapping[str, Any]) -> bool:
        updated = health.get("updated_at")
        if not isinstance(updated, str):
            return False
        try:
            parsed = datetime.fromisoformat(updated.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return (_now() - parsed.astimezone(timezone.utc)).total_seconds() <= PROCESS_HEALTH_STALE_SEC
        except ValueError:
            return False

    def status(self) -> dict[str, Any]:
        health = self._read_health(self.health_path)
        items = health.get("items") if isinstance(health.get("items"), dict) else {}
        wallet_health = items.get("bitget_wallet") if isinstance(items.get("bitget_wallet"), dict) else items.get("binance_agentic_wallet") if isinstance(items.get("binance_agentic_wallet"), dict) else {}
        loop_health = items.get("coordinator") if isinstance(items.get("coordinator"), dict) else items.get("survivor_balanced", {})
        survivor_health = items.get("survivor_balanced") if isinstance(items.get("survivor_balanced"), dict) else {}
        runtime_running = self._runtime_running(health)
        self._seed_wallet_from_health(wallet_health)
        # Status polling must never synchronously invoke the provider. The
        # runtime already publishes its last preflight in health.json; using
        # that snapshot keeps the 2-second dashboard refresh responsive even
        # when Binance is slow or unavailable.  A stopped/stale runtime is
        # fail-closed for manual actions until a fresh runtime health snapshot
        # is available.  The sell endpoint still performs its own live
        # preflight immediately before an order.
        if runtime_running:
            wallet = self._wallet_snapshot or {
                "status": "CONNECTED" if wallet_health.get("state") in {"HEALTHY", "DEGRADED"} else "UNCONNECTED",
                "bnb_balance": None,
                "probed_at": health.get("updated_at") or _now().isoformat(),
            }
        else:
            wallet = {
                "status": "UNCONNECTED",
                "bnb_balance": self._wallet_snapshot.get("bnb_balance"),
                "probed_at": health.get("updated_at") or _now().isoformat(),
                "error": "RUNTIME_STOPPED",
            }
        if wallet_health.get("state") in {"UNAVAILABLE", "ERROR"}:
            wallet = {**wallet, "status": "UNCONNECTED", "error": wallet_health.get("error_class") or "BITGET_UNAVAILABLE"}
        with self._connection(read_only=True) as connection:
            open_count = int(connection.execute("SELECT COUNT(*) FROM survivor_positions WHERE status='OPEN'").fetchone()[0])
            pending_count = int(connection.execute(
                "SELECT COUNT(*) FROM survivor_positions WHERE status='OPEN' AND exit_swap_status IN "
                "('MANUAL_SUBMITTING','SWAP_SUBMITTED','SWAP_PENDING','WAITING_CONFIRMATION','SWAP_UNKNOWN','BALANCE_SYNC_PENDING')"
            ).fetchone()[0])
            state_row = connection.execute(
                "SELECT value_json FROM runtime_state WHERE mode='live' AND state_key='survivor_balanced_v1'"
            ).fetchone()
        try:
            runtime_status = json.loads(str(state_row[0])) if state_row is not None else {}
        except (TypeError, ValueError, json.JSONDecodeError):
            runtime_status = {}
        capacity_details = survivor_health.get("details") if isinstance(survivor_health.get("details"), Mapping) else {}
        capacity = capacity_details.get("entry_capacity") if isinstance(capacity_details.get("entry_capacity"), Mapping) else runtime_status.get("entry_capacity") if isinstance(runtime_status.get("entry_capacity"), Mapping) else {}
        max_open = int(capacity.get("max_open_positions") or os.environ.get("BSC_BALANCED_LIVE_MAX_OPEN_POSITIONS", "2"))
        buy_reserved = int(capacity.get("buy_reserved") or 0)
        used_slots = int(capacity.get("used_slots") or (open_count + buy_reserved))
        available_slots = max(0, int(capacity.get("available_slots") if capacity.get("available_slots") is not None else max_open - used_slots))
        wallet_state = str(wallet.get("status") or wallet_health.get("state") or "UNCONNECTED")
        if wallet_state == "CONNECTED" and wallet_health.get("state") == "DEGRADED":
            wallet_state = "CONNECTED"
        wallet_error = wallet.get("error") or wallet_health.get("error_class")
        if wallet_error == "BITGET_BNB_BALANCE_INSUFFICIENT":
            details = wallet_health.get("details") if isinstance(wallet_health.get("details"), Mapping) else {}
            minimum = _decimal(details.get("minimum_live_balance_bnb")) or _decimal(os.environ.get("BSC_LIVE_TRADE_AMOUNT_BNB", "0.001")) or Decimal("0.001")
            current_balance = _decimal(wallet.get("bnb_balance")) or Decimal("0")
            if current_balance >= minimum:
                wallet_error = None
        return {
            "live": "RUNNING" if runtime_running else "STOPPED",
            "bitget": wallet_state,
            "wallet_address": wallet.get("wallet_address"),
            "bnb_balance": wallet.get("bnb_balance"),
            "positions": open_count,
            "max_open_positions": max_open,
            "buy_reserved": buy_reserved,
            "used_slots": used_slots,
            "available_slots": available_slots,
            "pending_exits": pending_count,
            "main_loop": "HEALTHY" if loop_health.get("state") == "HEALTHY" and runtime_running else "DEGRADED",
            "last_updated": health.get("updated_at") or wallet.get("probed_at") or _now().isoformat(),
            "wallet_error": wallet_error,
        }

    def _position_row(self, row: sqlite3.Row, intent: Mapping[str, Any]) -> dict[str, Any]:
        quantity = _decimal(row["remaining_quantity_token"]) or Decimal("0")
        entry = _decimal(row["actual_entry_price_native"]) or _decimal(row["entry_price_native"]) or Decimal("0")
        # Live position marks are owner-persisted facts. Candidate prices are
        # discovery/strategy state and must not be the sole PnL input.
        current_native = _decimal(row["position_mark_price_native"]) or _decimal(row["current_price_native"])
        current_usd = _decimal(row["position_mark_price_usd"])
        candidate = None
        with self._connection(read_only=True) as connection:
            candidate = connection.execute(
                "SELECT ath_price_native,ath_price_usd "
                "FROM survivor_candidates WHERE lower(mint)=lower(?)",
                (row["mint"],),
            ).fetchone()
        value_bnb = quantity * current_native if current_native is not None else None
        value_usd = quantity * current_usd if current_usd is not None else None
        invested = _decimal(row["invested_bnb"]) or Decimal("0")
        realized = _decimal(row["realized_bnb"]) or Decimal("0")
        pnl_bnb = realized + value_bnb - invested if value_bnb is not None else None
        pnl_pct = (pnl_bnb / invested * Decimal("100")) if pnl_bnb is not None and invested > 0 else None
        ath = _decimal(candidate["ath_price_native"]) if candidate is not None else None
        if ath is None and candidate is not None:
            ath = _decimal(candidate["ath_price_usd"])
        max_gain = (ath / entry - Decimal("1")) * Decimal("100") if ath is not None and entry > 0 else None
        opened = str(row["opened_at"])
        try:
            opened_dt = datetime.fromisoformat(opened.replace("Z", "+00:00"))
            if opened_dt.tzinfo is None:
                opened_dt = opened_dt.replace(tzinfo=timezone.utc)
            hold_sec = max(0, int((_now() - opened_dt.astimezone(timezone.utc)).total_seconds()))
        except ValueError:
            hold_sec = None
        swap_status = str(row["exit_swap_status"] or "")
        if swap_status in {"SWAP_SUBMITTED", "SWAP_PENDING", "MANUAL_SUBMITTING", "BALANCE_SYNC_PENDING"}:
            display_status = "SELL_PENDING"
        elif swap_status == "WAITING_CONFIRMATION":
            display_status = "WAITING_CONFIRMATION"
        elif swap_status == "SWAP_UNKNOWN":
            display_status = "SWAP_UNKNOWN"
        elif intent.get("state") == "EXIT_TRIGGERED_WAITING_ROUTE":
            display_status = "EXIT_TRIGGERED"
        else:
            display_status = "OPEN"
        updated_raw = row["position_price_updated_at"]
        try:
            updated_at = datetime.fromisoformat(str(updated_raw).replace("Z", "+00:00")) if updated_raw else None
            if updated_at is not None and updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=timezone.utc)
            price_age = max(0, int((_now() - updated_at.astimezone(timezone.utc)).total_seconds() * 1000)) if updated_at is not None else None
        except ValueError:
            price_age = None
        freshness = str(row["position_price_freshness"] or "")
        stale = freshness == "STALE" or (price_age is not None and price_age > PRICE_STALE_MS)
        return {
            "position_id": row["position_id"],
            "symbol": row["symbol"] or row["mint"],
            "mint": row["mint"],
            "mint_short": _short(row["mint"]),
            "quantity": str(quantity),
            "entry_at": opened,
            "entry_price_native": str(entry) if entry else None,
            "current_price_native": str(current_native) if current_native is not None else None,
            "current_price_usd": str(current_usd) if current_usd is not None else None,
            "position_mark_source": row["position_mark_source"],
            "position_price_updated_at": row["position_price_updated_at"],
            "position_price_freshness": freshness or ("FRESH" if current_native is not None else "UNAVAILABLE"),
            "value_bnb": str(value_bnb) if value_bnb is not None else None,
            "value_usd": str(value_usd) if value_usd is not None else None,
            "pnl_bnb": str(pnl_bnb) if pnl_bnb is not None else None,
            "pnl_pct": str(pnl_pct) if pnl_pct is not None else None,
            "max_gain_pct": str(max_gain) if max_gain is not None else None,
            "hold_seconds": hold_sec,
            "status": display_status,
            "price_status": "PRICE_STALE" if stale else "FRESH" if current_native is not None else "PRICE_UNAVAILABLE",
            "price_age_ms": price_age,
            "exit_reason": row["exit_reason"] or row["exit_trigger_reason"] or intent.get("reason"),
            "exit_swap_status": swap_status or None,
            "exit_percentage": intent.get("percentage"),
            "exit_order_id": row["exit_order_id"],
            "exit_order_short": _short(row["exit_order_id"]),
            "exit_tx_hash": row["exit_tx_hash"],
            "exit_tx_short": _short(row["exit_tx_hash"]),
            "buttons_enabled": swap_status not in ACTIVE_SELL_STATES,
            "retry_enabled": swap_status == "SWAP_FAILED",
        }

    def positions(self) -> dict[str, Any]:
        with self._connection(read_only=True) as connection:
            rows = connection.execute("SELECT * FROM survivor_positions WHERE status='OPEN' ORDER BY opened_at DESC").fetchall()
            items = [self._position_row(row, self._active_intent(connection, str(row["position_id"]))) for row in rows]
        return {"items": items, "count": len(items), "updated_at": _now().isoformat()}

    def _closed_position_row(self, row: sqlite3.Row, reconciliation: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Return persisted entry/exit facts only; never infer missing snapshots."""
        reconciliation = reconciliation or {}
        external_unpriced = bool(row["external_exit_unpriced"])
        entry_spend = _decimal(row["actual_entry_spend_bnb"]) or _decimal(row["invested_bnb"])
        exit_proceeds = None if external_unpriced else (_decimal(row["actual_exit_proceeds_bnb"]) or _decimal(row["realized_bnb"]))
        pnl_bnb = exit_proceeds - entry_spend if entry_spend is not None and exit_proceeds is not None else None
        pnl_pct = None if external_unpriced else (_decimal(reconciliation.get("pnl_pct")) or _decimal(row["pnl_pct"]))
        if pnl_pct is None and pnl_bnb is not None and entry_spend is not None and entry_spend > 0:
            pnl_pct = pnl_bnb / entry_spend * Decimal("100")
        pnl_amount = _decimal(reconciliation.get("pnl_usd"))
        pnl_currency = "USD" if pnl_amount is not None else "BNB"
        if pnl_amount is None:
            pnl_amount = pnl_bnb
        entry_price = _decimal(row["actual_entry_price_native"]) or _decimal(row["entry_price_native"])
        exit_price = None if external_unpriced else (_decimal(row["exit_price_native"]) or _decimal(row["position_mark_price_native"]) or _decimal(row["current_price_native"]))
        return {
            "position_id": row["position_id"],
            "symbol": row["symbol"] or row["mint"],
            "mint": row["mint"],
            "mint_short": _short(row["mint"]),
            "entry_at": row["entry_confirmed_at"] or row["opened_at"],
            "exit_at": row["exit_confirmed_at"] or row["closed_at"],
            "entry_price_native": str(entry_price) if entry_price is not None else None,
            "exit_price_native": str(exit_price) if exit_price is not None else None,
            "entry_price_usd": row["entry_price_usd"],
            "exit_price_usd": row["exit_price_usd"],
            "entry_holders": row["entry_holders"],
            "exit_holders": row["exit_holders"],
            "entry_liquidity_usd": row["entry_liquidity_usd"],
            "exit_liquidity_usd": row["exit_liquidity_usd"],
            "pnl_bnb": str(pnl_bnb) if pnl_bnb is not None else None,
            "pnl_amount": str(pnl_amount) if pnl_amount is not None else None,
            "pnl_currency": pnl_currency,
            "pnl_pct": str(pnl_pct) if pnl_pct is not None else None,
            "exit_reason": row["external_exit_status"] or row["exit_reason"] or row["exit_trigger_reason"],
            "exit_tx_hash": reconciliation.get("tx_hash") or row["exit_tx_hash"],
        }

    def closed_positions(self) -> dict[str, Any]:
        with self._connection(read_only=True) as connection:
            rows = connection.execute(
                "SELECT * FROM survivor_positions WHERE status='CLOSED' "
                "ORDER BY COALESCE(exit_confirmed_at, closed_at, updated_at) DESC LIMIT 100"
            ).fetchall()
        items = [
            self._closed_position_row(
                row,
                self._read_state(connection, f"external_close_reconciliation:{row['position_id']}"),
            )
            for row in rows
        ]
        return {"items": items, "count": len(items), "updated_at": _now().isoformat()}

    # ---------- manual sell lifecycle ----------

    def _set_sell_state(self, position_id: str, payload: Mapping[str, Any], *, status: str | None = None, fields: Mapping[str, Any] | None = None) -> None:
        now = _now()
        with self._db_lock, self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if fields:
                assignments = ",".join(f"{key}=?" for key in fields)
                connection.execute(
                    f"UPDATE survivor_positions SET {assignments},updated_at=? WHERE position_id=?",
                    tuple(fields.values()) + (now.isoformat(), position_id),
                )
            self._write_state(connection, f"live_exit_intent:{position_id}", payload, now)
            connection.commit()

    def request_sell(self, position_id: str, percentage: int) -> tuple[int, dict[str, Any]]:
        if percentage not in {25, 50, 100}:
            raise LiveDashboardError(400, "INVALID_PERCENTAGE", "percentage must be 25, 50, or 100")
        now = _now()
        reason = f"MANUAL_{percentage}"
        with self._db_lock, self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM survivor_positions WHERE position_id=?", (position_id,)).fetchone()
            if row is None:
                connection.rollback()
                raise LiveDashboardError(404, "POSITION_NOT_FOUND", "position not found")
            if row["status"] != "OPEN":
                connection.rollback()
                raise LiveDashboardError(409, "POSITION_NOT_OPEN", "position is not open")
            current_status = str(row["exit_swap_status"] or "")
            if current_status in ACTIVE_SELL_STATES:
                connection.rollback()
                raise LiveDashboardError(409, "SELL_ALREADY_IN_FLIGHT", "a sell is already in flight")
            intent = self._active_intent(connection, position_id)
            if intent.get("state") in {"SELLING", "WAITING_CONFIRMATION", "EXIT_TRIGGERED_WAITING_ROUTE"} and current_status != "SWAP_FAILED":
                connection.rollback()
                raise LiveDashboardError(409, "SELL_ALREADY_IN_FLIGHT", "a sell intent is already active")
            payload = {
                "state": "SELLING",
                "position_id": position_id,
                "mint": row["mint"],
                "percentage": percentage,
                "reason": reason,
                "source": "MANUAL_DASHBOARD",
                "requested_at": now.isoformat(),
            }
            connection.execute(
                "UPDATE survivor_positions SET exit_reason=?,exit_trigger_reason=?,exit_triggered_at=?,exit_swap_status='MANUAL_SUBMITTING',updated_at=? WHERE position_id=?",
                (reason, reason, now.isoformat(), now.isoformat(), position_id),
            )
            self._write_state(connection, f"live_exit_intent:{position_id}", payload, now)
            self._write_state(connection, f"exit_intent:{position_id}", {**payload, "state": "EXIT_TRIGGERED_WAITING_ROUTE", "quantity": str(row["remaining_quantity_token"])}, now)
            self._append_manual_history(connection, position_id, {"percentage": percentage, "reason": reason, "requested_at": now.isoformat(), "source": "MANUAL_DASHBOARD", "state": "REQUESTED"}, now)
            self._record_audit(connection, "SURVIVOR_MANUAL_EXIT_REQUESTED", payload, now)
            connection.commit()
            token = str(row["mint"])
        wallet_state = self._wallet_probe(force=True)
        if wallet_state.get("status") != "CONNECTED":
            self._mark_failed(position_id, reason, "BITGET_UNAVAILABLE")
            raise LiveDashboardError(503, "BITGET_UNAVAILABLE", "Bitget Wallet API is unavailable")
        actual_balance = self._token_balance(token)
        if actual_balance is None or actual_balance <= 0:
            self._mark_failed(position_id, reason, "TOKEN_BALANCE_UNAVAILABLE")
            raise LiveDashboardError(502, "TOKEN_BALANCE_UNAVAILABLE", "wallet token balance is unavailable")
        quantity = actual_balance if percentage == 100 else actual_balance * Decimal(percentage) / Decimal("100")
        if quantity <= 0:
            self._mark_failed(position_id, reason, "TOKEN_BALANCE_ZERO")
            raise LiveDashboardError(502, "TOKEN_BALANCE_ZERO", "wallet token balance is zero")
        with self._executor_lock:
            quote, quote_error = self.route_provider.quote_result(token, "sell", quantity)
        if quote is None or quote.output_quantity <= 0:
            self._mark_failed(position_id, reason, quote_error.reason if quote_error is not None else "SELL_QUOTE_FAILED")
            raise LiveDashboardError(502, "SELL_QUOTE_FAILED", quote_error.reason if quote_error is not None else "sell quote failed")
        with self._executor_lock:
            result = self.executor.sell(token, quantity)
        if not isinstance(result, LiveSwapResult):
            self._mark_failed(position_id, reason, "INVALID_SWAP_RESULT")
            raise LiveDashboardError(502, "INVALID_SWAP_RESULT", "invalid Bitget response")
        payload = {
            "state": "WAITING_CONFIRMATION" if result.stage == "SWAP_PENDING" else result.stage,
            "position_id": position_id,
            "mint": token,
            "percentage": percentage,
            "quantity": str(quantity),
            "reason": reason,
            "source": "MANUAL_DASHBOARD",
            "requested_at": now.isoformat(),
            "order_id": result.order_id,
            "tx_hash": result.tx_hash,
            "error_code": result.error_code,
        }
        fields: dict[str, Any] = {
            "exit_swap_status": result.stage,
            "exit_order_id": result.order_id,
            "exit_tx_hash": result.tx_hash,
            "exit_submitted_at": _iso(result.requested_at or now),
        }
        self._set_sell_state(position_id, payload, fields=fields)
        if result.stage == "SWAP_FAILED":
            reconcile = getattr(self.executor, "find_recent_order", None)
            if callable(reconcile):
                with self._executor_lock:
                    recovered = reconcile(token, quantity, side="sell", requested_at=now)
                if isinstance(recovered, LiveSwapResult) and recovered.order_id:
                    recovered_payload = {
                        "state": "WAITING_CONFIRMATION" if recovered.stage == "SWAP_PENDING" else recovered.stage,
                        "position_id": position_id,
                        "mint": token,
                        "percentage": percentage,
                        "quantity": str(quantity),
                        "reason": reason,
                        "source": "MANUAL_DASHBOARD",
                        "requested_at": now.isoformat(),
                        "order_id": recovered.order_id,
                        "tx_hash": recovered.tx_hash,
                        "reconciled_after_error": result.error_code,
                    }
                    self._set_sell_state(
                        position_id,
                        recovered_payload,
                        fields={
                            "exit_swap_status": recovered.stage,
                            "exit_order_id": recovered.order_id,
                            "exit_tx_hash": recovered.tx_hash,
                            "exit_submitted_at": _iso(recovered.requested_at or now),
                        },
                    )
                    if recovered.stage == "SWAP_CONFIRMED":
                        self._apply_finished(position_id, recovered, percentage, reason)
                        return 200, {"status": "FINISHED", "order_id": recovered.order_id, "position_id": position_id}
                    return 202, {"status": recovered.stage, "order_id": recovered.order_id, "position_id": position_id}
            raise LiveDashboardError(502, result.error_code or "SWAP_FAILED", result.error_message or "swap failed")
        if result.stage == "SWAP_UNKNOWN":
            return 202, {"status": "SWAP_UNKNOWN", "order_id": result.order_id, "position_id": position_id}
        if result.stage == "SWAP_CONFIRMED":
            self._apply_finished(position_id, result, percentage, reason)
            return 200, {"status": "FINISHED", "order_id": result.order_id, "position_id": position_id}
        return 202, {"status": result.stage, "order_id": result.order_id, "position_id": position_id}

    def _mark_failed(self, position_id: str, reason: str, error_code: str) -> None:
        now = _now()
        self._set_sell_state(position_id, {"state": "SWAP_FAILED", "reason": reason, "error_code": error_code, "failed_at": now.isoformat()}, fields={"exit_swap_status": "SWAP_FAILED"})

    def _apply_finished(self, position_id: str, result: LiveSwapResult, percentage: int | None, reason: str | None) -> None:
        token_balance = self._token_balance_from_worker(position_id)
        if token_balance is None:
            self._set_sell_state(position_id, {"state": "BALANCE_SYNC_PENDING", "order_id": result.order_id}, fields={"exit_swap_status": "BALANCE_SYNC_PENDING"})
            return
        now = _now()
        with self._db_lock, self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM survivor_positions WHERE position_id=?", (position_id,)).fetchone()
            if row is None:
                connection.rollback()
                return
            old_realized = _decimal(row["realized_bnb"]) or Decimal("0")
            proceeds = result.output_quantity or Decimal("0")
            input_qty = result.input_quantity or Decimal("0")
            current_price = proceeds / input_qty if proceeds > 0 and input_qty > 0 else _decimal(row["current_price_native"])
            requested_full = percentage == 100
            dust_limit = max(Decimal("1e-18"), input_qty * Decimal("0.001"))
            closed = requested_full and token_balance <= dust_limit
            remaining = Decimal("0") if closed else token_balance
            invested = _decimal(row["invested_bnb"]) or Decimal("0")
            pnl = (old_realized + proceeds + (remaining * current_price if current_price is not None else Decimal("0")) - invested) if invested > 0 else None
            pnl_pct = pnl / invested * Decimal("100") if pnl is not None and invested > 0 else None
            fields = {
                "status": "CLOSED" if closed else "OPEN",
                "remaining_quantity_token": str(remaining),
                "realized_bnb": str(old_realized + proceeds),
                "pnl_pct": str(pnl_pct) if pnl_pct is not None else None,
                "current_price_native": str(current_price) if current_price is not None else row["current_price_native"],
                "exit_swap_status": "SWAP_CONFIRMED",
                "exit_confirmed_at": now.isoformat(),
                "actual_exit_quantity_token": str(input_qty) if input_qty else None,
                "actual_exit_proceeds_bnb": str(proceeds) if proceeds else None,
                "exit_reason": reason or row["exit_reason"],
                "exit_order_id": result.order_id,
                "exit_tx_hash": result.tx_hash,
                "updated_at": now.isoformat(),
            }
            if closed:
                fields.update({"closed_at": now.isoformat(), "exit_price_native": str(current_price) if current_price else None})
            assignments = ",".join(f"{key}=?" for key in fields)
            connection.execute(f"UPDATE survivor_positions SET {assignments} WHERE position_id=?", tuple(fields.values()) + (position_id,))
            payload = {"state": "FINISHED", "position_id": position_id, "order_id": result.order_id, "tx_hash": result.tx_hash, "remaining_quantity": str(remaining), "closed": closed}
            self._write_state(connection, f"live_exit_intent:{position_id}", payload, now)
            self._write_state(connection, f"exit_intent:{position_id}", {**payload, "state": "COMPLETED"}, now)
            self._append_manual_history(connection, position_id, {"percentage": percentage, "reason": reason, "confirmed_at": now.isoformat(), "order_id": result.order_id, "tx_hash": result.tx_hash, "input_quantity": str(input_qty), "output_bnb": str(proceeds), "remaining_quantity": str(remaining), "state": "FINISHED"}, now)
            self._record_audit(connection, "SURVIVOR_MANUAL_EXIT_CONFIRMED", payload, now)
            connection.commit()

    def _token_balance_from_worker(self, position_id: str) -> Decimal | None:
        with self._connection(read_only=True) as connection:
            row = connection.execute("SELECT mint FROM survivor_positions WHERE position_id=?", (position_id,)).fetchone()
        return self._token_balance(str(row["mint"])) if row else None

    def _poll_loop(self) -> None:
        while not self._stop.wait(2.0):
            try:
                if time.monotonic() - self._last_wallet_probe >= 5.0:
                    self._refresh_wallet_balance_snapshot()
                    self._last_wallet_probe = time.monotonic()
                with self._connection(read_only=True) as connection:
                    rows = connection.execute(
                        "SELECT position_id,exit_order_id,exit_swap_status,exit_trigger_reason FROM survivor_positions "
                        "WHERE status='OPEN' AND exit_swap_status IN ('SWAP_SUBMITTED','SWAP_PENDING','BALANCE_SYNC_PENDING')"
                    ).fetchall()
                for row in rows:
                    order_id = row["exit_order_id"]
                    if row["exit_swap_status"] == "BALANCE_SYNC_PENDING" and not order_id:
                        continue
                    with self._executor_lock:
                        result = self.executor.get_order_status(str(order_id)) if order_id else None
                    if not isinstance(result, LiveSwapResult):
                        continue
                    if result.stage == "SWAP_CONFIRMED":
                        with self._connection(read_only=True) as intent_connection:
                            intent = self._active_intent(intent_connection, str(row["position_id"]))
                        self._apply_finished(str(row["position_id"]), result, intent.get("percentage"), row["exit_trigger_reason"])
                    elif result.stage == "SWAP_FAILED":
                        self._mark_failed(str(row["position_id"]), str(row["exit_trigger_reason"] or "MANUAL_EXIT"), result.error_code or "SWAP_FAILED")
                    elif result.stage == "SWAP_UNKNOWN" and row["exit_swap_status"] != "SWAP_UNKNOWN":
                        self._set_sell_state(str(row["position_id"]), {"state": "SWAP_UNKNOWN", "order_id": order_id}, fields={"exit_swap_status": "SWAP_UNKNOWN"})
            except Exception:
                # A transient dashboard/polling error must not kill the page.
                continue

    def order(self, order_id: str) -> dict[str, Any]:
        with self._connection(read_only=True) as connection:
            row = connection.execute(
                "SELECT position_id,mint,symbol,status,exit_swap_status,exit_order_id,exit_tx_hash,exit_reason,updated_at "
                "FROM survivor_positions WHERE exit_order_id=?",
                (order_id,),
            ).fetchone()
        if row is None:
            raise LiveDashboardError(404, "ORDER_NOT_FOUND", "order not found")
        return dict(row)

    def payload(self, path: str) -> tuple[int, object, str]:
        if path == "/":
            return 200, _read_static("live_dashboard.html"), "text/html; charset=utf-8"
        if path == "/app.js":
            return 200, _read_static("live_dashboard.js"), "application/javascript; charset=utf-8"
        if path == "/styles.css":
            return 200, _read_static("live_dashboard.css"), "text/css; charset=utf-8"
        if path == "/api/status":
            return 200, self.status(), "application/json; charset=utf-8"
        if path == "/api/positions":
            return 200, self.positions(), "application/json; charset=utf-8"
        if path == "/api/closed-positions":
            return 200, self.closed_positions(), "application/json; charset=utf-8"
        if path.startswith("/api/orders/"):
            return 200, self.order(path.rsplit("/", 1)[-1]), "application/json; charset=utf-8"
        raise LiveDashboardError(404, "NOT_FOUND", "not found")

    def close(self) -> None:
        self._stop.set()


class _Handler(BaseHTTPRequestHandler):
    service: LiveDashboardService

    def do_GET(self) -> None:  # noqa: N802
        try:
            status, payload, content_type = self.service.payload(urlparse(self.path).path)
        except LiveDashboardError as exc:
            status, payload, content_type = exc.status, {"error": exc.code, "message": exc.message}, "application/json; charset=utf-8"
        self._send(status, payload, content_type)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        prefix = "/api/positions/"
        if not parsed.path.startswith(prefix) or not parsed.path.endswith("/sell"):
            self._send(404, {"error": "NOT_FOUND"}, "application/json; charset=utf-8")
            return
        position_id = unquote(parsed.path[len(prefix) : -len("/sell")].strip("/"))
        try:
            length = max(0, min(10_000, int(self.headers.get("Content-Length", "0"))))
            body = json.loads(self.rfile.read(length).decode("utf-8")) if length else {}
            percentage = int(body.get("percentage")) if isinstance(body, Mapping) else 0
            status, result = self.service.request_sell(position_id, percentage)
        except LiveDashboardError as exc:
            status, result = exc.status, {"error": exc.code, "message": exc.message}
        except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError):
            status, result = 400, {"error": "INVALID_JSON"}
        self._send(status, result, "application/json; charset=utf-8")

    def log_message(self, _format: str, *args: object) -> None:
        return

    def _send(self, status: int, payload: object, content_type: str) -> None:
        body = payload.encode("utf-8") if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def create_server(service: LiveDashboardService) -> ThreadingHTTPServer:
    handler = type("LiveDashboardHandler", (_Handler,), {"service": service})
    return ThreadingHTTPServer((service.host, service.port), handler)


def serve(service: LiveDashboardService) -> None:
    server = create_server(service)
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        service.close()
        server.server_close()


def _read_static(name: str) -> str:
    return (Path(__file__).with_name("static") / name).read_text(encoding="utf-8")
