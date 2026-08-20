"""GMGN CLI adapters for BSC quotes and explicitly-gated Live execution.

Credentials remain in the GMGN CLI's local configuration and are never passed
as command-line arguments.  Paper callers use :class:`GmgnCliQuoteProvider`.
Only the explicitly constructed :class:`GmgnCliLiveExecutor` can call
``gmgn-cli swap``; it persists an order journal before submitting a swap so an
ambiguous timeout cannot cause an automatic duplicate order after a restart.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import subprocess
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

from meme_system.adapters.binance_agentic_wallet import LiveSwapResult, QuoteFailure
from meme_system.adapters.bsc_wss import BscRpcClient, normalize_bsc_address
from meme_system.adapters.protocols import ExecutableQuote


BSC_NATIVE = "0x0000000000000000000000000000000000000000"
GMGN_CLI_PROVIDER = "GMGN_CLI"
_ERC20_DECIMALS_SELECTOR = "0x313ce567"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def classify_gmgn_error(message: str) -> str:
    text = message.lower()
    # GMGN currently returns this code for a subset of pre-migration Flap SELL
    # quote requests.  BUY requests with the same API key succeed, so it must
    # not be mislabeled as a credential failure without an official mapping.
    if "40101600" in text:
        return "OTHER"
    if "429" in text or "rate limit" in text or "too many" in text:
        return "RATE_LIMIT"
    if "timeout" in text or "timed out" in text:
        return "TIMEOUT"
    if "security" in text or "risk" in text or "honeypot" in text:
        return "SECURITY_REJECT"
    if "unsupported" in text or "invalid token" in text:
        return "TOKEN_UNSUPPORTED"
    if "no route" in text or "route not found" in text or "insufficient liquidity" in text:
        return "NO_ROUTE"
    if "401" in text or "403" in text or "api key" in text or "signature" in text:
        return "AUTH_ERROR"
    return "OTHER"


@dataclass(frozen=True)
class GmgnQuote:
    success: bool
    input_token: str
    output_token: str
    input_amount: int
    output_amount: int | None
    min_output_amount: int | None
    slippage: Decimal | None
    latency_ms: int
    gas_limit: int | None = None
    route_type: str | None = None
    launch_exchange: str | None = None
    failure_reason: str | None = None
    error_code: str | None = None


class GmgnCliQuoteProvider:
    """Run the official CLI without ever exposing credentials in argv."""

    provider = GMGN_CLI_PROVIDER

    def __init__(self, wallet: str, *, cli: str = "gmgn-cli", timeout_sec: float = 20.0, slippage: Decimal = Decimal("30"), command_env: dict[str, str] | None = None) -> None:
        self.wallet = wallet.strip().lower()
        self.cli = cli
        self.timeout_sec = timeout_sec
        self.slippage = slippage
        # The verified GMGN path requires Node's native proxy support.  Keep
        # all other environment values (including the operator-controlled
        # GMGN automated-trade gate) inherited; never set that gate here.
        self.command_env = dict(os.environ if command_env is None else command_env)
        self.command_env.setdefault("NODE_USE_ENV_PROXY", "1")

    def quote(self, input_token: str, output_token: str, input_amount: int) -> GmgnQuote:
        started = time.monotonic()
        command = [
            self.cli, "order", "quote", "--chain", "bsc", "--from", self.wallet,
            "--input-token", input_token, "--output-token", output_token,
            "--amount", str(input_amount), "--slippage", str(self.slippage), "--raw",
        ]
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=self.timeout_sec, check=False, env=self.command_env)
        except subprocess.TimeoutExpired:
            return self._failure(input_token, output_token, input_amount, started, "TIMEOUT")
        if result.returncode != 0:
            message = result.stderr or result.stdout
            match = re.search(r"error=(\d+)", message) or re.search(r"code=(\d+)", message)
            return self._failure(input_token, output_token, input_amount, started, classify_gmgn_error(message), match.group(1) if match else None)
        try:
            payload: dict[str, Any] = json.loads(result.stdout)
            output = int(payload["output_amount"])
            minimum = int(payload["min_output_amount"])
            if output <= 0 or minimum <= 0 or minimum > output:
                raise ValueError("invalid output")
            tx = payload.get("tx") if isinstance(payload.get("tx"), dict) else {}
            launch = tx.get("token_launch_info") if isinstance(tx.get("token_launch_info"), dict) else {}
            return GmgnQuote(
                True, input_token, output_token, input_amount, output, minimum,
                Decimal(str(payload.get("slippage"))) if payload.get("slippage") is not None else None,
                round((time.monotonic() - started) * 1000),
                gas_limit=int(tx["gas_limit"]) if tx.get("gas_limit") else None,
                route_type=str(tx.get("type")) if tx.get("type") else None,
                launch_exchange=str(launch.get("exchange")) if launch.get("exchange") else None,
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return self._failure(input_token, output_token, input_amount, started, "INVALID_RESPONSE")

    def roundtrip(self, token: str, amount_wei: int) -> tuple[GmgnQuote, GmgnQuote | None]:
        buy = self.quote(BSC_NATIVE, token, amount_wei)
        sell = self.quote(token, BSC_NATIVE, buy.output_amount) if buy.success and buy.output_amount else None
        return buy, sell

    @staticmethod
    def _failure(input_token: str, output_token: str, amount: int, started: float, reason: str, error_code: str | None = None) -> GmgnQuote:
        return GmgnQuote(False, input_token, output_token, amount, None, None, None, round((time.monotonic() - started) * 1000), failure_reason=reason, error_code=error_code)


class GmgnExecutionJournal:
    """Executor-owned order journal, separate from the Balanced runtime DB."""

    ACTIVE = frozenset({"CREATING", "SUBMIT_UNKNOWN", "SUBMITTED", "PENDING"})

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.connection = sqlite3.connect(self.path, timeout=5, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA busy_timeout=5000")
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS execution_orders("
            "logical_key TEXT PRIMARY KEY, token TEXT NOT NULL, side TEXT NOT NULL, "
            "quantity TEXT NOT NULL, order_id TEXT, state TEXT NOT NULL, error_code TEXT, "
            "created_at TEXT NOT NULL, updated_at TEXT NOT NULL)"
        )
        self.connection.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS gmgn_execution_order_id_unique "
            "ON execution_orders(order_id) WHERE order_id IS NOT NULL"
        )
        self.connection.commit()
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass

    def claim(self, logical_key: str, token: str, side: str, quantity: Decimal) -> sqlite3.Row | None:
        now = _utc_now().isoformat()
        with self._lock:
            existing = self.connection.execute("SELECT * FROM execution_orders WHERE logical_key=?", (logical_key,)).fetchone()
            if existing is not None and str(existing["state"]) in self.ACTIVE:
                return existing
            self.connection.execute(
                "INSERT INTO execution_orders(logical_key,token,side,quantity,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?) "
                "ON CONFLICT(logical_key) DO UPDATE SET quantity=excluded.quantity,order_id=NULL,state='CREATING',error_code=NULL,created_at=excluded.created_at,updated_at=excluded.updated_at",
                (logical_key, token, side, str(quantity), "CREATING", now, now),
            )
            self.connection.commit()
            return None

    def bind_order(self, logical_key: str, order_id: str) -> None:
        with self._lock:
            self.connection.execute("UPDATE execution_orders SET order_id=?,state='SUBMITTED',updated_at=? WHERE logical_key=?", (order_id, _utc_now().isoformat(), logical_key))
            self.connection.commit()

    def update(self, *, logical_key: str | None = None, order_id: str | None = None, state: str, error_code: str | None = None) -> None:
        if not logical_key and not order_id:
            return
        field, value = ("logical_key", logical_key) if logical_key else ("order_id", order_id)
        with self._lock:
            self.connection.execute(f"UPDATE execution_orders SET state=?,error_code=?,updated_at=? WHERE {field}=?", (state, error_code, _utc_now().isoformat(), value))
            self.connection.commit()

    def pending(self) -> tuple[dict[str, Any], ...]:
        placeholders = ",".join("?" for _ in self.ACTIVE)
        with self._lock:
            rows = self.connection.execute(f"SELECT * FROM execution_orders WHERE state IN ({placeholders}) ORDER BY created_at", tuple(sorted(self.ACTIVE))).fetchall()
        return tuple(dict(row) for row in rows)

    def close(self) -> None:
        with self._lock:
            self.connection.close()


class GmgnCliRouteProvider(GmgnCliQuoteProvider):
    """Normalize official GMGN CLI quotes for the existing roundtrip gate."""

    _token_decimals_cache: dict[str, int]

    def __init__(
        self,
        wallet: str,
        *,
        rpc: BscRpcClient | None = None,
        metadata_decimals: Mapping[str, int] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(wallet, **kwargs)
        self._token_decimals_cache = {}
        # Token decimals are immutable on an ERC-20 contract.  Resolve them
        # directly from BSC first, then use GMGN metadata only as a fallback.
        # The route worker owns these reads; the Balanced SQLite owner never
        # performs RPC or CLI I/O.
        self._rpc = rpc or BscRpcClient.from_env()
        if metadata_decimals is None:
            try:
                configured_metadata = json.loads(os.environ.get("TOKEN_DECIMALS_JSON", "{}"))
            except json.JSONDecodeError:
                configured_metadata = {}
            metadata_decimals = configured_metadata if isinstance(configured_metadata, Mapping) else {}
        self._metadata_decimals = {
            normalized: int(value)
            for key, value in (metadata_decimals or {}).items()
            if (normalized := normalize_bsc_address(str(key))) is not None
            and isinstance(value, int)
            and not isinstance(value, bool)
            and 0 <= value <= 36
        }

    def _run_json(self, command: list[str], *, timeout_sec: float | None = None) -> tuple[dict[str, Any] | None, str | None, int]:
        started = time.monotonic()
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=timeout_sec or self.timeout_sec, check=False, env=self.command_env)
        except subprocess.TimeoutExpired:
            return None, "TIMEOUT", round((time.monotonic() - started) * 1000)
        if result.returncode != 0:
            return None, classify_gmgn_error(result.stderr or result.stdout), round((time.monotonic() - started) * 1000)
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError:
            return None, "INVALID_RESPONSE", round((time.monotonic() - started) * 1000)
        return payload if isinstance(payload, dict) else None, None, round((time.monotonic() - started) * 1000)

    def _token_decimals(self, token: str) -> int | None:
        normalized = normalize_bsc_address(token)
        if normalized is None:
            return None
        cached = self._token_decimals_cache.get(normalized)
        if cached is not None:
            return cached
        rpc_decimals = self._rpc.call_uint(normalized, _ERC20_DECIMALS_SELECTOR) if self._rpc.configured else None
        if rpc_decimals is not None and 0 <= rpc_decimals <= 36:
            self._token_decimals_cache[normalized] = rpc_decimals
            return rpc_decimals
        # Current GMGN CLI token commands require --address.  Do not silently
        # assume 18 decimals when metadata cannot be resolved.
        payload, _error, _latency = self._run_json([self.cli, "token", "info", "--chain", "bsc", "--address", normalized, "--raw"])
        data = payload.get("token") if isinstance(payload, dict) and isinstance(payload.get("token"), dict) else payload
        try:
            decimals = int((data or {}).get("decimals"))
        except (TypeError, ValueError):
            decimals = None
        if decimals is not None and 0 <= decimals <= 36:
            self._token_decimals_cache[normalized] = decimals
            return decimals
        metadata_decimals = self._metadata_decimals.get(normalized)
        if metadata_decimals is not None:
            self._token_decimals_cache[normalized] = metadata_decimals
            return metadata_decimals
        return None

    def quote_result(self, token: str, side: str, input_quantity: Decimal) -> tuple[ExecutableQuote | None, QuoteFailure | None]:
        normalized = normalize_bsc_address(token)
        if normalized is None or side not in {"buy", "sell"} or input_quantity <= 0:
            return None, QuoteFailure("INVALID_REQUEST")
        decimals = 18 if side == "buy" else self._token_decimals(normalized)
        if decimals is None:
            return None, QuoteFailure("TOKEN_DECIMALS_UNAVAILABLE")
        raw_input = int(input_quantity * (Decimal(10) ** decimals))
        if raw_input <= 0:
            return None, QuoteFailure("INVALID_REQUEST")
        raw_quote = self.quote(BSC_NATIVE if side == "buy" else normalized, normalized if side == "buy" else BSC_NATIVE, raw_input)
        if not raw_quote.success or raw_quote.output_amount is None:
            return None, QuoteFailure(raw_quote.failure_reason or "OTHER", detail=raw_quote.error_code, latency_ms=raw_quote.latency_ms)
        output_decimals = self._token_decimals(normalized) if side == "buy" else 18
        if output_decimals is None:
            return None, QuoteFailure("TOKEN_DECIMALS_UNAVAILABLE", latency_ms=raw_quote.latency_ms)
        received_at = _utc_now()
        raw_hash = hashlib.sha256(f"{raw_quote.input_amount}:{raw_quote.output_amount}:{received_at.isoformat()}".encode()).hexdigest()
        return ExecutableQuote(
            quote_id=f"gmgn:{side}:{normalized}:{raw_hash[:16]}", mint=normalized, side=side,
            input_quantity=input_quantity, output_quantity=Decimal(raw_quote.output_amount) / (Decimal(10) ** output_decimals),
            route_fee=None, price_impact_pct=None, quoted_at=received_at, age_ms=0,
            expires_at=received_at + timedelta(seconds=12), provider=self.provider,
            route=tuple(part for part in ("gmgn", raw_quote.route_type, raw_quote.launch_exchange) if part),
            requested_at=received_at, received_at=received_at, latency_ms=raw_quote.latency_ms,
            executable_style=True, confidence="provider_quote", raw_response_hash=f"sha256:{raw_hash}", quote_source=self.provider,
        ), None

    def quote_candidate(self, mint: str, amount_bnb: Decimal, _fields: object | None = None):
        buy, buy_failure = self.quote_result(mint, "buy", amount_bnb)
        if buy is None:
            return None, None, buy_failure.reason if buy_failure else "BUY_QUOTE_FAILED"
        sell, sell_failure = self.quote_result(mint, "sell", buy.output_quantity)
        if sell is None:
            return buy, None, sell_failure.reason if sell_failure else "SELL_QUOTE_FAILED"
        return buy, sell, None

    def quote_sell(self, mint: str, quantity: Decimal):
        quote, failure = self.quote_result(mint, "sell", quantity)
        return quote, failure.reason if failure else None


class GmgnCliLiveExecutor(GmgnCliRouteProvider):
    """GMGN BSC executor with explicit swap gate and persistent order recovery."""

    def __init__(self, wallet: str = "", *, swaps_enabled: bool = False, gas_reserve_bnb: Decimal = Decimal("0.003"), journal_path: Path = Path("data/bsc-balanced/live/gmgn_execution.db"), **kwargs: Any) -> None:
        super().__init__(wallet, **kwargs)
        self.swaps_enabled = bool(swaps_enabled)
        self.gas_reserve_bnb = Decimal(gas_reserve_bnb)
        self.journal = GmgnExecutionJournal(journal_path)

    @classmethod
    def from_env(cls, *, swaps_enabled: bool = False, env_file: Path = Path(".env")) -> "GmgnCliLiveExecutor":
        values = dict(os.environ)
        try:
            for line in env_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    values.setdefault(key.strip(), value.strip().strip("\"'"))
        except OSError:
            pass
        return cls(
            values.get("GMGN_BSC_WALLET_ADDRESS", ""), swaps_enabled=swaps_enabled,
            cli=values.get("GMGN_CLI_BIN", "gmgn-cli"), timeout_sec=float(values.get("GMGN_CLI_TIMEOUT_SEC", "20")),
            slippage=Decimal(values.get("GMGN_SLIPPAGE_PCT", "30")), gas_reserve_bnb=Decimal(values.get("GMGN_GAS_RESERVE_BNB", "0.003")),
            journal_path=Path(values.get("GMGN_EXECUTION_DB_PATH", "data/bsc-balanced/live/gmgn_execution.db")),
        )

    def _wallet_summary(self) -> tuple[str | None, Decimal | None, str | None]:
        payload, error, _latency = self._run_json([self.cli, "portfolio", "info", "--raw"])
        if payload is None:
            return None, None, error
        wallets = payload.get("wallets") if isinstance(payload.get("wallets"), list) else []
        item = next((value for value in wallets if isinstance(value, dict) and str(value.get("chain")).lower() == "bsc"), None)
        if not isinstance(item, dict):
            return None, None, "BSC_WALLET_UNAVAILABLE"
        address = normalize_bsc_address(str(item.get("address") or ""))
        balances = item.get("balances") if isinstance(item.get("balances"), list) else []
        bnb = next((value for value in balances if isinstance(value, dict) and str(value.get("symbol") or "").upper() == "BNB"), None)
        try:
            balance = Decimal(str((bnb or {}).get("balance")))
        except Exception:
            balance = None
        return address, balance, None if address and balance is not None else "BSC_BALANCE_UNAVAILABLE"

    def preflight(self) -> dict[str, Any]:
        try:
            check_result = subprocess.run([self.cli, "config", "--check"], capture_output=True, text=True, timeout=self.timeout_sec, check=False, env=self.command_env)
            check_ok = check_result.returncode == 0
            check_error = None if check_ok else classify_gmgn_error(check_result.stderr or check_result.stdout)
        except subprocess.TimeoutExpired:
            check_ok, check_error = False, "TIMEOUT"
        address, balance, wallet_error = self._wallet_summary()
        self.wallet = address or self.wallet
        automated = self.command_env.get("GMGN_ALLOW_AUTOMATED_TRADES") == "1"
        ok = check_ok and address is not None and balance is not None and automated and self.swaps_enabled
        return {"ok": ok, "bsc_supported": bool(address), "wallet_address": address, "bnb_balance": str(balance or 0), "swaps_enabled": self.swaps_enabled, "automated_trade_gate": automated, "auth": "HEALTHY" if check_ok else "DEGRADED", "error_class": check_error or wallet_error or (None if automated else "GMGN_AUTOMATED_TRADES_NOT_ENABLED")}

    def get_balance(self, token: str | None = None) -> dict[str, Any]:
        if token is None:
            address, balance, error = self._wallet_summary()
            return {"ok": error is None, "data": [{"symbol": "BNB", "address": address or "", "balance": str(balance or 0)}], "error_class": error}
        normalized = normalize_bsc_address(token)
        if normalized is None:
            return {"ok": False, "error_class": "INVALID_TOKEN"}
        wallet = self.wallet
        if not wallet:
            wallet, _balance, _error = self._wallet_summary()
            self.wallet = wallet or ""
        if not wallet:
            return {"ok": False, "error_class": "BSC_WALLET_UNAVAILABLE"}
        payload, error, _latency = self._run_json([self.cli, "portfolio", "token-balance", "--chain", "bsc", "--wallet", wallet, "--token", normalized, "--raw"])
        balances = payload.get("balances") if isinstance(payload, dict) and isinstance(payload.get("balances"), list) else []
        item = next((value for value in balances if isinstance(value, dict)), {})
        try:
            balance = Decimal(str(item.get("balance") or "0"))
        except Exception:
            balance = Decimal(0)
        return {"ok": error is None, "data": [{"address": normalized, "balance": str(balance), "decimals": self._token_decimals(normalized)}], "error_class": error}

    def _swap(self, token: str, quantity: Decimal, *, side: str) -> LiveSwapResult:
        requested_at = _utc_now()
        normalized = normalize_bsc_address(token)
        if not self.swaps_enabled:
            return LiveSwapResult(stage="SWAP_BLOCKED", error_code="REAL_SWAP_DISABLED", requested_at=requested_at, received_at=_utc_now())
        if normalized is None or quantity <= 0:
            return LiveSwapResult(stage="SWAP_FAILED", error_code="INVALID_REQUEST", requested_at=requested_at, received_at=_utc_now())
        key = f"{side}:{normalized}"
        existing = self.journal.claim(key, normalized, side, quantity)
        if existing is not None:
            return LiveSwapResult(stage="SWAP_UNKNOWN", order_id=existing["order_id"], provider_status=existing["state"], error_code="DUPLICATE_ORDER_BLOCKED", needs_reconciliation=True, requested_at=requested_at, received_at=_utc_now())
        if side == "buy":
            _address, bnb, error = self._wallet_summary()
            if error or bnb is None or bnb < quantity + self.gas_reserve_bnb:
                self.journal.update(logical_key=key, state="FAILED", error_code="GAS_RESERVE_INSUFFICIENT")
                return LiveSwapResult(stage="SWAP_FAILED", error_code="GAS_RESERVE_INSUFFICIENT", requested_at=requested_at, received_at=_utc_now())
            input_token, output_token, decimals = BSC_NATIVE, normalized, 18
        else:
            balance_payload = self.get_balance(normalized)
            available = self._balance_from_payload(balance_payload)
            decimals = self._token_decimals(normalized)
            if available is None or available <= 0 or decimals is None:
                self.journal.update(logical_key=key, state="FAILED", error_code="TOKEN_BALANCE_UNAVAILABLE")
                return LiveSwapResult(stage="SWAP_FAILED", error_code="TOKEN_BALANCE_UNAVAILABLE", requested_at=requested_at, received_at=_utc_now())
            quantity = min(quantity, available)
            input_token, output_token = normalized, BSC_NATIVE
        raw = int(quantity * (Decimal(10) ** decimals))
        if raw <= 0:
            self.journal.update(logical_key=key, state="FAILED", error_code="AMOUNT_TOO_SMALL")
            return LiveSwapResult(stage="SWAP_FAILED", error_code="AMOUNT_TOO_SMALL", requested_at=requested_at, received_at=_utc_now())
        command = [self.cli, "swap", "--chain", "bsc", "--from", self.wallet, "--input-token", input_token, "--output-token", output_token, "--amount", str(raw), "--slippage", str(self.slippage), "--yes", "--raw"]
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=self.timeout_sec, check=False, env=self.command_env)
        except subprocess.TimeoutExpired:
            self.journal.update(logical_key=key, state="SUBMIT_UNKNOWN", error_code="REQUEST_TIMEOUT")
            return LiveSwapResult(stage="SWAP_UNKNOWN", error_code="REQUEST_TIMEOUT", needs_reconciliation=True, requested_at=requested_at, received_at=_utc_now())
        if result.returncode != 0:
            error = classify_gmgn_error(result.stderr or result.stdout)
            self.journal.update(logical_key=key, state="FAILED", error_code=error)
            return LiveSwapResult(stage="SWAP_FAILED", error_code=error, error_message=(result.stderr or result.stdout)[:300], requested_at=requested_at, received_at=_utc_now())
        try:
            payload = json.loads(result.stdout)
            order_id = str(payload.get("order_id") or payload.get("orderId") or "")
            tx_hash = str(payload.get("hash") or payload.get("tx_hash") or "") or None
        except (json.JSONDecodeError, AttributeError):
            order_id = ""
            tx_hash = None
        if not order_id:
            self.journal.update(logical_key=key, state="SUBMIT_UNKNOWN", error_code="ORDER_ID_MISSING")
            return LiveSwapResult(stage="SWAP_UNKNOWN", error_code="ORDER_ID_MISSING", needs_reconciliation=True, requested_at=requested_at, received_at=_utc_now())
        self.journal.bind_order(key, order_id)
        return LiveSwapResult(stage="SWAP_SUBMITTED", order_id=order_id, tx_hash=tx_hash, needs_reconciliation=True, requested_at=requested_at, received_at=_utc_now(), input_quantity=quantity)

    @staticmethod
    def _balance_from_payload(payload: dict[str, Any]) -> Decimal | None:
        values = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(values, list) or not values:
            return None
        try:
            return Decimal(str(values[0].get("balance")))
        except Exception:
            return None

    def buy(self, token: str, bnb_amount: Decimal) -> LiveSwapResult:
        return self._swap(token, bnb_amount, side="buy")

    def sell(self, token: str, token_amount: Decimal) -> LiveSwapResult:
        return self._swap(token, token_amount, side="sell")

    def get_order_status(self, order_id: str) -> LiveSwapResult:
        requested_at = _utc_now()
        payload, error, latency = self._run_json([self.cli, "order", "get", "--chain", "bsc", "--order-id", order_id, "--raw"])
        if payload is None:
            return LiveSwapResult(stage="SWAP_UNKNOWN", order_id=order_id, error_code=error, needs_reconciliation=True, requested_at=requested_at, received_at=_utc_now(), latency_ms=latency)
        status = str(payload.get("status") or "").lower()
        tx_hash = str(payload.get("hash") or payload.get("tx_hash") or "") or None
        report = payload.get("report") if isinstance(payload.get("report"), dict) else {}
        if status in {"confirmed", "finished", "success"}:
            try:
                input_value = Decimal(str(report.get("input_amount"))) / (Decimal(10) ** int(report.get("input_token_decimals") or 18))
                output_value = Decimal(str(report.get("output_amount"))) / (Decimal(10) ** int(report.get("output_token_decimals") or 18))
            except Exception:
                input_value = output_value = None
            self.journal.update(order_id=order_id, state="CONFIRMED")
            return LiveSwapResult(stage="SWAP_CONFIRMED", order_id=order_id, tx_hash=tx_hash, provider_status=status, requested_at=requested_at, received_at=_utc_now(), latency_ms=latency, input_quantity=input_value, output_quantity=output_value)
        if status in {"failed", "cancelled", "reverted", "expired"}:
            self.journal.update(order_id=order_id, state="FAILED", error_code=status.upper())
            return LiveSwapResult(stage="SWAP_FAILED", order_id=order_id, tx_hash=tx_hash, provider_status=status, requested_at=requested_at, received_at=_utc_now(), latency_ms=latency)
        self.journal.update(order_id=order_id, state="PENDING")
        return LiveSwapResult(stage="SWAP_PENDING", order_id=order_id, tx_hash=tx_hash, provider_status=status or None, needs_reconciliation=True, requested_at=requested_at, received_at=_utc_now(), latency_ms=latency)

    def pending_orders(self) -> tuple[dict[str, Any], ...]:
        return self.journal.pending()

    def close(self) -> None:
        self.journal.close()
