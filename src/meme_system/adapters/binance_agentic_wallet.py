"""Binance Agentic Wallet quote and explicitly gated live-executor adapters.

This adapter deliberately invokes only ``baw market-order quote``.  It has no
swap, approval, signer, wallet-export, or broadcast capability.  The Balanced
entry gate consumes it only through the asynchronous result bridge below.

``BinanceAgenticWalletLiveExecutor`` is a separate, fail-closed capability
boundary.  It is not wired into the Paper runner and defaults to
``swaps_enabled=False``.  That keeps the non-trading preflight and order-state
parsing testable without making it possible for a normal Paper process to
submit a real swap.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from queue import Empty, Queue
from typing import Any, Callable, Sequence

from meme_system.adapters.protocols import ExecutableQuote
from meme_system.adapters.bsc_wss import normalize_bsc_address


BINANCE_AGENTIC_WALLET_PROVIDER = "BINANCE_AGENTIC_WALLET"
BSC_CHAIN_ID = "56"
NATIVE_BNB = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"


@dataclass(frozen=True)
class QuoteFailure:
    """A classified read-only quote failure, safe to include in benchmark output."""

    reason: str
    detail: str | None = None
    latency_ms: int | None = None


@dataclass(frozen=True)
class LiveSwapResult:
    """A state transition for one Agentic Wallet market order.

    ``orderId`` only establishes ``SWAP_SUBMITTED``.  A caller must query
    :meth:`BinanceAgenticWalletLiveExecutor.get_order_status` and only treat
    ``SWAP_CONFIRMED`` as a completed execution.
    """

    stage: str
    order_id: str | None = None
    tx_hash: str | None = None
    provider_status: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    retry_allowed: bool = False
    needs_reconciliation: bool = False
    requested_at: datetime | None = None
    received_at: datetime | None = None
    latency_ms: int | None = None
    input_quantity: Decimal | None = None
    output_quantity: Decimal | None = None
    fee_quantity: Decimal | None = None


class BinanceAgenticWalletLiveExecutorError(RuntimeError):
    """Raised only for invalid local executor requests/configuration."""


@dataclass(frozen=True)
class _CommandOutput:
    returncode: int
    stdout: str
    stderr: str


CommandRunner = Callable[[Sequence[str], float], _CommandOutput]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return parsed if parsed.is_finite() else None


def classify_baw_failure(*, stdout: str, stderr: str, timed_out: bool = False) -> str:
    """Map the official CLI response to stable, benchmark-safe categories."""

    if timed_out:
        return "REQUEST_TIMEOUT"
    text = f"{stdout}\n{stderr}".lower()
    if "not logged in" in text or "session" in text or "unconnected" in text:
        return "SESSION_ERROR"
    if "429" in text or "rate limit" in text or "too many requests" in text:
        return "RATE_LIMIT"
    if "security" in text or "risk" in text or "blocked" in text:
        return "SECURITY_BLOCK"
    if "no route" in text or "route unavailable" in text:
        return "NO_ROUTE"
    if "unsupported" in text or "not supported" in text:
        return "TOKEN_UNSUPPORTED"
    if "liquidity" in text or "insufficient" in text:
        return "INSUFFICIENT_LIQUIDITY"
    if "invalid" in text or "malformed" in text or "parse" in text:
        return "INVALID_RESPONSE"
    return "OTHER_ERROR"


class BinanceAgenticWalletRouteProvider:
    """BSC-only, quote-only provider implementing the project's quote boundary.

    Authentication stays in the official CLI's local credential store.  This
    class never reads, writes, logs, or serializes any session material.
    """

    provider = BINANCE_AGENTIC_WALLET_PROVIDER
    chain = "BSC"

    def __init__(
        self,
        *,
        baw_binary: str = "baw",
        timeout_sec: float = 15.0,
        command_runner: CommandRunner | None = None,
    ) -> None:
        self.baw_binary = baw_binary
        self.timeout_sec = max(1.0, float(timeout_sec))
        self._command_runner = command_runner or self._run_command

    @staticmethod
    def _run_command(command: Sequence[str], timeout_sec: float) -> _CommandOutput:
        completed = subprocess.run(
            list(command),
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
        return _CommandOutput(completed.returncode, completed.stdout, completed.stderr)

    def quote_result(
        self,
        mint: str,
        side: str,
        input_quantity: Decimal,
    ) -> tuple[ExecutableQuote | None, QuoteFailure | None]:
        """Return one BNB/token quote without attempting any transaction action."""

        token = normalize_bsc_address(mint)
        if token is None or side not in {"buy", "sell"} or input_quantity <= 0:
            return None, QuoteFailure("INVALID_RESPONSE", "invalid_quote_request")
        from_token, to_token = (NATIVE_BNB, token) if side == "buy" else (token, NATIVE_BNB)
        command = (
            self.baw_binary,
            "market-order",
            "quote",
            "--json",
            "--binanceChainId",
            BSC_CHAIN_ID,
            "--fromTokenQty",
            format(input_quantity, "f"),
            "--fromToken",
            from_token,
            "--toToken",
            to_token,
            "--slippage",
            "auto",
        )
        requested_at = _utc_now()
        try:
            response = self._command_runner(command, self.timeout_sec)
        except subprocess.TimeoutExpired:
            received_at = _utc_now()
            return None, QuoteFailure(
                "REQUEST_TIMEOUT",
                f"baw_quote_timeout_after_{self.timeout_sec:g}s",
                int((received_at - requested_at).total_seconds() * 1000),
            )
        except OSError as exc:
            received_at = _utc_now()
            return None, QuoteFailure(
                "OTHER_ERROR",
                f"baw_process_error:{type(exc).__name__}",
                int((received_at - requested_at).total_seconds() * 1000),
            )
        received_at = _utc_now()
        latency_ms = max(0, int((received_at - requested_at).total_seconds() * 1000))
        try:
            payload = json.loads(response.stdout)
        except json.JSONDecodeError:
            return None, QuoteFailure(
                classify_baw_failure(stdout=response.stdout, stderr=response.stderr),
                "baw_non_json_response",
                latency_ms,
            )
        if response.returncode != 0 or payload.get("success") is not True:
            detail = None
            error = payload.get("error")
            if isinstance(error, dict):
                detail = str(error.get("message") or error.get("name") or "baw_error")
            return None, QuoteFailure(
                classify_baw_failure(stdout=response.stdout, stderr=response.stderr),
                detail,
                latency_ms,
            )
        data = payload.get("data")
        if not isinstance(data, dict):
            return None, QuoteFailure("INVALID_RESPONSE", "baw_missing_data", latency_ms)
        try:
            returned_input = Decimal(str(data["fromCoinAmount"]))
            output = Decimal(str(data["toCoinAmount"]))
            slippage = Decimal(str(data.get("slippage", "0"))) * Decimal("100")
        except (KeyError, InvalidOperation, ValueError):
            return None, QuoteFailure("INVALID_RESPONSE", "baw_invalid_amount", latency_ms)
        if returned_input <= 0 or output <= 0:
            return None, QuoteFailure("INVALID_RESPONSE", "baw_non_positive_amount", latency_ms)
        raw_hash = hashlib.sha256(response.stdout.encode("utf-8")).hexdigest()
        return (
            ExecutableQuote(
                quote_id=f"binance-agentic-wallet:{raw_hash[:20]}",
                mint=token,
                side=side,
                input_quantity=returned_input,
                output_quantity=output,
                route_fee=None,
                price_impact_pct=None,
                quoted_at=received_at,
                age_ms=0,
                provider=self.provider,
                quote_source="binance_agentic_wallet_quote",
                route=(self.provider, "chain:56", f"slippage_pct:{slippage}"),
                requested_at=requested_at,
                received_at=received_at,
                latency_ms=latency_ms,
                executable_style=True,
                confidence="provider_quote",
                raw_response_hash=f"sha256:{raw_hash}",
            ),
            None,
        )

    def quote(self, mint: str, side: str, input_quantity: Decimal) -> ExecutableQuote | None:
        quote, _failure = self.quote_result(mint, side, input_quantity)
        return quote

    def quote_candidate(
        self,
        mint: str,
        amount_bnb: Decimal,
        _fields: object | None = None,
    ) -> tuple[ExecutableQuote | None, ExecutableQuote | None, str | None]:
        """Quote BUY then the exact quoted token amount back to BNB, fail-closed."""

        buy, buy_failure = self.quote_result(mint, "buy", amount_bnb)
        if buy is None:
            return None, None, buy_failure.reason if buy_failure else "OTHER_ERROR"
        sell, sell_failure = self.quote_result(mint, "sell", buy.output_quantity)
        if sell is None:
            return buy, None, sell_failure.reason if sell_failure else "OTHER_ERROR"
        return buy, sell, None


class BinanceAgenticWalletLiveExecutor:
    """Explicitly gated BSC market-order executor.

    The executor owns the official ``baw`` CLI boundary but is deliberately
    *not* enabled by default.  ``swaps_enabled=True`` is a separate capability
    that must only be provided by a future, explicitly enabled BSC Live
    process.  Paper/Shadow code never constructs this class.

    A timeout is classified as ``SWAP_UNKNOWN`` and is never retried by this
    class.  The caller must reconcile through ``market-order list`` before any
    operator-approved retry, preventing duplicate buys/sells after an
    ambiguous network response.
    """

    provider = BINANCE_AGENTIC_WALLET_PROVIDER
    chain = "BSC"

    def __init__(
        self,
        *,
        baw_binary: str = "baw",
        timeout_sec: float = 20.0,
        chain_id: str = BSC_CHAIN_ID,
        slippage: str = "auto",
        mev: bool = True,
        gas_level: str = "HIGH",
        swaps_enabled: bool = False,
        security_precheck: Callable[[str, str], bool] | None = None,
        command_runner: CommandRunner | None = None,
    ) -> None:
        if str(chain_id) != BSC_CHAIN_ID:
            raise BinanceAgenticWalletLiveExecutorError("BSC chain id must be 56")
        if not (0 < float(timeout_sec) <= 120):
            raise BinanceAgenticWalletLiveExecutorError("timeout_sec must be in (0, 120]")
        if str(slippage).lower() != "auto":
            try:
                slippage_value = Decimal(str(slippage))
            except InvalidOperation as exc:
                raise BinanceAgenticWalletLiveExecutorError("invalid slippage") from exc
            if slippage_value < 0 or slippage_value > 100:
                raise BinanceAgenticWalletLiveExecutorError("slippage must be auto or 0..100")
            slippage = format(slippage_value, "f")
        gas_level = str(gas_level).upper()
        if gas_level not in {"LOW", "MEDIUM", "HIGH"}:
            raise BinanceAgenticWalletLiveExecutorError("gas_level must be LOW, MEDIUM, or HIGH")
        self.baw_binary = baw_binary
        self.timeout_sec = float(timeout_sec)
        self.chain_id = BSC_CHAIN_ID
        self.slippage = str(slippage)
        self.mev = bool(mev)
        self.gas_level = gas_level
        self.swaps_enabled = bool(swaps_enabled)
        self.security_precheck = security_precheck
        self._command_runner = command_runner or BinanceAgenticWalletRouteProvider._run_command

    @staticmethod
    def _safe_error(payload: Any, *, fallback: str) -> tuple[str, str | None]:
        error = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(error, dict):
            name = str(error.get("name") or error.get("code") or fallback)[:80]
            message = str(error.get("message") or name)[:200]
            return name, message
        return fallback, None

    def _call_json(self, command: Sequence[str]) -> tuple[_CommandOutput | None, dict[str, Any] | None, LiveSwapResult | None]:
        requested_at = _utc_now()
        try:
            response = self._command_runner(command, self.timeout_sec)
        except subprocess.TimeoutExpired:
            received_at = _utc_now()
            return None, None, LiveSwapResult(
                stage="SWAP_UNKNOWN",
                error_code="REQUEST_TIMEOUT",
                error_message="baw request timed out; reconcile order history before retry",
                needs_reconciliation=True,
                requested_at=requested_at,
                received_at=received_at,
                latency_ms=max(0, int((received_at - requested_at).total_seconds() * 1000)),
            )
        except OSError as exc:
            received_at = _utc_now()
            return None, None, LiveSwapResult(
                stage="SWAP_UNKNOWN",
                error_code=f"PROCESS_{type(exc).__name__.upper()}",
                error_message="baw process failed before a response; reconcile order history before retry",
                needs_reconciliation=True,
                requested_at=requested_at,
                received_at=received_at,
                latency_ms=max(0, int((received_at - requested_at).total_seconds() * 1000)),
            )
        received_at = _utc_now()
        try:
            payload = json.loads(response.stdout)
        except json.JSONDecodeError:
            return response, None, LiveSwapResult(
                stage="SWAP_FAILED",
                error_code=classify_baw_failure(stdout=response.stdout, stderr=response.stderr),
                error_message="baw returned a non-JSON response",
                requested_at=requested_at,
                received_at=received_at,
                latency_ms=max(0, int((received_at - requested_at).total_seconds() * 1000)),
            )
        if response.returncode != 0 or payload.get("success") is not True:
            code, message = self._safe_error(payload, fallback=classify_baw_failure(stdout=response.stdout, stderr=response.stderr))
            return response, payload, LiveSwapResult(
                stage="SWAP_FAILED",
                error_code=code,
                error_message=message,
                requested_at=requested_at,
                received_at=received_at,
                latency_ms=max(0, int((received_at - requested_at).total_seconds() * 1000)),
            )
        return response, payload, None

    def _read_only(self, command: Sequence[str]) -> tuple[dict[str, Any] | None, LiveSwapResult | None]:
        _response, payload, failure = self._call_json(command)
        return payload, failure

    def preflight(self) -> dict[str, Any]:
        """Read wallet capabilities without invoking a state-changing command."""

        checks: dict[str, Any] = {}
        commands = {
            "status": ("wallet", "status", "--json"),
            "chains": ("wallet", "chains", "--json"),
            "settings": ("wallet", "settings", "--json"),
            "tx_lock": ("wallet", "tx-lock", "--binanceChainId", self.chain_id, "--json"),
            "left_quota": ("wallet", "left-quota", "--json"),
            "balance": ("wallet", "balance", "--binanceChainId", self.chain_id, "--json"),
        }
        for name, suffix in commands.items():
            payload, failure = self._read_only((self.baw_binary, *suffix))
            if failure is not None:
                checks[name] = {"ok": False, "stage": failure.stage, "error_code": failure.error_code, "error_message": failure.error_message}
            else:
                checks[name] = {"ok": True, "data": payload.get("data") if isinstance(payload, dict) else None}
        chains_data = checks.get("chains", {}).get("data")
        if isinstance(chains_data, dict):
            chains = chains_data.get("chains") or chains_data.get("list") or []
        else:
            chains = chains_data if isinstance(chains_data, list) else []
        checks["bsc_supported"] = any(str(item.get("binanceChainId")) == self.chain_id for item in chains if isinstance(item, dict))
        checks["swap_commands"] = {"quote_only": True, "swap_enabled": self.swaps_enabled}
        return checks

    def get_balance(self, token: str | None = None) -> dict[str, Any]:
        command: list[str] = [self.baw_binary, "wallet", "balance", "--binanceChainId", self.chain_id, "--json"]
        if token:
            normalized = normalize_bsc_address(token)
            if normalized is None:
                raise BinanceAgenticWalletLiveExecutorError("invalid BSC token address")
            command.extend(("--tokenAddress", normalized))
        payload, failure = self._read_only(tuple(command))
        if failure is not None:
            return {"ok": False, "stage": failure.stage, "error_code": failure.error_code, "error_message": failure.error_message}
        return {"ok": True, "data": payload.get("data") if isinstance(payload, dict) else None}

    def get_order_status(self, order_id: str) -> LiveSwapResult:
        if not str(order_id).strip():
            raise BinanceAgenticWalletLiveExecutorError("order_id is required")
        command = (
            self.baw_binary,
            "market-order",
            "list",
            "--json",
            "--binanceChainId",
            self.chain_id,
            "--orderId",
            str(order_id),
        )
        requested_at = _utc_now()
        _response, payload, failure = self._call_json(command)
        if failure is not None:
            return failure
        data = payload.get("data") if isinstance(payload, dict) else None
        orders = data.get("list") if isinstance(data, dict) else data
        order = orders[0] if isinstance(orders, list) and orders else data if isinstance(data, dict) and data.get("orderId") else None
        if not isinstance(order, dict):
            return LiveSwapResult(stage="SWAP_UNKNOWN", order_id=str(order_id), error_code="ORDER_NOT_FOUND", error_message="market-order list returned no matching order", needs_reconciliation=True, requested_at=requested_at, received_at=_utc_now())
        status = str(order.get("status") or "").upper()
        if status == "FINISHED":
            stage = "SWAP_CONFIRMED"
        elif status == "PENDING":
            stage = "SWAP_PENDING"
        elif status == "FAILED":
            stage = "SWAP_FAILED"
        else:
            stage = "SWAP_UNKNOWN"
        return LiveSwapResult(
            stage=stage,
            order_id=str(order.get("orderId") or order_id),
            tx_hash=str(order.get("txHash")) if order.get("txHash") else None,
            provider_status=status or None,
            error_code=(str(order.get("errorCode")) if order.get("errorCode") else None),
            error_message=(str(order.get("errorMessage"))[:200] if order.get("errorMessage") else None),
            needs_reconciliation=stage in {"SWAP_PENDING", "SWAP_UNKNOWN"},
            requested_at=requested_at,
            received_at=_utc_now(),
            input_quantity=_decimal_or_none(order.get("fromCoinAmount")),
            output_quantity=_decimal_or_none(order.get("toCoinAmount")),
            fee_quantity=_decimal_or_none(order.get("fee")),
        )

    def find_recent_order(
        self,
        token: str,
        quantity: Decimal,
        *,
        side: str = "sell",
        requested_at: datetime | None = None,
        window_sec: int = 180,
    ) -> LiveSwapResult | None:
        """Reconcile an ambiguous swap response without submitting anything.

        Agentic Wallet can return a non-success response after its order has
        already been accepted.  Before exposing ``SWAP_FAILED`` (and enabling
        a dangerous retry), inspect the recent read-only order list and match
        the exact token/quantity/time window.
        """
        normalized = normalize_bsc_address(token)
        if normalized is None or quantity <= 0 or side not in {"buy", "sell"}:
            return None
        _response, payload, failure = self._call_json(
            (self.baw_binary, "market-order", "list", "--json", "--binanceChainId", self.chain_id)
        )
        if failure is not None or not isinstance(payload, dict):
            return None
        data = payload.get("data")
        orders = data.get("list") if isinstance(data, dict) else data
        if not isinstance(orders, list):
            return None
        anchor = requested_at or _utc_now()
        if anchor.tzinfo is None:
            anchor = anchor.replace(tzinfo=timezone.utc)
        earliest = anchor - timedelta(seconds=max(1, int(window_sec)))
        native = NATIVE_BNB.lower()
        matches: list[tuple[datetime, dict[str, Any]]] = []
        for order in orders:
            if not isinstance(order, dict) or str(order.get("status") or "").upper() not in {"PENDING", "FINISHED"}:
                continue
            from_token = str(order.get("fromToken") or "").lower()
            to_token = str(order.get("toToken") or "").lower()
            if side == "sell":
                if from_token != normalized.lower() or to_token != native:
                    continue
            elif from_token != native or to_token != normalized.lower():
                continue
            order_quantity = _decimal_or_none(order.get("fromTokenQty") or order.get("fromCoinAmount"))
            if order_quantity is None:
                continue
            tolerance = max(Decimal("1e-18"), quantity.copy_abs() * Decimal("1e-12"))
            if abs(order_quantity - quantity) > tolerance:
                continue
            raw_time = order.get("bookTime") or order.get("updatedTime")
            try:
                observed = datetime.fromisoformat(str(raw_time).replace("Z", "+00:00"))
            except (TypeError, ValueError):
                continue
            if observed.tzinfo is None:
                observed = observed.replace(tzinfo=timezone.utc)
            observed_utc = observed.astimezone(timezone.utc)
            if observed_utc < earliest or observed_utc > anchor + timedelta(seconds=30):
                continue
            matches.append((observed_utc, order))
        if not matches:
            return None
        _observed, order = max(matches, key=lambda item: item[0])
        status = str(order.get("status") or "").upper()
        return LiveSwapResult(
            stage="SWAP_CONFIRMED" if status == "FINISHED" else "SWAP_PENDING",
            order_id=str(order.get("orderId")) if order.get("orderId") else None,
            tx_hash=str(order.get("txHash")) if order.get("txHash") else None,
            provider_status=status,
            needs_reconciliation=status != "FINISHED",
            requested_at=anchor,
            received_at=_utc_now(),
            input_quantity=_decimal_or_none(order.get("fromTokenQty") or order.get("fromCoinAmount")),
            output_quantity=_decimal_or_none(order.get("toTokenActualQty") or order.get("toCoinAmount")),
            fee_quantity=_decimal_or_none(order.get("fee")),
        )

    def _swap(self, token: str, quantity: Decimal, *, side: str) -> LiveSwapResult:
        normalized = normalize_bsc_address(token)
        if normalized is None or quantity <= 0 or side not in {"buy", "sell"}:
            raise BinanceAgenticWalletLiveExecutorError("invalid swap request")
        requested_at = _utc_now()
        if not self.swaps_enabled:
            return LiveSwapResult(stage="SWAP_BLOCKED", error_code="REAL_SWAP_DISABLED", error_message="live swap capability is disabled", requested_at=requested_at, received_at=requested_at)

        # Preflight is deliberately repeated immediately before a future live
        # submission.  A LOCKED wallet can mean a pending transaction or a
        # double-confirm request and must never be bypassed.
        status_payload, status_failure = self._read_only((self.baw_binary, "wallet", "status", "--json"))
        status = status_payload.get("data", {}).get("status") if isinstance(status_payload, dict) else None
        if status_failure is not None or status != "CONNECTED":
            return LiveSwapResult(stage="SWAP_FAILED", error_code="SESSION_ERROR", error_message="wallet is not CONNECTED", requested_at=requested_at, received_at=_utc_now())
        lock_payload, lock_failure = self._read_only((self.baw_binary, "wallet", "tx-lock", "--binanceChainId", self.chain_id, "--json"))
        lock_status = lock_payload.get("data", {}).get("status") if isinstance(lock_payload, dict) else None
        if lock_failure is not None or lock_status != "UNLOCKED":
            return LiveSwapResult(stage="SWAP_FAILED", error_code="TX_LOCKED", error_message="wallet transaction lock is not UNLOCKED", requested_at=requested_at, received_at=_utc_now())
        settings_payload, settings_failure = self._read_only((self.baw_binary, "wallet", "settings", "--json"))
        settings = settings_payload.get("data") if isinstance(settings_payload, dict) else None
        if settings_failure is not None or not isinstance(settings, dict):
            return LiveSwapResult(stage="SWAP_FAILED", error_code="SECURITY_PRECHECK_UNAVAILABLE", error_message="wallet security settings are unavailable", requested_at=requested_at, received_at=_utc_now())
        requires_confirmation = str(settings.get("abnormalTxnHandling") or "").lower() == "needconfirmation"
        if self.security_precheck is None:
            return LiveSwapResult(stage="SWAP_FAILED", error_code="SECURITY_PRECHECK_UNAVAILABLE", error_message="target-token security precheck is not configured", requested_at=requested_at, received_at=_utc_now())
        try:
            security_ok = bool(self.security_precheck(normalized, side))
        except Exception:
            security_ok = False
        if not security_ok:
            return LiveSwapResult(stage="SWAP_FAILED", error_code="SECURITY_BLOCK", error_message="target-token security precheck failed", requested_at=requested_at, received_at=_utc_now())
        from_token, to_token = (NATIVE_BNB, normalized) if side == "buy" else (normalized, NATIVE_BNB)
        command = (
            self.baw_binary,
            "market-order",
            "swap",
            "--json",
            "--binanceChainId",
            self.chain_id,
            "--fromTokenQty",
            format(quantity, "f"),
            "--fromToken",
            from_token,
            "--toToken",
            to_token,
            "--slippage",
            self.slippage,
            "--mev",
            "true" if self.mev else "false",
            "--gasLevel",
            self.gas_level,
        )
        _response, payload, failure = self._call_json(command)
        if failure is not None:
            # _call_json maps transport ambiguity to SWAP_UNKNOWN and never
            # retries.  A confirmed API rejection remains SWAP_FAILED.
            if requires_confirmation and failure.error_code in {"APP_CONFIRMATION_REQUIRED", "NEED_CONFIRMATION", "USER_CONFIRMATION_REQUIRED"}:
                return LiveSwapResult(stage="SWAP_PENDING", error_code="APP_CONFIRMATION_REQUIRED", error_message="wallet requires Binance App confirmation", needs_reconciliation=True, requested_at=requested_at, received_at=_utc_now())
            return failure
        data = payload.get("data") if isinstance(payload, dict) else None
        order_id = data.get("orderId") if isinstance(data, dict) else None
        if not order_id:
            return LiveSwapResult(stage="SWAP_FAILED", error_code="INVALID_RESPONSE", error_message="swap response missing orderId", requested_at=requested_at, received_at=_utc_now())
        return LiveSwapResult(stage="SWAP_SUBMITTED", order_id=str(order_id), needs_reconciliation=True, requested_at=requested_at, received_at=_utc_now())

    def buy(self, token: str, bnb_amount: Decimal) -> LiveSwapResult:
        return self._swap(token, bnb_amount, side="buy")

    def sell(self, token: str, token_amount: Decimal) -> LiveSwapResult:
        return self._swap(token, token_amount, side="sell")

    def close(self) -> None:
        """No persistent worker is owned by the synchronous CLI executor."""
        return None


class BinancePrimaryWithFallbackRouteProvider:
    """Prefer Wallet routes; only then call the existing verified provider.

    This composition is used only inside the asynchronous worker bridge below;
    neither provider's I/O ever runs on the Balanced owner loop.
    """

    def __init__(self, primary: BinanceAgenticWalletRouteProvider, fallback: Any) -> None:
        self.primary = primary
        self.fallback = fallback

    def quote_candidate(
        self,
        mint: str,
        amount_bnb: Decimal,
        fields: object | None = None,
    ) -> tuple[ExecutableQuote | None, ExecutableQuote | None, str | None]:
        buy, sell, error = self.primary.quote_candidate(mint, amount_bnb, fields)
        if buy is not None and sell is not None:
            return buy, sell, None
        fallback_quote = getattr(self.fallback, "quote_candidate", None)
        if not callable(fallback_quote):
            return buy, sell, error or "QUOTE_PROVIDER_UNAVAILABLE"
        fallback_buy, fallback_sell, fallback_error = fallback_quote(mint, amount_bnb, fields)
        if fallback_buy is not None and fallback_sell is not None:
            return fallback_buy, fallback_sell, None
        return fallback_buy or buy, fallback_sell or sell, fallback_error or error or "BUY_OR_SELL_QUOTE_UNAVAILABLE"

    def quote_sell(self, mint: str, quantity: Decimal) -> tuple[ExecutableQuote | None, str | None]:
        quote, failure = self.primary.quote_result(mint, "sell", quantity)
        if quote is not None:
            return quote, None
        fallback_quote = getattr(self.fallback, "quote", None)
        direct = fallback_quote(mint, "sell", quantity) if callable(fallback_quote) else None
        return direct, None if direct is not None else (failure.reason if failure else "SELL_QUOTE_UNAVAILABLE")


class AsyncRoundTripQuoteProvider:
    """Thread-safe bridge: worker quote I/O -> main-loop result consumption.

    Workers only return quote values through :class:`queue.Queue`; they never
    receive or touch a SQLite connection.
    """

    pending_reason = "QUOTE_PENDING"

    def __init__(self, provider: Any, *, max_workers: int = 2) -> None:
        self.provider = provider
        self._executor = ThreadPoolExecutor(max_workers=max(1, max_workers), thread_name_prefix="bsc-route-quote")
        self._completed: Queue[tuple[tuple[str, str], ExecutableQuote | None, ExecutableQuote | None, str | None]] = Queue()
        self._pending: set[tuple[str, str]] = set()
        self._ready: dict[tuple[str, str], tuple[ExecutableQuote | None, ExecutableQuote | None, str | None]] = {}
        self._closed = False
        self._sell_completed: Queue[tuple[tuple[str, str], ExecutableQuote | None, str | None]] = Queue()
        self._sell_pending: set[tuple[str, str]] = set()
        self._sell_ready: dict[tuple[str, str], tuple[ExecutableQuote | None, str | None]] = {}

    @staticmethod
    def _key(mint: str, amount_bnb: Decimal) -> tuple[str, str]:
        return (mint.lower(), format(amount_bnb, "f"))

    def _drain(self) -> None:
        while True:
            try:
                key, buy, sell, error = self._completed.get_nowait()
            except Empty:
                return
            self._pending.discard(key)
            self._ready[key] = (buy, sell, error)

    def _submit(self, key: tuple[str, str], mint: str, amount_bnb: Decimal, fields: object | None) -> None:
        self._pending.add(key)
        future = self._executor.submit(self.provider.quote_candidate, mint, amount_bnb, fields)

        def complete(result: object) -> None:
            try:
                buy, sell, error = result.result()  # type: ignore[attr-defined]
            except Exception as exc:
                buy, sell, error = None, None, f"QUOTE_WORKER_{type(exc).__name__.upper()}"
            self._completed.put((key, buy, sell, error))

        future.add_done_callback(complete)

    def quote_candidate(
        self,
        mint: str,
        amount_bnb: Decimal,
        fields: object | None = None,
    ) -> tuple[ExecutableQuote | None, ExecutableQuote | None, str | None]:
        self._drain()
        key = self._key(mint, amount_bnb)
        completed = self._ready.pop(key, None)
        if completed is not None:
            return completed
        if self._closed:
            return None, None, "QUOTE_PROVIDER_CLOSED"
        if key not in self._pending:
            self._submit(key, mint, amount_bnb, fields)
        return None, None, self.pending_reason

    def sell_quote(self, mint: str, quantity: Decimal) -> tuple[ExecutableQuote | None, str | None]:
        key = self._key(mint, quantity)
        while True:
            try:
                done_key, quote, error = self._sell_completed.get_nowait()
            except Empty:
                break
            self._sell_pending.discard(done_key)
            self._sell_ready[done_key] = (quote, error)
        ready = self._sell_ready.pop(key, None)
        if ready is not None:
            return ready
        if key not in self._sell_pending and not self._closed:
            self._sell_pending.add(key)
            future = self._executor.submit(self.provider.quote_sell, mint, quantity)
            def complete(result: object) -> None:
                try:
                    quote, error = result.result()  # type: ignore[attr-defined]
                except Exception as exc:
                    quote, error = None, f"QUOTE_WORKER_{type(exc).__name__.upper()}"
                self._sell_completed.put((key, quote, error))
            future.add_done_callback(complete)
        return None, self.pending_reason

    def status(self) -> dict[str, object]:
        self._drain()
        return {"provider": getattr(self.provider, "provider", type(self.provider).__name__), "async_io": True, "pending": len(self._pending), "ready": len(self._ready), "closed": self._closed}

    def close(self) -> None:
        self._closed = True
        self._executor.shutdown(wait=False, cancel_futures=True)


@dataclass(frozen=True)
class LiveExecutionEvent:
    """Immutable worker result consumed by the Balanced owner loop."""

    request_id: str
    action: str
    key: str
    token: str | None
    quantity: Decimal | None
    result: Any
    requested_at: datetime
    completed_at: datetime


class AsyncLiveExecutionBridge:
    """Run BAW swap/order/balance I/O off the SQLite-owning main loop.

    A key is single-flight.  In particular, a timeout never permits a second
    request for the same order to be submitted while reconciliation is still
    pending.  The bridge only transports immutable results; it never receives
    a database connection and never mutates strategy state.
    """

    def __init__(
        self,
        executor: Any,
        *,
        max_workers: int = 3,
        wallet_balance_reader: Callable[[str], Decimal | None] | None = None,
    ) -> None:
        self.executor = executor
        self.wallet_balance_reader = wallet_balance_reader
        self._pool = ThreadPoolExecutor(max_workers=max(1, int(max_workers)), thread_name_prefix="live-execution-io")
        self._completed: Queue[LiveExecutionEvent] = Queue()
        self._pending: set[str] = set()
        self._closed = False

    def submit(self, action: str, key: str, token: str | None = None, quantity: Decimal | None = None) -> str | None:
        if self._closed or key in self._pending:
            return None
        if action not in {"buy", "sell", "order_status", "balance", "quote_sell", "wallet_reconcile"}:
            raise ValueError("unsupported live execution action")
        request_id = hashlib.sha256(f"{action}:{key}:{_utc_now().timestamp()}".encode()).hexdigest()[:20]
        self._pending.add(key)
        requested_at = _utc_now()
        if action == "buy":
            call = lambda: self.executor.buy(str(token), quantity or Decimal("0"))
        elif action == "sell":
            call = lambda: self.executor.sell(str(token), quantity or Decimal("0"))
        elif action == "order_status":
            call = lambda: self.executor.get_order_status(key)
        elif action == "quote_sell":
            call = lambda: self.executor.quote_sell(str(token), quantity or Decimal("0"))
        elif action == "wallet_reconcile":
            reader = self.wallet_balance_reader
            call = (lambda: reader(str(token))) if reader is not None else (lambda: None)
        else:
            call = lambda: self.executor.get_balance(token)
        future = self._pool.submit(call)

        def complete(done: object) -> None:
            try:
                result = done.result()  # type: ignore[attr-defined]
            except Exception as exc:
                result = LiveSwapResult(
                    stage="SWAP_UNKNOWN",
                    error_code=f"WORKER_{type(exc).__name__.upper()}",
                    error_message="live execution worker failed; reconciliation required",
                    needs_reconciliation=True,
                )
            self._completed.put(LiveExecutionEvent(
                request_id=request_id,
                action=action,
                key=key,
                token=token,
                quantity=quantity,
                result=result,
                requested_at=requested_at,
                completed_at=_utc_now(),
            ))

        future.add_done_callback(complete)
        return request_id

    def poll(self, max_items: int = 32) -> tuple[LiveExecutionEvent, ...]:
        events: list[LiveExecutionEvent] = []
        for _ in range(max(1, int(max_items))):
            try:
                event = self._completed.get_nowait()
            except Empty:
                break
            self._pending.discard(event.key)
            events.append(event)
        return tuple(events)

    def status(self) -> dict[str, object]:
        return {"pending": len(self._pending), "queue_depth": self._completed.qsize(), "closed": self._closed}

    def close(self) -> None:
        self._closed = True
        self._pool.shutdown(wait=False, cancel_futures=True)
