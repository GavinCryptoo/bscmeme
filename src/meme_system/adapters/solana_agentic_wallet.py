"""Quote-only Binance Agentic Wallet adapter for Solana.

Only ``market-order quote`` is reachable from this module.  It deliberately
contains no wallet, swap, signing, transaction construction, or broadcast API.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Callable, Sequence

from meme_system.adapters.protocols import ExecutableQuote


SOLANA_CHAIN_ID = "CT_501"
NATIVE_SOL = "So11111111111111111111111111111111111111112"
PROVIDER = "BINANCE_AGENTIC_WALLET"


@dataclass(frozen=True)
class SolanaWalletQuoteFailure:
    reason: str
    latency_ms: int
    detail: str | None = None


@dataclass(frozen=True)
class CommandOutput:
    returncode: int
    stdout: str
    stderr: str


CommandRunner = Callable[[Sequence[str], float], CommandOutput]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def classify_failure(stdout: str, stderr: str, *, timed_out: bool = False) -> str:
    if timed_out:
        return "QUOTE_TIMEOUT"
    text = f"{stdout}\n{stderr}".lower()
    if "not logged in" in text or "session" in text or "unconnected" in text:
        return "SESSION_ERROR"
    if "429" in text or "rate limit" in text or "too many requests" in text:
        return "RATE_LIMIT"
    if "no route" in text or "route unavailable" in text or "not supported" in text:
        return "NO_ROUTE"
    if "liquidity" in text or "insufficient" in text:
        return "INSUFFICIENT_LIQUIDITY"
    if "invalid" in text or "malformed" in text or "parse" in text:
        return "INVALID_RESPONSE"
    return "OTHER"


class SolanaAgenticWalletQuoteProvider:
    """Read-only SOL quote provider backed by the official ``baw`` CLI."""

    provider = PROVIDER
    chain = "SOLANA"

    def __init__(
        self,
        *,
        baw_binary: str = "baw",
        timeout_sec: float = 8.0,
        command_runner: CommandRunner | None = None,
    ) -> None:
        self.baw_binary = baw_binary
        self.timeout_sec = max(1.0, float(timeout_sec))
        self._command_runner = command_runner or self._run

    @staticmethod
    def _run(command: Sequence[str], timeout_sec: float) -> CommandOutput:
        completed = subprocess.run(list(command), capture_output=True, text=True, check=False, timeout=timeout_sec)
        return CommandOutput(completed.returncode, completed.stdout, completed.stderr)

    def quote_result(
        self, mint: str, side: str, input_quantity: Decimal
    ) -> tuple[ExecutableQuote | None, SolanaWalletQuoteFailure | None]:
        if not mint or side not in {"buy", "sell"} or input_quantity <= 0:
            return None, SolanaWalletQuoteFailure("INVALID_RESPONSE", 0, "invalid_quote_request")
        from_token, to_token = (NATIVE_SOL, mint) if side == "buy" else (mint, NATIVE_SOL)
        command = (
            self.baw_binary, "--json", "market-order", "quote",
            "--binanceChainId", SOLANA_CHAIN_ID,
            "--fromTokenQty", format(input_quantity, "f"),
            "--fromToken", from_token,
            "--toToken", to_token,
            "--slippage", "auto",
        )
        requested_at = _now()
        try:
            output = self._command_runner(command, self.timeout_sec)
        except subprocess.TimeoutExpired:
            received_at = _now()
            latency = max(0, int((received_at - requested_at).total_seconds() * 1000))
            return None, SolanaWalletQuoteFailure("QUOTE_TIMEOUT", latency)
        except OSError as exc:
            received_at = _now()
            latency = max(0, int((received_at - requested_at).total_seconds() * 1000))
            return None, SolanaWalletQuoteFailure("OTHER", latency, type(exc).__name__)
        received_at = _now()
        latency = max(0, int((received_at - requested_at).total_seconds() * 1000))
        try:
            payload = json.loads(output.stdout)
        except json.JSONDecodeError:
            return None, SolanaWalletQuoteFailure(classify_failure(output.stdout, output.stderr), latency, "non_json")
        if output.returncode != 0 or not isinstance(payload, dict) or payload.get("success") is not True:
            return None, SolanaWalletQuoteFailure(classify_failure(output.stdout, output.stderr), latency)
        data = payload.get("data")
        if not isinstance(data, dict):
            return None, SolanaWalletQuoteFailure("INVALID_RESPONSE", latency, "missing_data")
        try:
            amount_in = Decimal(str(data["fromCoinAmount"]))
            amount_out = Decimal(str(data["toCoinAmount"]))
            impact_raw = data.get("priceImpactPct", data.get("priceImpact"))
            impact = Decimal(str(impact_raw)) if impact_raw is not None else None
        except (KeyError, InvalidOperation, ValueError):
            return None, SolanaWalletQuoteFailure("INVALID_RESPONSE", latency, "invalid_amount")
        if amount_in <= 0 or amount_out <= 0:
            return None, SolanaWalletQuoteFailure("INVALID_RESPONSE", latency, "non_positive_amount")
        raw_hash = hashlib.sha256(output.stdout.encode("utf-8")).hexdigest()
        route_value = data.get("route") or data.get("router") or data.get("provider") or PROVIDER
        quote = ExecutableQuote(
            quote_id=f"binance-agentic-wallet-sol:{raw_hash[:20]}",
            mint=mint,
            side=side,
            input_quantity=amount_in,
            output_quantity=amount_out,
            route_fee=None,
            price_impact_pct=impact,
            quoted_at=received_at,
            age_ms=0,
            provider=PROVIDER,
            route=(str(route_value), "chain:CT_501"),
            requested_at=requested_at,
            received_at=received_at,
            latency_ms=latency,
            executable_style=True,
            confidence="provider_quote",
            quote_source="binance_agentic_wallet_sol_quote",
            raw_response_hash=f"sha256:{raw_hash}",
        )
        return quote, None

    def quote(self, mint: str, side: str, input_quantity: Decimal) -> ExecutableQuote | None:
        quote, failure = self.quote_result(mint, side, input_quantity)
        if quote is not None:
            return quote
        now = _now()
        return ExecutableQuote(
            quote_id=f"binance-agentic-wallet-sol:unavailable:{mint}:{side}",
            mint=mint, side=side, input_quantity=input_quantity, output_quantity=Decimal("0"),
            route_fee=None, price_impact_pct=None, quoted_at=now, age_ms=0, expires_at=now,
            route_available=False, liquidity_available=False, provider=PROVIDER,
            requested_at=now, received_at=now, latency_ms=failure.latency_ms if failure else None,
            executable_style=False, confidence="unavailable",
            error_class=failure.reason if failure else "OTHER",
        )
