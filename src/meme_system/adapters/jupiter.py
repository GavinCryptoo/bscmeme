"""Jupiter read-only Quote provider.

Only ``GET /swap/v1/quote`` is implemented. The module intentionally has no
swap, swap-instructions, transaction, wallet, signing, or broadcast method.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from meme_system.adapters.protocols import ExecutableQuote


SOL_MINT = "So11111111111111111111111111111111111111112"
JUPITER_QUOTE_URL = "https://api.jup.ag/swap/v1/quote"


class JupiterQuoteError(RuntimeError):
    def __init__(self, message: str, *, error_class: str) -> None:
        self.error_class = error_class
        super().__init__(message[:500])


class JupiterReadOnlyQuoteProvider:
    """Create quote records from Jupiter's current public Quote endpoint."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        token_decimals: Mapping[str, int] | None = None,
        default_token_decimals: int | None = None,
        url: str | None = None,
        slippage_bps: int = 50,
        quote_ttl_ms: int = 1_500,
        timeout_sec: float = 10.0,
        price_impact_unit: str | None = None,
        transport: Callable[[str, Mapping[str, str], float], tuple[int, bytes, Mapping[str, str]]] | None = None,
    ) -> None:
        self.api_key = api_key if api_key is not None else os.environ.get("JUPITER_API_KEY", "").strip()
        self.token_decimals = dict(token_decimals or {})
        self.default_token_decimals = default_token_decimals
        self.url = url or os.environ.get("JUPITER_QUOTE_URL", "").strip() or JUPITER_QUOTE_URL
        self.slippage_bps = max(0, min(10_000, int(slippage_bps)))
        self.quote_ttl_ms = max(100, min(30_000, int(quote_ttl_ms)))
        self.timeout_sec = max(0.1, min(60.0, timeout_sec))
        if price_impact_unit not in {None, "ratio", "percent"}:
            raise ValueError("price_impact_unit must be ratio, percent, or None")
        self.price_impact_unit = price_impact_unit
        self.transport = transport or self._transport
        self.requests = 0
        self.last_error_class: str | None = None

    @classmethod
    def from_env(cls, *, token_decimals: Mapping[str, int] | None = None) -> "JupiterReadOnlyQuoteProvider":
        return cls(
            token_decimals=token_decimals,
            slippage_bps=_env_int("JUPITER_SLIPPAGE_BPS", 50, 0, 10_000),
            quote_ttl_ms=_env_int("JUPITER_QUOTE_TTL_MS", 1_500, 100, 30_000),
            timeout_sec=_env_float("JUPITER_TIMEOUT_SEC", 10.0, 0.1, 60.0),
            price_impact_unit=os.environ.get("JUPITER_PRICE_IMPACT_UNIT") or None,
        )

    def safe_status(self) -> dict[str, object]:
        return {
            "provider": "jupiter",
            "endpoint_configured": bool(self.url),
            "credentials_configured": bool(self.api_key),
            "read_only": True,
            "requests": self.requests,
            "last_error_class": self.last_error_class,
        }

    def quote(self, mint: str, side: str, input_quantity: Decimal) -> ExecutableQuote | None:
        if side == "buy":
            return self.quote_buy(mint, input_quantity)
        if side == "sell":
            return self.quote_sell(mint, input_quantity)
        raise ValueError("side must be buy or sell")

    def quote_buy(self, mint: str, input_sol: Decimal) -> ExecutableQuote:
        try:
            decimals = self._token_decimals(mint)
        except JupiterQuoteError as exc:
            self.last_error_class = exc.error_class
            return self._unavailable(mint, "buy", input_sol, exc.error_class)
        return self._quote(
            mint=mint,
            side="buy",
            input_quantity=input_sol,
            input_mint=SOL_MINT,
            output_mint=mint,
            input_decimals=9,
            output_decimals=decimals,
        )

    def quote_sell(self, mint: str, input_tokens: Decimal) -> ExecutableQuote:
        try:
            decimals = self._token_decimals(mint)
        except JupiterQuoteError as exc:
            self.last_error_class = exc.error_class
            return self._unavailable(mint, "sell", input_tokens, exc.error_class)
        return self._quote(
            mint=mint,
            side="sell",
            input_quantity=input_tokens,
            input_mint=mint,
            output_mint=SOL_MINT,
            input_decimals=decimals,
            output_decimals=9,
        )

    def _quote(
        self,
        *,
        mint: str,
        side: str,
        input_quantity: Decimal,
        input_mint: str,
        output_mint: str,
        input_decimals: int,
        output_decimals: int,
    ) -> ExecutableQuote:
        if input_quantity <= 0:
            raise ValueError("input_quantity must be positive")
        if not self.api_key:
            self.last_error_class = "jupiter_auth_missing"
            return self._unavailable(mint, side, input_quantity, "jupiter_auth_missing")
        raw_amount = _to_raw_amount(input_quantity, input_decimals)
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(raw_amount),
            "slippageBps": str(self.slippage_bps),
            "restrictIntermediateTokens": "true",
            "instructionVersion": "V2",
        }
        request_id = str(uuid.uuid4())
        requested_at = datetime.now(timezone.utc)
        started = time.monotonic()
        self.requests += 1
        try:
            status, body, _headers = self.transport(self.url + "?" + urlencode(params), {"x-api-key": self.api_key, "Accept": "application/json"}, self.timeout_sec)
            if status == 429:
                raise JupiterQuoteError("Jupiter rate limited", error_class="jupiter_rate_limited")
            if status >= 400:
                raise JupiterQuoteError(f"Jupiter HTTP {status}", error_class="jupiter_http_error")
            parsed = json.loads(body.decode("utf-8"))
            if not isinstance(parsed, Mapping):
                raise JupiterQuoteError("Jupiter quote is not an object", error_class="jupiter_schema_changed")
            out_amount = _decimal_int(parsed.get("outAmount"), "outAmount")
            in_amount = _decimal_int(parsed.get("inAmount"), "inAmount")
            output_quantity = out_amount / (Decimal(10) ** output_decimals)
            actual_input = in_amount / (Decimal(10) ** input_decimals)
            route_plan = parsed.get("routePlan")
            route_available = isinstance(route_plan, list) and len(route_plan) > 0 and output_quantity > 0
            received_at = datetime.now(timezone.utc)
            return ExecutableQuote(
                quote_id=f"jupiter:{request_id}",
                mint=mint,
                side=side,
                input_quantity=actual_input,
                output_quantity=output_quantity,
                route_fee=None,
                price_impact_pct=_price_impact_pct(parsed.get("priceImpactPct"), self.price_impact_unit),
                quoted_at=received_at,
                age_ms=0,
                expires_at=received_at + timedelta(milliseconds=self.quote_ttl_ms),
                route_available=route_available,
                liquidity_available=route_available,
                provider="jupiter",
                route=_route_labels(route_plan),
                quote_context_slot=_optional_int(parsed.get("contextSlot")),
                requested_at=requested_at,
                received_at=received_at,
                latency_ms=int((time.monotonic() - started) * 1000),
                executable_style=route_available,
                confidence="verified" if route_available else "unavailable",
                error_class=None if route_available else "jupiter_no_route",
                raw_response_hash=_payload_hash(parsed),
            )
        except JupiterQuoteError as exc:
            self.last_error_class = exc.error_class
            return self._unavailable(mint, side, input_quantity, exc.error_class, requested_at=requested_at, latency_ms=int((time.monotonic() - started) * 1000))
        except (TimeoutError, URLError, OSError) as exc:
            self.last_error_class = "jupiter_connection_error"
            return self._unavailable(mint, side, input_quantity, "jupiter_connection_error", requested_at=requested_at, latency_ms=int((time.monotonic() - started) * 1000))
        except (UnicodeDecodeError, json.JSONDecodeError, InvalidOperation, TypeError, ValueError) as exc:
            self.last_error_class = "jupiter_schema_changed"
            return self._unavailable(mint, side, input_quantity, "jupiter_schema_changed", requested_at=requested_at, latency_ms=int((time.monotonic() - started) * 1000))

    def _unavailable(
        self,
        mint: str,
        side: str,
        input_quantity: Decimal,
        error_class: str,
        *,
        requested_at: datetime | None = None,
        latency_ms: int | None = None,
    ) -> ExecutableQuote:
        now = datetime.now(timezone.utc)
        return ExecutableQuote(
            quote_id=f"jupiter:unavailable:{uuid.uuid4()}",
            mint=mint,
            side=side,
            input_quantity=input_quantity,
            output_quantity=Decimal("0"),
            route_fee=None,
            price_impact_pct=None,
            quoted_at=now,
            age_ms=0,
            expires_at=now,
            route_available=False,
            liquidity_available=False,
            provider="jupiter",
            requested_at=requested_at,
            received_at=now,
            latency_ms=latency_ms,
            executable_style=False,
            confidence="unavailable",
            error_class=error_class,
        )

    def _token_decimals(self, mint: str) -> int:
        value = self.token_decimals.get(mint, self.default_token_decimals)
        if value is None or not 0 <= int(value) <= 18:
            raise JupiterQuoteError("token decimals are unavailable", error_class="jupiter_token_decimals_missing")
        return int(value)

    @staticmethod
    def _transport(url: str, headers: Mapping[str, str], timeout_sec: float) -> tuple[int, bytes, Mapping[str, str]]:
        request = Request(url=url, headers=dict(headers), method="GET")
        try:
            with urlopen(request, timeout=timeout_sec) as response:
                return response.status, response.read(2_000_001), dict(response.headers.items())
        except HTTPError as exc:
            return exc.code, exc.read(2_000_001), dict(exc.headers.items())


def _to_raw_amount(value: Decimal, decimals: int) -> int:
    scaled = value * (Decimal(10) ** decimals)
    if scaled != scaled.to_integral_value():
        raise ValueError("input quantity cannot be represented in token base units")
    return int(scaled)


def _decimal_int(value: object, field: str) -> int:
    if isinstance(value, bool) or value is None:
        raise JupiterQuoteError(f"Jupiter {field} missing", error_class="jupiter_schema_changed")
    parsed = int(str(value))
    if parsed < 0:
        raise JupiterQuoteError(f"Jupiter {field} is negative", error_class="jupiter_schema_changed")
    return parsed


def _price_impact_pct(value: object, unit: str | None) -> Decimal | None:
    if value is None:
        return None
    if unit is None:
        # The current official schema exposes the field but does not define its
        # unit in the API contract. Do not convert it by inference.
        return None
    parsed = Decimal(str(value))
    return parsed * Decimal("100") if unit == "ratio" else parsed


def _route_labels(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    labels: list[str] = []
    for step in value:
        if isinstance(step, Mapping) and isinstance(step.get("swapInfo"), Mapping):
            info = step["swapInfo"]
            label = info.get("label") or info.get("ammKey")
            if label is not None:
                labels.append(str(label))
    return tuple(labels)


def _optional_int(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _payload_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _env_float(name: str, default: float, minimum: float, maximum: float) -> float:
    try:
        return min(maximum, max(minimum, float(os.environ.get(name, default))))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        return min(maximum, max(minimum, int(os.environ.get(name, default))))
    except (TypeError, ValueError):
        return default
