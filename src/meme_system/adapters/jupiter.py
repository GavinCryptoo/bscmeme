"""Jupiter read-only Quote provider.

Only read-only ``GET /swap/v2/order`` is implemented. The module intentionally has no
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
from pathlib import Path
from threading import Event, Lock, Semaphore
from typing import Any, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from meme_system.adapters.protocols import ExecutableQuote
from meme_system.adapters.solana_readonly import SolanaReadOnlyError, SolanaRpcClient


SOL_MINT = "So11111111111111111111111111111111111111112"
JUPITER_QUOTE_URL = "https://api.jup.ag/swap/v2/order"


class JupiterQuoteError(RuntimeError):
    def __init__(self, message: str, *, error_class: str) -> None:
        self.error_class = error_class
        super().__init__(message[:500])


class TokenDecimalsCache:
    """Resolve decimals from overrides, local cache, then Solana RPC."""

    def __init__(
        self,
        *,
        rpc: SolanaRpcClient | None,
        path: Path,
        overrides: Mapping[str, int] | None = None,
    ) -> None:
        self.rpc = rpc
        self.path = path
        self.overrides = {str(key): int(value) for key, value in (overrides or {}).items() if 0 <= int(value) <= 18}
        self._cache: dict[str, int] = {}
        self._lock = Lock()
        self._load()

    @classmethod
    def from_env(cls, *, rpc: SolanaRpcClient | None, overrides: Mapping[str, int] | None = None) -> "TokenDecimalsCache":
        path = Path(os.environ.get("TOKEN_DECIMALS_CACHE_PATH", "data/solana/token_decimals_cache.json"))
        return cls(rpc=rpc, path=path, overrides=overrides)

    def resolve(self, mint: str) -> int:
        if mint in self.overrides:
            return self.overrides[mint]
        with self._lock:
            cached = self._cache.get(mint)
        if cached is not None:
            return cached
        if self.rpc is None:
            raise JupiterQuoteError("Solana RPC is unavailable for token decimals", error_class="solana_rpc_missing")
        try:
            decimals = self.rpc.get_token_supply_decimals(mint)
        except SolanaReadOnlyError as exc:
            raise JupiterQuoteError("Solana token decimals lookup failed", error_class=exc.error_class) from exc
        with self._lock:
            self._cache[mint] = decimals
            self._persist_locked()
        return decimals

    def safe_status(self) -> dict[str, object]:
        with self._lock:
            cached_count = len(self._cache)
        return {"override_count": len(self.overrides), "cached_count": cached_count, "rpc_lookup_enabled": self.rpc is not None}

    def _load(self) -> None:
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            return
        if not isinstance(payload, Mapping):
            return
        for mint, decimals in payload.items():
            if isinstance(mint, str) and isinstance(decimals, int) and not isinstance(decimals, bool) and 0 <= decimals <= 18:
                self._cache[mint] = decimals

    def _persist_locked(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary.write_text(json.dumps(self._cache, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
        temporary.replace(self.path)


class JupiterReadOnlyQuoteProvider:
    """Create quote records from Jupiter Swap V2's read-only order endpoint."""

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
        max_retries: int = 1,
        error_cache_ttl_ms: int = 1_000,
        decimals_resolver: Callable[[str], int] | None = None,
        swap_v2_price_impact: bool = False,
        transport: Callable[[str, Mapping[str, str], float], tuple[int, bytes, Mapping[str, str]]] | None = None,
    ) -> None:
        self.api_key = api_key if api_key is not None else os.environ.get("JUPITER_API_KEY", "").strip()
        self.token_decimals = dict(token_decimals or {})
        self.default_token_decimals = default_token_decimals
        configured_url = (url if url is not None else os.environ.get("JUPITER_QUOTE_URL", "")).strip()
        # Do not silently keep using the retired Metis v1 path from an older
        # local env file. Custom non-v1 read-only gateways remain supported.
        if configured_url.rstrip("/").endswith("/swap/v1/quote"):
            configured_url = ""
        self.url = configured_url or JUPITER_QUOTE_URL
        self.slippage_bps = max(0, min(10_000, int(slippage_bps)))
        self.quote_ttl_ms = max(100, min(30_000, int(quote_ttl_ms)))
        self.timeout_sec = max(0.1, min(60.0, timeout_sec))
        if price_impact_unit not in {None, "ratio", "percent"}:
            raise ValueError("price_impact_unit must be ratio, percent, or None")
        self.price_impact_unit = price_impact_unit
        self.max_retries = max(0, min(2, int(max_retries)))
        self.error_cache_ttl_ms = max(100, min(5_000, int(error_cache_ttl_ms)))
        self.decimals_resolver = decimals_resolver
        self.swap_v2_price_impact = bool(swap_v2_price_impact)
        self.transport = transport or self._transport
        self.requests = 0
        self.last_error_class: str | None = None
        self._request_slots = Semaphore(2)
        # Candidate requests may use only one of the two global slots. The
        # second slot is kept available for an active-position sell quote.
        self._candidate_slots = Semaphore(1)
        self._cache_lock = Lock()
        self._quote_cache: dict[tuple[str, str, Decimal], tuple[ExecutableQuote, float]] = {}
        self._inflight: dict[tuple[str, str, Decimal], Event] = {}

    @classmethod
    def from_env(
        cls,
        *,
        token_decimals: Mapping[str, int] | None = None,
        decimals_resolver: Callable[[str], int] | None = None,
        swap_v2_price_impact: bool = False,
    ) -> "JupiterReadOnlyQuoteProvider":
        return cls(
            token_decimals=token_decimals,
            slippage_bps=_env_int("JUPITER_SLIPPAGE_BPS", 50, 0, 10_000),
            quote_ttl_ms=_env_int("JUPITER_QUOTE_TTL_MS", 1_500, 100, 30_000),
            timeout_sec=_env_float("JUPITER_TIMEOUT_SEC", 10.0, 0.1, 60.0),
            price_impact_unit=os.environ.get("JUPITER_PRICE_IMPACT_UNIT") or None,
            max_retries=_env_int("JUPITER_MAX_RETRIES", 1, 0, 2),
            error_cache_ttl_ms=_env_int("JUPITER_ERROR_CACHE_TTL_MS", 1_000, 100, 5_000),
            decimals_resolver=decimals_resolver,
            swap_v2_price_impact=swap_v2_price_impact,
        )

    def safe_status(self) -> dict[str, object]:
        return {
            "provider": "jupiter",
            "endpoint_configured": bool(self.url),
            "credentials_configured": bool(self.api_key),
            "read_only": True,
            "requests": self.requests,
            "cache_entries": len(self._quote_cache),
            "max_concurrency": 2,
            "max_retries": self.max_retries,
            "last_error_class": self.last_error_class,
        }

    def quote(self, mint: str, side: str, input_quantity: Decimal) -> ExecutableQuote | None:
        if side == "buy":
            return self.quote_buy(mint, input_quantity)
        if side == "sell":
            return self.quote_sell(mint, input_quantity)
        raise ValueError("side must be buy or sell")

    def quote_buy(self, mint: str, input_sol: Decimal) -> ExecutableQuote:
        return self._cached_quote(
            mint,
            "buy",
            input_sol,
            lambda: self._quote_buy_uncached(mint, input_sol),
        )

    def _quote_buy_uncached(self, mint: str, input_sol: Decimal) -> ExecutableQuote:
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
        return self._cached_quote(
            mint,
            "sell",
            input_tokens,
            lambda: self._quote_sell_uncached(mint, input_tokens),
        )

    def quote_position(self, mint: str, input_tokens: Decimal) -> ExecutableQuote:
        """Fetch a sell quote for an active position with priority over candidates."""

        return self._cached_quote(
            mint,
            "sell",
            input_tokens,
            lambda: self._quote_sell_uncached(mint, input_tokens),
            priority="position",
        )

    def _quote_sell_uncached(self, mint: str, input_tokens: Decimal) -> ExecutableQuote:
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
        }
        request_id = str(uuid.uuid4())
        requested_at = datetime.now(timezone.utc)
        started = time.monotonic()
        request_times: list[datetime] = []
        request_statuses: list[int] = []
        try:
            status = 0
            body = b""
            for retry_count in range(self.max_retries + 1):
                self.requests += 1
                request_times.append(datetime.now(timezone.utc))
                status, body, _headers = self.transport(
                    self.url + "?" + urlencode(params),
                    {
                        "x-api-key": self.api_key,
                        "Accept": "application/json",
                        # Jupiter's gateway rejects Python's implicit transport
                        # signature. This identifies the bounded read-only client;
                        # it does not alter the V2 endpoint or request semantics.
                        "User-Agent": "meme0801-readonly/1.0",
                    },
                    self.timeout_sec,
                )
                request_statuses.append(status)
                if status != 429 or retry_count >= self.max_retries:
                    break
                time.sleep(min(0.5, 0.25 * (2**retry_count)))
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
            router = parsed.get("router")
            # Swap V2 returns a router for quote-only requests (without taker)
            # and may omit the legacy routePlan field. Keep routePlan parsing
            # for deterministic legacy fixtures, but never require it for V2.
            route_available = output_quantity > 0 and (
                isinstance(router, str) and bool(router.strip())
                or isinstance(route_plan, list) and len(route_plan) > 0
            )
            received_at = datetime.now(timezone.utc)
            return ExecutableQuote(
                quote_id=f"jupiter:{request_id}",
                mint=mint,
                side=side,
                input_quantity=actual_input,
                output_quantity=output_quantity,
                route_fee=None,
                # Swap V2 documents ``priceImpact`` in percentage points
                # (for example -0.1 means -0.1%). ``priceImpactPct`` is the
                # deprecated decimal ratio and remains a fixture fallback.
                price_impact_pct=(
                    Decimal(str(parsed["priceImpact"]))
                    if self.swap_v2_price_impact and parsed.get("priceImpact") is not None
                    else _price_impact_pct(parsed.get("priceImpactPct"), self.price_impact_unit)
                ),
                quoted_at=received_at,
                age_ms=0,
                expires_at=received_at + timedelta(milliseconds=self.quote_ttl_ms),
                route_available=route_available,
                liquidity_available=route_available,
                provider="jupiter",
                route=((router.strip(),) if isinstance(router, str) and router.strip() else _route_labels(route_plan)),
                quote_context_slot=_optional_int(parsed.get("contextSlot")),
                requested_at=requested_at,
                received_at=received_at,
                latency_ms=int((time.monotonic() - started) * 1000),
                executable_style=route_available,
                confidence="verified" if route_available else "unavailable",
                quote_source="jupiter_quote" if route_available else None,
                error_class=None if route_available else "jupiter_no_route",
                raw_response_hash=_payload_hash(parsed),
                request_times=tuple(request_times),
                request_statuses=tuple(request_statuses),
            )
        except JupiterQuoteError as exc:
            self.last_error_class = exc.error_class
            return self._unavailable(
                mint,
                side,
                input_quantity,
                exc.error_class,
                requested_at=requested_at,
                latency_ms=int((time.monotonic() - started) * 1000),
                request_times=tuple(request_times),
                request_statuses=tuple(request_statuses),
            )
        except (TimeoutError, URLError, OSError) as exc:
            self.last_error_class = "jupiter_connection_error"
            return self._unavailable(
                mint,
                side,
                input_quantity,
                "jupiter_connection_error",
                requested_at=requested_at,
                latency_ms=int((time.monotonic() - started) * 1000),
                request_times=tuple(request_times),
                request_statuses=tuple(request_statuses),
            )
        except (UnicodeDecodeError, json.JSONDecodeError, InvalidOperation, TypeError, ValueError) as exc:
            self.last_error_class = "jupiter_schema_changed"
            return self._unavailable(
                mint,
                side,
                input_quantity,
                "jupiter_schema_changed",
                requested_at=requested_at,
                latency_ms=int((time.monotonic() - started) * 1000),
                request_times=tuple(request_times),
                request_statuses=tuple(request_statuses),
            )

    def _cached_quote(
        self,
        mint: str,
        side: str,
        input_quantity: Decimal,
        producer: Callable[[], ExecutableQuote],
        *,
        priority: str = "candidate",
    ) -> ExecutableQuote:
        if input_quantity <= 0:
            raise ValueError("input_quantity must be positive")
        key = (mint, side, input_quantity)
        now = time.monotonic()
        with self._cache_lock:
            cached = self._quote_cache.get(key)
            if cached is not None and cached[1] > now:
                return cached[0]
            waiter = self._inflight.get(key)
            if waiter is None:
                waiter = Event()
                self._inflight[key] = waiter
                owner = True
            else:
                owner = False
        if not owner:
            waiter.wait(timeout=self.timeout_sec + 2.0)
            with self._cache_lock:
                cached = self._quote_cache.get(key)
                if cached is not None and cached[1] > time.monotonic():
                    return cached[0]
            return self._unavailable(mint, side, input_quantity, "jupiter_request_inflight_timeout")

        try:
            if priority == "position":
                with self._request_slots:
                    quote = producer()
            else:
                with self._candidate_slots:
                    with self._request_slots:
                        quote = producer()
            deadline = time.monotonic() + self._cache_ttl_sec(quote)
            with self._cache_lock:
                self._quote_cache[key] = (quote, deadline)
            return quote
        finally:
            with self._cache_lock:
                event = self._inflight.pop(key, None)
                if event is not None:
                    event.set()

    def _cache_ttl_sec(self, quote: ExecutableQuote) -> float:
        if quote.error_class or not quote.route_available:
            return self.error_cache_ttl_ms / 1000
        if quote.expires_at is not None:
            return max(0.0, (quote.expires_at - datetime.now(timezone.utc)).total_seconds())
        return self.quote_ttl_ms / 1000

    def _unavailable(
        self,
        mint: str,
        side: str,
        input_quantity: Decimal,
        error_class: str,
        *,
        requested_at: datetime | None = None,
        latency_ms: int | None = None,
        request_times: tuple[datetime, ...] = (),
        request_statuses: tuple[int, ...] = (),
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
            request_times=request_times,
            request_statuses=request_statuses,
        )

    def _token_decimals(self, mint: str) -> int:
        value = self.token_decimals.get(mint, self.default_token_decimals)
        if value is None and self.decimals_resolver is not None:
            try:
                value = self.decimals_resolver(mint)
            except JupiterQuoteError:
                raise
            except Exception as exc:
                raise JupiterQuoteError("token decimals resolver failed", error_class="jupiter_token_decimals_lookup_failed") from exc
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
