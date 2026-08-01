"""Small stdlib-only HTTP client for documented public Binance Web3 endpoints."""

from __future__ import annotations

import json
import os
import time
import uuid
from dataclasses import dataclass
from email.message import Message
from typing import Any, Callable, Mapping, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from meme_system.adapters.binance_web3.auth import BinanceWeb3Auth
from meme_system.adapters.binance_web3.errors import BinanceWeb3Error, ErrorContext
from meme_system.adapters.binance_web3.models import EndpointSpec
from meme_system.adapters.binance_web3.rate_limit import RateLimitState, RetryPolicy


WEB3_HOST = "https://web3.binance.com"
KLINE_HOST = "https://dquery.sintral.io"
ENDPOINTS = {
    "meme_rush": EndpointSpec(
        "meme_rush", WEB3_HOST, "/bapi/defi/v1/public/wallet-direct/buw/wallet/market/token/pulse/rank/list/ai", "POST"
    ),
    "smart_money": EndpointSpec(
        "smart_money", WEB3_HOST, "/bapi/defi/v1/public/wallet-direct/buw/wallet/web/signal/smart-money/ai", "POST"
    ),
    "token_dynamic": EndpointSpec(
        "token_dynamic", WEB3_HOST, "/bapi/defi/v4/public/wallet-direct/buw/wallet/market/token/dynamic/info/ai", "GET"
    ),
    "kline": EndpointSpec(
        "kline", KLINE_HOST, "/u-kline/v1/k-line/candles", "GET"
    ),
}


@dataclass(frozen=True)
class HttpResponse:
    status: int
    body: bytes
    headers: Mapping[str, str]


@dataclass(frozen=True)
class ApiResponse:
    endpoint_type: str
    status: int
    payload: Mapping[str, Any]
    request_id: str
    api_latency_ms: int
    retry_count: int


Transport = Callable[[str, str, Mapping[str, str], Optional[bytes], float, int], HttpResponse]


def _header_map(headers: Message | Mapping[str, str]) -> dict[str, str]:
    if isinstance(headers, Message):
        return {key.lower(): value for key, value in headers.items()}
    return {str(key).lower(): str(value) for key, value in headers.items()}


def _stdlib_transport(
    method: str,
    url: str,
    headers: Mapping[str, str],
    body: bytes | None,
    timeout_sec: float,
    max_response_bytes: int,
) -> HttpResponse:
    request = Request(url=url, data=body, headers=dict(headers), method=method)
    try:
        with urlopen(request, timeout=timeout_sec) as response:
            data = response.read(max_response_bytes + 1)
            if len(data) > max_response_bytes:
                raise BinanceWeb3Error(
                    "response exceeds configured size limit",
                    context=ErrorContext("binance_response_too_large", "http"),
                )
            return HttpResponse(response.status, data, _header_map(response.headers))
    except HTTPError as exc:
        data = exc.read(max_response_bytes + 1)
        if len(data) > max_response_bytes:
            raise BinanceWeb3Error(
                "error response exceeds configured size limit",
                context=ErrorContext("binance_response_too_large", "http", http_status=exc.code),
            ) from exc
        return HttpResponse(exc.code, data, _header_map(exc.headers))
    except BinanceWeb3Error:
        raise
    except TimeoutError as exc:
        raise BinanceWeb3Error(
            "request timed out",
            context=ErrorContext("binance_timeout", "http", retryable=True),
        ) from exc
    except (URLError, OSError) as exc:
        raise BinanceWeb3Error(
            "connection failed",
            context=ErrorContext("binance_connection_error", "http", retryable=True),
        ) from exc


class BinanceWeb3Client:
    def __init__(
        self,
        *,
        auth: BinanceWeb3Auth | None = None,
        retry_policy: RetryPolicy | None = None,
        timeout_sec: float = 10.0,
        max_response_bytes: int = 1_000_000,
        transport: Transport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.auth = auth or BinanceWeb3Auth.from_env()
        self.retry_policy = retry_policy or RetryPolicy(request_budget_sec=timeout_sec)
        self.timeout_sec = timeout_sec
        self.max_response_bytes = max_response_bytes
        self.transport = transport or _stdlib_transport
        self.sleep = sleep
        self.rate_limit_state = RateLimitState()

    @classmethod
    def from_env(cls, *, transport: Transport | None = None, sleep: Callable[[float], None] = time.sleep) -> "BinanceWeb3Client":
        """Build a bounded client from non-secret runtime configuration."""

        timeout_sec = _env_float("BINANCE_WEB3_TIMEOUT_SEC", 10.0, minimum=0.1, maximum=60.0)
        max_retries = _env_int("BINANCE_WEB3_MAX_RETRIES", 1, minimum=0, maximum=3)
        max_response_bytes = _env_int("BINANCE_WEB3_MAX_RESPONSE_BYTES", 1_000_000, minimum=1_024, maximum=10_000_000)
        return cls(
            auth=BinanceWeb3Auth.from_env(),
            retry_policy=RetryPolicy(max_retries=max_retries, request_budget_sec=timeout_sec),
            timeout_sec=timeout_sec,
            max_response_bytes=max_response_bytes,
            transport=transport,
            sleep=sleep,
        )

    def request_json(
        self,
        endpoint_type: str,
        *,
        params: Mapping[str, object] | None = None,
        body: Mapping[str, object] | None = None,
    ) -> ApiResponse:
        if endpoint_type not in ENDPOINTS:
            raise ValueError(f"unsupported endpoint type: {endpoint_type}")
        spec = self._endpoint_spec(endpoint_type)
        query = urlencode({key: value for key, value in (params or {}).items() if value is not None}, doseq=True)
        url = spec.url + (f"?{query}" if query else "")
        payload = json.dumps(body or {}, ensure_ascii=False, separators=(",", ":")).encode("utf-8") if spec.method == "POST" else None
        headers = self.auth.headers()
        if payload is not None:
            headers = {**headers, "Content-Type": "application/json"}

        request_id = str(uuid.uuid4())
        started = time.monotonic()
        retry_count = 0
        while True:
            self.rate_limit_state.record_request()
            try:
                response = self.transport(
                    spec.method,
                    url,
                    headers,
                    payload,
                    self.timeout_sec,
                    self.max_response_bytes,
                )
            except BinanceWeb3Error as exc:
                error = BinanceWeb3Error(
                    exc.redacted_message,
                    context=ErrorContext(
                        exc.context.error_class,
                        endpoint_type,
                        request_id,
                        retryable=exc.context.retryable,
                        retry_count=retry_count,
                        http_status=exc.context.http_status,
                    ),
                )
                if not error.context.retryable or retry_count >= self.retry_policy.max_retries:
                    raise error
                retry_count += 1
                self.rate_limit_state.record_retry()
                self._bounded_sleep(started, retry_count)
                continue

            if response.status == 429:
                if retry_count >= self.retry_policy.max_retries:
                    raise BinanceWeb3Error(
                        "Binance Web3 rate limit response",
                        context=ErrorContext("binance_rate_limited", endpoint_type, request_id, retry_count=retry_count, http_status=429),
                    )
                retry_count += 1
                self.rate_limit_state.record_rate_limit()
                self.rate_limit_state.record_retry()
                self._bounded_sleep(started, retry_count)
                continue
            if response.status >= 500:
                if retry_count >= self.retry_policy.max_retries:
                    raise BinanceWeb3Error(
                        f"Binance Web3 HTTP {response.status}",
                        context=ErrorContext("binance_http_5xx", endpoint_type, request_id, retryable=False, retry_count=retry_count, http_status=response.status),
                    )
                retry_count += 1
                self.rate_limit_state.record_retry()
                self._bounded_sleep(started, retry_count)
                continue
            if response.status >= 400:
                error_class = "binance_auth_rejected" if response.status in {401, 403} else "binance_http_4xx"
                raise BinanceWeb3Error(
                    f"Binance Web3 HTTP {response.status}",
                    context=ErrorContext(error_class, endpoint_type, request_id, http_status=response.status),
                )
            try:
                parsed = json.loads(response.body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise BinanceWeb3Error(
                    "Binance Web3 returned non-JSON data",
                    context=ErrorContext("binance_invalid_json", endpoint_type, request_id, http_status=response.status),
                ) from exc
            if not isinstance(parsed, Mapping):
                raise BinanceWeb3Error(
                    "Binance Web3 JSON envelope is not an object",
                    context=ErrorContext("binance_schema_changed", endpoint_type, request_id, http_status=response.status),
                )
            business_code = parsed.get("code")
            if business_code is not None and str(business_code) != "000000":
                if str(business_code) == "100004":
                    if retry_count >= self.retry_policy.max_retries:
                        raise BinanceWeb3Error(
                            "Binance Web3 business rate limit response",
                            context=ErrorContext(
                                "binance_rate_limited",
                                endpoint_type,
                                request_id,
                                retry_count=retry_count,
                            ),
                        )
                    retry_count += 1
                    self.rate_limit_state.record_rate_limit()
                    self.rate_limit_state.record_retry()
                    self._bounded_sleep(started, retry_count)
                    continue
                raise BinanceWeb3Error(
                    "Binance Web3 business response was not successful",
                    context=ErrorContext(
                        "binance_business_error",
                        endpoint_type,
                        request_id,
                    ),
                )
            return ApiResponse(
                endpoint_type=endpoint_type,
                status=response.status,
                payload=parsed,
                request_id=request_id,
                api_latency_ms=max(0, int((time.monotonic() - started) * 1000)),
                retry_count=retry_count,
            )

    def _bounded_sleep(self, started: float, retry_count: int) -> None:
        delay = self.retry_policy.backoff(retry_count)
        remaining = self.retry_policy.request_budget_sec - (time.monotonic() - started)
        if remaining <= 0:
            raise BinanceWeb3Error(
                "request budget exhausted",
                context=ErrorContext("binance_timeout", "http", retryable=False, retry_count=retry_count),
            )
        self.sleep(min(delay, remaining))

    @staticmethod
    def _endpoint_spec(endpoint_type: str) -> EndpointSpec:
        spec = ENDPOINTS[endpoint_type]
        if endpoint_type == "kline":
            base_url = os.environ.get("BINANCE_WEB3_KLINE_BASE_URL", KLINE_HOST).rstrip("/")
        else:
            base_url = os.environ.get("BINANCE_WEB3_BASE_URL", WEB3_HOST).rstrip("/")
        return EndpointSpec(spec.endpoint_type, base_url, spec.path, spec.method, spec.auth_mode)


def _env_float(name: str, default: float, *, minimum: float, maximum: float) -> float:
    try:
        value = float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default
    return min(maximum, max(minimum, value))


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    try:
        value = int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default
    return min(maximum, max(minimum, value))
