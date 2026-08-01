"""Solana mainnet read-only RPC/WSS adapters for Gate A.

This module deliberately exposes only read methods and subscription methods.
There is no transaction builder, signer, send method, wallet object, or write
RPC method in this module.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import Message
from typing import Any, Awaitable, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


SOLANA_MAINNET_CHAIN = "solana-mainnet"
READ_METHODS = frozenset(
    {
        "getAccountInfo",
        "getBalance",
        "getBlock",
        "getBlockHeight",
        "getEpochInfo",
        "getLatestBlockhash",
        "getProgramAccounts",
        "getSignaturesForAddress",
        "getSlot",
        "getTokenAccountBalance",
        "getTokenAccountsByOwner",
        "getTokenSupply",
        "getTransaction",
        "getVersion",
    }
)
WSS_METHODS = frozenset(
    {
        "accountSubscribe",
        "logsSubscribe",
        "programSubscribe",
        "slotSubscribe",
    }
)


class SolanaReadOnlyError(RuntimeError):
    def __init__(self, message: str, *, error_class: str, method: str | None = None) -> None:
        self.error_class = error_class
        self.method = method
        super().__init__(message[:500])


@dataclass(frozen=True)
class SolanaRpcHealth:
    state: str
    last_success_at: datetime | None
    last_error_class: str | None
    latency_ms: int | None
    endpoint_configured: bool


@dataclass(frozen=True)
class SolanaWssHealth:
    state: str
    subscriptions: int
    last_message_at: datetime | None
    disconnect_count: int
    reconnect_count: int
    last_error_class: str | None


def _header_map(headers: Message | Mapping[str, str]) -> dict[str, str]:
    if isinstance(headers, Message):
        return {key.lower(): value for key, value in headers.items()}
    return {str(key).lower(): str(value) for key, value in headers.items()}


class SolanaRpcClient:
    """Bounded JSON-RPC client with an allow-list of read-only methods."""

    def __init__(
        self,
        url: str | None = None,
        *,
        timeout_sec: float = 10.0,
        max_retries: int = 1,
        transport: Callable[[str, bytes, Mapping[str, str], float], tuple[int, bytes, Mapping[str, str]]] | None = None,
    ) -> None:
        self.url = url if url is not None else os.environ.get("SOLANA_RPC_URL", "").strip()
        self.timeout_sec = max(0.1, min(60.0, timeout_sec))
        self.max_retries = max(0, min(3, max_retries))
        self.transport = transport or self._transport
        self.requests = 0
        self.failures = 0
        self.last_success_at: datetime | None = None
        self.last_error_class: str | None = None
        self.last_latency_ms: int | None = None

    @classmethod
    def from_env(cls) -> "SolanaRpcClient":
        return cls(
            timeout_sec=_env_float("SOLANA_RPC_TIMEOUT_SEC", 10.0, 0.1, 60.0),
            max_retries=_env_int("SOLANA_RPC_MAX_RETRIES", 1, 0, 3),
        )

    def safe_status(self) -> dict[str, object]:
        return {
            "chain": SOLANA_MAINNET_CHAIN,
            "endpoint_configured": bool(self.url),
            "requests": self.requests,
            "failures": self.failures,
            "last_error_class": self.last_error_class,
            "last_latency_ms": self.last_latency_ms,
        }

    def health(self) -> SolanaRpcHealth:
        state = "HEALTHY" if self.last_success_at and not self.last_error_class else "UNAVAILABLE"
        return SolanaRpcHealth(state, self.last_success_at, self.last_error_class, self.last_latency_ms, bool(self.url))

    def call(self, method: str, params: list[object] | None = None) -> Any:
        if method not in READ_METHODS:
            raise SolanaReadOnlyError(
                "Solana RPC method is not in the read-only allow-list",
                error_class="solana_write_method_blocked",
                method=method,
            )
        if not self.url:
            raise SolanaReadOnlyError("SOLANA_RPC_URL is not configured", error_class="solana_rpc_missing", method=method)
        payload = json.dumps(
            {"jsonrpc": "2.0", "id": str(uuid.uuid4()), "method": method, "params": params or []},
            separators=(",", ":"),
        ).encode("utf-8")
        started = time.monotonic()
        self.requests += 1
        last_error: SolanaReadOnlyError | None = None
        for attempt in range(self.max_retries + 1):
            try:
                status, body, _headers = self.transport(self.url, payload, {"Content-Type": "application/json"}, self.timeout_sec)
                if status >= 400:
                    raise SolanaReadOnlyError(f"Solana RPC HTTP {status}", error_class="solana_http_error", method=method)
                parsed = json.loads(body.decode("utf-8"))
                if not isinstance(parsed, Mapping):
                    raise SolanaReadOnlyError("Solana RPC response is not an object", error_class="solana_invalid_json", method=method)
                if parsed.get("error") is not None:
                    raise SolanaReadOnlyError("Solana RPC returned a JSON-RPC error", error_class="solana_rpc_error", method=method)
                if "result" not in parsed:
                    raise SolanaReadOnlyError("Solana RPC response has no result", error_class="solana_schema_changed", method=method)
                self.last_success_at = datetime.now(timezone.utc)
                self.last_error_class = None
                self.last_latency_ms = int((time.monotonic() - started) * 1000)
                return parsed["result"]
            except SolanaReadOnlyError as exc:
                last_error = exc
                self.last_error_class = exc.error_class
                if attempt >= self.max_retries or exc.error_class in {"solana_rpc_error", "solana_http_error", "solana_schema_changed"}:
                    break
                time.sleep(min(0.25 * (2**attempt), 1.0))
            except (TimeoutError, URLError, OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
                last_error = SolanaReadOnlyError("Solana RPC request failed", error_class="solana_connection_error", method=method)
                self.last_error_class = last_error.error_class
                if attempt >= self.max_retries:
                    break
        self.failures += 1
        self.last_latency_ms = int((time.monotonic() - started) * 1000)
        assert last_error is not None
        raise last_error

    def get_slot(self, commitment: str = "processed") -> int:
        value = self.call("getSlot", [{"commitment": commitment}])
        if not isinstance(value, int):
            raise SolanaReadOnlyError("getSlot result is not an integer", error_class="solana_schema_changed", method="getSlot")
        return value

    def get_account_info(self, pubkey: str, *, encoding: str = "base64", commitment: str = "confirmed") -> Mapping[str, object] | None:
        value = self.call("getAccountInfo", [pubkey, {"encoding": encoding, "commitment": commitment}])
        if value is None:
            return None
        if not isinstance(value, Mapping):
            raise SolanaReadOnlyError("getAccountInfo result is not an object", error_class="solana_schema_changed", method="getAccountInfo")
        return value

    def get_signatures_for_address(self, address: str, *, limit: int = 100, commitment: str = "confirmed") -> list[Mapping[str, object]]:
        value = self.call("getSignaturesForAddress", [address, {"limit": max(1, min(1000, limit)), "commitment": commitment}])
        if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
            raise SolanaReadOnlyError("getSignaturesForAddress result is not a list", error_class="solana_schema_changed", method="getSignaturesForAddress")
        return list(value)

    def get_transaction(self, signature: str, *, commitment: str = "confirmed") -> Mapping[str, object] | None:
        value = self.call("getTransaction", [signature, {"encoding": "jsonParsed", "commitment": commitment, "maxSupportedTransactionVersion": 0}])
        if value is None:
            return None
        if not isinstance(value, Mapping):
            raise SolanaReadOnlyError("getTransaction result is not an object", error_class="solana_schema_changed", method="getTransaction")
        return value

    def get_program_accounts(self, program_id: str, *, filters: list[Mapping[str, object]] | None = None) -> list[Mapping[str, object]]:
        value = self.call("getProgramAccounts", [program_id, {"encoding": "base64", "filters": filters or [], "commitment": "confirmed"}])
        if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
            raise SolanaReadOnlyError("getProgramAccounts result is not a list", error_class="solana_schema_changed", method="getProgramAccounts")
        return list(value)

    def _transport(self, url: str, body: bytes, headers: Mapping[str, str], timeout_sec: float) -> tuple[int, bytes, Mapping[str, str]]:
        request = Request(url=url, data=body, headers=dict(headers), method="POST")
        try:
            with urlopen(request, timeout=timeout_sec) as response:
                return response.status, response.read(2_000_001), _header_map(response.headers)
        except HTTPError as exc:
            return exc.code, exc.read(2_000_001), _header_map(exc.headers)


class SolanaWssMonitor:
    """Optional websocket subscription monitor with reconnect and stale state."""

    def __init__(self, url: str | None = None, *, stale_after_sec: float = 30.0) -> None:
        self.url = url if url is not None else os.environ.get("SOLANA_WS_URL", "").strip()
        self.stale_after_sec = max(1.0, stale_after_sec)
        self.subscription_methods: list[tuple[str, list[object]]] = []
        self.state = "DISCONNECTED"
        self.last_message_at: datetime | None = None
        self.disconnect_count = 0
        self.reconnect_count = 0
        self.last_error_class: str | None = None

    def add_subscription(self, method: str, params: list[object]) -> None:
        if method not in WSS_METHODS:
            raise SolanaReadOnlyError("Solana WSS method is not read-only subscription", error_class="solana_wss_method_blocked", method=method)
        self.subscription_methods.append((method, params))

    def health(self) -> SolanaWssHealth:
        state = self.state
        if self.last_message_at and (datetime.now(timezone.utc) - self.last_message_at).total_seconds() > self.stale_after_sec:
            state = "STALE"
        return SolanaWssHealth(state, len(self.subscription_methods), self.last_message_at, self.disconnect_count, self.reconnect_count, self.last_error_class)

    def safe_status(self) -> dict[str, object]:
        health = self.health()
        return {"endpoint_configured": bool(self.url), "state": health.state, "subscriptions": health.subscriptions, "disconnect_count": health.disconnect_count, "reconnect_count": health.reconnect_count, "last_error_class": health.last_error_class}

    async def run(self, on_event: Callable[[Mapping[str, object]], Awaitable[None] | None], stop_event: asyncio.Event) -> None:
        if not self.url:
            self.state = "UNAVAILABLE"
            self.last_error_class = "solana_wss_missing"
            return
        try:
            import websockets  # type: ignore
        except ImportError as exc:
            self.state = "UNAVAILABLE"
            self.last_error_class = "solana_wss_dependency_missing"
            raise SolanaReadOnlyError("websockets package is required for WSS monitoring", error_class=self.last_error_class) from exc
        delay = 0.5
        while not stop_event.is_set():
            try:
                self.state = "CONNECTING"
                async with websockets.connect(self.url, ping_interval=20, ping_timeout=20, close_timeout=5) as socket:
                    self.state = "HEALTHY"
                    self.reconnect_count += 1
                    for index, (method, params) in enumerate(self.subscription_methods, start=1):
                        await socket.send(json.dumps({"jsonrpc": "2.0", "id": index, "method": method, "params": params}, separators=(",", ":")))
                    delay = 0.5
                    while not stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(socket.recv(), timeout=self.stale_after_sec)
                        except asyncio.TimeoutError:
                            self.state = "STALE"
                            continue
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8")
                        event = json.loads(raw)
                        if not isinstance(event, Mapping):
                            continue
                        self.last_message_at = datetime.now(timezone.utc)
                        self.state = "HEALTHY"
                        callback_result = on_event(event)
                        if asyncio.iscoroutine(callback_result):
                            await callback_result
            except asyncio.CancelledError:
                raise
            except Exception:
                self.disconnect_count += 1
                self.state = "ERROR"
                self.last_error_class = "solana_wss_connection_error"
                if stop_event.is_set():
                    break
                await asyncio.sleep(delay)
                delay = min(30.0, delay * 2)
        self.state = "STOPPED"


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
