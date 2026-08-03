"""Bounded, read-only BSC pool log monitoring.

This module deliberately has no wallet, signer, transaction, or broadcast
dependency.  It resolves a confirmed pool through read-only JSON-RPC calls,
subscribes to that pool's logs over ``eth_subscribe``.  Events only wake a
fresh executable quote; they are never simulated fill prices.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import threading
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Callable, Mapping, Sequence


SWAP_EVENT_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
SYNC_EVENT_TOPIC = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
PAIR_EVENT_TOPICS = (SWAP_EVENT_TOPIC, SYNC_EVENT_TOPIC)
V2_POOL_TYPE = "v2"
V3_POOL_TYPE = "v3"
BONDING_CURVE_POOL_TYPE = "bonding_curve"
FOUR_MEME_TOKEN_MANAGER = "0x5c952063c7fc8610ffdb798152d69f0b9550762b"
FOUR_MEME_PROTOCOL = 2002
FOUR_TOKEN_PURCHASE_TOPIC = "0x7db52723a3b2cdd6164364b3b766e65e540d7be48ffa89582956d8eaebe62942"
FOUR_TOKEN_SALE_TOPIC = "0x0a5575b3648bae2210cee56bf33254cc1ddfbc7bf637c0af2ac18b14fb1bae19"
FOUR_LIQUIDITY_ADDED_TOPIC = "0xc18aa71171b358b706fe3dd345299685ba21a5316c66ffa9e319268b033c44b0"
_ZERO_ADDRESS = "0x" + "0" * 40
_UNSUPPORTED_ANCHOR = "0x" + "e" * 40
BSC_WBNB_ADDRESS = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
_TOKEN0_SELECTOR = "0x0dfe1681"
_TOKEN1_SELECTOR = "0xd21220a7"
_DECIMALS_SELECTOR = "0x313ce567"
_GET_RESERVES_SELECTOR = "0x0902f1ac"
_SLOT0_SELECTOR = "0x3850c7bd"


def normalize_bsc_address(value: object | None) -> str | None:
    """Return a safe EVM address for read-only RPC/log filtering, if valid."""

    if not isinstance(value, str):
        return None
    address = value.strip().lower()
    if (
        len(address) != 42
        or not address.startswith("0x")
        or address in {_ZERO_ADDRESS, _UNSUPPORTED_ANCHOR}
        or any(char not in "0123456789abcdef" for char in address[2:])
    ):
        return None
    return address


def _env_urls(*names: str) -> tuple[str, ...]:
    values: list[str] = []
    for name in names:
        raw = os.environ.get(name, "")
        values.extend(part.strip() for part in raw.split(","))
    return tuple(dict.fromkeys(
        value for value in values if value.startswith(("ws://", "wss://"))
    ))


def _env_http_urls(*names: str) -> tuple[str, ...]:
    values: list[str] = []
    for name in names:
        raw = os.environ.get(name, "")
        values.extend(part.strip() for part in raw.split(","))
    return tuple(dict.fromkeys(
        value for value in values if value.startswith(("http://", "https://"))
    ))


@dataclass(frozen=True)
class BscPoolDescriptor:
    """A pool/curve descriptor confirmed by the read-only resolver."""

    address: str
    pool_type: str
    mint: str | None = None
    token0: str | None = None
    token1: str | None = None
    token0_decimals: int | None = None
    token1_decimals: int | None = None
    event_topics: tuple[str, ...] = ()

    @property
    def pair_address(self) -> str:
        # Keep the legacy name used by the coordinator and audit records.
        return self.address

    @property
    def subscription_topics(self) -> tuple[str, ...]:
        if self.pool_type == V2_POOL_TYPE:
            return PAIR_EVENT_TOPICS
        if self.pool_type == V3_POOL_TYPE:
            return (SWAP_EVENT_TOPIC,)
        if self.pool_type == "unknown":
            return PAIR_EVENT_TOPICS
        return self.event_topics


@dataclass(frozen=True)
class BscPoolPrice:
    mint: str
    pool_address: str
    pool_type: str
    native_token_price: Decimal
    observed_at: datetime
    block_number: int | None
    transaction_hash: str | None
    log_index: int | None
    raw_response_hash: str


@dataclass(frozen=True)
class BscPairEvent:
    pair_address: str
    event_type: str
    block_number: int | None
    transaction_hash: str | None
    log_index: int | None
    observed_at: datetime
    pool_type: str = "unknown"
    data: str | None = None
    topics: tuple[str, ...] = ()


class BscRpcClient:
    """Small bounded JSON-RPC reader; it cannot submit transactions."""

    def __init__(self, urls: Sequence[str] = (), *, timeout_sec: float = 3.0) -> None:
        self.urls = tuple(dict.fromkeys(url for url in urls if url.startswith(("http://", "https://"))))
        self.timeout_sec = max(0.5, min(10.0, float(timeout_sec)))
        self._request_id = 0
        self._lock = threading.Lock()

    @classmethod
    def from_env(cls) -> "BscRpcClient":
        return cls(_env_http_urls("BSC_RPC_URL", "BSC_HTTP_RPC_URL", "BSC_RPC_BACKUP_URLS"))

    @property
    def configured(self) -> bool:
        return bool(self.urls)

    def call(self, method: str, params: Sequence[object]) -> object | None:
        if not self.urls:
            return None
        with self._lock:
            self._request_id += 1
            request_id = self._request_id
        payload = json.dumps({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": list(params),
        }, separators=(",", ":")).encode("utf-8")
        for endpoint in self.urls:
            try:
                request = urllib.request.Request(
                    endpoint,
                    data=payload,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(request, timeout=self.timeout_sec) as response:
                    decoded = json.loads(response.read().decode("utf-8"))
                if not isinstance(decoded, Mapping) or decoded.get("error") is not None:
                    continue
                return decoded.get("result")
            except (OSError, urllib.error.URLError, TimeoutError, ValueError, TypeError):
                continue
        return None

    def call_hex(self, to: str, data: str) -> str | None:
        result = self.call("eth_call", [{"to": to, "data": data}, "latest"])
        return result if isinstance(result, str) and result.startswith("0x") else None

    def call_address(self, to: str, selector: str) -> str | None:
        raw = self.call_hex(to, selector)
        if raw is None or len(raw) < 42:
            return None
        return normalize_bsc_address("0x" + raw[-40:])

    def call_uint(self, to: str, selector: str) -> int | None:
        raw = self.call_hex(to, selector)
        if raw is None:
            return None
        try:
            return int(raw, 16)
        except ValueError:
            return None


class BscPoolResolver:
    """Resolve V2/V3 pool type and derive a pool spot price from log state."""

    def __init__(
        self,
        rpc: BscRpcClient | None = None,
        *,
        bonding_curve_event_topics: Sequence[str] = (),
    ) -> None:
        self.rpc = rpc or BscRpcClient()
        self.bonding_curve_event_topics = tuple(
            topic.lower() for topic in bonding_curve_event_topics if _valid_topic(topic)
        )
        self._cache: dict[tuple[str, str], BscPoolDescriptor | None] = {}
        self._lock = threading.Lock()

    @classmethod
    def from_env(cls) -> "BscPoolResolver":
        raw_topics = os.environ.get("BSC_BONDING_CURVE_EVENT_TOPICS", "")
        return cls(
            BscRpcClient.from_env(),
            bonding_curve_event_topics=tuple(part.strip() for part in raw_topics.split(",")),
        )

    @property
    def configured(self) -> bool:
        return self.rpc.configured

    def resolve(
        self,
        mint: str,
        pair_address: object | None,
        bonding_curve_address: object | None = None,
        *,
        protocol: object | None = None,
        migrate_status: object | None = None,
    ) -> BscPoolDescriptor | None:
        pair = normalize_bsc_address(pair_address)
        if pair is not None:
            key = (mint.lower(), pair)
            with self._lock:
                if key in self._cache:
                    return self._cache[key]
            descriptor = self._resolve_pair(mint, pair)
            with self._lock:
                self._cache[key] = descriptor
            return descriptor
        curve = normalize_bsc_address(bonding_curve_address)
        if curve is not None and self.bonding_curve_event_topics:
            return BscPoolDescriptor(
                address=curve,
                pool_type=BONDING_CURVE_POOL_TYPE,
                mint=mint,
                event_topics=self.bonding_curve_event_topics,
            )
        try:
            is_four_bonding = int(protocol) == FOUR_MEME_PROTOCOL and int(migrate_status) == 0
        except (TypeError, ValueError):
            is_four_bonding = False
        if is_four_bonding:
            return BscPoolDescriptor(
                address=FOUR_MEME_TOKEN_MANAGER,
                pool_type=BONDING_CURVE_POOL_TYPE,
                mint=mint,
                event_topics=(
                    FOUR_TOKEN_PURCHASE_TOPIC,
                    FOUR_TOKEN_SALE_TOPIC,
                    FOUR_LIQUIDITY_ADDED_TOPIC,
                ),
            )
        if curve is None or not self.bonding_curve_event_topics:
            # The source did not identify a usable curve contract/ABI.  Do not
            # turn the token contract or a marker address into a fake pool.
            return None

    def _resolve_pair(self, mint: str, address: str) -> BscPoolDescriptor | None:
        if not self.rpc.configured:
            return None
        token0 = self.rpc.call_address(address, _TOKEN0_SELECTOR)
        token1 = self.rpc.call_address(address, _TOKEN1_SELECTOR)
        reserves = self.rpc.call_hex(address, _GET_RESERVES_SELECTOR)
        pool_type: str | None = V2_POOL_TYPE if _has_words(reserves, 3) else None
        if pool_type is None:
            slot0 = self.rpc.call_hex(address, _SLOT0_SELECTOR)
            pool_type = V3_POOL_TYPE if _has_words(slot0, 7) else None
        if pool_type is None:
            return None
        return BscPoolDescriptor(
            address=address,
            pool_type=pool_type,
            mint=mint,
            token0=token0,
            token1=token1,
            token0_decimals=self._decimals(token0),
            token1_decimals=self._decimals(token1),
        )

    def _decimals(self, address: str | None) -> int | None:
        if address is None:
            return None
        value = self.rpc.call_uint(address, _DECIMALS_SELECTOR)
        return value if value is not None and 0 <= value <= 36 else None

    def price_from_event(
        self,
        descriptor: BscPoolDescriptor,
        event: BscPairEvent,
    ) -> BscPoolPrice | None:
        if descriptor.pool_type == BONDING_CURVE_POOL_TYPE:
            # The event ABI and price fields are intentionally not guessed.
            return None
        if descriptor.mint is None:
            return None
        if descriptor.token0 is None or descriptor.token1 is None:
            return None
        if descriptor.token0_decimals is None or descriptor.token1_decimals is None:
            return None
        reserves: tuple[int, int] | None = None
        sqrt_price_x96: int | None = None
        if descriptor.pool_type == V2_POOL_TYPE:
            if event.event_type == "sync":
                words = _data_words(event.data, 2)
                if words is not None:
                    reserves = (words[0], words[1])
            elif event.event_type == "swap":
                raw = self.rpc.call_hex(descriptor.address, _GET_RESERVES_SELECTOR)
                words = _data_words(raw, 3)
                if words is not None:
                    reserves = (words[0], words[1])
        elif descriptor.pool_type == V3_POOL_TYPE and event.event_type == "swap":
            words = _data_words(event.data, 5)
            if words is not None:
                sqrt_price_x96 = words[2]
        native_price: Decimal | None
        if reserves is not None:
            native_price = _v2_native_price(descriptor, reserves[0], reserves[1])
        elif sqrt_price_x96 is not None:
            native_price = _v3_native_price(descriptor, sqrt_price_x96)
        else:
            native_price = None
        if native_price is None or not native_price.is_finite() or native_price <= 0:
            return None
        raw_hash = hashlib.sha256(
            json.dumps({
                "pool": descriptor.address,
                "type": descriptor.pool_type,
                "event": event.event_type,
                "data": event.data,
                "block": event.block_number,
                "tx": event.transaction_hash,
                "log": event.log_index,
            }, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return BscPoolPrice(
            mint=descriptor.mint,
            pool_address=descriptor.address,
            pool_type=descriptor.pool_type,
            native_token_price=native_price,
            observed_at=event.observed_at,
            block_number=event.block_number,
            transaction_hash=event.transaction_hash,
            log_index=event.log_index,
            raw_response_hash=raw_hash,
        )


class BscPairWssMonitor:
    """Subscribe once per pool type and fail closed on WSS errors."""

    def __init__(
        self,
        *,
        urls: Sequence[str] = (),
        callback: Callable[[BscPairEvent], None] | None = None,
    ) -> None:
        self.urls = tuple(dict.fromkeys(url for url in urls if url.startswith(("ws://", "wss://"))))
        self.callback = callback
        self._pool_descriptors: dict[str, BscPoolDescriptor] = {}
        self._pool_addresses: set[str] = set()
        self._pool_lock = threading.Lock()
        self._pool_changed = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._status_lock = threading.Lock()
        self.state = "DISCONNECTED"
        self.last_error_class: str | None = None
        self.last_message_at: datetime | None = None
        self.connection_count = 0
        self.disconnect_count = 0
        self.callback_error_count = 0
        self._disabled_after_failure = False

    @classmethod
    def from_env(cls) -> "BscPairWssMonitor":
        return cls(urls=_env_urls("BSC_WSS_URL", "BSC_BACKUP_WSS_URLS", "BSC_WSS_BACKUP_URLS"))

    def set_pool_descriptors(self, descriptors: Sequence[BscPoolDescriptor]) -> None:
        normalized: dict[str, BscPoolDescriptor] = {}
        for descriptor in descriptors:
            address = normalize_bsc_address(descriptor.address)
            if address is None or not descriptor.subscription_topics:
                continue
            normalized[address] = BscPoolDescriptor(
                address=address,
                pool_type=descriptor.pool_type,
                mint=descriptor.mint,
                token0=descriptor.token0,
                token1=descriptor.token1,
                token0_decimals=descriptor.token0_decimals,
                token1_decimals=descriptor.token1_decimals,
                event_topics=descriptor.event_topics,
            )
        with self._pool_lock:
            changed = normalized != self._pool_descriptors
            self._pool_descriptors = normalized
            self._pool_addresses = set(normalized)
        if changed:
            self._pool_changed.set()

    def set_pool_addresses(self, addresses: Sequence[object]) -> None:
        """Legacy test/compatibility hook; runtime uses resolved descriptors."""

        descriptors = [
            BscPoolDescriptor(address=address, pool_type="unknown")
            for value in addresses
            if (address := normalize_bsc_address(value)) is not None
        ]
        self.set_pool_descriptors(descriptors)

    def pool_addresses(self) -> tuple[str, ...]:
        with self._pool_lock:
            return tuple(sorted(self._pool_addresses))

    def pool_descriptors(self) -> tuple[BscPoolDescriptor, ...]:
        with self._pool_lock:
            return tuple(self._pool_descriptors.values())

    def safe_status(self) -> dict[str, object]:
        with self._status_lock:
            state = self.state
            error = self.last_error_class
            last_message_at = self.last_message_at
            connection_count = self.connection_count
            disconnect_count = self.disconnect_count
            callback_error_count = self.callback_error_count
        descriptors = self.pool_descriptors()
        pool_types = tuple(sorted({descriptor.pool_type for descriptor in descriptors}))
        topics = tuple(sorted({
            label
            for descriptor in descriptors
            for label in (
                "Swap" if SWAP_EVENT_TOPIC in descriptor.subscription_topics else None,
                "Sync" if SYNC_EVENT_TOPIC in descriptor.subscription_topics else None,
            )
            if label is not None
        }))
        return {
            "endpoint_configured": bool(self.urls),
            "configured_endpoints": len(self.urls),
            "pool_addresses": len(descriptors),
            "pool_types": pool_types,
            "topics": topics,
            "state": state,
            "last_error_class": error,
            "last_message_at": last_message_at.isoformat() if last_message_at else None,
            "connection_count": connection_count,
            "disconnect_count": disconnect_count,
            "callback_error_count": callback_error_count,
            "fallback_poll_sec": 2.0,
            "disabled_after_failure": self._disabled_after_failure,
        }

    def start(self) -> None:
        if self._thread is not None:
            return
        if not self.urls:
            self._set_status("UNAVAILABLE", "bsc_wss_missing")
            return
        self._thread = threading.Thread(target=self._run_thread, name="bsc-pair-wss", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._pool_changed.set()
        thread = self._thread
        if thread is not None and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=2.0)
        if thread is not None and not thread.is_alive():
            self._set_status("STOPPED", self.last_error_class)

    def _run_thread(self) -> None:
        try:
            asyncio.run(self._run_once())
        except Exception:
            self._set_status("DEGRADED", "bsc_wss_runtime_error")
            self._disabled_after_failure = True

    async def _run_once(self) -> None:
        try:
            import websockets  # type: ignore
        except ImportError:
            self._set_status("UNAVAILABLE", "bsc_wss_dependency_missing")
            return

        while not self._stop.is_set() and not self._disabled_after_failure:
            descriptors = self.pool_descriptors()
            groups = self._subscription_groups(descriptors)
            if not groups:
                self._set_status("NO_POOL_ADDRESS", None)
                await asyncio.sleep(0.5)
                continue
            endpoint = self.urls[0]
            try:
                self._set_status("CONNECTING", None)
                async with websockets.connect(
                    endpoint,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    open_timeout=5,
                ) as socket:
                    for request_id, (_key, addresses, topics) in enumerate(groups, start=1):
                        request = {
                            "jsonrpc": "2.0",
                            "id": request_id,
                            "method": "eth_subscribe",
                            "params": [
                                "logs",
                                {"address": list(addresses), "topics": [list(topics)]},
                            ],
                        }
                        await socket.send(json.dumps(request, separators=(",", ":")))
                    pending_ids = set(range(1, len(groups) + 1))
                    while pending_ids:
                        acknowledgement = await asyncio.wait_for(socket.recv(), timeout=5)
                        message = _json_message(acknowledgement)
                        if not isinstance(message, Mapping) or "id" not in message:
                            continue
                        request_id = message.get("id")
                        if request_id not in pending_ids:
                            continue
                        if message.get("error") is not None or not isinstance(message.get("result"), str):
                            raise RuntimeError("bsc_wss_subscription_rejected")
                        pending_ids.remove(request_id)
                    with self._status_lock:
                        self.connection_count += 1
                    self._set_status("HEALTHY", None)
                    self._pool_changed.clear()
                    while not self._stop.is_set():
                        if self._pool_changed.is_set():
                            break
                        try:
                            raw = await asyncio.wait_for(socket.recv(), timeout=0.5)
                        except asyncio.TimeoutError:
                            continue
                        event = self._decode_event(raw)
                        if event is None:
                            continue
                        with self._status_lock:
                            self.last_message_at = event.observed_at
                        callback = self.callback
                        if callback is not None:
                            try:
                                callback(event)
                            except Exception:
                                with self._status_lock:
                                    self.callback_error_count += 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                with self._status_lock:
                    self.disconnect_count += 1
                self._disabled_after_failure = True
                error_class = (
                    "bsc_wss_subscription_rejected"
                    if str(exc) == "bsc_wss_subscription_rejected"
                    else "bsc_wss_connection_error"
                )
                self._set_status("DEGRADED", error_class)
                return

        if self._stop.is_set():
            self._set_status("STOPPED", self.last_error_class)

    def _subscription_groups(
        self,
        descriptors: Sequence[BscPoolDescriptor],
    ) -> tuple[tuple[tuple[str, tuple[str, ...]], tuple[str, ...], tuple[str, ...]], ...]:
        grouped: dict[tuple[str, tuple[str, ...]], list[str]] = {}
        for descriptor in descriptors:
            topics = descriptor.subscription_topics
            if not topics:
                continue
            key = (descriptor.pool_type, topics)
            grouped.setdefault(key, []).append(descriptor.address)
        return tuple(
            (key, tuple(sorted(addresses)), key[1])
            for key, addresses in sorted(grouped.items())
        )

    def _decode_event(self, raw: object) -> BscPairEvent | None:
        message = _json_message(raw)
        if not isinstance(message, Mapping) or message.get("method") != "eth_subscription":
            return None
        params = message.get("params")
        result = params.get("result") if isinstance(params, Mapping) else None
        if not isinstance(result, Mapping) or result.get("removed") is True:
            return None
        pair_address = normalize_bsc_address(result.get("address"))
        topics_raw = result.get("topics")
        if pair_address is None or not isinstance(topics_raw, list) or not topics_raw:
            return None
        topics = tuple(str(topic).lower() for topic in topics_raw if isinstance(topic, str))
        if not topics:
            return None
        with self._pool_lock:
            descriptor = self._pool_descriptors.get(pair_address)
        if descriptor is None or topics[0] not in descriptor.subscription_topics:
            return None
        topic = topics[0]
        if topic == SWAP_EVENT_TOPIC:
            event_type = "swap"
        elif topic == SYNC_EVENT_TOPIC:
            event_type = "sync"
        else:
            event_type = "bonding_curve_event"
        data = result.get("data") if isinstance(result.get("data"), str) else None
        return BscPairEvent(
            pair_address=pair_address,
            event_type=event_type,
            block_number=_hex_int(result.get("blockNumber")),
            transaction_hash=result.get("transactionHash") if isinstance(result.get("transactionHash"), str) else None,
            log_index=_hex_int(result.get("logIndex")),
            observed_at=datetime.now(timezone.utc),
            pool_type=descriptor.pool_type,
            data=data,
            topics=topics,
        )

    def _set_status(self, state: str, error_class: str | None) -> None:
        with self._status_lock:
            self.state = state
            self.last_error_class = error_class


def _valid_topic(value: object) -> bool:
    return isinstance(value, str) and len(value) == 66 and value.lower().startswith("0x") and all(
        char in "0123456789abcdef" for char in value.lower()[2:]
    )


def _json_message(raw: object) -> Mapping[str, object] | None:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    if not isinstance(raw, str):
        return None
    try:
        message = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return message if isinstance(message, Mapping) else None


def _has_words(value: object, count: int) -> bool:
    return _data_words(value, count) is not None


def _data_words(value: object, count: int) -> tuple[int, ...] | None:
    if not isinstance(value, str) or not value.startswith("0x"):
        return None
    raw = value[2:]
    if len(raw) < count * 64 or len(raw) % 64 != 0:
        return None
    try:
        return tuple(int(raw[index:index + 64], 16) for index in range(0, count * 64, 64))
    except ValueError:
        return None


def _v2_native_price(descriptor: BscPoolDescriptor, reserve0: int, reserve1: int) -> Decimal | None:
    if reserve0 <= 0 or reserve1 <= 0:
        return None
    if descriptor.token0_decimals is None or descriptor.token1_decimals is None:
        return None
    try:
        amount0 = Decimal(reserve0) / (Decimal(10) ** descriptor.token0_decimals)
        amount1 = Decimal(reserve1) / (Decimal(10) ** descriptor.token1_decimals)
        if descriptor.token0 == BSC_WBNB_ADDRESS and descriptor.token1 == (descriptor.mint or "").lower():
            return amount0 / amount1
        if descriptor.token1 == BSC_WBNB_ADDRESS and descriptor.token0 == (descriptor.mint or "").lower():
            return amount1 / amount0
    except (InvalidOperation, ZeroDivisionError):
        return None
    return None


def _v3_native_price(descriptor: BscPoolDescriptor, sqrt_price_x96: int) -> Decimal | None:
    if sqrt_price_x96 <= 0 or descriptor.token0_decimals is None or descriptor.token1_decimals is None:
        return None
    try:
        raw_price = Decimal(sqrt_price_x96 * sqrt_price_x96) / Decimal(2 ** 192)
        human_price = raw_price * (Decimal(10) ** descriptor.token0_decimals) / (Decimal(10) ** descriptor.token1_decimals)
        if descriptor.token0 == (descriptor.mint or "").lower() and descriptor.token1 == BSC_WBNB_ADDRESS:
            return human_price
        if descriptor.token0 == BSC_WBNB_ADDRESS and descriptor.token1 == (descriptor.mint or "").lower():
            return Decimal("1") / human_price
    except (InvalidOperation, ZeroDivisionError):
        return None
    return None


def _hex_int(value: object) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if not isinstance(value, str):
        return None
    try:
        return int(value, 16) if value.startswith("0x") else int(value)
    except ValueError:
        return None
