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
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Callable, Mapping, Sequence


SWAP_EVENT_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
SYNC_EVENT_TOPIC = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
PAIR_EVENT_TOPICS = (SWAP_EVENT_TOPIC, SYNC_EVENT_TOPIC)
TRANSFER_EVENT_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
V2_POOL_TYPE = "v2"
V3_POOL_TYPE = "v3"
BONDING_CURVE_POOL_TYPE = "bonding_curve"
FLAP_PORTAL_POOL_TYPE = "flap_portal"
FOUR_MEME_TOKEN_MANAGER = "0x5c952063c7fc8610ffdb798152d69f0b9550762b"
FOUR_MEME_PROTOCOL = 2002
FOUR_TOKEN_PURCHASE_TOPIC = "0x7db52723a3b2cdd6164364b3b766e65e540d7be48ffa89582956d8eaebe62942"
FOUR_TOKEN_SALE_TOPIC = "0x0a5575b3648bae2210cee56bf33254cc1ddfbc7bf637c0af2ac18b14fb1bae19"
FOUR_LIQUIDITY_ADDED_TOPIC = "0xc18aa71171b358b706fe3dd345299685ba21a5316c66ffa9e319268b033c44b0"
FLAP_PORTAL_ADDRESS = "0xe2ce6ab80874fa9fa2aae65d277dd6b8e65c9de0"
FLAP_TOKEN_BOUGHT_TOPIC = "0xa800a2038683844fac66747f771bfdfae862eb28b16bcfa387afa9fbacce8ff7"
FLAP_TOKEN_SOLD_TOPIC = "0x03a4693e592f5e75dc7c136acb39b146d2b4966c0e509c34f362dee02b3b861a"
FLAP_LAUNCHED_TO_DEX_TOPIC = "0x6e4f47630b8745b8cacbd44f42a8a33e7eea7cc08ef22fc7630f4f385784ff7d"
FLAP_PORTAL_EVENT_TOPICS = (FLAP_TOKEN_BOUGHT_TOPIC, FLAP_TOKEN_SOLD_TOPIC, FLAP_LAUNCHED_TO_DEX_TOPIC)
_ZERO_ADDRESS = "0x" + "0" * 40
_UNSUPPORTED_ANCHOR = "0x" + "e" * 40
BSC_WBNB_ADDRESS = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
BSC_USDT_ADDRESS = "0x55d398326f99059ff775485246999027b3197955"
BSC_USDC_ADDRESS = "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d"
# Canonical PancakeSwap V2 factory on BNB Smart Chain.  Pool discovery is
# read-only and only accepts pairs returned by this factory.
PANCAKE_V2_FACTORY = "0xca143ce32fe78f1f7019d7d551a6402fc5350c73"
# Kept in sync with the installed official @pancakeswap/v3-sdk BSC mapping.
PANCAKE_V3_FACTORY = "0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865"
PAIR_CREATED_EVENT_TOPIC = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
POOL_CREATED_EVENT_TOPIC = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"
SUPPORTED_BSC_QUOTE_ASSETS = frozenset((BSC_WBNB_ADDRESS, BSC_USDT_ADDRESS, BSC_USDC_ADDRESS))
_TOKEN0_SELECTOR = "0x0dfe1681"
_TOKEN1_SELECTOR = "0xd21220a7"
_DECIMALS_SELECTOR = "0x313ce567"
_BALANCE_OF_SELECTOR = "0x70a08231"
_GET_RESERVES_SELECTOR = "0x0902f1ac"
_SLOT0_SELECTOR = "0x3850c7bd"
_GET_PAIR_SELECTOR = "0xe6a43905"
_GET_POOL_SELECTOR = "0x1698ee82"
_FEE_AMOUNT_TICK_SPACING_SELECTOR = "0x22afcccb"
_FACTORY_SELECTOR = "0xc45a0155"
_FEE_SELECTOR = "0xddca3f43"
_LIQUIDITY_SELECTOR = "0x1a686502"
_EIP1967_IMPLEMENTATION_SLOT = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
# PancakeSwap documents these public V3 fee tiers. Each is separately checked
# against feeAmountTickSpacing before getPool is called; no single tier is
# assumed to exist.
_V3_FEE_CANDIDATES = (100, 500, 2500, 10000)


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
        if self.pool_type == FLAP_PORTAL_POOL_TYPE:
            return FLAP_PORTAL_EVENT_TOPICS
        if self.pool_type == "unknown":
            return PAIR_EVENT_TOPICS
        return self.event_topics


@dataclass(frozen=True)
class BscPoolResolution:
    """Read-only pool-resolution outcome retained for candidate diagnostics."""

    descriptor: BscPoolDescriptor | None
    status: str
    pool_source: str
    binance_address: str | None = None
    token0: str | None = None
    token1: str | None = None
    reserves: tuple[int, int] | None = None
    quote_asset: str | None = None
    liquidity_usd: Decimal | None = None
    factory: str | None = None
    fee: int | None = None


VENUE_CAPABILITY_NAMES = (
    "DISCOVERY", "PRICE", "LIQUIDITY", "WSS", "FLOW", "BUY_QUOTE", "SELL_QUOTE", "PAPER_FILL",
)


@dataclass(frozen=True)
class BscVenueInspection:
    """Fail-safe, read-only fingerprint of an arbitrary BSC trading venue.

    A Binance ``pairAddress`` is an address hint, not proof that it is a
    Pancake V2 pair.  This record intentionally preserves real contracts even
    when no currently supported execution adapter can use them.
    """

    address: str
    chain: str
    is_contract: bool
    code_size: int | None = None
    bytecode_hash: str | None = None
    implementation: str | None = None
    factory: str | None = None
    token0: str | None = None
    token1: str | None = None
    reserves: tuple[int, int] | None = None
    slot0: bool = False
    liquidity: int | None = None
    fee: int | None = None
    selector_bitmap: str = ""
    protocol_fingerprint: str = ""
    protocol_family: str = "UNKNOWN"
    capabilities: tuple[tuple[str, str], ...] = ()
    error_class: str | None = None

    def capability(self, name: str) -> str:
        return dict(self.capabilities).get(name, "UNKNOWN")

    @property
    def capabilities_json(self) -> str:
        return json.dumps(dict(self.capabilities), sort_keys=True, separators=(",", ":"))


class VenueAdapter:
    """Common, intentionally read-only capability surface for BSC venues.

    The strategy uses this registry only for classification.  Actual quotes
    remain delegated to the existing read-only quote provider, so an unknown
    family can never become tradable merely because it was discovered.
    """

    family: str = "UNKNOWN"
    capabilities: Mapping[str, str] = {name: "UNSUPPORTED" for name in VENUE_CAPABILITY_NAMES}

    def detect(self, inspection: BscVenueInspection) -> bool:
        return inspection.protocol_family == self.family

    # The method names are deliberately uniform for future adapters.  The
    # current Pancake implementation remains in BscPoolResolver / quote
    # provider; these stubs prevent strategy code from depending on a venue
    # specific ABI.
    def get_assets(self, inspection: BscVenueInspection) -> tuple[str | None, str | None]:
        return inspection.token0, inspection.token1

    def get_price(self, *_args: object, **_kwargs: object) -> None:
        return None

    def get_liquidity(self, *_args: object, **_kwargs: object) -> None:
        return None

    def subscribe_market_data(self, *_args: object, **_kwargs: object) -> None:
        return None

    def parse_flow(self, *_args: object, **_kwargs: object) -> None:
        return None

    def quote_buy(self, *_args: object, **_kwargs: object) -> None:
        return None

    def quote_sell(self, *_args: object, **_kwargs: object) -> None:
        return None


class PancakeV2VenueAdapter(VenueAdapter):
    family = "PANCAKE_V2"
    capabilities = {name: "SUPPORTED" for name in VENUE_CAPABILITY_NAMES}


class PancakeV3VenueAdapter(VenueAdapter):
    family = "PANCAKE_V3"
    capabilities = {name: "SUPPORTED" for name in VENUE_CAPABILITY_NAMES}


class FourMemeBondingCurveVenueAdapter(VenueAdapter):
    """Verified Four.meme TokenManager venue, kept fail-closed for fills.

    Price and liquidity originate from the existing Four.meme/Binance market
    observations.  The manager's buy/sell events identify activity, but their
    amounts are not decoded until an ABI-backed parser is available, so they
    cannot by themselves unlock a Paper fill.
    """

    family = "FOURMEME_BONDING_CURVE"
    capabilities = {
        "DISCOVERY": "SUPPORTED",
        "PRICE": "SUPPORTED",
        "LIQUIDITY": "SUPPORTED",
        "WSS": "SUPPORTED",
        "FLOW": "UNSUPPORTED",
        "BUY_QUOTE": "UNKNOWN",
        "SELL_QUOTE": "UNKNOWN",
        "PAPER_FILL": "UNSUPPORTED",
    }


class FlapContextVenueAdapter(VenueAdapter):
    """Verified Flap Portal bonding-curve venue, kept fail-closed for fills.

    Flap's Portal supplies an authoritative token context and read-only
    buy/sell quote path before migration.  Portal trade events are decoded by
    the Balanced owner loop and fed into the shared flow/price pipeline.
    """

    family = "FLAP_CONTEXT"
    capabilities = {
        "DISCOVERY": "SUPPORTED",
        "PRICE": "SUPPORTED",
        "LIQUIDITY": "SUPPORTED",
        "WSS": "SUPPORTED",
        "FLOW": "SUPPORTED",
        "BUY_QUOTE": "SUPPORTED",
        "SELL_QUOTE": "SUPPORTED",
        "PAPER_FILL": "UNSUPPORTED",
    }


class UnknownVenueAdapter(VenueAdapter):
    """A persisted venue with no verified trade path; always fail closed."""

    family = "UNKNOWN"
    capabilities = {
        "DISCOVERY": "SUPPORTED",
        "PRICE": "UNKNOWN",
        "LIQUIDITY": "UNKNOWN",
        "WSS": "UNSUPPORTED",
        "FLOW": "UNSUPPORTED",
        "BUY_QUOTE": "UNSUPPORTED",
        "SELL_QUOTE": "UNSUPPORTED",
        "PAPER_FILL": "UNSUPPORTED",
    }


class VenueAdapterRegistry:
    """Family -> adapter mapping; adding a venue requires one adapter only."""

    def __init__(self) -> None:
        self._adapters: tuple[VenueAdapter, ...] = (
            PancakeV2VenueAdapter(), PancakeV3VenueAdapter(), FourMemeBondingCurveVenueAdapter(),
            FlapContextVenueAdapter(),
        )
        self._unknown = UnknownVenueAdapter()

    def adapter_for(self, inspection: BscVenueInspection) -> VenueAdapter:
        for adapter in self._adapters:
            if adapter.detect(inspection):
                return adapter
        return self._unknown

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
    transfer_from: str | None = None
    transfer_to: str | None = None
    transfer_value: int | None = None
    source: str = "WSS"


class BscRpcClient:
    """Small bounded JSON-RPC reader; it cannot submit transactions."""

    def __init__(self, urls: Sequence[str] = (), *, timeout_sec: float = 3.0) -> None:
        self.urls = tuple(dict.fromkeys(url for url in urls if url.startswith(("http://", "https://"))))
        self.timeout_sec = max(0.5, min(10.0, float(timeout_sec)))
        self._request_id = 0
        self._lock = threading.Lock()
        self.get_logs_call_count = 0
        self.get_logs_error_count = 0
        self.get_logs_429_count = 0
        self.get_logs_latency_ms: list[float] = []
        self.get_logs_supported: bool | None = None

    @classmethod
    def from_env(cls) -> "BscRpcClient":
        return cls(_env_http_urls("BSC_RPC_URL", "BSC_HTTP_RPC_URL", "BSC_RPC_BACKUP_URLS"))

    @classmethod
    def logs_from_env(cls) -> "BscRpcClient":
        return cls(_env_http_urls("BSC_LOGS_RPC_URL"))

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
            started = time.monotonic()
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
                    if method == "eth_getLogs":
                        self.get_logs_error_count += 1
                        self.get_logs_supported = False
                    continue
                if method == "eth_getLogs":
                    self.get_logs_call_count += 1
                    self.get_logs_latency_ms.append((time.monotonic() - started) * 1000)
                    self.get_logs_supported = True
                return decoded.get("result")
            except urllib.error.HTTPError as exc:
                if method == "eth_getLogs":
                    self.get_logs_call_count += 1
                    self.get_logs_error_count += 1
                    self.get_logs_429_count += int(exc.code == 429)
                    self.get_logs_supported = False
                    self.get_logs_latency_ms.append((time.monotonic() - started) * 1000)
                continue
            except (OSError, urllib.error.URLError, TimeoutError, ValueError, TypeError):
                if method == "eth_getLogs":
                    self.get_logs_call_count += 1
                    self.get_logs_error_count += 1
                    self.get_logs_latency_ms.append((time.monotonic() - started) * 1000)
                    self.get_logs_supported = False
                continue
        return None

    def call_batch(self, calls: Sequence[tuple[str, Sequence[object]]]) -> tuple[object | None, ...]:
        """Execute a small read-only JSON-RPC batch, preserving call order.

        Venue fingerprinting needs several independent interface probes.  A
        batch prevents an unknown venue from serially delaying the main loop
        by one network round trip per selector.  Any partial/error response is
        represented by ``None`` for that selector and remains fail-safe.
        """

        if not self.urls or not calls:
            return tuple(None for _ in calls)
        with self._lock:
            start_id = self._request_id + 1
            self._request_id += len(calls)
        payload = [
            {"jsonrpc": "2.0", "id": start_id + index, "method": method, "params": list(params)}
            for index, (method, params) in enumerate(calls)
        ]
        for endpoint in self.urls:
            try:
                request = urllib.request.Request(
                    endpoint,
                    data=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(request, timeout=self.timeout_sec) as response:
                    decoded = json.loads(response.read().decode("utf-8"))
                if not isinstance(decoded, list):
                    continue
                by_id = {
                    item.get("id"): item.get("result")
                    for item in decoded
                    if isinstance(item, Mapping) and item.get("error") is None
                }
                return tuple(by_id.get(start_id + index) for index in range(len(calls)))
            except (OSError, urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError, TypeError):
                continue
        return tuple(None for _ in calls)

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

    def get_code(self, address: str) -> str | None:
        result = self.call("eth_getCode", [address, "latest"])
        return result if isinstance(result, str) and result.startswith("0x") else None

    def get_storage_at(self, address: str, slot: str) -> str | None:
        result = self.call("eth_getStorageAt", [address, slot, "latest"])
        return result if isinstance(result, str) and result.startswith("0x") else None


class BscPoolResolver:
    """Resolve V2/V3 pool type and derive a pool spot price from log state."""

    def __init__(
        self,
        rpc: BscRpcClient | None = None,
        logs_rpc: BscRpcClient | None = None,
        *,
        bonding_curve_event_topics: Sequence[str] = (),
    ) -> None:
        self.rpc = rpc or BscRpcClient()
        self.logs_rpc = logs_rpc or BscRpcClient()
        self.bonding_curve_event_topics = tuple(
            topic.lower() for topic in bonding_curve_event_topics if _valid_topic(topic)
        )
        self.venue_adapters = VenueAdapterRegistry()
        self._cache: dict[tuple[str, str], BscPoolDescriptor | None] = {}
        self._venue_cache: dict[str, BscVenueInspection] = {}
        self._lock = threading.Lock()

    @classmethod
    def from_env(cls) -> "BscPoolResolver":
        raw_topics = os.environ.get("BSC_BONDING_CURVE_EVENT_TOPICS", "")
        return cls(
            BscRpcClient.from_env(),
            BscRpcClient.logs_from_env(),
            bonding_curve_event_topics=tuple(part.strip() for part in raw_topics.split(",")),
        )

    @property
    def configured(self) -> bool:
        return self.rpc.configured

    @staticmethod
    def _contract_code(code: str | None) -> bool:
        return code not in {None, "0x", "0x0", "0x00"}

    def inspect_venue(self, address: object | None) -> BscVenueInspection | None:
        """Fingerprint any source-supplied venue without assuming its ABI.

        Individual interface calls are deliberately independent.  A revert on
        ``getReserves`` or ``slot0`` means that selector is absent, not that
        the address is invalid.  Only an empty code result denotes a
        non-contract; an RPC failure remains explicit and is never converted
        into a fake protocol family.
        """

        venue = normalize_bsc_address(address)
        if venue is None:
            return None
        with self._lock:
            cached = self._venue_cache.get(venue)
        if cached is not None:
            return cached
        selector_calls: list[tuple[str, Sequence[object]]] = [
            ("eth_getCode", [venue, "latest"]),
            ("eth_getStorageAt", [venue, _EIP1967_IMPLEMENTATION_SLOT, "latest"]),
            *(("eth_call", [{"to": venue, "data": selector}, "latest"]) for selector in (
                _TOKEN0_SELECTOR, _TOKEN1_SELECTOR, _FACTORY_SELECTOR, _GET_RESERVES_SELECTOR,
                _SLOT0_SELECTOR, _LIQUIDITY_SELECTOR, _FEE_SELECTOR,
            )),
        ]
        code, implementation_raw, token0_raw, token1_raw, factory_raw, reserves_raw, slot0_raw, liquidity_raw, fee_raw = self.rpc.call_batch(selector_calls)
        code = code if isinstance(code, str) and code.startswith("0x") else None
        if code is None:
            inspection = BscVenueInspection(
                address=venue, chain="bsc:56", is_contract=False,
                protocol_family="RPC_READ_FAILED", error_class="RPC_READ_FAILED",
            )
            return inspection
        if not self._contract_code(code):
            inspection = BscVenueInspection(
                address=venue, chain="bsc:56", is_contract=False,
                code_size=0, protocol_family="NOT_A_CONTRACT", error_class="NOT_A_CONTRACT",
            )
            with self._lock:
                self._venue_cache[venue] = inspection
            return inspection
        runtime = code[2:]
        bytecode_hash = "sha256:" + hashlib.sha256(runtime.encode("ascii", "ignore")).hexdigest()
        implementation = None
        if implementation_raw is not None and len(implementation_raw) >= 42:
            implementation = normalize_bsc_address("0x" + implementation_raw[-40:])
        token0 = normalize_bsc_address("0x" + token0_raw[-40:]) if isinstance(token0_raw, str) and len(token0_raw) >= 42 else None
        token1 = normalize_bsc_address("0x" + token1_raw[-40:]) if isinstance(token1_raw, str) and len(token1_raw) >= 42 else None
        factory = normalize_bsc_address("0x" + factory_raw[-40:]) if isinstance(factory_raw, str) and len(factory_raw) >= 42 else None
        reserves_words = _data_words(reserves_raw if isinstance(reserves_raw, str) else None, 3)
        reserves = (reserves_words[0], reserves_words[1]) if reserves_words is not None else None
        slot0 = _has_words(slot0_raw if isinstance(slot0_raw, str) else None, 7)
        try:
            liquidity = int(liquidity_raw, 16) if isinstance(liquidity_raw, str) else None
        except ValueError:
            liquidity = None
        try:
            fee = int(fee_raw, 16) if isinstance(fee_raw, str) else None
        except ValueError:
            fee = None
        selector_values = {
            "factory": factory is not None,
            "token0": token0 is not None,
            "token1": token1 is not None,
            "getReserves": reserves is not None,
            "slot0": slot0,
            "liquidity": liquidity is not None,
            "fee": fee is not None,
        }
        selector_bitmap = ";".join(f"{name}={int(present)}" for name, present in selector_values.items())
        fingerprint_seed = "|".join((factory or "", implementation or "", bytecode_hash, selector_bitmap))
        protocol_fingerprint = "sha256:" + hashlib.sha256(fingerprint_seed.encode("utf-8")).hexdigest()
        v2_like = token0 is not None and token1 is not None and reserves is not None
        v3_like = token0 is not None and token1 is not None and slot0 and liquidity is not None and fee is not None
        if factory == PANCAKE_V2_FACTORY and v2_like:
            family = "PANCAKE_V2"
        elif factory == PANCAKE_V3_FACTORY and v3_like:
            family = "PANCAKE_V3"
        elif v2_like:
            family = "UNISWAP_V2_LIKE"
        elif v3_like:
            family = "UNISWAP_V3_LIKE"
        else:
            # Family identity intentionally contains no token address: future
            # venues with the same implementation/fingerprint cluster here.
            family = "UNKNOWN_FAMILY_" + protocol_fingerprint.split(":", 1)[1][:12]
        provisional = BscVenueInspection(
            address=venue, chain="bsc:56", is_contract=True,
            code_size=len(runtime) // 2, bytecode_hash=bytecode_hash,
            implementation=implementation, factory=factory, token0=token0, token1=token1,
            reserves=reserves, slot0=slot0, liquidity=liquidity, fee=fee,
            selector_bitmap=selector_bitmap, protocol_fingerprint=protocol_fingerprint,
            protocol_family=family,
        )
        adapter = self.venue_adapters.adapter_for(provisional)
        inspection = BscVenueInspection(
            **{**provisional.__dict__, "capabilities": tuple(sorted(adapter.capabilities.items()))}
        )
        with self._lock:
            self._venue_cache[venue] = inspection
        return inspection

    def resolve(
        self,
        mint: str,
        pair_address: object | None,
        bonding_curve_address: object | None = None,
        *,
        protocol: object | None = None,
        migrate_status: object | None = None,
    ) -> BscPoolDescriptor | None:
        resolution = self.resolve_pancake_v2(mint, (pair_address,))
        if resolution.descriptor is not None:
            return resolution.descriptor
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
            return self.resolve_fourmeme_bonding_curve(mint)
        if curve is None or not self.bonding_curve_event_topics:
            # The source did not identify a usable curve contract/ABI.  Do not
            # turn the token contract or a marker address into a fake pool.
            return None

    def resolve_pancake_v2(
        self,
        mint: str,
        binance_addresses: Sequence[object] = (),
        *,
        quote_asset_usd: Mapping[str, Decimal] | None = None,
    ) -> BscPoolResolution:
        """Validate source addresses, then discover a real Pancake V2 pair.

        A Binance field is discovery input only.  It is never subscribed until
        its bytecode, V2 shape, token membership and non-zero reserves have
        all been verified through the configured read-only RPC.
        """

        token = normalize_bsc_address(mint)
        if token is None:
            return BscPoolResolution(None, "INVALID_PAIR", "NONE")
        if not self.rpc.configured:
            return BscPoolResolution(None, "RPC_READ_FAILED", "NONE")
        normalized = tuple(dict.fromkeys(
            address for value in binance_addresses if (address := normalize_bsc_address(value)) is not None
        ))
        last_status = "NO_PANCAKE_PAIR"
        for address in normalized:
            checked = self._validate_v2_pair(token, address, quote_asset_usd=quote_asset_usd)
            if checked.descriptor is not None:
                return BscPoolResolution(
                    checked.descriptor, "VALID", "BINANCE_VALIDATED", address,
                    checked.token0, checked.token1, checked.reserves, checked.quote_asset,
                    checked.liquidity_usd, PANCAKE_V2_FACTORY,
                )
            last_status = checked.status

        discovered: list[BscPoolResolution] = []
        rpc_failed = False
        for quote in SUPPORTED_BSC_QUOTE_ASSETS:
            pair = self._factory_get_pair(token, quote)
            if pair is None:
                rpc_failed = True
                continue
            if pair == _ZERO_ADDRESS:
                continue
            checked = self._validate_v2_pair(token, pair, quote_asset_usd=quote_asset_usd)
            if checked.descriptor is not None:
                discovered.append(BscPoolResolution(
                    checked.descriptor, "VALID", "PANCAKE_V2_FACTORY", None,
                    checked.token0, checked.token1, checked.reserves, checked.quote_asset,
                    checked.liquidity_usd, PANCAKE_V2_FACTORY,
                ))
            else:
                last_status = checked.status
        if discovered:
            # Scores are twice the verified quote-side reserve in USD.  The
            # runner supplies BNB/USD from the existing snapshot; stablecoins
            # are valued at their on-chain quote unit.  Missing prices sort
            # last rather than inventing a value.
            return max(discovered, key=lambda item: item.liquidity_usd if item.liquidity_usd is not None else Decimal("-1"))
        if rpc_failed and not normalized:
            last_status = "RPC_READ_FAILED"
        return BscPoolResolution(None, last_status, "FALLBACK_POOL_DISCOVERY", normalized[0] if normalized else None)

    def resolve_pancake_v3(
        self,
        mint: str,
        binance_addresses: Sequence[object] = (),
    ) -> BscPoolResolution:
        """Discover a verified Pancake V3 pool using only read-only RPC."""

        token = normalize_bsc_address(mint)
        if token is None:
            return BscPoolResolution(None, "INVALID_PAIR", "NONE")
        if not self.rpc.configured:
            return BscPoolResolution(None, "RPC_READ_FAILED", "NONE")
        enabled_fees = tuple(fee for fee in _V3_FEE_CANDIDATES if self._v3_fee_enabled(fee))
        if not enabled_fees:
            return BscPoolResolution(None, "V3_FEES_UNAVAILABLE", "PANCAKE_V3_FACTORY")
        checked_addresses = tuple(dict.fromkeys(
            address for value in binance_addresses if (address := normalize_bsc_address(value)) is not None
        ))
        results: list[BscPoolResolution] = []
        last_status = "NO_PANCAKE_V3_POOL"
        for address in checked_addresses:
            checked = self._validate_v3_pool(token, address)
            if checked.descriptor is not None:
                results.append(checked)
            else:
                last_status = checked.status
        for quote in SUPPORTED_BSC_QUOTE_ASSETS:
            for fee in enabled_fees:
                address = self._v3_get_pool(token, quote, fee)
                if address in {None, _ZERO_ADDRESS}:
                    continue
                checked = self._validate_v3_pool(token, address)
                if checked.descriptor is not None:
                    results.append(checked)
                else:
                    last_status = checked.status
        if results:
            # V3 liquidity is not comparable in USD without tick accounting;
            # prefer the pool with the largest on-chain active liquidity.
            return max(results, key=lambda item: item.liquidity_usd or Decimal("-1"))
        return BscPoolResolution(None, last_status, "PANCAKE_V3_FACTORY")

    def discover_factory_pools(
        self,
        mint: str,
        *,
        from_block: int,
        to_block: int | None = None,
        chunk_size: int = 2000,
        quote_asset_usd: Mapping[str, Decimal] | None = None,
    ) -> tuple[BscPoolResolution, ...]:
        """Find Pancake pools for one token from Factory events only.

        The query is token-topic scoped in both indexed token positions.  It
        never scans all factory history and is deliberately read-only.
        """
        token = normalize_bsc_address(mint)
        if token is None or not self.logs_rpc.configured:
            return ()
        if self.logs_rpc.get_logs_supported is False:
            return ()
        if to_block is None:
            raw = self.logs_rpc.call("eth_blockNumber", ())
            try:
                to_block = int(str(raw), 16)
            except (TypeError, ValueError):
                return ()
        start, end = max(0, int(from_block)), max(0, int(to_block))
        if end < start:
            return ()
        topic_address = "0x" + "0" * 24 + token[2:]
        found: list[BscPoolResolution] = []
        seen: set[str] = set()
        for factory, event_topic, pool_type in (
            (PANCAKE_V2_FACTORY, PAIR_CREATED_EVENT_TOPIC, V2_POOL_TYPE),
            (PANCAKE_V3_FACTORY, POOL_CREATED_EVENT_TOPIC, V3_POOL_TYPE),
        ):
            for indexed_position in (1, 2):
                for lower in range(start, end + 1, max(2000, min(5000, int(chunk_size)))):
                    upper = min(end, lower + max(2000, min(5000, int(chunk_size))) - 1)
                    topics: list[object] = [event_topic, None, None]
                    topics[indexed_position] = topic_address
                    logs = self.logs_rpc.call("eth_getLogs", [{
                        "address": factory, "fromBlock": hex(lower), "toBlock": hex(upper), "topics": topics,
                    }])
                    if not isinstance(logs, list):
                        if self.logs_rpc.get_logs_supported is False:
                            return tuple(found)
                        continue
                    for log in logs:
                        parsed = _factory_pool_from_log(log, token, pool_type)
                        if parsed is None or parsed[0] in seen:
                            continue
                        address, _fee = parsed
                        checked = (
                            self._validate_v2_pair(token, address, quote_asset_usd=quote_asset_usd, allow_unsupported_quote=True)
                            if pool_type == V2_POOL_TYPE else self._validate_v3_pool(token, address, allow_unsupported_quote=True)
                        )
                        if checked.descriptor is None:
                            continue
                        seen.add(address)
                        quote = checked.quote_asset
                        # A pool's protocol validity is independent from the
                        # quote asset's USD conversion.  Unknown ERC-20 quote
                        # assets are resolved asynchronously by the shared
                        # QuoteAssetUsdResolver after discovery; they must not
                        # be discarded at the registry boundary.
                        status = "VALID"
                        found.append(BscPoolResolution(
                            checked.descriptor, status, f"PANCAKE_{pool_type.upper()}_FACTORY_EVENT", address,
                            checked.token0, checked.token1, checked.reserves, quote, checked.liquidity_usd,
                            factory, checked.fee,
                        ))
        return tuple(found)

    def block_at_or_before(self, target: datetime) -> int | None:
        """Binary-search the logs provider for the nearest block at/before UTC time."""
        raw = self.logs_rpc.call("eth_blockNumber", ())
        try:
            high = int(str(raw), 16)
        except (TypeError, ValueError):
            return None
        wanted = int(target.timestamp())
        low, answer = 0, 0
        while low <= high:
            mid = (low + high) // 2
            block = self.logs_rpc.call("eth_getBlockByNumber", [hex(mid), False])
            try:
                timestamp = int(str(block["timestamp"]), 16)  # type: ignore[index]
            except (TypeError, KeyError, ValueError):
                return None
            if timestamp <= wanted:
                answer, low = mid, mid + 1
            else:
                high = mid - 1
        return answer

    def factory_logs(self, pool_type: str, start: int, end: int) -> list[Mapping[str, object]] | None:
        """One bounded Factory-log chunk for main-loop reconnect recovery."""
        factory, topic = (PANCAKE_V2_FACTORY, PAIR_CREATED_EVENT_TOPIC) if pool_type == V2_POOL_TYPE else (PANCAKE_V3_FACTORY, POOL_CREATED_EVENT_TOPIC)
        logs = self.logs_rpc.call("eth_getLogs", [{"address": factory, "fromBlock": hex(start), "toBlock": hex(end), "topics": [topic]}])
        return logs if isinstance(logs, list) else None

    def _v3_fee_enabled(self, fee: int) -> bool:
        raw = self.rpc.call_hex(PANCAKE_V3_FACTORY, _FEE_AMOUNT_TICK_SPACING_SELECTOR + f"{fee:064x}")
        words = _data_words(raw, 1)
        return words is not None and words[0] != 0

    def _v3_get_pool(self, token: str, quote: str, fee: int) -> str | None:
        data = _GET_POOL_SELECTOR + token[2:].rjust(64, "0") + quote[2:].rjust(64, "0") + f"{fee:064x}"
        raw = self.rpc.call_hex(PANCAKE_V3_FACTORY, data)
        if raw is None:
            return None
        if len(raw) < 42:
            return _ZERO_ADDRESS
        return normalize_bsc_address("0x" + raw[-40:]) or _ZERO_ADDRESS

    def _validate_v3_pool(self, mint: str, address: str, *, allow_unsupported_quote: bool = False) -> BscPoolResolution:
        code = self.rpc.get_code(address)
        if code is None:
            return BscPoolResolution(None, "RPC_READ_FAILED", "V3_VALIDATION", binance_address=address)
        if code in {"0x", "0x0"}:
            return BscPoolResolution(None, "INVALID_PAIR", "V3_VALIDATION", binance_address=address)
        token0 = self.rpc.call_address(address, _TOKEN0_SELECTOR)
        token1 = self.rpc.call_address(address, _TOKEN1_SELECTOR)
        factory = self.rpc.call_address(address, _FACTORY_SELECTOR)
        fee = self.rpc.call_uint(address, _FEE_SELECTOR)
        slot0 = self.rpc.call_hex(address, _SLOT0_SELECTOR)
        liquidity = self.rpc.call_uint(address, _LIQUIDITY_SELECTOR)
        if token0 is None or token1 is None or factory != PANCAKE_V3_FACTORY or fee is None:
            return BscPoolResolution(None, "UNSUPPORTED_POOL_TYPE", "V3_VALIDATION", address, token0, token1, factory=factory)
        if mint not in {token0, token1}:
            return BscPoolResolution(None, "INVALID_PAIR", "V3_VALIDATION", address, token0, token1, factory=factory, fee=fee)
        quote = token1 if token0 == mint else token0
        if not self._v3_fee_enabled(fee) or not _has_words(slot0, 7) or liquidity is None or liquidity <= 0:
            return BscPoolResolution(None, "ZERO_LIQUIDITY", "V3_VALIDATION", address, token0, token1, quote_asset=quote, factory=factory, fee=fee)
        token0_decimals = self._decimals(token0)
        token1_decimals = self._decimals(token1)
        if token0_decimals is None or token1_decimals is None:
            return BscPoolResolution(None, "RPC_READ_FAILED", "V3_VALIDATION", address, token0, token1, quote_asset=quote, factory=factory, fee=fee)
        return BscPoolResolution(BscPoolDescriptor(address, V3_POOL_TYPE, mint, token0, token1, token0_decimals, token1_decimals), "VALID", "PANCAKE_V3_FACTORY", address, token0, token1, quote_asset=quote, liquidity_usd=Decimal(liquidity), factory=factory, fee=fee)

    def _factory_get_pair(self, token: str, quote: str) -> str | None:
        data = _GET_PAIR_SELECTOR + token[2:].rjust(64, "0") + quote[2:].rjust(64, "0")
        raw = self.rpc.call_hex(PANCAKE_V2_FACTORY, data)
        if raw is None:
            return None
        if len(raw) < 42:
            return _ZERO_ADDRESS
        return normalize_bsc_address("0x" + raw[-40:]) or _ZERO_ADDRESS

    def _validate_v2_pair(
        self,
        mint: str,
        address: str,
        *,
        quote_asset_usd: Mapping[str, Decimal] | None,
        allow_unsupported_quote: bool = False,
    ) -> BscPoolResolution:
        code = self.rpc.get_code(address)
        if code is None:
            return BscPoolResolution(None, "RPC_READ_FAILED", "VALIDATION", binance_address=address)
        if code in {"0x", "0x0"}:
            return BscPoolResolution(None, "INVALID_PAIR", "VALIDATION", binance_address=address)
        token0 = self.rpc.call_address(address, _TOKEN0_SELECTOR)
        token1 = self.rpc.call_address(address, _TOKEN1_SELECTOR)
        reserves_raw = self.rpc.call_hex(address, _GET_RESERVES_SELECTOR)
        reserves = _data_words(reserves_raw, 3)
        if token0 is None or token1 is None or reserves is None:
            return BscPoolResolution(None, "UNSUPPORTED_POOL_TYPE", "VALIDATION", address, token0, token1)
        if mint not in {token0, token1}:
            return BscPoolResolution(None, "INVALID_PAIR", "VALIDATION", address, token0, token1, (reserves[0], reserves[1]))
        quote = token1 if token0 == mint else token0
        if reserves[0] <= 0 or reserves[1] <= 0:
            return BscPoolResolution(None, "ZERO_LIQUIDITY", "VALIDATION", address, token0, token1, (reserves[0], reserves[1]), quote)
        token0_decimals = self._decimals(token0)
        token1_decimals = self._decimals(token1)
        if token0_decimals is None or token1_decimals is None:
            return BscPoolResolution(None, "RPC_READ_FAILED", "VALIDATION", address, token0, token1, (reserves[0], reserves[1]), quote)
        quote_reserve = reserves[0] if token0 == quote else reserves[1]
        quote_decimals = token0_decimals if token0 == quote else token1_decimals
        quote_usd = Decimal("1") if quote in {BSC_USDT_ADDRESS, BSC_USDC_ADDRESS} else (quote_asset_usd or {}).get(quote)
        liquidity_usd = None if quote_usd is None else Decimal(2) * Decimal(quote_reserve) / (Decimal(10) ** quote_decimals) * quote_usd
        return BscPoolResolution(
            BscPoolDescriptor(address, V2_POOL_TYPE, mint, token0, token1, token0_decimals, token1_decimals),
            "VALID", "VALIDATION", address, token0, token1, (reserves[0], reserves[1]), quote, liquidity_usd,
            PANCAKE_V2_FACTORY,
        )

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

    def erc20_balance_of(self, token: object | None, owner: object | None) -> Decimal | None:
        """Read a wallet's ERC-20 balance from the configured BSC RPC only.

        A failed RPC or metadata read is deliberately ``None`` rather than a
        zero balance, so callers cannot release a live position slot on an
        unverified chain read.
        """

        mint = normalize_bsc_address(token)
        wallet = normalize_bsc_address(owner)
        if mint is None or wallet is None or not self.rpc.configured:
            return None
        decimals = self._decimals(mint)
        if decimals is None:
            return None
        raw = self.rpc.call_hex(mint, _BALANCE_OF_SELECTOR + wallet[2:].rjust(64, "0"))
        if raw is None:
            return None
        try:
            return Decimal(int(raw, 16)) / (Decimal(10) ** decimals)
        except (ValueError, ArithmeticError):
            return None

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

    def resolve_fourmeme_bonding_curve(self, mint: str) -> BscPoolDescriptor:
        """Return the protocol-level Four.meme manager descriptor."""

        return BscPoolDescriptor(
            address=FOUR_MEME_TOKEN_MANAGER,
            pool_type=BONDING_CURVE_POOL_TYPE,
            mint=normalize_bsc_address(mint) or mint.lower(),
            event_topics=(FOUR_TOKEN_PURCHASE_TOPIC, FOUR_TOKEN_SALE_TOPIC, FOUR_LIQUIDITY_ADDED_TOPIC),
        )

    def inspect_fourmeme_bonding_curve(self) -> BscVenueInspection | None:
        """Fingerprint the shared TokenManager as a Four.meme venue record."""

        inspection = self.inspect_venue(FOUR_MEME_TOKEN_MANAGER)
        adapter = FourMemeBondingCurveVenueAdapter()
        if inspection is None or not inspection.is_contract:
            return BscVenueInspection(
                address=FOUR_MEME_TOKEN_MANAGER,
                chain="bsc:56",
                is_contract=True,
                protocol_family=adapter.family,
                capabilities=tuple(sorted(adapter.capabilities.items())),
                selector_bitmap="fourmeme_token_manager_events=1",
                protocol_fingerprint="FOURMEME_TOKEN_MANAGER_V2",
            )
        return replace(
            inspection,
            protocol_family=adapter.family,
            capabilities=tuple(sorted(adapter.capabilities.items())),
            error_class=None,
        )

    def inspect_flap_context(self, portal_address: object | None) -> BscVenueInspection | None:
        """Fingerprint an on-chain validated Flap Portal Context."""

        portal = normalize_bsc_address(portal_address)
        if portal is None:
            return None
        inspection = self.inspect_venue(portal)
        adapter = FlapContextVenueAdapter()
        if inspection is None or not inspection.is_contract:
            return BscVenueInspection(
                address=portal,
                chain="bsc:56",
                is_contract=True,
                protocol_family=adapter.family,
                capabilities=tuple(sorted(adapter.capabilities.items())),
                selector_bitmap="flap_portal_context=1",
                protocol_fingerprint="FLAP_PORTAL_CONTEXT_V1",
            )
        return replace(
            inspection,
            protocol_family=adapter.family,
            capabilities=tuple(sorted(adapter.capabilities.items())),
            error_class=None,
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
        self._factory_descriptors: dict[str, BscPoolDescriptor] = {}
        self._pool_addresses: set[str] = set()
        self._pool_lock = threading.Lock()
        self._pool_changed = threading.Event()
        self._holder_tokens: set[str] = set()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._status_lock = threading.Lock()
        self.state = "DISCONNECTED"
        self.last_error_class: str | None = None
        self.last_message_at: datetime | None = None
        self.last_block_number: int | None = None
        self.connection_count = 0
        self.disconnect_count = 0
        self.retry_count = 0
        self.callback_error_count = 0
        self._disabled_after_failure = False
        self.last_subscription_request: dict[str, object] | None = None
        self.last_subscription_response: dict[str, object] | None = None
        self.last_subscription_error_code: object | None = None
        self.last_subscription_error_message: str | None = None

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

    def enable_factory_registry(self, enabled: bool = True) -> None:
        """Subscribe to Pancake Factory creation events separately from pools."""

        descriptors = {
            PANCAKE_V2_FACTORY: BscPoolDescriptor(PANCAKE_V2_FACTORY, "factory_v2", event_topics=(PAIR_CREATED_EVENT_TOPIC,)),
            PANCAKE_V3_FACTORY: BscPoolDescriptor(PANCAKE_V3_FACTORY, "factory_v3", event_topics=(POOL_CREATED_EVENT_TOPIC,)),
        } if enabled else {}
        with self._pool_lock:
            changed = descriptors != self._factory_descriptors
            self._factory_descriptors = descriptors
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

    def set_holder_token_addresses(self, addresses: Sequence[object]) -> None:
        normalized = {address for value in addresses if (address := normalize_bsc_address(value)) is not None}
        with self._pool_lock:
            changed = normalized != self._holder_tokens
            self._holder_tokens = normalized
        if changed:
            self._pool_changed.set()

    def pool_addresses(self) -> tuple[str, ...]:
        with self._pool_lock:
            return tuple(sorted(self._pool_addresses))

    def pool_descriptors(self) -> tuple[BscPoolDescriptor, ...]:
        with self._pool_lock:
            return tuple(self._pool_descriptors.values())

    def factory_descriptors(self) -> tuple[BscPoolDescriptor, ...]:
        with self._pool_lock:
            return tuple(self._factory_descriptors.values())

    def safe_status(self) -> dict[str, object]:
        with self._status_lock:
            state = self.state
            error = self.last_error_class
            last_message_at = self.last_message_at
            connection_count = self.connection_count
            disconnect_count = self.disconnect_count
            callback_error_count = self.callback_error_count
        descriptors = self.pool_descriptors()
        factories = self.factory_descriptors()
        pool_types = tuple(sorted({descriptor.pool_type for descriptor in descriptors}))
        topics = tuple(sorted({
            label
            for descriptor in descriptors
            for label in (
                "Swap" if SWAP_EVENT_TOPIC in descriptor.subscription_topics else None,
                "Sync" if SYNC_EVENT_TOPIC in descriptor.subscription_topics else None,
                "FlapTokenBought" if FLAP_TOKEN_BOUGHT_TOPIC in descriptor.subscription_topics else None,
                "FlapTokenSold" if FLAP_TOKEN_SOLD_TOPIC in descriptor.subscription_topics else None,
                "FlapLaunchedToDEX" if FLAP_LAUNCHED_TO_DEX_TOPIC in descriptor.subscription_topics else None,
            )
            if label is not None
        }))
        return {
            "endpoint_configured": bool(self.urls),
            "configured_endpoints": len(self.urls),
            "pool_addresses": len(descriptors),
            "factory_v2_wss": "CONFIGURED" if PANCAKE_V2_FACTORY in {item.address for item in factories} else "DISABLED",
            "factory_v3_wss": "CONFIGURED" if PANCAKE_V3_FACTORY in {item.address for item in factories} else "DISABLED",
            "flap_portal_wss": "CONFIGURED" if FLAP_PORTAL_ADDRESS in {item.address for item in descriptors} else "DISABLED",
            "pool_address_list": tuple(sorted(descriptor.address for descriptor in descriptors)),
            "pool_types": pool_types,
            "topics": topics,
            "state": state,
            "last_error_class": error,
            "last_message_at": last_message_at.isoformat() if last_message_at else None,
            "last_block_number": self.last_block_number,
            "connection_count": connection_count,
            "disconnect_count": disconnect_count,
            "retry_count": self.retry_count,
            "callback_error_count": callback_error_count,
            "last_subscription_request": self.last_subscription_request,
            "last_subscription_response": self.last_subscription_response,
            "last_subscription_error_code": self.last_subscription_error_code,
            "last_subscription_error_message": self.last_subscription_error_message,
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

    def request_resubscribe(self) -> None:
        """Force a clean reconnect after a completeness miss.

        This only wakes the existing WSS worker; it does not create another
        connection and never touches SQLite.
        """

        if not self._stop.is_set():
            self._pool_changed.set()

    def _run_thread(self) -> None:
        try:
            asyncio.run(self._run_once())
        except Exception as exc:
            self._set_status("DEGRADED", self._failure_class(exc))

    async def _run_once(self) -> None:
        try:
            import websockets  # type: ignore
        except ImportError:
            self._set_status("UNAVAILABLE", "bsc_wss_dependency_missing")
            return

        while not self._stop.is_set():
            descriptors = (*self.pool_descriptors(), *self.factory_descriptors())
            groups = list(self._subscription_groups(descriptors))
            with self._pool_lock:
                holder_tokens = tuple(sorted(self._holder_tokens))
            if holder_tokens:
                groups.append((("erc20_transfer", (TRANSFER_EVENT_TOPIC,)), holder_tokens, (TRANSFER_EVENT_TOPIC,)))
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
                        with self._status_lock:
                            self.last_subscription_request = request
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
                            error = message.get("error")
                            with self._status_lock:
                                self.last_subscription_response = dict(message)
                                self.last_subscription_error_code = error.get("code") if isinstance(error, Mapping) else None
                                self.last_subscription_error_message = (
                                    str(error.get("message")) if isinstance(error, Mapping) and error.get("message") is not None
                                    else "invalid_subscription_response"
                                )
                            raise RuntimeError("bsc_wss_subscription_rejected")
                        with self._status_lock:
                            self.last_subscription_response = dict(message)
                            self.last_subscription_error_code = None
                            self.last_subscription_error_message = None
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
                            self.last_block_number = event.block_number
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
                    self.retry_count += 1
                    if str(exc) != "bsc_wss_subscription_rejected":
                        self.last_subscription_error_code = None
                        self.last_subscription_error_message = str(exc)[:500] or type(exc).__name__
                error_class = self._failure_class(exc)
                self._set_status("DEGRADED", error_class)
                # Keep the worker local to this runtime.  A transient stale
                # public connection must not permanently suppress all future
                # candidate subscriptions, and no other strategy is touched.
                await asyncio.sleep(min(5.0, 0.5 * max(1, self.retry_count)))
                continue

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

    @staticmethod
    def _failure_class(exc: Exception) -> str:
        if str(exc) == "bsc_wss_subscription_rejected":
            return "SUBSCRIBE_RPC_ERROR"
        name = type(exc).__name__.lower()
        if "timeout" in name or isinstance(exc, asyncio.TimeoutError):
            return "TIMEOUT"
        if "handshake" in name or "status" in name:
            return "HANDSHAKE_FAILED"
        if "closed" in name:
            return "CONNECTION_CLOSED"
        return "PROVIDER_UNAVAILABLE"

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
            descriptor = self._pool_descriptors.get(pair_address) or self._factory_descriptors.get(pair_address)
        if descriptor is None:
            with self._pool_lock:
                is_holder_token = pair_address in self._holder_tokens
            if not is_holder_token or topics[0] != TRANSFER_EVENT_TOPIC:
                return None
            value = _data_words(result.get("data"), 1)
            if len(topics) < 3 or value is None:
                return None
            return BscPairEvent(pair_address, "transfer", _hex_int(result.get("blockNumber")), result.get("transactionHash") if isinstance(result.get("transactionHash"), str) else None, _hex_int(result.get("logIndex")), datetime.now(timezone.utc), data=result.get("data") if isinstance(result.get("data"), str) else None, topics=topics, transfer_from=normalize_bsc_address("0x" + topics[1][-40:]), transfer_to=normalize_bsc_address("0x" + topics[2][-40:]), transfer_value=value[0])
        if topics[0] not in descriptor.subscription_topics:
            return None
        topic = topics[0]
        if topic == SWAP_EVENT_TOPIC:
            event_type = "swap"
        elif topic == SYNC_EVENT_TOPIC:
            event_type = "sync"
        elif topic == PAIR_CREATED_EVENT_TOPIC:
            event_type = "factory_pair_created"
        elif topic == POOL_CREATED_EVENT_TOPIC:
            event_type = "factory_pool_created"
        elif topic == FLAP_TOKEN_BOUGHT_TOPIC:
            event_type = "flap_token_bought"
        elif topic == FLAP_TOKEN_SOLD_TOPIC:
            event_type = "flap_token_sold"
        elif topic == FLAP_LAUNCHED_TO_DEX_TOPIC:
            event_type = "flap_launched_to_dex"
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


def _factory_pool_from_log(log: object, mint: str, pool_type: str) -> tuple[str, int | None] | None:
    """Extract a pool address from a token-scoped V2/V3 Factory event."""
    if not isinstance(log, Mapping):
        return None

    topics = log.get("topics")
    data = log.get("data")
    if not isinstance(topics, list) or len(topics) < 3 or not isinstance(data, str):
        return None
    topic0 = str(topics[0]).lower() if topics else ""
    token0 = normalize_bsc_address("0x" + str(topics[1])[-40:])
    token1 = normalize_bsc_address("0x" + str(topics[2])[-40:])
    if mint not in {token0, token1}:
        return None
    words = _data_words(data, 2 if pool_type == V2_POOL_TYPE else 2)
    if words is None:
        return None
    if pool_type == V2_POOL_TYPE:
        if topic0 != PAIR_CREATED_EVENT_TOPIC:
            return None
        return normalize_bsc_address("0x" + f"{words[0]:064x}"[-40:]), None
    if topic0 != POOL_CREATED_EVENT_TOPIC or len(topics) < 4:
        return None
    try:
        fee = int(str(topics[3]), 16)
    except ValueError:
        return None
    return normalize_bsc_address("0x" + f"{words[1]:064x}"[-40:]), fee


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
