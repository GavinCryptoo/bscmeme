"""Normalized read-only Binance Web3 records."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Mapping

from meme_system.domain.models import Signal


@dataclass(frozen=True)
class EndpointSpec:
    endpoint_type: str
    host: str
    path: str
    method: str
    auth_mode: str = "none"

    @property
    def url(self) -> str:
        return self.host.rstrip("/") + "/" + self.path.lstrip("/")


@dataclass(frozen=True)
class ObservedField:
    value: Any
    source: str
    source_field: str
    observed_at: datetime
    source_timestamp: datetime | None
    age_ms: int | None
    available: bool
    parse_error: str | None = None
    adapter_version: str = "binance_web3_v1"


@dataclass(frozen=True)
class BinanceNormalizedSignal:
    signal: Signal
    source_signal_id: str | None
    source_timestamp: datetime | None
    fetched_at: datetime
    historical_bootstrap: bool
    raw_response_hash: str
    fields: Mapping[str, ObservedField] = field(default_factory=dict)
    endpoint_type: str = "meme_rush"
    chain_id: str = "CT_501"


@dataclass(frozen=True)
class BinanceMarketSnapshot:
    mint: str
    chain_id: str
    observed_at: datetime
    source_timestamp: datetime | None
    raw_response_hash: str
    fields: Mapping[str, ObservedField]
    endpoint_type: str = "token_dynamic"

    def value(self, name: str) -> Any:
        field = self.fields.get(name)
        return field.value if field and field.available else None


@dataclass(frozen=True)
class BinanceKline:
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    open_time_ms: int
    trade_count: int


@dataclass(frozen=True)
class BinanceKlineResult:
    mint: str
    chain_id: str
    interval: str
    candles: tuple[BinanceKline, ...]
    fetched_at: datetime
    raw_response_hash: str
    api_latency_ms: int
    endpoint_type: str = "kline"


@dataclass(frozen=True)
class BinanceSmartMoneyRecord:
    normalized: BinanceNormalizedSignal
    direction: str | None
    smart_money_count: int | None
    exit_rate: int | None
    max_gain: Decimal | None
