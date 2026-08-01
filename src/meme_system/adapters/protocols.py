"""Stage 0 adapter Protocols; no network implementation is included."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Iterable, Protocol

from meme_system.domain.models import Signal


@dataclass(frozen=True)
class ExecutableQuote:
    quote_id: str
    mint: str
    side: str
    input_quantity: Decimal
    output_quantity: Decimal
    route_fee: Decimal | None
    price_impact_pct: Decimal | None
    quoted_at: datetime
    age_ms: int
    expires_at: datetime | None = None
    route_available: bool = True
    liquidity_available: bool = True

    def unusable_reason(self, now: datetime) -> str | None:
        if not self.route_available:
            return "no_route"
        if not self.liquidity_available:
            return "no_liquidity"
        if self.expires_at is not None and now >= self.expires_at:
            return "quote_expired"
        return None


class SignalSource(Protocol):
    def signals(self) -> Iterable[Signal]: ...


class MarketDataAdapter(Protocol):
    def snapshot(self, mint: str) -> object: ...


class QuoteProvider(Protocol):
    def quote(self, mint: str, side: str, input_quantity: Decimal) -> ExecutableQuote | None: ...


class BscAdapterProtocol(Protocol):
    """Future interface only; Stage 0 must not connect to BSC."""

    def chain_id(self) -> int: ...
