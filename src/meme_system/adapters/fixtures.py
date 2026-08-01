"""Deterministic Stage A fixture/replay adapters."""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Iterator

from meme_system.adapters.protocols import ExecutableQuote
from meme_system.domain.models import Signal


class FixtureSignalSource:
    def __init__(self, signals: Iterable[Signal] = ()) -> None:
        self._signals = tuple(signals)

    def signals(self) -> tuple[Signal, ...]:
        return self._signals


class ReplaySignalSource:
    """Reads only a prevalidated local JSONL fixture; no network access."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def signals(self) -> Iterator[Signal]:
        with self.path.open(encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                row = json.loads(line)
                yield Signal(
                    signal_id=str(row["signal_id"]),
                    mint=str(row["mint"]),
                    observed_at=datetime.fromisoformat(row["observed_at"]),
                    source=str(row["source"]),
                )


class FixtureMarketDataAdapter:
    def __init__(self, snapshots: dict[str, object] | None = None) -> None:
        self._snapshots = snapshots or {}

    def snapshot(self, mint: str) -> object | None:
        return self._snapshots.get(mint)


class ReplayQuoteProvider:
    def __init__(self, quotes: Iterable[ExecutableQuote] = ()) -> None:
        self._quotes = tuple(quotes)

    def quote(self, mint: str, side: str, input_quantity: Decimal) -> ExecutableQuote | None:
        for quote in self._quotes:
            if quote.mint == mint and quote.side == side and quote.input_quantity == input_quantity:
                return quote
        return None

