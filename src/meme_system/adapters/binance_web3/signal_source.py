"""Meme Rush read-only source with bootstrap and in-memory deduplication."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping

from meme_system.adapters.binance_web3.client import BinanceWeb3Client
from meme_system.adapters.binance_web3.errors import BinanceWeb3Error, ErrorContext
from meme_system.adapters.binance_web3.models import BinanceNormalizedSignal
from meme_system.adapters.binance_web3.normalizer import normalize_meme_row


def _rows(payload: Mapping[str, Any], endpoint_type: str) -> list[Mapping[str, Any]]:
    if payload.get("code") != "000000":
        raise BinanceWeb3Error(
            "Binance Web3 business response was not successful",
            context=ErrorContext("binance_business_error", endpoint_type),
        )
    data = payload.get("data")
    if isinstance(data, list):
        return [row for row in data if isinstance(row, Mapping)]
    if isinstance(data, Mapping) and isinstance(data.get("list"), list):
        return [row for row in data["list"] if isinstance(row, Mapping)]
    raise BinanceWeb3Error(
        "Binance Web3 response data is not a list",
        context=ErrorContext("binance_schema_changed", endpoint_type),
    )


@dataclass
class BootstrapTracker:
    runner_started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    seen_signal_ids: set[str] = field(default_factory=set)
    bootstrap_complete: bool = False

    def mark(self, signal_id: str) -> tuple[bool, bool]:
        if signal_id in self.seen_signal_ids:
            return False, True
        self.seen_signal_ids.add(signal_id)
        historical = not self.bootstrap_complete
        return True, historical

    def complete(self) -> None:
        self.bootstrap_complete = True


class BinanceWeb3SignalSource:
    def __init__(
        self,
        client: BinanceWeb3Client,
        *,
        chain_id: str = "CT_501",
        rank_type: int = 10,
        limit: int = 40,
        tracker: BootstrapTracker | None = None,
    ) -> None:
        if chain_id not in {"CT_501", "56"}:
            raise BinanceWeb3Error(
                "only Solana CT_501 and BSC 56 are enabled in this phase",
                context=ErrorContext("binance_unsupported_chain", "meme_rush"),
            )
        if rank_type not in {10, 20, 30}:
            raise ValueError("rank_type must be 10, 20, or 30")
        if not 1 <= limit <= 200:
            raise ValueError("limit must be between 1 and 200")
        self.client = client
        self.chain_id = chain_id
        self.rank_type = rank_type
        self.limit = limit
        self.tracker = tracker or BootstrapTracker()

    def fetch_once(self) -> tuple[BinanceNormalizedSignal, ...]:
        fetched_at = datetime.now(timezone.utc)
        response = self.client.request_json(
            "meme_rush",
            body={"chainId": self.chain_id, "rankType": self.rank_type, "limit": self.limit},
        )
        records: list[BinanceNormalizedSignal] = []
        for row in _rows(response.payload, "meme_rush"):
            normalized = normalize_meme_row(
                row,
                fetched_at=fetched_at,
                historical_bootstrap=False,
                chain_id=self.chain_id,
            )
            is_new, historical = self.tracker.mark(normalized.signal.signal_id)
            if not is_new:
                continue
            records.append(
                normalize_meme_row(
                    row,
                    fetched_at=fetched_at,
                    historical_bootstrap=historical,
                    chain_id=self.chain_id,
                )
            )
        self.tracker.complete()
        return tuple(records)

    def signals(self) -> tuple[BinanceNormalizedSignal, ...]:
        """Protocol-friendly alias for one bounded polling cycle."""

        return self.fetch_once()
