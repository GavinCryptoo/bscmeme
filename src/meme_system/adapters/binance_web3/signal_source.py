"""Meme Rush read-only source with bootstrap and in-memory deduplication."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from collections import Counter, deque

from meme_system.adapters.binance_web3.client import BinanceWeb3Client
from meme_system.adapters.binance_web3.errors import BinanceWeb3Error, ErrorContext
from meme_system.adapters.binance_web3.models import BinanceNormalizedSignal, ObservedField
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
        rank_types: Sequence[int] | None = None,
        limit: int = 40,
        tracker: BootstrapTracker | None = None,
    ) -> None:
        if chain_id not in {"CT_501", "56"}:
            raise BinanceWeb3Error(
                "only Solana CT_501 and BSC 56 are enabled in this phase",
                context=ErrorContext("binance_unsupported_chain", "meme_rush"),
            )
        requested_rank_types = tuple(rank_types) if rank_types is not None else (rank_type,)
        if not requested_rank_types or any(item not in {10, 20, 30} for item in requested_rank_types):
            raise ValueError("rank_types must contain only 10, 20, or 30")
        if not 1 <= limit <= 200:
            raise ValueError("limit must be between 1 and 200")
        self.client = client
        self.chain_id = chain_id
        self.rank_types = tuple(dict.fromkeys(int(item) for item in requested_rank_types))
        self.rank_type = self.rank_types[0]
        self.limit = limit
        self.tracker = tracker or BootstrapTracker()
        self._latest_by_mint: dict[str, BinanceNormalizedSignal] = {}
        self._latest_rank_by_mint: dict[str, int] = {}
        self._event_times: deque[tuple[datetime, str, str]] = deque(maxlen=100000)
        self._last_payload_by_key: dict[tuple[int, str], str] = {}
        self._last_stats: dict[str, object] = {}

    @staticmethod
    def _lifecycle(rank_type: int) -> str:
        return {10: "MEME_NEW", 20: "MEME_FINALIZING", 30: "MEME_MIGRATED"}[rank_type]

    def fetch_once(self) -> tuple[BinanceNormalizedSignal, ...]:
        fetched_at = datetime.now(timezone.utc)
        records: list[BinanceNormalizedSignal] = []
        fetched_by_lifecycle: Counter[str] = Counter()
        emitted_by_lifecycle: Counter[str] = Counter()
        for rank_type in self.rank_types:
            response = self.client.request_json(
                "meme_rush",
                body={"chainId": self.chain_id, "rankType": rank_type, "limit": self.limit},
            )
            lifecycle = self._lifecycle(rank_type)
            for row in _rows(response.payload, "meme_rush"):
                fetched_by_lifecycle[lifecycle] += 1
                normalized = normalize_meme_row(
                    row,
                    fetched_at=fetched_at,
                    historical_bootstrap=False,
                    chain_id=self.chain_id,
                    rank_type=rank_type,
                    lifecycle=lifecycle,
                )
                mint = normalized.signal.mint.lower()
                current_rank = self._latest_rank_by_mint.get(mint)
                # If a token appears in more than one list in one cycle, the
                # later lifecycle wins without creating a second Survivor row.
                if current_rank is None or rank_type >= current_rank:
                    self._latest_by_mint[mint] = normalized
                    self._latest_rank_by_mint[mint] = rank_type
                # Deduplicate only the exact consecutive payload for this
                # contract within this lifecycle. A later changed snapshot is
                # a new event, even if its upstream id was reused or an older
                # payload reappears after a state transition.
                dedup_key = (rank_type, mint)
                is_new = self._last_payload_by_key.get(dedup_key) != normalized.signal.signal_id
                self._last_payload_by_key[dedup_key] = normalized.signal.signal_id
                historical = not self.tracker.bootstrap_complete
                if not is_new:
                    continue
                emitted_by_lifecycle[lifecycle] += 1
                self._event_times.append((fetched_at, lifecycle, mint))
                records.append(replace(normalized, historical_bootstrap=historical))
        self.tracker.complete()
        cutoff = fetched_at.timestamp() - 3600
        while self._event_times and self._event_times[0][0].timestamp() < cutoff:
            self._event_times.popleft()
        last_hour_events = Counter(item[1] for item in self._event_times)
        last_hour_tokens = {lifecycle: len({item[2] for item in self._event_times if item[1] == lifecycle}) for lifecycle in ("MEME_NEW", "MEME_FINALIZING", "MEME_MIGRATED")}
        self._last_stats = {
            "source": "binance_web3:meme_rush",
            "rank_types": list(self.rank_types),
            "fetched_by_lifecycle": dict(fetched_by_lifecycle),
            "emitted_by_lifecycle": dict(emitted_by_lifecycle),
            "last_hour_event_counts": dict(last_hour_events),
            "last_hour_unique_tokens_by_lifecycle": last_hour_tokens,
            "last_fetched": len(records),
            "latest_unique_tokens": len(self._latest_by_mint),
        }
        return tuple(records)

    def latest_records(self) -> tuple[BinanceNormalizedSignal, ...]:
        """Return the latest bounded Meme Rush snapshot without another request."""

        return tuple(self._latest_by_mint.values())

    def signals(self) -> tuple[BinanceNormalizedSignal, ...]:
        """Protocol-friendly alias for one bounded polling cycle."""

        return self.fetch_once()

    def stats(self) -> dict[str, object]:
        return dict(self._last_stats)


class BscPaperCandidateMirrorSource:
    """Read only new BSC Paper candidates as Live's canonical Binance feed.

    Paper remains the sole process which polls Binance Meme Rush.  Live starts
    from the Paper cursor that exists at its own boot, then consumes only later
    Paper candidate rows.  No Paper position, execution, or wallet data is
    read or copied into Live.
    """

    def __init__(self, paper_db: Path, *, limit: int = 200) -> None:
        self.paper_db = Path(paper_db)
        self.limit = max(1, min(500, int(limit)))
        self._cursor = self._latest_rowid()
        self.last_fetched_count = 0
        self.total_fetched_count = 0

    def fetch_once(self) -> tuple[BinanceNormalizedSignal, ...]:
        try:
            connection = self._connect()
            try:
                rows = connection.execute(
                    "SELECT c.rowid AS candidate_rowid, c.candidate_id, c.mint, c.status, "
                    "c.soft_features_json, COALESCE(v.token_name, '') AS token_name "
                    "FROM candidates AS c "
                    "LEFT JOIN virtual_positions AS v ON v.position_id = c.candidate_id || ':position' "
                    "WHERE c.mode = 'paper' AND c.rowid > ? "
                    "ORDER BY c.rowid ASC LIMIT ?",
                    (self._cursor, self.limit),
                ).fetchall()
            finally:
                connection.close()
        except sqlite3.Error as exc:
            raise BinanceWeb3Error(
                "BSC Paper candidate feed is unavailable",
                context=ErrorContext("paper_candidate_feed_unavailable", "meme_rush"),
            ) from exc

        records: list[BinanceNormalizedSignal] = []
        for row in rows:
            self._cursor = int(row["candidate_rowid"])
            record = self._normalize_row(row)
            if record is not None:
                records.append(record)
        self.last_fetched_count = len(records)
        self.total_fetched_count += self.last_fetched_count
        return tuple(records)

    def stats(self) -> dict[str, int | str]:
        return {
            "source": "paper_candidate_mirror",
            "last_fetched": self.last_fetched_count,
            "total_fetched": self.total_fetched_count,
            "cursor": self._cursor,
        }

    def _latest_rowid(self) -> int:
        try:
            connection = self._connect()
            try:
                row = connection.execute(
                    "SELECT COALESCE(MAX(rowid), 0) FROM candidates WHERE mode = 'paper'"
                ).fetchone()
            finally:
                connection.close()
        except sqlite3.Error as exc:
            raise BinanceWeb3Error(
                "BSC Paper candidate feed is unavailable",
                context=ErrorContext("paper_candidate_feed_unavailable", "meme_rush"),
            ) from exc
        return int(row[0]) if row is not None else 0

    def _connect(self) -> sqlite3.Connection:
        if not self.paper_db.is_file():
            raise sqlite3.OperationalError("paper candidate database is missing")
        connection = sqlite3.connect(
            f"file:{self.paper_db.resolve()}?mode=ro",
            uri=True,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=3000")
        return connection

    def _normalize_row(self, row: sqlite3.Row) -> BinanceNormalizedSignal | None:
        try:
            soft = json.loads(row["soft_features_json"] or "{}")
        except (TypeError, ValueError):
            return None
        if not isinstance(soft, Mapping):
            return None
        mint = str(row["mint"] or "").strip()
        candidate_id = str(row["candidate_id"] or "").strip()
        if not mint or not candidate_id:
            return None
        raw: dict[str, object] = {
            "id": f"paper-candidate:{candidate_id}",
            "contractAddress": mint,
        }
        field_map = {
            "pair_address": "pairAddress",
            "bonding_curve_address": "bondingCurveAddress",
            "protocol": "protocol",
            "token_version": "tokenVersion",
            "token_decimals": "decimals",
            "price_usd": "price",
            "market_cap_usd": "marketCap",
            "liquidity_usd": "liquidity",
            "holders": "holders",
            "progress_pct": "progress",
            "migrate_status": "migrateStatus",
        }
        for source_name, target_name in field_map.items():
            value = soft.get(source_name)
            if value is not None:
                raw[target_name] = value
        token_name = str(row["token_name"] or "").strip()
        if token_name:
            raw["name"] = token_name
            raw["symbol"] = token_name
        now = datetime.now(timezone.utc)
        normalized = normalize_meme_row(
            raw,
            fetched_at=now,
            historical_bootstrap=False,
            chain_id="56",
        )
        status = str(row["status"] or "REJECTED")
        return replace(
            normalized,
            endpoint_type="paper_candidate_mirror",
            fields={
                **normalized.fields,
                "paper_candidate_status": ObservedField(
                    value=status,
                    source="paper_candidate_mirror",
                    source_field="candidates.status",
                    observed_at=now,
                    source_timestamp=None,
                    age_ms=0,
                    available=True,
                ),
            },
        )
