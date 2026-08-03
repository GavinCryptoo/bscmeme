"""Persisted, time-bounded market snapshots for closed Paper/Shadow rows."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class StoredMarketSnapshot:
    market_cap_usd: Decimal | None
    liquidity_usd: Decimal | None
    observed_at: datetime
    source: str


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() and parsed >= 0 else None


def _candidate_snapshots(
    connection: sqlite3.Connection,
    *,
    mode: str,
    mint: str,
    position_id: str,
    start: datetime,
    end: datetime,
) -> list[StoredMarketSnapshot]:
    entry_candidate_id = (
        position_id[:-len(":position")]
        if position_id.endswith(":position")
        else None
    )
    rows = connection.execute(
        "SELECT c.candidate_id, c.soft_features_json, s.observed_at "
        "FROM candidates c JOIN signals s ON s.signal_id = c.signal_id "
        "WHERE c.mode = ? AND c.mint = ? AND s.observed_at >= ? AND s.observed_at <= ? "
        "AND (? IS NULL OR c.candidate_id != ?) "
        "ORDER BY s.observed_at DESC, c.rowid DESC",
        (mode, mint, start.isoformat(), end.isoformat(), entry_candidate_id, entry_candidate_id),
    )
    snapshots: list[StoredMarketSnapshot] = []
    for row in rows:
        observed_at = _parse_datetime(row[2])
        if observed_at is None:
            continue
        try:
            features: Any = json.loads(row[1] or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if not isinstance(features, dict):
            continue
        market_cap = _decimal(features.get("market_cap_usd"))
        liquidity = _decimal(features.get("liquidity_usd"))
        if market_cap is None and liquidity is None:
            continue
        snapshots.append(
            StoredMarketSnapshot(
                market_cap_usd=market_cap,
                liquidity_usd=liquidity,
                observed_at=observed_at,
                source="database_candidate_snapshot",
            )
        )
    return snapshots


def nearest_historical_exit_market_snapshot(
    connection: sqlite3.Connection,
    *,
    mode: str,
    position_id: str,
    mint: str,
    closed_at: datetime,
    max_age_sec: int = 10,
) -> StoredMarketSnapshot | None:
    """Return only an already stored pre-exit snapshot within the bounded window."""

    window = timedelta(seconds=max(0, int(max_age_sec)))
    snapshots = _candidate_snapshots(
        connection,
        mode=mode,
        mint=mint,
        position_id=position_id,
        start=closed_at - window,
        end=closed_at,
    )
    if not snapshots:
        return None
    return min(snapshots, key=lambda item: abs((item.observed_at - closed_at).total_seconds()))


def backfill_historical_exit_market(
    connection: sqlite3.Connection,
    *,
    mode: str,
    max_age_sec: int = 10,
) -> int:
    """Materialize qualifying historical snapshots; never query a live source."""

    if mode not in {"paper", "shadow"}:
        return 0
    rows = connection.execute(
        "SELECT position_id, mint, closed_at FROM virtual_positions "
        "WHERE mode = ? AND status = 'CLOSED' AND closed_at IS NOT NULL "
        "AND (exit_market_status IS NULL OR exit_market_status = '')",
        (mode,),
    ).fetchall()
    updated = 0
    for row in rows:
        closed_at = _parse_datetime(row[2])
        if closed_at is None:
            continue
        snapshot = nearest_historical_exit_market_snapshot(
            connection,
            mode=mode,
            position_id=row[0],
            mint=row[1],
            closed_at=closed_at,
            max_age_sec=max_age_sec,
        )
        if snapshot is None:
            connection.execute(
                "UPDATE virtual_positions SET exit_market_status = 'unavailable' "
                "WHERE position_id = ? AND mode = ?",
                (row[0], mode),
            )
        else:
            connection.execute(
                "UPDATE virtual_positions SET exit_market_cap_usd = ?, "
                "exit_liquidity_usd = ?, exit_market_observed_at = ?, "
                "exit_market_source = ?, exit_market_status = 'completed' "
                "WHERE position_id = ? AND mode = ?",
                (
                    str(snapshot.market_cap_usd) if snapshot.market_cap_usd is not None else None,
                    str(snapshot.liquidity_usd) if snapshot.liquidity_usd is not None else None,
                    snapshot.observed_at.isoformat(),
                    snapshot.source,
                    row[0],
                    mode,
                ),
            )
        updated += 1
    if updated:
        connection.commit()
    return updated


def persist_exit_market_update(
    path: Path,
    *,
    mode: str,
    position_id: str,
    market_cap_usd: Decimal | None,
    liquidity_usd: Decimal | None,
    observed_at: datetime | None,
    source: str | None,
    status: str,
) -> None:
    """Apply one asynchronous result without blocking the close lifecycle."""

    if status not in {"completed", "unavailable"}:
        raise ValueError("background exit market status must be completed or unavailable")
    if status == "completed" and market_cap_usd is None and liquidity_usd is None:
        raise ValueError("completed exit market update requires a value")
    for name, value in (("market cap", market_cap_usd), ("liquidity", liquidity_usd)):
        if value is not None and (not value.is_finite() or value < 0):
            raise ValueError(f"exit {name} must be a non-negative finite value or None")
    connection = sqlite3.connect(path, timeout=5.0)
    try:
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute(
            "UPDATE virtual_positions SET exit_market_cap_usd = ?, "
            "exit_liquidity_usd = ?, exit_market_observed_at = ?, "
            "exit_market_source = ?, exit_market_status = ? "
            "WHERE position_id = ? AND mode = ? AND status = 'CLOSED' "
            "AND exit_market_status = 'pending'",
            (
                str(market_cap_usd) if market_cap_usd is not None else None,
                str(liquidity_usd) if liquidity_usd is not None else None,
                observed_at.isoformat() if observed_at is not None else None,
                source,
                status,
                position_id,
                mode,
            ),
        )
        connection.commit()
    finally:
        connection.close()
