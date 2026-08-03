"""Safe persistence and historical recovery for exit holder snapshots.

This module deliberately never asks a live data source for historical rows.
Historical recovery only considers holder values already stored in SQLite
events and only when their event time is close to the recorded exit time.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class StoredHolderSnapshot:
    holders: int
    observed_at: datetime
    source: str


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _holder_value(payload: object) -> int | None:
    if not isinstance(payload, dict):
        return None
    # These are explicit observed-holder fields written by existing exit
    # audit paths.  Do not fall back to entry holders or current UI values.
    for key in ("exit_holders", "current_holders", "holders"):
        value = payload.get(key)
        if value is None or isinstance(value, bool):
            continue
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        if parsed >= 0:
            return parsed
    return None


def _event_snapshots(
    connection: sqlite3.Connection,
    table: str,
    *,
    mode: str,
    position_id: str,
    start: datetime,
    end: datetime,
) -> list[StoredHolderSnapshot]:
    if table == "lifecycle_events":
        rows = connection.execute(
            "SELECT occurred_at, payload_json FROM lifecycle_events "
            "WHERE mode = ? AND position_id = ? AND occurred_at >= ? AND occurred_at <= ?",
            (mode, position_id, start.isoformat(), end.isoformat()),
        )
    else:
        rows = connection.execute(
            "SELECT occurred_at, payload_json FROM audit_events "
            "WHERE mode = ? AND occurred_at >= ? AND occurred_at <= ?",
            (mode, start.isoformat(), end.isoformat()),
        )
    result: list[StoredHolderSnapshot] = []
    for row in rows:
        observed_at = _parse_datetime(row[0])
        if observed_at is None:
            continue
        try:
            payload: Any = json.loads(row[1] or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if table == "audit_events" and (
            not isinstance(payload, dict) or payload.get("position_id") != position_id
        ):
            continue
        holders = _holder_value(payload)
        if holders is not None:
            result.append(StoredHolderSnapshot(holders, observed_at, "database_snapshot"))
    return result


def nearest_historical_exit_holders(
    connection: sqlite3.Connection,
    *,
    mode: str,
    position_id: str,
    closed_at: datetime,
    max_age_sec: int = 10,
) -> StoredHolderSnapshot | None:
    """Return the nearest already-stored holder event around an exit."""

    window = timedelta(seconds=max(0, int(max_age_sec)))
    snapshots = []
    for table in ("audit_events", "lifecycle_events"):
        snapshots.extend(
            _event_snapshots(
                connection,
                table,
                mode=mode,
                position_id=position_id,
                start=closed_at - window,
                end=closed_at + window,
            )
        )
    if not snapshots:
        return None
    return min(snapshots, key=lambda item: abs((item.observed_at - closed_at).total_seconds()))


def backfill_historical_exit_holders(
    connection: sqlite3.Connection,
    *,
    mode: str,
    max_age_sec: int = 10,
) -> int:
    """Backfill only closed rows with a qualifying stored event snapshot."""

    if mode not in {"paper", "shadow"}:
        return 0

    rows = connection.execute(
        "SELECT position_id, closed_at FROM virtual_positions "
        "WHERE mode = ? AND status = 'CLOSED' AND exit_holders IS NULL "
        "AND (exit_holders_status IS NULL OR exit_holders_status = '') "
        "AND closed_at IS NOT NULL",
        (mode,),
    ).fetchall()
    updated = 0
    for row in rows:
        closed_at = _parse_datetime(row[1])
        if closed_at is None:
            continue
        snapshot = nearest_historical_exit_holders(
            connection,
            mode=mode,
            position_id=row[0],
            closed_at=closed_at,
            max_age_sec=max_age_sec,
        )
        if snapshot is None:
            continue
        connection.execute(
            "UPDATE virtual_positions SET exit_holders = ?, "
            "exit_holders_observed_at = ?, exit_holders_source = ?, "
            "exit_holders_status = 'completed' WHERE position_id = ? AND mode = ?",
            (
                snapshot.holders,
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


def database_path(connection: sqlite3.Connection) -> Path | None:
    """Resolve a file-backed SQLite connection for background updates."""

    try:
        row = connection.execute("PRAGMA database_list").fetchone()
    except sqlite3.Error:
        return None
    if row is None or not row[2]:
        return None
    return Path(str(row[2]))


def persist_exit_holders_update(
    path: Path,
    *,
    mode: str,
    position_id: str,
    holders: int | None,
    observed_at: datetime | None,
    source: str | None,
    status: str,
) -> None:
    """Apply one asynchronous result using a separate SQLite connection."""

    if status not in {"completed", "unavailable"}:
        raise ValueError("background exit holders status must be completed or unavailable")
    if holders is not None and (isinstance(holders, bool) or holders < 0):
        raise ValueError("exit holders must be a non-negative integer or None")
    if status == "completed" and holders is None:
        raise ValueError("completed exit holders update requires a value")
    connection = sqlite3.connect(path, timeout=5.0)
    try:
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute(
            "UPDATE virtual_positions SET exit_holders = ?, "
            "exit_holders_observed_at = ?, exit_holders_source = ?, "
            "exit_holders_status = ? WHERE position_id = ? AND mode = ? "
            "AND status = 'CLOSED' AND exit_holders_status = 'pending'",
            (
                holders,
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
