"""Small SQLite writer for isolated runtime state and observability facts."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Mapping


def utc_now_iso() -> str:
    """Return the current UTC time in ISO-8601 form for SQLite records."""
    return datetime.now(timezone.utc).isoformat()


class RuntimeStore:
    """Small owner-thread writer for runtime state and observability facts."""

    def __init__(self, connection: sqlite3.Connection, mode: str) -> None:
        """Bind the store to a connection and one Paper/Shadow/Live mode."""
        if mode not in {"paper", "shadow", "live"}:
            raise ValueError("mode must be paper, shadow or live")
        self.connection = connection
        self.mode = mode

    def set_state(self, key: str, value: object) -> None:
        """Upsert one JSON-serializable runtime state value."""
        self.connection.execute(
            "INSERT INTO runtime_state(mode, state_key, value_json, updated_at) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(mode, state_key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at",
            (self.mode, key, json.dumps(value, ensure_ascii=False, sort_keys=True, default=str), utc_now_iso()),
        )
        self.connection.commit()

    def record_health(
        self,
        component: str,
        state: str,
        *,
        error_class: str | None = None,
        latency_ms: int | None = None,
        details: Mapping[str, object] | None = None,
    ) -> None:
        """Append a component health observation with optional diagnostics."""
        self.connection.execute(
            "INSERT INTO health_events(mode, component, state, error_class, latency_ms, details_json, recorded_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                self.mode,
                component,
                state,
                error_class,
                latency_ms,
                json.dumps(dict(details or {}), ensure_ascii=False, sort_keys=True, default=str),
                utc_now_iso(),
            ),
        )
        self.connection.commit()

    def record_latency(self, stage: str, milliseconds: float) -> None:
        """Append one measured runtime stage latency."""
        self.connection.execute(
            "INSERT INTO latency_events(mode, stage, latency_ms, recorded_at) VALUES (?, ?, ?, ?)",
            (self.mode, stage, float(milliseconds), utc_now_iso()),
        )
        self.connection.commit()

    def latest_health(self, limit: int = 100) -> tuple[dict[str, object], ...]:
        """Return newest health observations for the configured mode."""
        rows = self.connection.execute(
            "SELECT * FROM health_events WHERE mode = ? ORDER BY recorded_at DESC, health_id DESC LIMIT ?",
            (self.mode, max(1, min(1000, int(limit)))),
        )
        return tuple(dict(row) for row in rows)
