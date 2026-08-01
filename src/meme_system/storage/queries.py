"""Stable read-only query contracts for the future Dashboard."""

from __future__ import annotations

import json
import sqlite3
from typing import Any


def _decode_json(value: str | None) -> Any:
    if value is None:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


class LedgerQueries:
    """Read-only, newest-first views over one Paper or Shadow database."""

    def __init__(self, connection: sqlite3.Connection, mode: str) -> None:
        if mode not in {"paper", "shadow"}:
            raise ValueError("mode must be paper or shadow")
        self.connection = connection
        self.mode = mode

    def _rows(self, sql: str, params: tuple[object, ...] = ()) -> tuple[dict[str, object], ...]:
        return tuple(dict(row) for row in self.connection.execute(sql, params))

    def signals(self, limit: int = 100) -> tuple[dict[str, object], ...]:
        self._validate_limit(limit)
        return self._rows(
            "SELECT s.* FROM signals s "
            "JOIN candidates c ON c.signal_id = s.signal_id AND c.mode = ? "
            "GROUP BY s.signal_id ORDER BY s.observed_at DESC, s.rowid DESC LIMIT ?",
            (self.mode, limit),
        )

    def candidates(self, limit: int = 100) -> tuple[dict[str, object], ...]:
        self._validate_limit(limit)
        rows = self._rows(
            "SELECT c.*, s.observed_at AS signal_observed_at "
            "FROM candidates c JOIN signals s ON s.signal_id = c.signal_id "
            "WHERE c.mode = ? ORDER BY s.observed_at DESC, c.rowid DESC LIMIT ?",
            (self.mode, limit),
        )
        decoded: list[dict[str, object]] = []
        for row in rows:
            rule_checks = _decode_json(row.pop("rule_checks_json", None))
            soft_features = _decode_json(row.pop("soft_features_json", None))
            row["rule_checks"] = rule_checks
            row["soft_features"] = soft_features
            decoded.append(row)
        return tuple(decoded)

    def positions(
        self,
        limit: int = 100,
        status: str | None = None,
    ) -> tuple[dict[str, object], ...]:
        self._validate_limit(limit)
        where = "WHERE mode = ?"
        params: list[object] = [self.mode]
        if status is not None:
            where += " AND status = ?"
            params.append(status)
        return self._rows(
            "SELECT * FROM virtual_positions "
            + where
            + " ORDER BY COALESCE(last_observed_at, opened_at) DESC, rowid DESC LIMIT ?",
            (*params, limit),
        )

    def executions(self, limit: int = 100) -> tuple[dict[str, object], ...]:
        self._validate_limit(limit)
        return self._rows(
            "SELECT * FROM executions WHERE mode = ? "
            "ORDER BY recorded_at DESC, rowid DESC LIMIT ?",
            (self.mode, limit),
        )

    def shadow_outcomes(self, limit: int = 100) -> tuple[dict[str, object], ...]:
        if self.mode != "shadow":
            raise ValueError("shadow_outcomes is only available for shadow mode")
        self._validate_limit(limit)
        rows = self._rows(
            "SELECT * FROM shadow_outcomes "
            "ORDER BY recorded_at DESC, rowid DESC LIMIT ?",
            (limit,),
        )
        decoded: list[dict[str, object]] = []
        for row in rows:
            row["returns_after_exit"] = _decode_json(
                row.pop("returns_after_exit_json", None)
            )
            decoded.append(row)
        return tuple(decoded)

    def lifecycle_events(
        self,
        limit: int = 200,
        position_id: str | None = None,
    ) -> tuple[dict[str, object], ...]:
        self._validate_limit(limit)
        where = "WHERE mode = ?"
        params: list[object] = [self.mode]
        if position_id is not None:
            where += " AND position_id = ?"
            params.append(position_id)
        rows = self._rows(
            "SELECT * FROM lifecycle_events "
            + where
            + " ORDER BY occurred_at DESC, rowid DESC LIMIT ?",
            (*params, limit),
        )
        decoded: list[dict[str, object]] = []
        for row in rows:
            row["payload"] = _decode_json(row.pop("payload_json", None))
            decoded.append(row)
        return tuple(decoded)

    @staticmethod
    def _validate_limit(limit: int) -> None:
        if not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ValueError("limit must be an integer between 1 and 1000")
