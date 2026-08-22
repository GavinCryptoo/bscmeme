"""Stable read-only query contracts for the future Dashboard."""

from __future__ import annotations

import json
import sqlite3
from decimal import Decimal, InvalidOperation
from typing import Any

from meme_system.domain.naming import clean_token_name


def _decode_json(value: str | None) -> Any:
    """Decode persisted JSON while preserving malformed legacy text."""
    if value is None:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _unit_price(numerator: object, denominator: object) -> str | None:
    """Return a precise string price while preserving unavailable values as null."""
    if numerator in (None, "") or denominator in (None, "", "0"):
        return None
    try:
        return str(Decimal(str(numerator)) / Decimal(str(denominator)))
    except (InvalidOperation, TypeError, ValueError, ZeroDivisionError):
        return None


def _percentage(numerator: object, denominator: object) -> str | None:
    """Return a precise percentage string or null for unavailable values."""
    value = _unit_price(numerator, denominator)
    if value is None:
        return None
    try:
        return str(Decimal(value) * Decimal("100"))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _price_source(quote_id: object) -> str | None:
    """Expose the current quote venue; old values remain readable."""

    if not isinstance(quote_id, str):
        return None
    if quote_id.startswith("bsc-pool-wss:"):
        return "bsc_pool_wss"
    if quote_id.startswith("binance-indicative:"):
        return "binance_indicative_fallback"
    if quote_id.startswith("bsc-quote:pancakeswap:"):
        return "pancakeswap_router"
    if quote_id.startswith("bsc-quote:bonding_curve:"):
        return "bonding_curve"
    return None


def _price_source_label(quote_source: object, pricing_mode: object, legacy: object) -> str | None:
    """Use stable UI labels without changing the persisted venue name."""

    if bool(legacy) or pricing_mode == "legacy_binance_indicative":
        return "binance_indicative_reference"
    if quote_source == "bonding_curve":
        return "bonding_curve_quote"
    if quote_source == "pancakeswap_router":
        return "pancakeswap_quote"
    return None


def _price_delta_pct(local_price: object, jupiter_price: object) -> str | None:
    """Compare two prices without treating missing data as zero."""
    if local_price in (None, "", "0") or jupiter_price in (None, ""):
        return None
    try:
        local = Decimal(str(local_price))
        jupiter = Decimal(str(jupiter_price))
        if local <= 0:
            return None
        return str((jupiter / local - Decimal("1")) * Decimal("100"))
    except (InvalidOperation, TypeError, ValueError, ZeroDivisionError):
        return None


class LedgerQueries:
    """Read-only, newest-first views over one Paper or Shadow database."""

    def __init__(
        self,
        connection: sqlite3.Connection,
        mode: str,
        display_since: str | None = None,
    ) -> None:
        """Bind read-only views to one Paper or Shadow connection."""
        if mode not in {"paper", "shadow"}:
            raise ValueError("mode must be paper or shadow")
        self.connection = connection
        self.mode = mode
        self.display_since = display_since

    def _rows(self, sql: str, params: tuple[object, ...] = ()) -> tuple[dict[str, object], ...]:
        """Execute a read-only query and convert rows to plain dictionaries."""
        return tuple(dict(row) for row in self.connection.execute(sql, params))

    def signals(self, limit: int = 100) -> tuple[dict[str, object], ...]:
        """Return newest normalized discovery signals."""
        self._validate_limit(limit)
        where = ""
        params: list[object] = [self.mode]
        if self.display_since is not None:
            where = " WHERE s.observed_at >= ?"
            params.append(self.display_since)
        params.append(limit)
        return self._rows(
            "SELECT s.* FROM signals s "
            "JOIN candidates c ON c.signal_id = s.signal_id AND c.mode = ? "
            + where
            + " GROUP BY s.signal_id ORDER BY s.observed_at DESC, s.rowid DESC LIMIT ?",
            tuple(params),
        )

    def candidates(self, limit: int = 100) -> tuple[dict[str, object], ...]:
        """Return newest candidates with decoded rule checks and features."""
        self._validate_limit(limit)
        where = "WHERE c.mode = ?"
        params: list[object] = [self.mode]
        if self.display_since is not None:
            where += " AND s.observed_at >= ?"
            params.append(self.display_since)
        params.append(limit)
        rows = self._rows(
            "SELECT c.*, s.observed_at AS signal_observed_at "
            "FROM candidates c JOIN signals s ON s.signal_id = c.signal_id "
            + where
            + " ORDER BY s.observed_at DESC, c.rowid DESC LIMIT ?",
            tuple(params),
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
        """Return newest positions, optionally restricted to one status."""
        self._validate_limit(limit)
        where = "WHERE vp.mode = ?"
        params: list[object] = [self.mode]
        if self.display_since is not None:
            where += " AND vp.opened_at >= ?"
            params.append(self.display_since)
        if status is not None:
            where += " AND vp.status = ?"
            params.append(status)
        rows = list(self._rows(
            "SELECT vp.*, "
            "COALESCE("
            "(SELECT c.soft_features_json FROM candidates c "
            " WHERE c.mode = vp.mode "
            " AND c.candidate_id = substr(vp.position_id, 1, length(vp.position_id) - length(':position')) "
            " LIMIT 1), "
            "(SELECT c.soft_features_json FROM candidates c "
            " JOIN signals s ON s.signal_id = c.signal_id "
            " WHERE c.mode = vp.mode AND c.mint = vp.mint "
            " AND s.observed_at <= vp.opened_at "
            " ORDER BY s.observed_at DESC, c.rowid DESC LIMIT 1)"
            ") AS position_soft_features_json "
            "FROM virtual_positions vp "
            + where
            + " ORDER BY COALESCE(vp.last_observed_at, vp.opened_at) DESC, vp.rowid DESC LIMIT ?",
            (*params, limit),
        ))
        for row in rows:
            row["entry_price_snapshot"] = _decode_json(row.pop("entry_price_snapshot_json", None))
            row["exit_price_snapshot"] = _decode_json(row.pop("exit_price_snapshot_json", None))
            row["price_snapshot_version"] = int(row.get("price_snapshot_version") or 0)
            row["display_name"] = row.get("display_name") or clean_token_name(row.get("token_name"))
            row["price_snapshot_status"] = (
                "atomic" if row["price_snapshot_version"] >= 1 else "legacy_incomplete"
            )
            soft_features = _decode_json(row.pop("position_soft_features_json", None))
            if isinstance(soft_features, dict):
                for name in (
                    "price_usd",
                    "holders",
                    "market_cap_usd",
                    "liquidity_usd",
                    "quote_queue_wait_ms",
                    "quote_queue_started_at",
                ):
                    row[name] = soft_features.get(name)
                if row.get("entry_holders") is None:
                    row["entry_holders"] = soft_features.get("holders")
            else:
                if row.get("entry_holders") is None:
                    row["entry_holders"] = None
            row["price_source"] = _price_source(row.get("last_quote_id"))
            try:
                entry_holders = int(row["entry_holders"]) if row.get("entry_holders") is not None else None
                current_holders = int(row["current_holders"]) if row.get("current_holders") is not None else None
                row["holders_change"] = current_holders - entry_holders if current_holders is not None and entry_holders is not None else None
                row["holders_change_pct"] = (row["holders_change"] / entry_holders * 100) if entry_holders else None
            except (TypeError, ValueError):
                row["holders_change"] = row["holders_change_pct"] = None
            row["price_delta_pct"] = _price_delta_pct(
                row.get("local_price_sol_per_token"),
                row.get("jupiter_price_sol_per_token"),
            )
        if status == "CLOSED":
            self._attach_closed_trade_details(rows)
        return rows
    def _attach_closed_trade_details(self, rows: list[dict[str, object]]) -> None:
        """Attach read-only trade details and time-bounded market snapshots."""
        for row in rows:
            entry = self.connection.execute(
                "SELECT quote_input_quantity, quote_output_quantity, quote_quoted_at, recorded_at, price_snapshot_json, "
                "quote_source, quote_route, pricing_mode, legacy_valuation "
                "FROM executions WHERE mode = ? AND position_id = ? AND action = 'entry' "
                "ORDER BY recorded_at ASC, rowid ASC LIMIT 1",
                (self.mode, row["position_id"]),
            ).fetchone()
            exit_row = self.connection.execute(
                "SELECT quote_input_quantity, quote_output_quantity, "
                "net_pnl_estimated_sol, quote_quoted_at, recorded_at, price_snapshot_json, "
                "quote_source, quote_route, pricing_mode, legacy_valuation, pnl_status "
                "FROM executions WHERE mode = ? AND position_id = ? AND action = 'exit' "
                "ORDER BY recorded_at DESC, rowid DESC LIMIT 1",
                (self.mode, row["position_id"]),
            ).fetchone()
            entry_price_snapshot = row.get("entry_price_snapshot")
            if not isinstance(entry_price_snapshot, dict) and entry is not None:
                entry_price_snapshot = _decode_json(entry["price_snapshot_json"])
            exit_price_snapshot = row.get("exit_price_snapshot")
            if not isinstance(exit_price_snapshot, dict) and exit_row is not None:
                exit_price_snapshot = _decode_json(exit_row["price_snapshot_json"])
            row["entry_price_snapshot"] = entry_price_snapshot if isinstance(entry_price_snapshot, dict) else None
            row["exit_price_snapshot"] = exit_price_snapshot if isinstance(exit_price_snapshot, dict) else None
            row["buy_price_sol"] = _unit_price(
                entry["quote_input_quantity"], entry["quote_output_quantity"]
                if entry is not None
                else None,
            ) if entry is not None else None
            row["sell_price_sol"] = _unit_price(
                exit_row["quote_output_quantity"], exit_row["quote_input_quantity"]
                if exit_row is not None
                else None,
            ) if exit_row is not None else None
            entry_quote_at = (
                (entry_price_snapshot or {}).get("quoted_at")
                if isinstance(entry_price_snapshot, dict)
                else row.get("entry_quote_at")
            ) or (entry["quote_quoted_at"] if entry is not None else None)
            exit_quote_at = (
                (exit_price_snapshot or {}).get("quoted_at")
                if isinstance(exit_price_snapshot, dict)
                else row.get("exit_quote_at")
            ) or (exit_row["quote_quoted_at"] if exit_row is not None else None)
            row["entry_quote_at"] = entry_quote_at
            row["exit_quote_at"] = exit_quote_at
            row["buy_time"] = entry_quote_at
            row["sell_time"] = exit_quote_at
            row["entry_time_status"] = "atomic_snapshot" if isinstance(entry_price_snapshot, dict) else ("quote_quoted_at" if entry_quote_at else "unknown")
            row["exit_time_status"] = "atomic_snapshot" if isinstance(exit_price_snapshot, dict) else ("quote_quoted_at" if exit_quote_at else "unknown")
            if not row.get("signal_observed_at"):
                signal_row = self.connection.execute(
                    "SELECT s.observed_at FROM candidates c "
                    "JOIN signals s ON s.signal_id = c.signal_id "
                    "WHERE c.mode = ? AND c.candidate_id = ? LIMIT 1",
                    (
                        self.mode,
                        str(row["position_id"]).removesuffix(":position"),
                    ),
                ).fetchone()
                row["signal_observed_at"] = (
                    signal_row["observed_at"] if signal_row is not None else None
                )
            row["exit_reason"] = row.get("closed_reason")
            pnl_sol = exit_row["net_pnl_estimated_sol"] if exit_row is not None else None
            row["pnl_sol"] = pnl_sol
            row["pnl_rate_pct"] = _percentage(pnl_sol, row.get("quantity_sol"))
            source_row = exit_row or entry
            if source_row is not None:
                row["quote_source"] = source_row["quote_source"]
                row["quote_route"] = _decode_json(source_row["quote_route"])
                row["pricing_mode"] = source_row["pricing_mode"]
                row["legacy_valuation"] = bool(source_row["legacy_valuation"])
                row["pnl_status"] = exit_row["pnl_status"] if exit_row is not None else None
                row["price_source_label"] = _price_source_label(
                    row["quote_source"], row["pricing_mode"], row["legacy_valuation"]
                )
            if row.get("price_snapshot_version", 0) < 1:
                row["price_snapshot_status"] = "legacy_incomplete"
            elif not isinstance(entry_price_snapshot, dict) or not isinstance(exit_price_snapshot, dict):
                row["price_snapshot_status"] = "incomplete"
            else:
                row["price_snapshot_status"] = "atomic"
            self._attach_market_snapshots(row, entry, exit_row)

    def _attach_market_snapshots(
        self,
        row: dict[str, object],
        entry_execution: sqlite3.Row | None,
        exit_execution: sqlite3.Row | None,
    ) -> None:
        """Attach snapshots without substituting a current or post-close value."""
        position_id = str(row["position_id"])
        entry_candidate_id = (
            position_id[:-len(":position")]
            if position_id.endswith(":position")
            else None
        )
        entry_snapshot = None
        if entry_candidate_id is not None:
            entry_snapshot = self.connection.execute(
                "SELECT c.candidate_id, c.soft_features_json, s.observed_at "
                "FROM candidates c JOIN signals s ON s.signal_id = c.signal_id "
                "WHERE c.mode = ? AND c.candidate_id = ? LIMIT 1",
                (self.mode, entry_candidate_id),
            ).fetchone()

        opened_at = row.get("opened_at")
        if entry_snapshot is None and opened_at is not None:
            entry_snapshot = self.connection.execute(
                "SELECT c.candidate_id, c.soft_features_json, s.observed_at "
                "FROM candidates c JOIN signals s ON s.signal_id = c.signal_id "
                "WHERE c.mode = ? AND c.mint = ? AND s.observed_at <= ? "
                "ORDER BY s.observed_at DESC, c.rowid DESC LIMIT 1",
                (self.mode, row["mint"], opened_at),
            ).fetchone()

        entry_features = _decode_json(
            entry_snapshot["soft_features_json"] if entry_snapshot is not None else None
        )
        row["entry_market_cap_usd"] = (
            entry_features.get("market_cap_usd")
            if isinstance(entry_features, dict)
            else None
        )
        row["entry_liquidity_usd"] = (
            entry_features.get("liquidity_usd")
            if isinstance(entry_features, dict)
            else None
        )

        # Exit values are read only from the persisted close-time snapshot.
        # Never infer them from the latest candidate or the current market.
        row["exit_market_cap_usd"] = row.get("exit_market_cap_usd")
        row["exit_liquidity_usd"] = row.get("exit_liquidity_usd")

    def executions(self, limit: int = 100) -> tuple[dict[str, object], ...]:
        """Return newest persisted quote and execution records."""
        self._validate_limit(limit)
        where = "WHERE e.mode = ?"
        params: list[object] = [self.mode]
        if self.display_since is not None:
            where += " AND p.opened_at >= ?"
            params.append(self.display_since)
        params.append(limit)
        return self._rows(
            "SELECT e.* FROM executions e "
            "JOIN virtual_positions p ON p.position_id = e.position_id "
            + where
            + " ORDER BY e.recorded_at DESC, e.rowid DESC LIMIT ?",
            tuple(params),
        )

    def shadow_outcomes(self, limit: int = 100) -> tuple[dict[str, object], ...]:
        """Return newest Shadow-only post-exit outcome records."""
        if self.mode != "shadow":
            raise ValueError("shadow_outcomes is only available for shadow mode")
        self._validate_limit(limit)
        where = "WHERE p.mode = ?"
        params: list[object] = [self.mode]
        if self.display_since is not None:
            where += " AND p.opened_at >= ?"
            params.append(self.display_since)
        params.append(limit)
        rows = self._rows(
            "SELECT so.* FROM shadow_outcomes so "
            "JOIN virtual_positions p ON p.position_id = so.position_id "
            + where
            + " ORDER BY so.recorded_at DESC, so.rowid DESC LIMIT ?",
            tuple(params),
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
        """Return newest lifecycle events, optionally for one position."""
        self._validate_limit(limit)
        where = "WHERE le.mode = ?"
        params: list[object] = [self.mode]
        if self.display_since is not None:
            where += " AND vp.opened_at >= ?"
            params.append(self.display_since)
        if position_id is not None:
            where += " AND le.position_id = ?"
            params.append(position_id)
        rows = self._rows(
            "SELECT le.* FROM lifecycle_events le "
            "JOIN virtual_positions vp ON vp.position_id = le.position_id "
            + where
            + " ORDER BY le.occurred_at DESC, le.rowid DESC LIMIT ?",
            (*params, limit),
        )
        decoded: list[dict[str, object]] = []
        for row in rows:
            row["payload"] = _decode_json(row.pop("payload_json", None))
            decoded.append(row)
        return tuple(decoded)

    def runtime_state(self) -> tuple[dict[str, object], ...]:
        """Return current JSON runtime state values for this mode."""
        return self._rows(
            "SELECT * FROM runtime_state WHERE mode = ? ORDER BY updated_at DESC",
            (self.mode,),
        )

    def health_events(self, limit: int = 100) -> tuple[dict[str, object], ...]:
        """Return newest component health events with decoded details."""
        self._validate_limit(limit)
        rows = self._rows(
            "SELECT * FROM health_events WHERE mode = ? ORDER BY recorded_at DESC, health_id DESC LIMIT ?",
            (self.mode, limit),
        )
        for row in rows:
            row["details"] = _decode_json(row.pop("details_json", None))
        return rows

    def latency_events(self, limit: int = 100) -> tuple[dict[str, object], ...]:
        """Return newest measured stage latency events."""
        self._validate_limit(limit)
        return self._rows(
            "SELECT * FROM latency_events WHERE mode = ? ORDER BY recorded_at DESC, latency_id DESC LIMIT ?",
            (self.mode, limit),
        )

    @staticmethod
    def _validate_limit(limit: int) -> None:
        """Enforce the bounded result size used by Dashboard-facing queries."""
        if not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ValueError("limit must be an integer between 1 and 1000")
