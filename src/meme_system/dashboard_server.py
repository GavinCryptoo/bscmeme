"""Local read-only Dashboard HTTP service with safe Paper/Shadow controls."""

from __future__ import annotations

import json
import mimetypes
import os
import sqlite3
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import Any, Mapping
from urllib.parse import parse_qs, urlparse

from meme_system.config.dashboard import DashboardConfig
from meme_system.config.runtime import RuntimePaths
from meme_system.config.safety import SafetyConfig
from meme_system.domain.models import BSC_BASELINE_IDENTITY, BASELINE_IDENTITY, SOL_SURVIVOR_REVERSAL_IDENTITY, SURVIVOR_REVERSAL_IDENTITY
from meme_system.runtime_ops import RuntimeControl
from meme_system.strategies.baseline import bsc_baseline_config, solana_baseline_config
from meme_system.strategies.survivor_reversal import SurvivorReversalConfig
from meme_system.strategies.sol_survivor_reversal import SolSurvivorReversalConfig
from meme_system.storage.database import initialize_database
from meme_system.storage.queries import LedgerQueries


_DISPLAY_SINCE_KEY = "dashboard_display_since"
_OPTIONAL_UNAVAILABLE_REASON_CODES = frozenset(
    {
        "token_age_unavailable",
        "unique_buyers_unavailable",
        "buy_sell_ratio_unavailable",
        "net_buy_unavailable",
        "flow_window_unavailable",
        "creator_sell_unavailable",
    }
)


class DashboardService:
    def __init__(
        self,
        *,
        paths: RuntimePaths,
        config: DashboardConfig | None = None,
        safety: SafetyConfig | None = None,
        control: RuntimeControl | None = None,
    ) -> None:
        self.paths = paths
        self.config = config or DashboardConfig()
        self.config.validate()
        self.safety = safety or SafetyConfig()
        self.safety.validate()
        self.control = control or RuntimeControl(paths.control_file)
        self._chain_paths: dict[str, RuntimePaths] = {"solana": paths}

    def payload(self, path: str, query: Mapping[str, list[str]]) -> tuple[int, object]:
        chain = _chain(query)
        if path == "/api/status":
            return 200, self.status(chain)
        if path == "/api/health":
            return 200, self.health(chain)
        if path == "/api/survivor":
            return 200, self.survivor(chain, _survivor_limit(query), _strategy(query))
        if path == "/api/control":
            return 200, self._control(chain).snapshot()
        if path == "/api/config":
            return 200, self.config_payload(chain)
        if path == "/api/analytics":
            mode = _one(query, "mode", "paper")
            if mode not in {"paper", "shadow"}:
                return 400, {"error": "mode must be paper or shadow"}
            with self._connection(mode, chain) as connection:
                return 200, self.analytics(
                    connection,
                    mode,
                    chain,
                    strategy=_one(query, "strategy", None),
                    token=_one(query, "token", None),
                    window=_one(query, "window", "24h"),
                    trend_window=_one(query, "trend_window", None),
                    outcome=_one(query, "outcome", "all"),
                    display_since=self._display_since(connection, mode, chain),
                )
        if path in {
            "/api/signals",
            "/api/candidates",
            "/api/positions",
            "/api/executions",
            "/api/events",
            "/api/shadow-outcomes",
        }:
            mode = _one(query, "mode", "paper")
            if mode not in {"paper", "shadow"}:
                return 400, {"error": "mode must be paper or shadow"}
            limit = _limit(query)
            with self._connection(mode, chain) as connection:
                display_since = self._display_since(connection, mode, chain)
                queries = LedgerQueries(connection, mode, display_since=display_since)
                if path == "/api/signals":
                    rows = queries.signals(limit)
                elif path == "/api/candidates":
                    rows = queries.candidates(limit)
                elif path == "/api/positions":
                    rows = queries.positions(limit, _one(query, "status", None))
                elif path == "/api/executions":
                    rows = queries.executions(limit)
                elif path == "/api/events":
                    rows = queries.lifecycle_events(limit, _one(query, "position_id", None))
                else:
                    if mode != "shadow":
                        return 400, {"error": "shadow-outcomes requires mode=shadow"}
                    rows = queries.shadow_outcomes(limit)
                return 200, {"mode": mode, "items": self._normalize_chain_rows(rows, chain)}
        if path == "/":
            return 200, _read_static("index.html")
        if path in {"/app.js", "/styles.css"}:
            return 200, _read_static(path.lstrip("/"))
        return 404, {"error": "not_found"}

    @staticmethod
    def analytics(
        connection: sqlite3.Connection,
        mode: str,
        chain: str,
        *,
        strategy: str | None = None,
        token: str | None = None,
        window: str | None = "24h",
        trend_window: str | None = None,
        outcome: str | None = "all",
        display_since: str | None = None,
    ) -> dict[str, object]:
        """Return filterable dashboard aggregates without changing the ledger schema."""
        if mode not in {"paper", "shadow"}:
            raise ValueError("mode must be paper or shadow")
        if chain not in {"solana", "bsc"}:
            raise ValueError("chain must be solana or bsc")
        window_value = window if window in {"24h", "7d", "all"} else "24h"
        trend_window_value = trend_window if trend_window in {"24h", "3d", "7d", "1m", "all"} else "24h"
        outcome_value = outcome if outcome in {"all", "profit", "loss"} else "all"
        now = datetime.now(timezone.utc)
        window_cutoff = {
            "24h": now - timedelta(hours=24),
            "7d": now - timedelta(days=7),
            "all": None,
        }[window_value]
        display_cutoff = None
        if display_since:
            display_cutoff = parsed_display_since = None
            try:
                parsed_display_since = datetime.fromisoformat(display_since.replace("Z", "+00:00"))
                if parsed_display_since.tzinfo is None:
                    parsed_display_since = parsed_display_since.replace(tzinfo=timezone.utc)
                display_cutoff = parsed_display_since.astimezone(timezone.utc)
            except ValueError:
                display_cutoff = None
        if display_cutoff and (window_cutoff is None or display_cutoff > window_cutoff):
            window_cutoff = display_cutoff

        def parsed_time(value: object) -> datetime | None:
            if not isinstance(value, str) or not value:
                return None
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)

        def in_window(value: object) -> bool:
            parsed = parsed_time(value)
            return parsed is not None and (window_cutoff is None or parsed >= window_cutoff)

        def token_matches(*values: object) -> bool:
            needle = (token or "").strip().lower()
            if not needle:
                return True
            return any(needle in str(value or "").lower() for value in values)

        candidate_rows = connection.execute(
            "SELECT c.strategy_name, c.status, c.filter_reason, c.mint, s.observed_at "
            "FROM candidates c JOIN signals s ON s.signal_id = c.signal_id "
            "WHERE c.mode = ? ORDER BY s.observed_at DESC, c.rowid DESC",
            (mode,),
        ).fetchall()
        candidates: list[sqlite3.Row] = []
        for row in candidate_rows:
            if strategy and strategy != "all" and row["strategy_name"] != strategy:
                continue
            if not token_matches(row["mint"]):
                continue
            if not in_window(row["observed_at"]):
                continue
            candidates.append(row)

        position_rows = connection.execute(
            "SELECT position_id, strategy_name, mint, token_name, mode, status, quantity_sol, "
            "opened_at, closed_at, closed_reason FROM virtual_positions WHERE mode = ? "
            "ORDER BY COALESCE(closed_at, opened_at) DESC, rowid DESC",
            (mode,),
        ).fetchall()
        positions: list[sqlite3.Row] = []
        for row in position_rows:
            if strategy and strategy != "all" and row["strategy_name"] != strategy:
                continue
            if not token_matches(row["mint"], row["token_name"]):
                continue
            if not in_window(row["closed_at"] or row["opened_at"]):
                continue
            positions.append(row)

        legacy_filter = " AND COALESCE(e.legacy_valuation, 0) = 0" if chain == "bsc" else ""
        exit_rows = connection.execute(
            "SELECT e.position_id, e.reason, e.net_pnl_estimated_sol, e.recorded_at, "
            "p.strategy_name, p.mint, p.token_name, p.quantity_sol, p.closed_reason "
            "FROM executions e JOIN virtual_positions p ON p.position_id = e.position_id "
            "WHERE e.mode = ? AND e.action = 'exit' AND e.net_pnl_estimated_sol IS NOT NULL"
            + legacy_filter
            + " "
            "ORDER BY e.recorded_at DESC, e.rowid DESC",
            (mode,),
        ).fetchall()
        latest_exit_by_position: dict[str, sqlite3.Row] = {}
        for row in exit_rows:
            if strategy and strategy != "all" and row["strategy_name"] != strategy:
                continue
            if not token_matches(row["mint"], row["token_name"]):
                continue
            if not in_window(row["recorded_at"]):
                continue
            amount = Decimal(str(row["net_pnl_estimated_sol"]))
            if outcome_value == "profit" and amount <= 0:
                continue
            if outcome_value == "loss" and amount >= 0:
                continue
            latest_exit_by_position.setdefault(str(row["position_id"]), row)
        exits = list(latest_exit_by_position.values())

        trend_latest_exit_by_position: dict[str, sqlite3.Row] = {}
        for row in exit_rows:
            if strategy and strategy != "all" and row["strategy_name"] != strategy:
                continue
            if not token_matches(row["mint"], row["token_name"]):
                continue
            timestamp = parsed_time(row["recorded_at"])
            if timestamp is None or timestamp > now or (display_cutoff and timestamp < display_cutoff):
                continue
            amount = Decimal(str(row["net_pnl_estimated_sol"]))
            if outcome_value == "profit" and amount <= 0:
                continue
            if outcome_value == "loss" and amount >= 0:
                continue
            trend_latest_exit_by_position.setdefault(str(row["position_id"]), row)
        trend_exits = list(trend_latest_exit_by_position.values())

        strategy_names = sorted({
            str(row["strategy_name"])
            for row in (*candidates, *positions, *exits)
            if row["strategy_name"]
        })
        candidate_counts = Counter(str(row["strategy_name"]) for row in candidates)
        rejected_counts = Counter(str(row["strategy_name"]) for row in candidates if row["status"] == "REJECTED")
        open_counts = Counter(str(row["strategy_name"]) for row in positions if row["status"] != "CLOSED")
        closed_counts = Counter(str(row["strategy_name"]) for row in positions if row["status"] == "CLOSED")
        pnl_by_strategy: dict[str, dict[str, object]] = defaultdict(lambda: {
            "amount": Decimal("0"),
            "invested": Decimal("0"),
            "trades": 0,
            "wins": 0,
            "losses": 0,
        })
        for row in exits:
            name = str(row["strategy_name"])
            amount = Decimal(str(row["net_pnl_estimated_sol"]))
            summary = pnl_by_strategy[name]
            summary["amount"] += amount  # type: ignore[operator]
            summary["invested"] += Decimal(str(row["quantity_sol"]))  # type: ignore[operator]
            summary["trades"] = int(summary["trades"]) + 1
            if amount > 0:
                summary["wins"] = int(summary["wins"]) + 1
            elif amount < 0:
                summary["losses"] = int(summary["losses"]) + 1

        def strategy_pnl(name: str) -> dict[str, object]:
            values = pnl_by_strategy[name]
            amount = values["amount"]
            invested = values["invested"]
            rate = amount / invested * Decimal("100") if invested else Decimal("0")  # type: ignore[operator]
            return {
                "amount_native": str(amount),
                "rate_pct": str(rate),
                "closed_trade_count": values["trades"],
                "profitable_trade_count": values["wins"],
                "losing_trade_count": values["losses"],
            }

        strategies = [
            {
                "strategy_name": name,
                "candidate_count": candidate_counts[name],
                "rejected_count": rejected_counts[name],
                "open_positions": open_counts[name],
                "closed_positions": closed_counts[name],
                "pnl": strategy_pnl(name),
            }
            for name in strategy_names
        ]
        selected_names = set(strategy_names)

        def aggregate_pnl(names: set[str]) -> dict[str, object]:
            selected = [item for item in strategies if item["strategy_name"] in names]
            amount = sum((Decimal(str(item["pnl"]["amount_native"])) for item in selected), Decimal("0"))
            invested = sum((pnl_by_strategy[str(item["strategy_name"])]["invested"] for item in selected), Decimal("0"))
            rate = amount / invested * Decimal("100") if invested else Decimal("0")
            return {
                "candidate_count": sum(int(item["candidate_count"]) for item in selected),
                "rejected_count": sum(int(item["rejected_count"]) for item in selected),
                "open_positions": sum(int(item["open_positions"]) for item in selected),
                "closed_positions": sum(int(item["closed_positions"]) for item in selected),
                "closed_trade_count": sum(int(item["pnl"]["closed_trade_count"]) for item in selected),
                "profitable_trade_count": sum(int(item["pnl"]["profitable_trade_count"]) for item in selected),
                "losing_trade_count": sum(int(item["pnl"]["losing_trade_count"]) for item in selected),
                "amount_native": str(amount),
                "rate_pct": str(rate),
            }

        selected_summary = aggregate_pnl(selected_names)

        loss_counter: Counter[str] = Counter()
        for row in exits:
            if Decimal(str(row["net_pnl_estimated_sol"])) < 0:
                reason = row["closed_reason"] or row["reason"] or "unknown"
                loss_counter[str(reason)] += 1

        rejection_counter: Counter[str] = Counter()
        unavailable_counter: Counter[str] = Counter()
        for row in candidates:
            reasons = {part.strip() for part in str(row["filter_reason"] or "unknown").split(",") if part.strip()}
            optional_unavailable = reasons & _OPTIONAL_UNAVAILABLE_REASON_CODES
            for reason in optional_unavailable:
                unavailable_counter[reason] += 1
            if row["status"] != "REJECTED":
                continue
            for reason in (reasons - _OPTIONAL_UNAVAILABLE_REASON_CODES) or {"unknown"}:
                rejection_counter[reason] += 1

        def share_rows(counter: Counter[str]) -> list[dict[str, object]]:
            total = sum(counter.values())
            if not total:
                return []
            top = counter.most_common(7)
            remainder = total - sum(count for _, count in top)
            if remainder:
                top.append(("other", remainder))
            return [
                {"reason": reason, "count": count, "share_pct": str(Decimal(count) / Decimal(total) * Decimal("100"))}
                for reason, count in top
            ]

        trend_specs = {
            "24h": ("hour", 24),
            "3d": ("hour", 72),
            "7d": ("day", 7),
            "1m": ("day", 30),
        }
        trend_timestamps = [
            timestamp
            for row in trend_exits
            if (timestamp := parsed_time(row["recorded_at"])) is not None
        ]
        if trend_window_value == "all":
            trend_end = now.replace(hour=0, minute=0, second=0, microsecond=0)
            trend_start = min(trend_timestamps, default=trend_end).replace(hour=0, minute=0, second=0, microsecond=0)
            if not trend_timestamps:
                trend_start = trend_end - timedelta(days=29)
            if trend_start > trend_end:
                trend_start = trend_end
            span_days = (trend_end - trend_start).days
            if span_days > 365:
                trend_unit = "week"
                trend_start -= timedelta(days=trend_start.weekday())
                trend_end -= timedelta(days=trend_end.weekday())
                trend_count = (trend_end - trend_start).days // 7 + 1
            else:
                trend_unit = "day"
                trend_count = span_days + 1
        else:
            trend_unit, trend_count = trend_specs[trend_window_value]
            if trend_unit == "hour":
                trend_end = now.replace(minute=0, second=0, microsecond=0)
                trend_start = trend_end - timedelta(hours=trend_count - 1)
            else:
                trend_end = now.replace(hour=0, minute=0, second=0, microsecond=0)
                trend_start = trend_end - timedelta(days=trend_count - 1)

        def trend_bucket(timestamp: datetime) -> datetime:
            if trend_unit == "hour":
                return timestamp.replace(minute=0, second=0, microsecond=0)
            day = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
            return day - timedelta(days=day.weekday()) if trend_unit == "week" else day

        trend_amounts: dict[datetime, Decimal] = defaultdict(lambda: Decimal("0"))
        for row in trend_exits:
            timestamp = parsed_time(row["recorded_at"])
            if timestamp is None or timestamp < trend_start or timestamp > now:
                continue
            bucket = trend_bucket(timestamp)
            trend_amounts[bucket] += Decimal(str(row["net_pnl_estimated_sol"]))
        cumulative = Decimal("0")
        trend: list[dict[str, object]] = []
        for offset in range(trend_count):
            if trend_unit == "hour":
                bucket = trend_start + timedelta(hours=offset)
            elif trend_unit == "week":
                bucket = trend_start + timedelta(weeks=offset)
            else:
                bucket = trend_start + timedelta(days=offset)
            amount = trend_amounts[bucket]
            cumulative += amount
            trend.append({"timestamp": bucket.isoformat(), "amount_native": str(amount), "cumulative_native": str(cumulative)})

        return {
            "chain_key": chain,
            "native_symbol": "BNB" if chain == "bsc" else "SOL",
            "mode": mode,
            "window": window_value,
            "trend_window": trend_window_value,
            "strategy": strategy if strategy and strategy != "all" else "all",
            "token": token or "",
            "outcome": outcome_value,
            "summary": selected_summary,
            "strategies": strategies,
            "loss_reasons": share_rows(loss_counter),
            "rejection_reasons": share_rows(rejection_counter),
            "unavailable_reasons": share_rows(unavailable_counter),
            "pnl_trend": trend,
        }

    @staticmethod
    def _normalize_chain_rows(rows: object, chain: str) -> object:
        """Expose chain-neutral native units without changing SQLite compatibility columns."""
        if chain != "bsc":
            return rows
        rename = {
            "quantity_sol": "quantity_bnb",
            "buy_price_sol": "buy_price_bnb",
            "sell_price_sol": "sell_price_bnb",
            "pnl_sol": "pnl_bnb",
            "gross_pnl_sol": "gross_pnl_bnb",
            "net_pnl_estimated_sol": "net_pnl_estimated_bnb",
        }
        normalized: list[dict[str, object]] = []
        for raw in rows:  # type: ignore[union-attr]
            row = dict(raw)
            if row.get("strategy_name") in {
                BASELINE_IDENTITY.strategy_name,
                BSC_BASELINE_IDENTITY.strategy_name,
            }:
                row["strategy_name"] = BSC_BASELINE_IDENTITY.strategy_name
            for legacy_name, bnb_name in rename.items():
                if legacy_name in row:
                    row[bnb_name] = row.pop(legacy_name)
            normalized.append(row)
        return normalized

    def set_control(self, payload: Mapping[str, object]) -> tuple[int, object]:
        mode = payload.get("mode")
        paused = payload.get("paused")
        if mode not in {"paper", "shadow"} or not isinstance(paused, bool):
            return 400, {"error": "expected mode=paper|shadow and boolean paused"}
        chain = str(payload.get("chain", "solana"))
        if chain not in {"solana", "bsc"}:
            return 400, {"error": "chain must be solana or bsc"}
        return 200, self._control(chain).set_paused(str(mode), paused)

    def status(self, chain: str = "solana") -> dict[str, object]:
        modes: dict[str, object] = {}
        control = self._control(chain)
        for mode in ("paper", "shadow"):
            with self._connection(mode, chain) as connection:
                display_since = self._display_since(connection, mode, chain)
                signal_where = ""
                signal_params: tuple[object, ...] = (mode,)
                candidate_where = ""
                candidate_params: tuple[object, ...] = (mode,)
                position_where = ""
                position_params: tuple[object, ...] = (mode,)
                if display_since is not None:
                    signal_where = " AND s.observed_at >= ?"
                    signal_params = (mode, display_since)
                    candidate_where = " AND s.observed_at >= ?"
                    candidate_params = (mode, display_since)
                    position_where = " AND opened_at >= ?"
                    position_params = (mode, display_since)
                counts = {
                    "signals": connection.execute(
                        "SELECT COUNT(*) FROM signals s "
                        "JOIN candidates c ON c.signal_id = s.signal_id AND c.mode = ?"
                        + signal_where,
                        signal_params,
                    ).fetchone()[0],
                    "candidates": connection.execute(
                        "SELECT COUNT(*) FROM candidates c "
                        "JOIN signals s ON s.signal_id = c.signal_id "
                        "WHERE c.mode = ?" + candidate_where,
                        candidate_params,
                    ).fetchone()[0],
                    "accepted_candidates": connection.execute(
                        "SELECT COUNT(*) FROM candidates c "
                        "JOIN signals s ON s.signal_id = c.signal_id "
                        "WHERE c.mode = ? AND c.status = 'ACCEPTED'" + candidate_where,
                        candidate_params,
                    ).fetchone()[0],
                    "rejected_candidates": connection.execute(
                        "SELECT COUNT(*) FROM candidates c "
                        "JOIN signals s ON s.signal_id = c.signal_id "
                        "WHERE c.mode = ? AND c.status = 'REJECTED'" + candidate_where,
                        candidate_params,
                    ).fetchone()[0],
                    "open_positions": connection.execute(
                        "SELECT COUNT(*) FROM virtual_positions "
                        "WHERE mode = ? AND status != 'CLOSED'" + position_where,
                        position_params,
                    ).fetchone()[0],
                    "closed_positions": connection.execute(
                        "SELECT COUNT(*) FROM virtual_positions "
                        "WHERE mode = ? AND status = 'CLOSED'" + position_where,
                        position_params,
                    ).fetchone()[0],
                    "executions": connection.execute("SELECT COUNT(*) FROM executions WHERE mode = ?", (mode,)).fetchone()[0],
                }
                if display_since is not None:
                    counts["executions"] = connection.execute(
                        "SELECT COUNT(*) FROM executions e "
                        "JOIN virtual_positions p ON p.position_id = e.position_id "
                        "WHERE e.mode = ? AND p.opened_at >= ?",
                        (mode, display_since),
                    ).fetchone()[0]
                pnl = self._pnl_summary(connection, mode, chain, display_since)
            runner_control = self._runner_control_state(chain, mode) if chain == "solana" else None
            mode_payload: dict[str, object] = {
                "counts": counts,
                "pnl": pnl,
                # Solana displays the state last applied by the actual runner,
                # never merely the desired flag persisted by the dashboard.
                "new_entries_paused": (
                    runner_control["new_entries_paused"]
                    if runner_control is not None
                    else (None if chain == "solana" else control.paused(mode))
                ),
            }
            if mode == "paper":
                mode_payload["survivor_reversal"] = self._survivor_state(connection, chain)
            if chain == "solana":
                mode_payload["runtime_control"] = runner_control or {
                    "state": "UNKNOWN",
                    "new_entries_paused": None,
                    "requested_new_entries_paused": control.paused(mode),
                }
            if display_since is not None:
                mode_payload["display_since"] = display_since
            modes[mode] = mode_payload
        return {
            "status": "ok",
            "chain": "bsc-mainnet" if chain == "bsc" else "solana-mainnet",
            "chain_key": chain,
            "chain_id": "56" if chain == "bsc" else "CT_501",
            "read_only": True,
            "pricing": {
                "pricing_mode": "bsc_executable_quote" if chain == "bsc" else "jupiter_quote",
                "executable_quote": True,
                "net_pnl_is_estimated": False if chain == "bsc" else None,
                "reference_pricing_mode": "binance_meme_rush_layer_a" if chain == "bsc" else None,
            },
            "safety": {
                "paper_only": self.safety.paper_only,
                "live_trading": self.safety.live_trading,
                "wallet_enabled": self.safety.wallet_enabled,
                "signing_enabled": self.safety.signing_enabled,
                "broadcast_enabled": self.safety.broadcast_enabled,
                "telegram_enabled": self.safety.telegram_enabled,
            },
            "modes": modes,
        }

    def survivor(self, chain: str = "bsc", limit: int = 100, strategy: str = "high") -> dict[str, object]:
        """Expose the strategy-specific state written by the Paper runner."""

        if strategy == "balanced" and chain == "bsc":
            path = Path("data/bsc-balanced/paper/runtime.db")
            if not path.exists():
                return {"status": "ok", "chain": "bsc-mainnet", "mode": "paper", "strategy": strategy, "survivor_reversal": {"state": "NOT_STARTED"}, "ledger_summary": {}, "candidates": [], "positions": []}
            connection = sqlite3.connect(path, timeout=10.0, check_same_thread=False)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only=ON")
        else:
            connection = self._connection("paper", chain)
        with connection:
            state = self._survivor_state(connection, chain, strategy)
            display_since = self._display_since(connection, "paper", chain)
            ledger_summary = self._survivor_paper_summary(
                connection,
                "SOL" if chain == "solana" else "BNB",
                display_since=display_since,
            )
            candidate_limit = max(1, min(5000, int(limit)))
            # The current-candidate module is a trading-facing view.  Do not
            # mix in raw discovery history or LIGHT_TRACKING/EXPIRED rows;
            # those records remain available through runtime diagnostics and
            # database history, but they are not current candidates.
            if chain == "solana":
                # A read-only display defense: the Paper engine is the source
                # of truth, but a stale persisted row above the SOL hard cap
                # must never reappear as a trading-facing Candidate.
                max_price = Decimal(os.environ.get("SOL_MAX_CANDIDATE_PRICE_USD", "0.0001"))
                active_candidate_rows = connection.execute(
                    "SELECT * FROM survivor_candidates "
                    "WHERE state = 'ACTIVE_CANDIDATE' AND active_candidate=1 "
                    "AND (current_price_usd IS NULL OR CAST(current_price_usd AS REAL) < ?) "
                    "ORDER BY updated_at DESC, first_seen_at DESC",
                    (float(max_price),),
                ).fetchall()
            else:
                active_candidate_rows = connection.execute(
                    "SELECT * FROM survivor_candidates "
                    "WHERE state = 'ACTIVE_CANDIDATE' "
                    "ORDER BY updated_at DESC, first_seen_at DESC"
                ).fetchall()
            candidate_rows = active_candidate_rows
            # Balanced Paper was intentionally introduced with a narrower
            # survivor_positions schema.  The Dashboard must remain a
            # read-only consumer of both that runtime and the richer legacy
            # schema; missing presentation-only snapshot fields are NULL, not
            # an API failure.
            position_columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(survivor_positions)").fetchall()
            }
            optional_position_fields = (
                "entry_price_usd", "exit_price_native", "exit_price_usd",
                "entry_holders", "exit_holders", "entry_market_cap_usd",
                "exit_market_cap_usd", "entry_liquidity_usd", "exit_liquidity_usd",
                "exit_trigger_reason", "exit_trigger_pnl_pct",
                "exit_trigger_price_native", "exit_triggered_at",
            )
            optional_position_select = ", ".join(
                f"p.{field}" if field in position_columns else f"NULL AS {field}"
                for field in optional_position_fields
            )
            survivor_position_rows = connection.execute(
                "SELECT p.position_id, p.mint, p.symbol, p.opened_at, p.closed_at, p.status, "
                "p.entry_price_native, p.current_price_native, p.quantity_token, p.last_trade_at, "
                "p.remaining_quantity_token, p.invested_bnb, p.realized_bnb, p.pnl_pct, "
                "p.exit_reason, p.tp1_at, p.tp2_at, p.trailing_active, p.quote_source, p.updated_at, "
                + optional_position_select + ", "
                "c.holders AS holders, c.market_cap_usd AS market_cap_usd, c.liquidity_usd AS liquidity_usd, "
                "c.price_updated_at AS price_updated_at "
                "FROM survivor_positions p "
                "LEFT JOIN survivor_candidates c ON c.mint = p.mint "
                "ORDER BY p.opened_at DESC LIMIT ?",
                (max(1, min(5000, int(limit))),),
            ).fetchall()
            survivor_positions = []
            for row in survivor_position_rows:
                payload = dict(row)
                # Survivor pnl_pct is stored in percentage points.  Expose an
                # explicit field so the UI never treats it as a decimal ratio.
                payload["pnl_rate_pct"] = payload.get("pnl_pct")
                payload["pnl_basis"] = "EXECUTABLE_QUOTE" if payload.get("status") == "CLOSED" else "WSS_MARK"
                if payload.get("status") != "CLOSED" and payload.get("price_updated_at"):
                    try:
                        updated_at = datetime.fromisoformat(str(payload["price_updated_at"]).replace("Z", "+00:00"))
                        if updated_at.tzinfo is None:
                            updated_at = updated_at.replace(tzinfo=timezone.utc)
                        payload["price_is_stale"] = (datetime.now(timezone.utc) - updated_at).total_seconds() > 1800
                    except ValueError:
                        payload["price_is_stale"] = True
                else:
                    payload["price_is_stale"] = False
                payload["source"] = "survivor_reversal"
                survivor_positions.append(payload)
            return {
                "status": "ok",
                "chain": "bsc-mainnet" if chain == "bsc" else "solana-mainnet",
                "mode": "paper",
                "strategy": strategy,
                "survivor_reversal": state,
                "ledger_summary": ledger_summary,
                "candidates": [dict(row) | {"source": "survivor_reversal"} for row in candidate_rows],
                "positions": survivor_positions,
            }

    @staticmethod
    def _paper_ledger_summary(
        connection: sqlite3.Connection,
        display_since: str | None = None,
    ) -> dict[str, object]:
        """Small summary query for the dashboard; avoids the heavier analytics join."""

        since_signal = " AND s.observed_at >= ?" if display_since else ""
        since_candidate = " AND s.observed_at >= ?" if display_since else ""
        since_position = " AND opened_at >= ?" if display_since else ""
        since_params = (display_since,) if display_since else ()
        signals = int(connection.execute(
            "SELECT COUNT(*) FROM signals s "
            "JOIN candidates c ON c.signal_id = s.signal_id AND c.mode = 'paper' "
            "WHERE 1=1" + since_signal,
            since_params,
        ).fetchone()[0])
        accepted = int(connection.execute(
            "SELECT COUNT(*) FROM candidates c JOIN signals s ON s.signal_id = c.signal_id "
            "WHERE c.mode = 'paper' AND c.status = 'ACCEPTED'" + since_candidate,
            since_params,
        ).fetchone()[0])
        rejected = int(connection.execute(
            "SELECT COUNT(*) FROM candidates c JOIN signals s ON s.signal_id = c.signal_id "
            "WHERE c.mode = 'paper' AND c.status = 'REJECTED'" + since_candidate,
            since_params,
        ).fetchone()[0])
        open_positions = int(connection.execute(
            "SELECT COUNT(*) FROM virtual_positions WHERE mode = 'paper' AND status != 'CLOSED'" + since_position,
            since_params,
        ).fetchone()[0])
        closed_positions = int(connection.execute(
            "SELECT COUNT(*) FROM virtual_positions WHERE mode = 'paper' AND status = 'CLOSED'" + since_position,
            since_params,
        ).fetchone()[0])
        row = connection.execute(
            "SELECT COUNT(*) AS count, "
            "SUM(CASE WHEN e.net_pnl_estimated_sol > 0 THEN 1 ELSE 0 END) AS wins, "
            "SUM(CASE WHEN e.net_pnl_estimated_sol < 0 THEN 1 ELSE 0 END) AS losses, "
            "COALESCE(SUM(e.net_pnl_estimated_sol), 0) AS pnl "
            "FROM executions e JOIN virtual_positions p ON p.position_id = e.position_id "
            "WHERE e.mode = 'paper' AND e.action = 'exit' "
            "AND e.net_pnl_estimated_sol IS NOT NULL"
            + (" AND p.opened_at >= ?" if display_since else ""),
            since_params,
        ).fetchone()
        return {
            "signals": signals,
            "accepted_candidates": accepted,
            "rejected_candidates": rejected,
            "open_positions": open_positions,
            "closed_positions": closed_positions,
            "closed_trade_count": int(row["count"] or 0),
            "profitable_trade_count": int(row["wins"] or 0),
            "losing_trade_count": int(row["losses"] or 0),
            "amount_bnb": str(row["pnl"] or "0"),
            "amount_native": str(row["pnl"] or "0"),
            "native_symbol": "BNB",
            "positions_included": False,
        }

    @staticmethod
    def _survivor_paper_summary(
        connection: sqlite3.Connection,
        native_symbol: str,
        display_since: str | None = None,
    ) -> dict[str, object]:
        """Summarize only this Survivor strategy's positions in its native asset."""

        row = connection.execute(
            "SELECT "
            "SUM(CASE WHEN status != 'CLOSED' THEN 1 ELSE 0 END) AS open_positions, "
            "SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END) AS closed_positions, "
            "SUM(CASE WHEN status = 'CLOSED' AND (CAST(realized_bnb AS REAL) - CAST(invested_bnb AS REAL)) > 0 THEN 1 ELSE 0 END) AS wins, "
            "SUM(CASE WHEN status = 'CLOSED' AND (CAST(realized_bnb AS REAL) - CAST(invested_bnb AS REAL)) < 0 THEN 1 ELSE 0 END) AS losses, "
            "COALESCE(SUM(CASE WHEN status = 'CLOSED' THEN CAST(realized_bnb AS REAL) - CAST(invested_bnb AS REAL) ELSE 0 END), 0) AS pnl "
            "FROM survivor_positions"
            + (" WHERE opened_at >= ?" if display_since else ""),
            (display_since,) if display_since else (),
        ).fetchone()
        closed = int(row["closed_positions"] or 0)
        wins = int(row["wins"] or 0)
        losses = int(row["losses"] or 0)
        counted = wins + losses
        return {
            "signals": 0,
            "accepted_candidates": 0,
            "rejected_candidates": 0,
            "open_positions": int(row["open_positions"] or 0),
            "closed_positions": closed,
            "closed_trade_count": closed,
            "profitable_trade_count": wins,
            "losing_trade_count": losses,
            "win_rate_pct": (wins / counted * 100) if counted else None,
            "amount_native": str(row["pnl"] or "0"),
            "native_symbol": native_symbol,
            "positions_included": True,
        }

    @staticmethod
    def _survivor_state(connection: sqlite3.Connection, chain: str = "bsc", strategy: str = "high") -> dict[str, object]:
        state_key = "survivor_balanced_v1" if chain == "bsc" and strategy == "balanced" else "survivor_reversal_v1" if chain == "bsc" else "survivor_reversal_sol_v1"
        strategy = SURVIVOR_REVERSAL_IDENTITY.strategy_name if chain == "bsc" else SOL_SURVIVOR_REVERSAL_IDENTITY.strategy_name
        row = connection.execute(
            "SELECT value_json, updated_at FROM runtime_state WHERE mode = 'paper' AND state_key = ?",
            (state_key,),
        ).fetchone()
        if row is None:
            return {"state": "NOT_STARTED", "strategy": strategy}
        try:
            value = json.loads(row["value_json"])
        except (TypeError, ValueError, json.JSONDecodeError):
            value = {"state": "CORRUPT"}
        if not isinstance(value, dict):
            value = {"state": "CORRUPT"}
        value.setdefault("updated_at", row["updated_at"])
        return value

    @staticmethod
    def _pnl_summary(
        connection: sqlite3.Connection,
        mode: str,
        chain: str = "solana",
        display_since: str | None = None,
    ) -> dict[str, object]:
        where = (
            "WHERE e.mode = ? AND e.action = 'exit' AND e.net_pnl_estimated_sol IS NOT NULL"
        )
        if chain == "bsc":
            where += " AND COALESCE(e.legacy_valuation, 0) = 0"
        params: tuple[object, ...] = (mode,)
        if display_since is not None:
            where += " AND p.opened_at >= ?"
            params = (mode, display_since)
        rows = connection.execute(
            "SELECT e.net_pnl_estimated_sol, e.net_pnl_is_estimated, p.quantity_sol "
            "FROM executions e JOIN virtual_positions p ON p.position_id = e.position_id "
            + where,
            params,
        ).fetchall()
        amount = Decimal("0")
        invested = Decimal("0")
        estimated = False
        profitable = 0
        losing = 0
        for net_pnl, is_estimated, quantity_sol in rows:
            pnl_value = Decimal(str(net_pnl))
            amount += pnl_value
            invested += Decimal(str(quantity_sol))
            estimated = estimated or bool(is_estimated)
            if pnl_value > 0:
                profitable += 1
            elif pnl_value < 0:
                losing += 1
        rate_pct = (amount / invested * Decimal("100")) if invested else Decimal("0")
        result = {
            "amount_native": str(amount),
            "rate_pct": str(rate_pct),
            "is_estimated": estimated,
            "closed_trade_count": len(rows),
            "profitable_trade_count": profitable,
            "losing_trade_count": losing,
        }
        result["amount_bnb" if chain == "bsc" else "amount_sol"] = str(amount)
        return result

    @staticmethod
    def _display_since(
        connection: sqlite3.Connection,
        mode: str,
        chain: str,
    ) -> str | None:
        """Return the optional Paper/Shadow dashboard session boundary."""
        if chain not in {"solana", "bsc"} or mode not in {"paper", "shadow"}:
            return None
        row = connection.execute(
            "SELECT value_json FROM runtime_state WHERE mode = ? AND state_key = ?",
            (mode, _DISPLAY_SINCE_KEY),
        ).fetchone()
        if row is None:
            return None
        try:
            value = json.loads(row["value_json"])
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        since = value.get("since") if isinstance(value, dict) else None
        return since if isinstance(since, str) and since else None

    def health(self, chain: str = "solana") -> dict[str, object]:
        result: dict[str, object] = {"status": "ok", "modes": {}}
        paths = self._paths(chain)
        for mode, path in (("paper", paths.paper_health_file), ("shadow", paths.shadow_health_file)):
            snapshot: object = {"state": "UNKNOWN"}
            try:
                snapshot = json.loads(path.read_text(encoding="utf-8"))
            except (FileNotFoundError, OSError, json.JSONDecodeError):
                pass
            with self._connection(mode, chain) as connection:
                events = LedgerQueries(connection, mode).health_events(20)
            result["modes"][mode] = {"snapshot": snapshot, "recent_events": events}
        return result

    def _runner_control_state(self, chain: str, mode: str) -> dict[str, object] | None:
        """Return the most recent control state acknowledged by one runner."""
        paths = self._paths(chain)
        path = paths.paper_health_file if mode == "paper" else paths.shadow_health_file
        try:
            snapshot = json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            return None
        if not isinstance(snapshot, dict):
            return None
        items = snapshot.get("items")
        if not isinstance(items, dict):
            return None
        item = items.get("runtime_control")
        if not isinstance(item, dict) or item.get("state") != "HEALTHY":
            return None
        details = item.get("details")
        if not isinstance(details, dict) or not isinstance(details.get("new_entries_paused"), bool):
            return None
        return {
            "state": "APPLIED",
            "new_entries_paused": details["new_entries_paused"],
            "updated_at": item.get("updated_at"),
            "control_updated_at": details.get("control_updated_at"),
        }

    def config_payload(self, chain: str = "solana") -> dict[str, object]:
        is_bsc = chain == "bsc"
        if is_bsc:
            survivor_config = SurvivorReversalConfig.from_env()
            return {
                "chain": "bsc-mainnet",
                "chain_key": "bsc",
                "chain_id": "56",
                "pricing": {
                    "pricing_mode": "bsc_executable_quote",
                    "executable_quote": True,
                    "net_pnl_is_estimated": False,
                    "reference_pricing_mode": None,
                },
                "strategy": SURVIVOR_REVERSAL_IDENTITY.strategy_name,
                "ruleset_name": SURVIVOR_REVERSAL_IDENTITY.ruleset_name,
                "ruleset_version": SURVIVOR_REVERSAL_IDENTITY.ruleset_version,
                "display_name": "Meme Survivor Reversal V1 Paper",
                "display_ruleset_name": SURVIVOR_REVERSAL_IDENTITY.strategy_name,
                "live_engine": False,
                "wallet_path": None,
                "signing_path": None,
                "survivor_v1": survivor_config.effective_values(),
                "config_self_check": survivor_config.self_check(),
                "legacy_gates_enabled": False,
                "legacy_fields_for_gate": [],
                "allow_live_trading": False,
                "execution_provider": "paper",
            }
        sol_survivor = SolSurvivorReversalConfig.from_env()
        if sol_survivor.enabled:
            return {
                "chain": "solana-mainnet",
                "chain_key": "solana",
                "chain_id": "CT_501",
                "pricing": {
                    "pricing_mode": "solana_protocol_or_jupiter_readonly_quote",
                    "executable_quote": True,
                    "binance_indicative_reference_only": True,
                },
                "strategy": SOL_SURVIVOR_REVERSAL_IDENTITY.strategy_name,
                "ruleset_name": SOL_SURVIVOR_REVERSAL_IDENTITY.ruleset_name,
                "ruleset_version": SOL_SURVIVOR_REVERSAL_IDENTITY.ruleset_version,
                "display_name": "Meme Survivor Reversal SOL V1 Paper",
                "display_ruleset_name": SOL_SURVIVOR_REVERSAL_IDENTITY.strategy_name,
                "live_engine": False,
                "wallet_path": None,
                "signing_path": None,
                "survivor_v1": sol_survivor.effective_values(),
                "config_self_check": sol_survivor.self_check(),
                "allow_live_trading": False,
                "execution_provider": "paper",
            }
        config = bsc_baseline_config() if is_bsc else solana_baseline_config()
        entry_config = (
            {
                "pricing_mode": "bsc_executable_quote",
                "executable_quote": True,
                "require_readonly_buy_quote": True,
                "require_readonly_sell_quote": True,
                "binance_indicative_reference_only": True,
                "observation_delay_sec": config.observation_delay_sec,
                "observation_price_rise_required": True,
                "require_holders_non_decreasing_after_observation": config.require_holders_non_decreasing_after_observation,
                "min_holders": config.min_holders,
                "min_holders_inclusive": config.min_holders_inclusive,
                "min_market_cap_usd": str(config.min_market_cap_usd),
                "min_liquidity_usd": str(config.min_liquidity_usd),
                "optional_unavailable_fields": [
                    "token_age",
                    "unique_buyers_15s",
                    "buy_sell_ratio_15s",
                    "net_buy_15s",
                    "flow_windows",
                    "creator_sell_confirmation",
                ],
            }
            if is_bsc
            else {
                "observation_delay_sec": config.observation_delay_sec,
                "observation_price_rise_required": config.require_observation_price and False,
                "require_holders_non_decreasing_after_observation": config.require_holders_non_decreasing_after_observation,
                "observation_price_policy": "hard" if config.require_observation_price else "record_only",
                "observation_liquidity_policy": "hard" if config.require_observation_liquidity else "record_only",
                "token_age_sec": [config.token_age_min_sec, config.token_age_max_sec],
                "unique_buyers_15s_min": config.unique_buyers_15s_min,
                "buy_sell_count_ratio_15s_min": str(config.buy_sell_count_ratio_15s_min),
                "net_buy_15s": "> 0",
                "require_two_non_negative_flow_windows": config.require_two_non_negative_flow_windows,
                "creator_confirmed_sold_at_entry": config.creator_confirmed_sold_at_entry,
                "require_executable_buy_route": config.require_executable_buy_route,
                "require_executable_sell_route": config.require_executable_sell_route,
                "max_buy_price_impact_pct": str(config.max_buy_price_impact_pct),
                "max_immediate_exit_impact_pct": str(config.max_immediate_exit_impact_pct),
                "min_holders": config.min_holders,
                "min_holders_inclusive": config.min_holders_inclusive,
                "holders_policy": "hard" if config.enforce_holders else "record_only",
                "min_market_cap_usd": str(config.min_market_cap_usd),
                "market_cap_policy": "hard" if config.enforce_market_cap else "record_only",
                "min_liquidity_usd": str(config.min_liquidity_usd),
                "liquidity_policy": "hard" if config.enforce_liquidity else "record_only",
            }
        )
        risk_config = {
            "position_size_bnb" if is_bsc else "position_size_sol": str(config.position_size_sol),
            "initial_virtual_balance_bnb" if is_bsc else "initial_virtual_balance_sol": str(config.initial_virtual_balance_sol),
            "max_open_positions": config.max_open_positions,
            "same_name_cooldown_sec": config.same_name_cooldown_sec,
            "one_trade_per_mint": config.one_trade_per_mint,
            "daily_full_loss_bnb" if is_bsc else "daily_full_loss_sol": str(config.daily_full_loss_sol_limit),
            "pause_new_entries_after_large_losses": config.pause_new_entries_after_large_losses,
            "large_loss_threshold_pct": str(config.large_loss_threshold_pct * 100),
        }
        return {
            "chain": "bsc-mainnet" if chain == "bsc" else "solana-mainnet",
            "chain_key": chain,
            "chain_id": "56" if chain == "bsc" else "CT_501",
            "pricing": {
                "pricing_mode": "bsc_executable_quote" if is_bsc else "jupiter_quote",
                "executable_quote": True,
                "net_pnl_is_estimated": False if is_bsc else None,
                "reference_pricing_mode": "binance_indicative_reference" if is_bsc else None,
            },
            "strategy": BSC_BASELINE_IDENTITY.strategy_name if is_bsc else config.identity.strategy_name,
            "ruleset_name": config.identity.ruleset_name,
            "ruleset_version": config.identity.ruleset_version,
            "display_name": "BSC 链上只读报价 Paper/Shadow 策略" if is_bsc else "Solana 超早期基线策略",
            "display_ruleset_name": "BSC 链上报价最小规则" if is_bsc else "超早期最小规则",
            "live_engine": False,
            "wallet_path": None,
            "signing_path": None,
            "broadcast_path": None,
            "editable_controls": ["paper_new_entries_paused", "shadow_new_entries_paused"],
            "strategy_config": {
                "entry": entry_config,
                "paper_exit": {
                    "take_profit_pct": str(config.take_profit_pct * 100),
                    "stop_loss_trigger_pct": str(config.stop_loss_trigger_pct * 100),
                    "max_hold_sec": config.max_hold_sec,
                    "take_profit_sell_pct": str(config.partial_take_profit_sell_pct * 100),
                    "stop_loss_sell_pct": "100",
                    "partial_take_profit_enabled": config.partial_take_profit_enabled,
                    "take_profit_1_pct": str(config.take_profit_1_pct * 100),
                    "take_profit_1_sell_pct": str(config.partial_take_profit_sell_pct * 100),
                    "take_profit_2_pct": str(config.take_profit_2_pct * 100),
                    "take_profit_2_sell_pct": str(config.partial_take_profit_sell_pct * 100),
                    "tp2_breakeven_exit_enabled": config.tp2_breakeven_exit_enabled,
                    "moving_stop_enabled": config.tp2_breakeven_exit_enabled,
                },
                "shadow_exit": {
                    "shadow_defense_pct": str(config.shadow_defense_pct * 100),
                    "shadow_time_sec": config.shadow_time_sec,
                    "shadow_mfe_pct": str(config.shadow_mfe_pct * 100),
                    "shadow_holders_drop_pct": str(config.shadow_holders_drop_pct * 100),
                    "shadow_liquidity_drop_pct": str(config.shadow_liquidity_drop_pct * 100),
                    "rules": [
                        "paper_take_profit",
                        "paper_stop_loss",
                        "paper_max_hold_timeout",
                        "shadow_holders_drop_over_10pct",
                        "shadow_liquidity_drop_over_15pct",
                        "shadow_defense_v1",
                        "shadow_creator_sell",
                        "shadow_time_exit",
                    ],
                },
                "risk": {
                    **risk_config,
                },
            },
        }

    def _paths(self, chain: str) -> RuntimePaths:
        if chain not in {"solana", "bsc"}:
            raise ValueError("chain must be solana or bsc")
        if chain not in self._chain_paths:
            self._chain_paths[chain] = RuntimePaths.from_env(chain)
        return self._chain_paths[chain]

    def _control(self, chain: str) -> RuntimeControl:
        if chain == "solana":
            return self.control
        return RuntimeControl(self._paths(chain).control_file)

    def _connection(self, mode: str, chain: str = "solana") -> sqlite3.Connection:
        paths = self._paths(chain)
        path = paths.paper_db if mode == "paper" else paths.shadow_db
        if not path.exists():
            return initialize_database(path)
        connection = sqlite3.connect(path, timeout=10.0, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=10000")
        connection.execute("PRAGMA query_only=ON")
        return connection


class _Handler(BaseHTTPRequestHandler):
    service: DashboardService

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        status, payload = self.service.payload(parsed.path, parse_qs(parsed.query))
        content_type = "text/html; charset=utf-8" if parsed.path == "/" else _asset_content_type(parsed.path)
        self._send(status, payload, content_type=content_type)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path != "/api/control":
            self._send(404, {"error": "not_found"})
            return
        try:
            length = max(0, min(10_000, int(self.headers.get("Content-Length", "0"))))
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            if not isinstance(payload, Mapping):
                raise ValueError
            status, result = self.service.set_control(payload)
        except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError):
            status, result = 400, {"error": "invalid_json"}
        self._send(status, result)

    def log_message(self, format: str, *args: object) -> None:
        # Do not mirror query strings or request data into logs.
        return

    def _send(self, status: int, payload: object, *, content_type: str = "application/json; charset=utf-8") -> None:
        body = payload.encode("utf-8") if isinstance(payload, str) and content_type != "application/json; charset=utf-8" else json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def create_server(service: DashboardService) -> ThreadingHTTPServer:
    handler = type("DashboardHandler", (_Handler,), {"service": service})
    return ThreadingHTTPServer((service.config.host, service.config.port), handler)


def serve(service: DashboardService) -> None:
    server = create_server(service)
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        server.server_close()


def _read_static(name: str) -> str:
    path = Path(__file__).with_name("static") / name
    return path.read_text(encoding="utf-8")


def _asset_content_type(path: str) -> str:
    return mimetypes.guess_type(path)[0] or "application/octet-stream"


def _one(query: Mapping[str, list[str]], key: str, default: str | None) -> str | None:
    values = query.get(key)
    return values[0] if values else default


def _chain(query: Mapping[str, list[str]]) -> str:
    value = (_one(query, "chain", "solana") or "solana").lower()
    if value not in {"solana", "bsc"}:
        return "solana"
    return value


def _strategy(query: Mapping[str, list[str]]) -> str:
    return "balanced" if (_one(query, "strategy", "high") or "high").lower() == "balanced" else "high"


def _limit(query: Mapping[str, list[str]]) -> int:
    try:
        return max(1, min(1000, int(_one(query, "limit", "100") or "100")))
    except ValueError:
        return 100


def _survivor_limit(query: Mapping[str, list[str]]) -> int:
    try:
        return max(1, min(5000, int(_one(query, "limit", "100") or "100")))
    except ValueError:
        return 100
