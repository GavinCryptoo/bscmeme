"""Independent Paper/Shadow ledgers, lifecycle events and restart recovery."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime
from decimal import Decimal
from typing import Mapping

from meme_system.adapters.protocols import ExecutableQuote
from meme_system.domain.models import (
    Candidate,
    ExecutionRecord,
    LifecycleEvent,
    PositionObservation,
    ShadowOutcome,
    Signal,
    StrategyIdentity,
    VirtualPosition,
)


def _json_default(value: object) -> str:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _parse_datetime(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


@dataclass
class SimulationLedger:
    mode: str
    connection: sqlite3.Connection | None = None
    candidates: list[Candidate] = field(default_factory=list)
    positions: dict[str, VirtualPosition] = field(default_factory=dict)
    closed_positions: dict[str, VirtualPosition] = field(default_factory=dict)
    executions: list[ExecutionRecord] = field(default_factory=list)
    shadow_outcomes: list[ShadowOutcome] = field(default_factory=list)
    lifecycle_events: list[LifecycleEvent] = field(default_factory=list)
    large_loss_count: int = 0
    full_loss_units: int = 0

    def __post_init__(self) -> None:
        if self.mode not in {"paper", "shadow"}:
            raise ValueError("SimulationLedger mode must be paper or shadow")

    @classmethod
    def recover(
        cls,
        mode: str,
        connection: sqlite3.Connection,
        large_loss_threshold_pct: Decimal = Decimal("-0.40"),
    ) -> "SimulationLedger":
        """Rebuild active and closed lifecycles from the SQLite fact source."""
        ledger = cls(mode=mode, connection=connection)
        rows = connection.execute(
            "SELECT position_id, mint, mode, strategy_name, ruleset_name, "
            "ruleset_version, config_version, quantity_sol, opened_at, status, "
            "entry_quantity_token, remaining_quantity_token, entry_quote_id, token_name, "
            "mfe_pct, mae_pct, last_return_pct, last_observed_at, last_quote_id, "
            "closed_at, closed_reason "
            "FROM virtual_positions WHERE mode = ? ORDER BY opened_at ASC",
            (mode,),
        )
        for row in rows:
            position = VirtualPosition(
                position_id=row["position_id"],
                mint=row["mint"],
                mode=row["mode"],
                identity=StrategyIdentity(
                    strategy_name=row["strategy_name"],
                    ruleset_name=row["ruleset_name"],
                    ruleset_version=row["ruleset_version"],
                    config_version=row["config_version"],
                ),
                quantity_sol=Decimal(row["quantity_sol"]),
                opened_at=datetime.fromisoformat(row["opened_at"]),
                entry_quantity_token=Decimal(row["entry_quantity_token"]),
                remaining_quantity_token=Decimal(row["remaining_quantity_token"]),
                entry_quote_id=row["entry_quote_id"],
                token_name=row["token_name"],
                status=row["status"],
                mfe_pct=Decimal(row["mfe_pct"]),
                mae_pct=Decimal(row["mae_pct"]),
                last_return_pct=(
                    Decimal(row["last_return_pct"])
                    if row["last_return_pct"] is not None
                    else None
                ),
                last_observed_at=_parse_datetime(row["last_observed_at"]),
                last_quote_id=row["last_quote_id"],
                closed_at=_parse_datetime(row["closed_at"]),
                closed_reason=row["closed_reason"],
            )
            if position.status == "CLOSED":
                ledger.closed_positions[position.position_id] = position
            else:
                ledger.positions[position.position_id] = position
        loss_rows = connection.execute(
            "SELECT gross_pnl_pct FROM executions "
            "WHERE mode = ? AND action = 'exit' AND gross_pnl_pct IS NOT NULL",
            (mode,),
        )
        for row in loss_rows:
            gross_pnl_pct = Decimal(row["gross_pnl_pct"])
            if gross_pnl_pct <= large_loss_threshold_pct:
                ledger.large_loss_count += 1
            if gross_pnl_pct <= Decimal("-1"):
                ledger.full_loss_units += 1
        return ledger

    @property
    def active_positions(self) -> tuple[VirtualPosition, ...]:
        return tuple(self.positions.values())

    def record_signal(self, signal: Signal) -> None:
        if self.connection is None:
            return
        self.connection.execute(
            "INSERT OR IGNORE INTO signals "
            "(signal_id, chain, mint, source, observed_at) VALUES (?, ?, ?, ?, ?)",
            (
                "solana:" + signal.signal_id,
                "solana",
                signal.mint,
                signal.source,
                signal.observed_at.isoformat(),
            ),
        )
        self.connection.commit()

    def record_candidate(self, candidate: Candidate) -> None:
        self.candidates.append(candidate)
        if self.connection is None:
            return
        failed = [check.reason_code for check in candidate.checks if check.reason_code]
        checks_json = json.dumps(
            [asdict(check) for check in candidate.checks],
            default=_json_default,
        )
        soft_json = json.dumps(candidate.soft_features or {}, default=_json_default)
        self.connection.execute(
            "INSERT INTO candidates "
            "(candidate_id, signal_id, mint, mode, strategy_name, ruleset_name, "
            "ruleset_version, config_version, status, filter_reason, "
            "rule_checks_json, soft_features_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                candidate.candidate_id,
                "solana:" + candidate.signal_id,
                candidate.mint,
                self.mode,
                candidate.identity.strategy_name,
                candidate.identity.ruleset_name,
                candidate.identity.ruleset_version,
                candidate.identity.config_version,
                candidate.status,
                ",".join(failed) if failed else None,
                checks_json,
                soft_json,
            ),
        )
        self.connection.commit()

    def open_position(self, position: VirtualPosition) -> None:
        if position.position_id in self.positions or position.position_id in self.closed_positions:
            raise ValueError("position_id already exists")
        self.positions[position.position_id] = position
        if self.connection is not None:
            self.connection.execute(
                "INSERT INTO virtual_positions "
                "(position_id, mint, mode, strategy_name, ruleset_name, ruleset_version, "
                "config_version, quantity_sol, opened_at, status, entry_quantity_token, "
                "remaining_quantity_token, entry_quote_id, token_name, mfe_pct, mae_pct, "
                "last_return_pct, last_observed_at, last_quote_id, closed_at, closed_reason) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    position.position_id,
                    position.mint,
                    position.mode,
                    position.identity.strategy_name,
                    position.identity.ruleset_name,
                    position.identity.ruleset_version,
                    position.identity.config_version,
                    str(position.quantity_sol),
                    position.opened_at.isoformat(),
                    position.status,
                    str(position.entry_quantity_token),
                    str(position.remaining_quantity_token),
                    position.entry_quote_id,
                    position.token_name,
                    str(position.mfe_pct),
                    str(position.mae_pct),
                    str(position.last_return_pct)
                    if position.last_return_pct is not None
                    else None,
                    position.last_observed_at.isoformat()
                    if position.last_observed_at is not None
                    else None,
                    position.last_quote_id,
                    position.closed_at.isoformat()
                    if position.closed_at is not None
                    else None,
                    position.closed_reason,
                ),
            )
            self.connection.commit()
        self.record_event(
            position.position_id,
            "POSITION_CREATED",
            position.opened_at,
            {"status": position.status},
        )

    def transition_position(
        self,
        position_id: str,
        status: str,
        occurred_at: datetime,
        payload: Mapping[str, object] | None = None,
    ) -> VirtualPosition:
        if position_id not in self.positions:
            raise KeyError(position_id)
        position = replace(self.positions[position_id], status=status)
        self.positions[position_id] = position
        if self.connection is not None:
            self.connection.execute(
                "UPDATE virtual_positions SET status = ? WHERE position_id = ?",
                (status, position_id),
            )
            self.connection.commit()
        self.record_event(position_id, status, occurred_at, payload or {})
        return position

    def record_observation(
        self,
        position_id: str,
        observed_at: datetime,
        sell_quote: ExecutableQuote | None,
    ) -> PositionObservation:
        position = self.positions[position_id]
        if sell_quote is None:
            return PositionObservation(
                position_id,
                observed_at,
                None,
                None,
                position.mfe_pct,
                position.mae_pct,
                False,
                "sell_quote_unavailable",
            )
        error = sell_quote.unusable_reason(observed_at)
        if error is not None:
            return PositionObservation(
                position_id,
                observed_at,
                sell_quote.quote_id,
                None,
                position.mfe_pct,
                position.mae_pct,
                False,
                error,
            )
        if sell_quote.input_quantity != position.active_quantity_token:
            return PositionObservation(
                position_id,
                observed_at,
                sell_quote.quote_id,
                None,
                position.mfe_pct,
                position.mae_pct,
                False,
                "sell_quote_quantity_mismatch",
            )
        return_pct = (
            sell_quote.output_quantity - position.quantity_sol
        ) / position.quantity_sol
        updated = replace(
            position,
            mfe_pct=max(position.mfe_pct, return_pct),
            mae_pct=min(position.mae_pct, return_pct),
            last_return_pct=return_pct,
            last_observed_at=observed_at,
            last_quote_id=sell_quote.quote_id,
        )
        self.positions[position_id] = updated
        if self.connection is not None:
            self.connection.execute(
                "UPDATE virtual_positions SET mfe_pct = ?, mae_pct = ?, "
                "last_return_pct = ?, last_observed_at = ?, last_quote_id = ? "
                "WHERE position_id = ?",
                (
                    str(updated.mfe_pct),
                    str(updated.mae_pct),
                    str(updated.last_return_pct),
                    observed_at.isoformat(),
                    sell_quote.quote_id,
                    position_id,
                ),
            )
            self.connection.commit()
        self.record_event(
            position_id,
            "MARK_OBSERVED",
            observed_at,
            {
                "quote_id": sell_quote.quote_id,
                "return_pct": return_pct,
                "mfe_pct": updated.mfe_pct,
                "mae_pct": updated.mae_pct,
            },
        )
        return PositionObservation(
            position_id,
            observed_at,
            sell_quote.quote_id,
            return_pct,
            updated.mfe_pct,
            updated.mae_pct,
            True,
        )

    def close_position(
        self,
        position_id: str,
        closed_at: datetime | None = None,
        closed_reason: str | None = None,
    ) -> VirtualPosition:
        position = self.positions.pop(position_id)
        closed_at = closed_at or position.last_observed_at or position.opened_at
        closed = replace(
            position,
            remaining_quantity_token=Decimal("0"),
            status="CLOSED",
            closed_at=closed_at,
            closed_reason=closed_reason,
        )
        self.closed_positions[position_id] = closed
        if self.connection is not None:
            self.connection.execute(
                "UPDATE virtual_positions SET status = ?, remaining_quantity_token = ?, "
                "closed_at = ?, closed_reason = ? WHERE position_id = ?",
                (
                    "CLOSED",
                    "0",
                    closed_at.isoformat(),
                    closed_reason,
                    position_id,
                ),
            )
            self.connection.commit()
        self.record_event(
            position_id,
            "CLOSED",
            closed_at,
            {"reason": closed_reason},
        )
        return closed

    def record_execution(self, execution: ExecutionRecord) -> None:
        self.executions.append(execution)
        if self.connection is None:
            return
        cost = execution.cost
        self.connection.execute(
            "INSERT INTO executions "
            "(execution_id, position_id, mode, action, reason, quote_id, quote_age_ms, "
            "quote_input_quantity, quote_output_quantity, price_impact_pct, quote_quoted_at, "
            "route_fee, estimated_network_fee, estimated_priority_fee, gross_pnl_sol, "
            "gross_pnl_pct, net_pnl_estimated_sol, net_pnl_is_estimated, recorded_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                execution.execution_id,
                execution.position_id,
                execution.mode,
                execution.action,
                execution.reason,
                execution.quote_id,
                execution.quote_age_ms,
                str(execution.quote_input_quantity)
                if execution.quote_input_quantity is not None
                else None,
                str(execution.quote_output_quantity)
                if execution.quote_output_quantity is not None
                else None,
                str(execution.price_impact_pct)
                if execution.price_impact_pct is not None
                else None,
                execution.quote_quoted_at.isoformat()
                if execution.quote_quoted_at is not None
                else None,
                str(cost.route_fee_sol) if cost and cost.route_fee_sol is not None else None,
                str(cost.estimated_network_fee_sol)
                if cost and cost.estimated_network_fee_sol is not None
                else None,
                str(cost.estimated_priority_fee_sol)
                if cost and cost.estimated_priority_fee_sol is not None
                else None,
                str(cost.gross_pnl_sol) if cost else None,
                str(cost.gross_pnl_pct) if cost else None,
                str(cost.net_pnl_estimated_sol) if cost else None,
                int(cost.net_pnl_is_estimated) if cost else 1,
                execution.recorded_at.isoformat()
                if execution.recorded_at is not None
                else "",
            ),
        )
        self.connection.commit()

    def record_shadow_outcome(self, outcome: ShadowOutcome) -> None:
        self.shadow_outcomes.append(outcome)
        if self.connection is None:
            return
        outcome_id = f"{outcome.position_id}:outcome:{len(self.shadow_outcomes)}"
        returns_json = json.dumps(outcome.returns_after_exit_pct, default=_json_default)
        self.connection.execute(
            "INSERT INTO shadow_outcomes "
            "(outcome_id, position_id, strategy_name, ruleset_name, ruleset_version, "
            "config_version, returns_after_exit_json, paper_tp_reached, avoided_loss_pct, "
            "missed_profit_pct, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                outcome_id,
                outcome.position_id,
                outcome.identity.strategy_name,
                outcome.identity.ruleset_name,
                outcome.identity.ruleset_version,
                outcome.identity.config_version,
                returns_json,
                int(outcome.paper_tp_reached),
                str(outcome.avoided_loss_pct)
                if outcome.avoided_loss_pct is not None
                else None,
                str(outcome.missed_profit_pct)
                if outcome.missed_profit_pct is not None
                else None,
                outcome.recorded_at.isoformat()
                if outcome.recorded_at is not None
                else "",
            ),
        )
        self.connection.commit()

    def record_event(
        self,
        position_id: str,
        event_type: str,
        occurred_at: datetime,
        payload: Mapping[str, object],
    ) -> LifecycleEvent:
        event_id = f"{position_id}:{event_type}:{len(self.lifecycle_events)}"
        event = LifecycleEvent(
            event_id=event_id,
            position_id=position_id,
            mode=self.mode,
            event_type=event_type,
            occurred_at=occurred_at,
            payload=dict(payload),
        )
        self.lifecycle_events.append(event)
        if self.connection is not None:
            payload_json = json.dumps(payload, default=_json_default)
            self.connection.execute(
                "INSERT INTO lifecycle_events "
                "(event_id, position_id, mode, event_type, occurred_at, payload_json) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    event.event_id,
                    event.position_id,
                    event.mode,
                    event.event_type,
                    event.occurred_at.isoformat(),
                    payload_json,
                ),
            )
            self.connection.execute(
                "INSERT INTO audit_events "
                "(event_id, mode, event_type, occurred_at, payload_json) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    "audit:" + event.event_id,
                    event.mode,
                    event.event_type,
                    event.occurred_at.isoformat(),
                    payload_json,
                ),
            )
            self.connection.commit()
        return event

    def has_mint_lifecycle(self, mint: str, identity: StrategyIdentity) -> bool:
        return any(
            position.mint == mint
            and position.identity.lifecycle_key(mint) == identity.lifecycle_key(mint)
            for position in (*self.positions.values(), *self.closed_positions.values())
        )

    def name_in_cooldown(
        self,
        token_name: str | None,
        now: datetime,
        cooldown_sec: int,
    ) -> bool:
        if not token_name:
            return False
        for position in (*self.positions.values(), *self.closed_positions.values()):
            age = (now - position.opened_at).total_seconds()
            if position.token_name == token_name and 0 <= age < cooldown_sec:
                return True
        return False
