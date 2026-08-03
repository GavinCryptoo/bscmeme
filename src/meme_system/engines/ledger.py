"""Independent Paper/Shadow/Live ledgers, lifecycle events and recovery."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime
from decimal import Decimal, InvalidOperation
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
    PriceSnapshot,
)
from meme_system.storage.exit_holders import backfill_historical_exit_holders
from meme_system.storage.exit_market import backfill_historical_exit_market


def _json_default(value: object) -> str:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _parse_datetime(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _parse_decimal(value: object | None) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _snapshot_json(snapshot: PriceSnapshot | None) -> str | None:
    return json.dumps(snapshot.as_dict(), default=_json_default) if snapshot is not None else None


def _parse_snapshot(value: object | None) -> PriceSnapshot | None:
    if not value:
        return None
    try:
        payload = json.loads(str(value)) if isinstance(value, str) else value
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return PriceSnapshot(
        price_native=_parse_decimal(payload.get("price_native")),
        native_symbol=str(payload["native_symbol"]) if payload.get("native_symbol") else None,
        native_usd=_parse_decimal(payload.get("native_usd")),
        price_usd=_parse_decimal(payload.get("price_usd")),
        price_source=str(payload["price_source"]) if payload.get("price_source") else None,
        quoted_at=_parse_datetime(payload.get("quoted_at")),
        executable_quote=bool(payload.get("executable_quote", False)),
        estimated=bool(payload.get("estimated", True)),
        price_age_ms=(int(payload["price_age_ms"]) if payload.get("price_age_ms") is not None else None),
    )


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
    full_loss_sol_by_day: dict[str, Decimal] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.mode not in {"paper", "shadow", "live"}:
            raise ValueError("SimulationLedger mode must be paper, shadow, or live")

    @classmethod
    def recover(
        cls,
        mode: str,
        connection: sqlite3.Connection,
        large_loss_threshold_pct: Decimal = Decimal("-0.40"),
        identity: StrategyIdentity | None = None,
    ) -> "SimulationLedger":
        """Rebuild active and closed lifecycles from the SQLite fact source."""
        # Database-only recovery. This never asks a live provider for a
        # historical value and never substitutes a current holder count.
        backfill_historical_exit_holders(connection, mode=mode, max_age_sec=10)
        if identity is None or identity.strategy_name != "bsc_binance_indicative":
            backfill_historical_exit_market(connection, mode=mode, max_age_sec=10)
        ledger = cls(mode=mode, connection=connection)
        rows = connection.execute(
            "SELECT position_id, mint, mode, strategy_name, ruleset_name, "
            "ruleset_version, config_version, quantity_sol, opened_at, status, "
            "entry_quantity_token, remaining_quantity_token, entry_quote_id, token_name, "
            "entry_holders, entry_liquidity_usd, exit_holders, exit_holders_observed_at, "
            "exit_holders_source, exit_holders_status, exit_market_cap_usd, exit_liquidity_usd, "
            "exit_market_observed_at, exit_market_source, exit_market_status, "
            "mfe_pct, mae_pct, last_return_pct, last_observed_at, last_quote_id, "
            "local_price_sol_per_token, local_price_observed_at, local_price_source, local_return_pct, "
            "jupiter_price_sol_per_token, jupiter_price_observed_at, jupiter_return_pct, "
            "closed_at, closed_reason, signal_observed_at, evaluated_at, "
            "entry_quote_at, exit_quote_at, raw_name, display_name, symbol, "
            "entry_price_snapshot_json, exit_price_snapshot_json "
            "FROM virtual_positions WHERE mode = ? ORDER BY opened_at ASC",
            (mode,),
        )
        backfills: list[tuple[int | None, str | None, str]] = []
        for row in rows:
            row_identity = StrategyIdentity(
                strategy_name=row["strategy_name"],
                ruleset_name=row["ruleset_name"],
                ruleset_version=row["ruleset_version"],
                config_version=row["config_version"],
            )
            if identity is not None and row_identity.strategy_name in {
                "sol_ultra_early_baseline",
                identity.strategy_name,
            }:
                row_identity = identity
            entry_holders = int(row["entry_holders"]) if row["entry_holders"] is not None else None
            entry_liquidity_usd = (
                Decimal(row["entry_liquidity_usd"])
                if row["entry_liquidity_usd"] is not None
                else None
            )
            if entry_holders is None or entry_liquidity_usd is None:
                candidate_id = (
                    row["position_id"][:-len(":position")]
                    if row["position_id"].endswith(":position")
                    else None
                )
                candidate_row = (
                    connection.execute(
                        "SELECT soft_features_json FROM candidates WHERE candidate_id = ?",
                        (candidate_id,),
                    ).fetchone()
                    if candidate_id is not None
                    else None
                )
                if candidate_row is not None:
                    try:
                        soft_features = json.loads(candidate_row["soft_features_json"] or "{}")
                    except (TypeError, ValueError, json.JSONDecodeError):
                        soft_features = {}
                    if entry_holders is None:
                        raw_holders = soft_features.get("holders")
                        if raw_holders is not None and not isinstance(raw_holders, bool):
                            try:
                                parsed_holders = int(raw_holders)
                                if parsed_holders >= 0:
                                    entry_holders = parsed_holders
                            except (TypeError, ValueError):
                                pass
                    if entry_liquidity_usd is None:
                        raw_liquidity = soft_features.get("liquidity_usd")
                        try:
                            parsed_liquidity = Decimal(str(raw_liquidity))
                            if parsed_liquidity.is_finite() and parsed_liquidity >= 0:
                                entry_liquidity_usd = parsed_liquidity
                        except (InvalidOperation, TypeError, ValueError):
                            pass
                    if row["entry_holders"] is None or row["entry_liquidity_usd"] is None:
                        backfills.append(
                            (
                                entry_holders,
                                str(entry_liquidity_usd) if entry_liquidity_usd is not None else None,
                                row["position_id"],
                            )
                        )
            position = VirtualPosition(
                position_id=row["position_id"],
                mint=row["mint"],
                mode=row["mode"],
                identity=row_identity,
                quantity_sol=Decimal(row["quantity_sol"]),
                opened_at=datetime.fromisoformat(row["opened_at"]),
                entry_quantity_token=Decimal(row["entry_quantity_token"]),
                remaining_quantity_token=Decimal(row["remaining_quantity_token"]),
                entry_quote_id=row["entry_quote_id"],
                token_name=row["token_name"],
                entry_holders=entry_holders,
                entry_liquidity_usd=entry_liquidity_usd,
                exit_holders=(int(row["exit_holders"]) if row["exit_holders"] is not None else None),
                exit_holders_observed_at=_parse_datetime(row["exit_holders_observed_at"]),
                exit_holders_source=row["exit_holders_source"],
                exit_holders_status=row["exit_holders_status"],
                exit_market_cap_usd=(
                    Decimal(row["exit_market_cap_usd"])
                    if row["exit_market_cap_usd"] is not None
                    else None
                ),
                exit_liquidity_usd=(
                    Decimal(row["exit_liquidity_usd"])
                    if row["exit_liquidity_usd"] is not None
                    else None
                ),
                exit_market_observed_at=_parse_datetime(row["exit_market_observed_at"]),
                exit_market_source=row["exit_market_source"],
                exit_market_status=row["exit_market_status"],
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
                local_price_sol_per_token=(Decimal(row["local_price_sol_per_token"]) if row["local_price_sol_per_token"] is not None else None),
                local_price_observed_at=_parse_datetime(row["local_price_observed_at"]),
                local_price_source=row["local_price_source"],
                local_return_pct=(Decimal(row["local_return_pct"]) if row["local_return_pct"] is not None else None),
                jupiter_price_sol_per_token=(Decimal(row["jupiter_price_sol_per_token"]) if row["jupiter_price_sol_per_token"] is not None else None),
                jupiter_price_observed_at=_parse_datetime(row["jupiter_price_observed_at"]),
                jupiter_return_pct=(Decimal(row["jupiter_return_pct"]) if row["jupiter_return_pct"] is not None else None),
                closed_at=_parse_datetime(row["closed_at"]),
                closed_reason=row["closed_reason"],
                signal_observed_at=_parse_datetime(row["signal_observed_at"]),
                evaluated_at=_parse_datetime(row["evaluated_at"]),
                entry_quote_at=_parse_datetime(row["entry_quote_at"]),
                exit_quote_at=_parse_datetime(row["exit_quote_at"]),
                raw_name=row["raw_name"],
                display_name=row["display_name"] or row["token_name"],
                symbol=row["symbol"],
                entry_price_snapshot=_parse_snapshot(row["entry_price_snapshot_json"]),
                exit_price_snapshot=_parse_snapshot(row["exit_price_snapshot_json"]),
            )
            if position.status == "CLOSED":
                ledger.closed_positions[position.position_id] = position
            else:
                ledger.positions[position.position_id] = position
        for entry_holders, entry_liquidity_usd, position_id in backfills:
            connection.execute(
                "UPDATE virtual_positions SET entry_holders = ?, entry_liquidity_usd = ? "
                "WHERE position_id = ?",
                (entry_holders, entry_liquidity_usd, position_id),
            )
        if backfills:
            connection.commit()
        loss_rows = connection.execute(
            "SELECT e.gross_pnl_pct, e.recorded_at, p.quantity_sol "
            "FROM executions e JOIN virtual_positions p ON p.position_id = e.position_id "
            "WHERE e.mode = ? AND e.action = 'exit' AND e.gross_pnl_pct IS NOT NULL",
            (mode,),
        )
        for row in loss_rows:
            gross_pnl_pct = Decimal(row["gross_pnl_pct"])
            if gross_pnl_pct <= large_loss_threshold_pct:
                ledger.large_loss_count += 1
            if gross_pnl_pct <= Decimal("-1"):
                ledger.record_full_loss(
                    datetime.fromisoformat(row["recorded_at"]),
                    Decimal(row["quantity_sol"]),
                )
        return ledger

    def daily_full_loss_sol(self, moment: datetime) -> Decimal:
        return self.full_loss_sol_by_day.get(moment.date().isoformat(), Decimal("0"))

    def record_full_loss(self, moment: datetime, quantity_sol: Decimal) -> None:
        day = moment.date().isoformat()
        self.full_loss_sol_by_day[day] = self.daily_full_loss_sol(moment) + quantity_sol

    @property
    def active_positions(self) -> tuple[VirtualPosition, ...]:
        return tuple(self.positions.values())

    def record_signal(self, signal: Signal) -> None:
        if self.connection is None:
            return
        signal_key = f"{signal.chain}:{signal.signal_id}"
        self.connection.execute(
            "INSERT OR IGNORE INTO signals "
            "(signal_id, chain, mint, source, observed_at) VALUES (?, ?, ?, ?, ?)",
            (
                signal_key,
                signal.chain,
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
        signal_key = f"{candidate.chain}:{candidate.signal_id}"
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
                signal_key,
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

    def candidate_exists(self, candidate_id: str) -> bool:
        if any(candidate.candidate_id == candidate_id for candidate in self.candidates):
            return True
        if self.connection is None:
            return False
        row = self.connection.execute(
            "SELECT 1 FROM candidates WHERE candidate_id = ? LIMIT 1",
            (candidate_id,),
        ).fetchone()
        return row is not None

    def open_position(self, position: VirtualPosition) -> None:
        if position.position_id in self.positions or position.position_id in self.closed_positions:
            raise ValueError("position_id already exists")
        self.positions[position.position_id] = position
        if self.connection is not None:
            self.connection.execute(
                "INSERT INTO virtual_positions "
                "(position_id, mint, mode, strategy_name, ruleset_name, ruleset_version, "
                "config_version, quantity_sol, opened_at, status, entry_quantity_token, "
                "remaining_quantity_token, entry_quote_id, token_name, entry_holders, entry_liquidity_usd, exit_holders, "
                "exit_holders_observed_at, exit_holders_source, exit_holders_status, "
                "exit_market_cap_usd, exit_liquidity_usd, exit_market_observed_at, "
                "exit_market_source, exit_market_status, mfe_pct, mae_pct, last_return_pct, "
                "last_observed_at, last_quote_id, "
                "closed_at, closed_reason, signal_observed_at, evaluated_at, "
                "entry_quote_at, exit_quote_at, raw_name, display_name, symbol, "
                "entry_price_snapshot_json, exit_price_snapshot_json) "
                "VALUES (" + ", ".join("?" for _ in range(41)) + ")",
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
                    position.entry_holders,
                    str(position.entry_liquidity_usd)
                    if position.entry_liquidity_usd is not None
                    else None,
                    position.exit_holders,
                    position.exit_holders_observed_at.isoformat()
                    if position.exit_holders_observed_at is not None
                    else None,
                    position.exit_holders_source,
                    position.exit_holders_status,
                    str(position.exit_market_cap_usd)
                    if position.exit_market_cap_usd is not None
                    else None,
                    str(position.exit_liquidity_usd)
                    if position.exit_liquidity_usd is not None
                    else None,
                    position.exit_market_observed_at.isoformat()
                    if position.exit_market_observed_at is not None
                    else None,
                    position.exit_market_source,
                    position.exit_market_status,
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
                    position.signal_observed_at.isoformat()
                    if position.signal_observed_at is not None
                    else None,
                    position.evaluated_at.isoformat()
                    if position.evaluated_at is not None
                    else None,
                    position.entry_quote_at.isoformat()
                    if position.entry_quote_at is not None
                    else None,
                    position.exit_quote_at.isoformat()
                    if position.exit_quote_at is not None
                    else None,
                    position.raw_name,
                    position.display_name,
                    position.symbol,
                    _snapshot_json(position.entry_price_snapshot),
                    _snapshot_json(position.exit_price_snapshot),
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
        jupiter_price = (
            sell_quote.output_quantity / sell_quote.input_quantity
            if sell_quote.input_quantity > 0
            else None
        )
        updated = replace(
            position,
            mfe_pct=max(position.mfe_pct, return_pct),
            mae_pct=min(position.mae_pct, return_pct),
            last_return_pct=return_pct,
            last_observed_at=observed_at,
            last_quote_id=sell_quote.quote_id,
            jupiter_price_sol_per_token=jupiter_price,
            jupiter_price_observed_at=observed_at,
            jupiter_return_pct=return_pct,
        )
        self.positions[position_id] = updated
        if self.connection is not None:
            self.connection.execute(
                "UPDATE virtual_positions SET mfe_pct = ?, mae_pct = ?, "
                "last_return_pct = ?, last_observed_at = ?, last_quote_id = ?, "
                "jupiter_price_sol_per_token = ?, jupiter_price_observed_at = ?, jupiter_return_pct = ? "
                "WHERE position_id = ?",
                (
                    str(updated.mfe_pct),
                    str(updated.mae_pct),
                    str(updated.last_return_pct),
                    observed_at.isoformat(),
                    sell_quote.quote_id,
                    str(jupiter_price) if jupiter_price is not None else None,
                    observed_at.isoformat(),
                    str(return_pct),
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

    def record_local_observation(
        self,
        position_id: str,
        observed_at: datetime,
        price_sol_per_token: Decimal,
        *,
        source: str = "pool_wss_indicative",
        account_address: str | None = None,
        observed_slot: int | None = None,
    ) -> PositionObservation:
        """Persist a local pool/curve observation without replacing Jupiter state."""
        position = self.positions[position_id]
        if price_sol_per_token <= 0 or position.active_quantity_token <= 0:
            return PositionObservation(position_id, observed_at, None, None, position.mfe_pct, position.mae_pct, False, "local_price_unavailable")
        return_pct = (price_sol_per_token * position.active_quantity_token - position.quantity_sol) / position.quantity_sol
        updated = replace(
            position,
            mfe_pct=max(position.mfe_pct, return_pct),
            mae_pct=min(position.mae_pct, return_pct),
            local_price_sol_per_token=price_sol_per_token,
            local_price_observed_at=observed_at,
            local_price_source=source,
            local_return_pct=return_pct,
        )
        self.positions[position_id] = updated
        if self.connection is not None:
            self.connection.execute(
                "UPDATE virtual_positions SET mfe_pct = ?, mae_pct = ?, local_price_sol_per_token = ?, "
                "local_price_observed_at = ?, local_price_source = ?, local_return_pct = ? WHERE position_id = ?",
                (str(updated.mfe_pct), str(updated.mae_pct), str(price_sol_per_token), observed_at.isoformat(), source, str(return_pct), position_id),
            )
            self.connection.commit()
        self.record_event(position_id, "LOCAL_PRICE_OBSERVED", observed_at, {
            "price_sol_per_token": price_sol_per_token,
            "price_source": source,
            "return_pct": return_pct,
            "mfe_pct": updated.mfe_pct,
            "mae_pct": updated.mae_pct,
            "account_address": account_address,
            "observed_slot": observed_slot,
        })
        return PositionObservation(position_id, observed_at, None, return_pct, updated.mfe_pct, updated.mae_pct, True)

    def set_exit_holders_snapshot(
        self,
        position_id: str,
        holders: int | None,
        *,
        observed_at: datetime | None = None,
        source: str | None = None,
        status: str | None = None,
    ) -> VirtualPosition:
        """Persist holder value and collection status for an active/closed row."""

        if position_id not in self.positions and position_id not in self.closed_positions:
            raise KeyError(position_id)
        if holders is not None and (isinstance(holders, bool) or holders < 0):
            raise ValueError("exit holders must be a non-negative integer or None")
        if status not in {None, "pending", "completed", "unavailable"}:
            raise ValueError("invalid exit holders status")
        if status == "completed" and holders is None:
            raise ValueError("completed exit holders status requires a value")
        if status == "pending":
            holders = None
            observed_at = None
            source = None
        if status is None and holders is not None:
            status = "completed"
        current = self.positions.get(position_id) or self.closed_positions[position_id]
        updated = replace(
            current,
            exit_holders=holders,
            exit_holders_observed_at=observed_at,
            exit_holders_source=source,
            exit_holders_status=status,
        )
        if position_id in self.positions:
            self.positions[position_id] = updated
        else:
            self.closed_positions[position_id] = updated
        if self.connection is not None:
            self.connection.execute(
                "UPDATE virtual_positions SET exit_holders = ?, "
                "exit_holders_observed_at = ?, exit_holders_source = ?, "
                "exit_holders_status = ? WHERE position_id = ?",
                (
                    holders,
                    observed_at.isoformat() if observed_at is not None else None,
                    source,
                    status,
                    position_id,
                ),
            )
            self.connection.commit()
        return updated

    def set_exit_holders(self, position_id: str, holders: int | None) -> VirtualPosition:
        """Backward-compatible wrapper for an immediately available snapshot."""

        return self.set_exit_holders_snapshot(
            position_id,
            holders,
            source="memory_snapshot" if holders is not None else None,
            status="completed" if holders is not None else None,
        )

    def set_exit_market_snapshot(
        self,
        position_id: str,
        market_cap_usd: Decimal | None,
        liquidity_usd: Decimal | None,
        *,
        observed_at: datetime | None = None,
        source: str | None = None,
        status: str | None = None,
    ) -> VirtualPosition:
        """Persist a real or explicitly unavailable exit market snapshot."""

        if position_id not in self.positions and position_id not in self.closed_positions:
            raise KeyError(position_id)
        for name, value in (("market cap", market_cap_usd), ("liquidity", liquidity_usd)):
            if value is not None and (not value.is_finite() or value < 0):
                raise ValueError(f"exit {name} must be a non-negative finite value or None")
        if status not in {None, "pending", "completed", "unavailable"}:
            raise ValueError("invalid exit market status")
        if status == "completed" and market_cap_usd is None and liquidity_usd is None:
            raise ValueError("completed exit market status requires a value")
        if status == "pending":
            market_cap_usd = None
            liquidity_usd = None
            observed_at = None
            source = None
        if status is None and (market_cap_usd is not None or liquidity_usd is not None):
            status = "completed"
        current = self.positions.get(position_id) or self.closed_positions[position_id]
        updated = replace(
            current,
            exit_market_cap_usd=market_cap_usd,
            exit_liquidity_usd=liquidity_usd,
            exit_market_observed_at=observed_at,
            exit_market_source=source,
            exit_market_status=status,
        )
        if position_id in self.positions:
            self.positions[position_id] = updated
        else:
            self.closed_positions[position_id] = updated
        if self.connection is not None:
            self.connection.execute(
                "UPDATE virtual_positions SET exit_market_cap_usd = ?, "
                "exit_liquidity_usd = ?, exit_market_observed_at = ?, "
                "exit_market_source = ?, exit_market_status = ? WHERE position_id = ?",
                (
                    str(market_cap_usd) if market_cap_usd is not None else None,
                    str(liquidity_usd) if liquidity_usd is not None else None,
                    observed_at.isoformat() if observed_at is not None else None,
                    source,
                    status,
                    position_id,
                ),
            )
            self.connection.commit()
        return updated

    def close_position(
        self,
        position_id: str,
        closed_at: datetime | None = None,
        closed_reason: str | None = None,
        exit_quote_at: datetime | None = None,
        exit_price_snapshot: PriceSnapshot | None = None,
    ) -> VirtualPosition:
        position = self.positions.pop(position_id)
        closed_at = closed_at or position.last_observed_at or position.opened_at
        if exit_quote_at is None:
            exit_quote_at = position.exit_quote_at
        closed = replace(
            position,
            remaining_quantity_token=Decimal("0"),
            status="CLOSED",
            closed_at=closed_at,
            closed_reason=closed_reason,
            exit_quote_at=exit_quote_at,
            exit_price_snapshot=exit_price_snapshot,
        )
        self.closed_positions[position_id] = closed
        if self.connection is not None:
            self.connection.execute(
                "UPDATE virtual_positions SET status = ?, remaining_quantity_token = ?, "
                "closed_at = ?, closed_reason = ?, exit_quote_at = ?, "
                "exit_price_snapshot_json = ? WHERE position_id = ?",
                (
                    "CLOSED",
                    "0",
                    closed_at.isoformat(),
                    closed_reason,
                    exit_quote_at.isoformat() if exit_quote_at is not None else None,
                    _snapshot_json(exit_price_snapshot),
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
            "gross_pnl_pct, net_pnl_estimated_sol, net_pnl_is_estimated, recorded_at, "
            "pricing_mode, executable_quote, exit_status, pnl_status) "
            "VALUES (" + ", ".join("?" for _ in range(23)) + ") ",
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
                execution.pricing_mode,
                int(execution.executable_quote),
                execution.exit_status,
                execution.pnl_status,
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

    def update_shadow_outcome(self, outcome: ShadowOutcome) -> None:
        """Persist the completed post-exit comparison for one Shadow position."""
        replaced = False
        self.shadow_outcomes = [
            replace(item, **{
                "returns_after_exit_pct": outcome.returns_after_exit_pct,
                "paper_tp_reached": outcome.paper_tp_reached,
                "avoided_loss_pct": outcome.avoided_loss_pct,
                "missed_profit_pct": outcome.missed_profit_pct,
                "recorded_at": outcome.recorded_at,
            })
            if item.position_id == outcome.position_id
            else item
            for item in self.shadow_outcomes
        ]
        if any(item.position_id == outcome.position_id for item in self.shadow_outcomes):
            replaced = True
        if self.connection is None:
            if not replaced:
                self.shadow_outcomes.append(outcome)
            return
        returns_json = json.dumps(outcome.returns_after_exit_pct, default=_json_default)
        self.connection.execute(
            "UPDATE shadow_outcomes SET returns_after_exit_json = ?, paper_tp_reached = ?, "
            "avoided_loss_pct = ?, missed_profit_pct = ? WHERE position_id = ?",
            (
                returns_json,
                int(outcome.paper_tp_reached),
                str(outcome.avoided_loss_pct) if outcome.avoided_loss_pct is not None else None,
                str(outcome.missed_profit_pct) if outcome.missed_profit_pct is not None else None,
                outcome.position_id,
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
        sequence = len(self.lifecycle_events)
        if self.connection is not None:
            row = self.connection.execute(
                "SELECT COUNT(*) AS event_count FROM lifecycle_events WHERE mode = ?",
                (self.mode,),
            ).fetchone()
            if row is not None:
                sequence = max(sequence, int(row["event_count"]))
            while self.connection.execute(
                "SELECT 1 FROM lifecycle_events WHERE event_id = ? LIMIT 1",
                (f"{position_id}:{event_type}:{sequence}",),
            ).fetchone() is not None:
                sequence += 1
        event_id = f"{position_id}:{event_type}:{sequence}"
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
