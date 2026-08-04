"""Deterministic Paper/Shadow lifecycle simulation and isolated BSC Live engine."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from typing import Callable, Mapping
import uuid

from meme_system.domain.models import (
    Candidate,
    EntryDecision,
    EntryFeatures,
    ExecutionRecord,
    ExitDecision,
    RuleCheck,
    ShadowExitFeatures,
    ShadowOutcome,
    Signal,
    VirtualPosition,
    PriceSnapshot,
)
from meme_system.engines.ledger import SimulationLedger
from meme_system.strategies.baseline import BaselineStrategy


@dataclass(frozen=True)
class EntryResult:
    candidate: Candidate
    decision: EntryDecision
    position: VirtualPosition | None


@dataclass(frozen=True)
class ExitResult:
    position: VirtualPosition
    decision: ExitDecision
    closed_position: VirtualPosition | None
    outcome: ShadowOutcome | None = None


class DeterministicSimulation:
    def __init__(
        self,
        mode: str,
        strategy: BaselineStrategy | None = None,
        ledger: SimulationLedger | None = None,
        *,
        pricing_mode: str = "executable_quote",
        executable_quote: bool = True,
        live_executor: object | None = None,
        live_max_entries: int = 0,
    ) -> None:
        if mode not in {"paper", "shadow", "live"}:
            raise ValueError("mode must be paper, shadow, or live")
        if mode == "live" and live_executor is None:
            raise ValueError("live mode requires a BSC live executor")
        if live_max_entries < 0:
            raise ValueError("live_max_entries must be non-negative")
        self.mode = mode
        self.strategy = strategy or BaselineStrategy()
        self.ledger = ledger or SimulationLedger(mode=mode)
        self.pricing_mode = pricing_mode
        self.executable_quote = executable_quote
        self.live_executor = live_executor
        self.live_max_entries = live_max_entries
        self._live_exit_attempted: set[str] = set()
        if self.mode == "live" and self.ledger.connection is not None:
            rows = self.ledger.connection.execute(
                "SELECT position_id FROM executions WHERE mode = 'live' AND action = 'exit_attempt'"
            )
            self._live_exit_attempted.update(str(row[0]) for row in rows)

    def process_entry(
        self,
        signal: Signal,
        features: EntryFeatures,
        candidate_id: str,
        position_id: str,
        block_reason: str | None = None,
    ) -> EntryResult:
        decision = self.strategy.evaluate_entry(features)
        checks = list(decision.checks)
        if block_reason is not None:
            block_reason_zh = {
                "runtime_paused": "运行控制已暂停新入场",
                "observation_price_unavailable": "观察期后的价格不可用，拒绝开仓",
                "price_not_up_after_observation": "观察期后的价格未高于首次发现价格，跳过开仓",
                "price_below_after_observation": "观察期后的价格低于首次发现价格，跳过开仓",
                "observation_liquidity_unavailable": "观察期后的流动性不可用，拒绝开仓",
                "observation_liquidity_below_min": "观察期后的流动性低于最低值，拒绝开仓",
                "liquidity_below_first_discovery_after_observation": "观察期后的流动性低于首次发现值，跳过开仓",
                "holders_observation_unavailable": "观察期后的持币地址数不可用，拒绝开仓",
                "holders_below_first_discovery_after_observation": "观察期后的持币地址数低于首次发现值，跳过开仓",
            }.get(block_reason, "入场观察条件未满足，拒绝开仓")
            checks.append(
                self._check(
                    "runtime_entry_gate",
                    False,
                    block_reason,
                    "entry_enabled",
                    block_reason,
                    block_reason_zh,
                )
            )
        if decision.accepted and block_reason is None:
            checks.extend(self._lifecycle_checks(signal, features))
        accepted = all(check.passed for check in checks)
        final_decision = EntryDecision(
            accepted=accepted,
            identity=decision.identity,
            checks=tuple(checks),
            soft_features=decision.soft_features,
        )
        recorded_reasons = (
            *final_decision.unavailable_reason_codes,
            *final_decision.failed_reason_codes,
        )
        candidate = Candidate(
            candidate_id=candidate_id,
            signal_id=signal.signal_id,
            mint=signal.mint,
            identity=final_decision.identity,
            # A runtime pause is an operator control skip, not a strategy
            # rejection.  Keep it separate from rejection-rate analytics.
            status="ACCEPTED" if accepted else ("SKIPPED" if block_reason == "runtime_paused" else "REJECTED"),
            chain=signal.chain,
            filter_reason=",".join(recorded_reasons) if recorded_reasons else None,
            checks=final_decision.checks,
            soft_features=final_decision.soft_features,
        )
        self.ledger.record_signal(signal)
        self.ledger.record_candidate(candidate)
        if not accepted:
            return EntryResult(candidate=candidate, decision=final_decision, position=None)

        if self.mode == "live":
            assert self.live_executor is not None
            if self.live_max_entries and self._live_entry_count() >= self.live_max_entries:
                self.ledger.record_event(
                    position_id,
                    "LIVE_ENTRY_LIMIT_REACHED",
                    features.evaluated_at,
                    {
                        "mint": signal.mint,
                        "token_name": features.token_name,
                        "strategy_name": final_decision.identity.strategy_name,
                        "max_entries": self.live_max_entries,
                    },
                )
                return EntryResult(candidate=candidate, decision=final_decision, position=None)
            assert features.buy_quote is not None
            try:
                execution = self.live_executor.buy(
                    signal.mint,
                    features.buy_quote.input_quantity,
                    expected_quote=features.buy_quote,
                )
                actual_quote = execution.quote
                actual_received = execution.actual_received
            except Exception as exc:
                self.ledger.record_event(
                    position_id,
                    "LIVE_ENTRY_FAILED",
                    features.evaluated_at,
                    {
                        "mint": signal.mint,
                        "token_name": features.token_name,
                        "strategy_name": final_decision.identity.strategy_name,
                        "error_class": type(exc).__name__,
                    },
                )
                return EntryResult(candidate=candidate, decision=final_decision, position=None)
            if actual_received <= 0:
                self.ledger.record_event(
                    position_id,
                    "LIVE_ENTRY_FAILED",
                    features.evaluated_at,
                    {
                        "mint": signal.mint,
                        "token_name": features.token_name,
                        "strategy_name": final_decision.identity.strategy_name,
                        "error_class": "zero_actual_received",
                    },
                )
                return EntryResult(candidate=candidate, decision=final_decision, position=None)
            position = VirtualPosition(
                position_id=position_id,
                mint=signal.mint,
                mode=self.mode,
                identity=final_decision.identity,
                quantity_sol=actual_quote.input_quantity,
                opened_at=features.evaluated_at,
                entry_quantity_token=actual_received,
                remaining_quantity_token=actual_received,
                entry_quote_id=actual_quote.quote_id,
                token_name=features.token_name,
                entry_holders=features.holders,
                entry_liquidity_usd=features.liquidity_usd,
                status="ENTRY_PENDING",
            )
            self.ledger.open_position(position)
            position = self.ledger.transition_position(
                position_id,
                "OPEN",
                features.evaluated_at,
                {"quote_id": actual_quote.quote_id, "tx_hash": execution.tx_hash},
            )
            self.ledger.record_execution(
                ExecutionRecord(
                    execution_id=f"{position_id}:entry",
                    position_id=position_id,
                    mode=self.mode,
                    action="entry",
                    reason="entry_accepted",
                    quote_id=actual_quote.quote_id,
                    quote_age_ms=actual_quote.age_ms,
                    cost=None,
                    quote_input_quantity=actual_quote.input_quantity,
                    quote_output_quantity=actual_received,
                    price_impact_pct=actual_quote.price_impact_pct,
                    quote_quoted_at=actual_quote.quoted_at,
                    quote_source=actual_quote.quote_source or actual_quote.provider,
                    quote_route=actual_quote.route,
                    recorded_at=features.evaluated_at,
                    pricing_mode=self.pricing_mode,
                    executable_quote=True,
                )
            )
            self.ledger.record_event(
                position_id,
                "LIVE_ENTRY_CONFIRMED",
                features.evaluated_at,
                {
                    "mint": signal.mint,
                    "token_name": features.token_name,
                    "strategy_name": final_decision.identity.strategy_name,
                    "tx_hash": execution.tx_hash,
                    "input_quantity": actual_quote.input_quantity,
                    "actual_received": actual_received,
                    "settlement_verified": execution.settlement_verified,
                },
            )
            return EntryResult(candidate=candidate, decision=final_decision, position=position)

        assert features.buy_quote is not None
        entry_quote_at = self._quote_lifecycle_timestamp(features.buy_quote)
        opened_at = entry_quote_at or features.evaluated_at
        entry_price_snapshot = self.build_price_snapshot(
            features.buy_quote,
            native_symbol=self._native_symbol(),
            native_usd=features.native_usd,
            pricing_mode=self.pricing_mode,
            executable_quote=self.executable_quote,
            now=opened_at,
        )
        position = VirtualPosition(
            position_id=position_id,
            mint=signal.mint,
            mode=self.mode,
            identity=final_decision.identity,
            quantity_sol=self.strategy.config.position_size_sol,
            opened_at=opened_at,
            entry_quantity_token=features.buy_quote.output_quantity,
            remaining_quantity_token=features.buy_quote.output_quantity,
            entry_quote_id=features.buy_quote.quote_id,
            token_name=features.token_name,
            raw_name=features.raw_name,
            display_name=features.display_name or features.token_name,
            symbol=features.symbol,
            entry_price_snapshot=entry_price_snapshot,
            price_snapshot_version=(
                1 if self._native_symbol() == "SOL" and entry_price_snapshot is not None else 0
            ),
            entry_holders=features.holders,
            entry_liquidity_usd=features.liquidity_usd,
            status="ENTRY_PENDING",
            signal_observed_at=signal.observed_at,
            evaluated_at=features.evaluated_at,
            entry_quote_at=entry_quote_at,
        )
        self.ledger.open_position(position)
        position = self.ledger.transition_position(
            position_id,
            "OPEN",
            opened_at,
            {"quote_id": features.buy_quote.quote_id},
        )
        self.ledger.record_execution(
            ExecutionRecord(
                execution_id=f"{position_id}:entry",
                position_id=position_id,
                mode=self.mode,
                action="entry",
                reason="entry_accepted",
                quote_id=features.buy_quote.quote_id,
                quote_age_ms=features.buy_quote.age_ms,
                cost=None,
                quote_input_quantity=features.buy_quote.input_quantity,
                quote_output_quantity=features.buy_quote.output_quantity,
                price_impact_pct=features.buy_quote.price_impact_pct,
                quote_quoted_at=features.buy_quote.quoted_at,
                quote_source=features.buy_quote.quote_source or features.buy_quote.provider,
                quote_route=features.buy_quote.route,
                recorded_at=opened_at if entry_quote_at is not None else features.evaluated_at,
                pricing_mode=self.pricing_mode,
                executable_quote=self.executable_quote,
                price_snapshot=entry_price_snapshot,
            )
        )
        return EntryResult(candidate=candidate, decision=final_decision, position=position)

    def process_paper_exit(
        self,
        position_id: str,
        now: datetime,
        sell_quote,
        estimated_network_fee_sol: Decimal | None = None,
        estimated_priority_fee_sol: Decimal | None = None,
        exit_holders: int | None = None,
        exit_holders_loader: Callable[[], int | None] | None = None,
        price_snapshot: PriceSnapshot | None = None,
        pricing_mode: str | None = None,
    ) -> ExitResult:
        if self.mode != "paper":
            raise ValueError("process_paper_exit requires paper mode")
        resolved_pricing_mode = pricing_mode or self.pricing_mode
        position = self.ledger.positions[position_id]
        self.ledger.record_observation(position_id, now, sell_quote)
        position = self.ledger.positions[position_id]
        decision = self.strategy.evaluate_paper_exit(
            position,
            now,
            sell_quote,
            estimated_network_fee_sol,
            estimated_priority_fee_sol,
        )
        if self._is_bsc_executable_mode() and sell_quote is None:
            age_sec = max(0, int((now - position.opened_at).total_seconds()))
            unavailable_reason = None
            if age_sec >= self.strategy.config.max_hold_sec:
                unavailable_reason = "max_hold_timeout"
            elif position.last_return_pct is not None:
                if position.last_return_pct <= self.strategy.config.stop_loss_trigger_pct:
                    unavailable_reason = "stop_loss"
                elif position.last_return_pct >= self.strategy.config.take_profit_pct:
                    unavailable_reason = "take_profit"
            if unavailable_reason is not None:
                decision = ExitDecision(
                    triggered=True,
                    identity=position.identity,
                    reason=unavailable_reason,
                    position_age_sec=age_sec,
                    return_pct=position.last_return_pct,
                    quote=None,
                    cost=None,
                )
        if not decision.triggered:
            self._record_exit_attempt(position, decision, now, pricing_mode=resolved_pricing_mode)
            return ExitResult(position, decision, None)
        if decision.cost is None:
            self._record_exit_attempt(position, decision, now, pricing_mode=resolved_pricing_mode)
            if self._is_bsc_executable_mode():
                return self.process_bsc_unavailable_exit(
                    position_id,
                    now,
                    decision.reason or "sell_quote_unavailable",
                    exit_holders=exit_holders,
                    exit_holders_loader=exit_holders_loader,
                )
            return ExitResult(position, decision, None)
        if exit_holders is None and exit_holders_loader is not None:
            try:
                exit_holders = exit_holders_loader()
            except Exception:
                exit_holders = None
        if exit_holders is not None:
            self.ledger.set_exit_holders(position_id, exit_holders)
            position = self.ledger.positions[position_id]
        exit_quote_at = self._quote_lifecycle_timestamp(decision.quote)
        close_time = exit_quote_at or now
        snapshot = price_snapshot or self.build_price_snapshot(
            decision.quote,
            native_symbol=self._native_symbol(),
            pricing_mode=resolved_pricing_mode,
            executable_quote=self.executable_quote,
            now=close_time,
        )
        self._record_exit_attempt(
            position,
            decision,
            now,
            price_snapshot=snapshot if self._native_symbol() == "SOL" else None,
            pricing_mode=resolved_pricing_mode,
        )
        self.ledger.transition_position(
            position_id,
            "EXIT_TRIGGERED",
            close_time,
            {"reason": decision.reason},
        )
        closed = self.ledger.close_position(
            position_id,
            close_time,
            decision.reason,
            exit_quote_at=exit_quote_at,
            exit_price_snapshot=snapshot,
        )
        self._record_loss_counters(position, decision, now)
        return ExitResult(position, decision, closed)

    def process_bsc_unavailable_exit(
        self,
        position_id: str,
        now: datetime,
        reason: str,
        *,
        exit_holders: int | None = None,
        exit_holders_loader: Callable[[], int | None] | None = None,
    ) -> ExitResult:
        """Close a BSC lifecycle without inventing a price or PnL."""
        if not self._is_bsc_executable_mode() or self.mode not in {"paper", "shadow"}:
            raise ValueError("BSC unavailable exit requires BSC Paper/Shadow")
        position = self.ledger.positions[position_id]
        decision = ExitDecision(
            triggered=True,
            identity=position.identity,
            reason=reason,
            position_age_sec=max(0, int((now - position.opened_at).total_seconds())),
            return_pct=None,
            quote=None,
            cost=None,
        )
        if exit_holders is None and exit_holders_loader is not None:
            try:
                exit_holders = exit_holders_loader()
            except Exception:
                exit_holders = None
        if exit_holders is not None:
            self.ledger.set_exit_holders(position_id, exit_holders)
            position = self.ledger.positions[position_id]
        self.ledger.transition_position(
            position_id,
            "EXIT_TRIGGERED",
            now,
            {"reason": reason, "pnl_status": "unknown", "quote_unavailable": True},
        )
        closed = self.ledger.close_position(
            position_id,
            now,
            reason,
            exit_quote_at=None,
            exit_price_snapshot=None,
        )
        self.ledger.record_execution(
            ExecutionRecord(
                execution_id=f"{position_id}:exit:unknown:{uuid.uuid4().hex}",
                position_id=position_id,
                mode=self.mode,
                action="exit",
                reason=reason,
                quote_id=None,
                quote_age_ms=None,
                cost=None,
                recorded_at=now,
                pricing_mode="bsc_executable_quote",
                executable_quote=False,
                exit_status="valuation_unavailable",
                pnl_status="unknown",
                price_snapshot=None,
            )
        )
        outcome = None
        if self.mode == "shadow":
            outcome = ShadowOutcome(
                position_id=position_id,
                identity=position.identity,
                returns_after_exit_pct={window: None for window in (5, 15, 30, 60, 120)},
                paper_tp_reached=False,
                avoided_loss_pct=None,
                missed_profit_pct=None,
                recorded_at=now,
            )
            self.ledger.record_shadow_outcome(outcome)
        return ExitResult(position, decision, closed, outcome)

    def process_triggered_indicative_exit(
        self,
        position_id: str,
        now: datetime,
        reason: str,
        settlement_quote,
        *,
        pricing_mode: str,
        executable_quote: bool,
        local_observation: bool = False,
        price_snapshot: PriceSnapshot | None = None,
    ) -> ExitResult:
        """Settle a TP/SL trigger using Jupiter or a bounded local valuation."""
        if self.mode not in {"paper", "shadow"}:
            raise ValueError("indicative exit requires paper or shadow mode")
        position = self.ledger.positions[position_id]
        if settlement_quote is not None:
            if local_observation:
                if position.local_return_pct is None:
                    self.ledger.record_local_observation(
                        position_id,
                        now,
                        settlement_quote.output_quantity / settlement_quote.input_quantity,
                        account_address=None,
                    )
            else:
                self.ledger.record_observation(
                    position_id,
                    settlement_quote.received_at or settlement_quote.quoted_at or now,
                    settlement_quote,
                )
            position = self.ledger.positions[position_id]
        cost = self.strategy._cost_breakdown(position, settlement_quote, None, None) if settlement_quote is not None else None
        return_pct = cost.gross_pnl_pct if cost is not None else position.local_return_pct
        decision = ExitDecision(True, position.identity, reason, max(0, int((now - position.opened_at).total_seconds())), return_pct, settlement_quote, cost)
        self.ledger.transition_position(position_id, "EXIT_TRIGGERED", now, {"reason": reason, "pricing_mode": pricing_mode})
        exit_quote_at = self._quote_lifecycle_timestamp(settlement_quote)
        close_time = exit_quote_at or now
        snapshot = price_snapshot or self.build_price_snapshot(
            settlement_quote,
            native_symbol=self._native_symbol(),
            pricing_mode=pricing_mode,
            executable_quote=executable_quote,
            now=close_time,
        )
        closed = self.ledger.close_position(
            position_id,
            close_time,
            reason,
            exit_quote_at=exit_quote_at,
            exit_price_snapshot=snapshot,
        )
        self.ledger.record_execution(
            ExecutionRecord(
                execution_id=f"{position_id}:exit:triggered:{uuid.uuid4().hex}",
                position_id=position_id,
                mode=self.mode,
                action="exit",
                reason=reason,
                quote_id=settlement_quote.quote_id if settlement_quote is not None else None,
                quote_age_ms=settlement_quote.age_ms if settlement_quote is not None else None,
                cost=cost,
                quote_input_quantity=settlement_quote.input_quantity if settlement_quote is not None else None,
                quote_output_quantity=settlement_quote.output_quantity if settlement_quote is not None else None,
                price_impact_pct=settlement_quote.price_impact_pct if settlement_quote is not None else None,
                quote_quoted_at=settlement_quote.quoted_at if settlement_quote is not None else None,
                quote_source=(settlement_quote.quote_source or settlement_quote.provider) if settlement_quote is not None else None,
                quote_route=settlement_quote.route if settlement_quote is not None else (),
                recorded_at=now,
                pricing_mode=pricing_mode,
                executable_quote=executable_quote,
                exit_status="closed",
                pnl_status="estimated" if not executable_quote else "available",
                price_snapshot=snapshot,
            )
        )
        self._record_loss_counters(position, decision, now)
        outcome = None
        if self.mode == "shadow":
            outcome = ShadowOutcome(
                position_id=position_id,
                identity=position.identity,
                returns_after_exit_pct={window: None for window in (5, 15, 30, 60, 120)},
                paper_tp_reached=return_pct is not None and return_pct >= self.strategy.config.take_profit_pct,
                avoided_loss_pct=None,
                missed_profit_pct=None,
                recorded_at=now,
            )
            self.ledger.record_shadow_outcome(outcome)
        return ExitResult(position, decision, closed, outcome)

    def process_timeout_exit(
        self,
        position_id: str,
        now: datetime,
        sell_quote,
        indicative_quote=None,
        *,
        exit_holders: int | None = None,
        price_snapshot: PriceSnapshot | None = None,
        fallback_price_snapshot: PriceSnapshot | None = None,
        pricing_mode: str | None = None,
    ) -> ExitResult:
        """Close a Solana Paper/Shadow position on the hard max-hold deadline.

        The coordinator calls this path as soon as the configured max-hold age
        is reached.  It records EXIT_TRIGGERED before waiting for a usable
        Jupiter quote, never lets quote errors extend the deadline, and closes
        by the hard deadline with an indicative valuation or an explicit
        valuation-unavailable result.
        """
        if self.mode not in {"paper", "shadow"}:
            raise ValueError("process_timeout_exit requires paper or shadow mode")
        position = self.ledger.positions[position_id]
        age_sec = max(0, int((now - position.opened_at).total_seconds()))
        max_hold_sec = self.strategy.config.max_hold_sec
        if age_sec < max_hold_sec:
            raise ValueError("position has not reached max hold")

        if position.status != "EXIT_TRIGGERED":
            position = self.ledger.transition_position(
                position_id,
                "EXIT_TRIGGERED",
                now,
                {
                    "reason": "max_hold_timeout",
                    "max_hold_sec": max_hold_sec,
                },
            )

        jupiter_quote = self._usable_timeout_quote(position, sell_quote, now)
        if jupiter_quote is None and age_sec < max_hold_sec + 10:
            decision = ExitDecision(
                triggered=True,
                identity=position.identity,
                reason="max_hold_timeout",
                position_age_sec=age_sec,
                return_pct=position.last_return_pct,
                quote=sell_quote,
                cost=None,
            )
            return ExitResult(position, decision, None)

        quote = jupiter_quote
        pricing_mode = pricing_mode or self.pricing_mode
        executable_quote = self.executable_quote
        exit_status = "closed"
        pnl_status = "estimated"
        if quote is None:
            quote = self._usable_timeout_quote(position, indicative_quote, now)
            pricing_mode = "indicative_timeout_fallback"
            executable_quote = False
        if quote is None:
            exit_status = "valuation_unavailable"
            pnl_status = "unknown"
            pricing_mode = "valuation_unavailable"
            executable_quote = False
            decision = ExitDecision(
                triggered=True,
                identity=position.identity,
                reason="max_hold_timeout",
                position_age_sec=age_sec,
                return_pct=None,
                quote=None,
                cost=None,
            )
        else:
            self.ledger.record_observation(position_id, now, quote)
            position = self.ledger.positions[position_id]
            cost = self.strategy._cost_breakdown(position, quote, None, None)
            decision = ExitDecision(
                triggered=True,
                identity=position.identity,
                reason="max_hold_timeout",
                position_age_sec=age_sec,
                return_pct=cost.gross_pnl_pct,
                quote=quote,
                cost=cost,
            )

        if exit_holders is not None:
            self.ledger.set_exit_holders(position_id, exit_holders)
            position = self.ledger.positions[position_id]
        self.ledger.record_event(
            position_id,
            "TIMEOUT_EXIT_SETTLED",
            now,
            {
                "reason": "max_hold_timeout",
                "exit_status": exit_status,
                "pnl_status": pnl_status,
                "pricing_mode": pricing_mode,
                "executable_quote": executable_quote,
                "net_pnl_is_estimated": (
                    decision.cost.net_pnl_is_estimated if decision.cost is not None else True
                ),
            },
        )
        exit_quote_at = self._quote_lifecycle_timestamp(quote)
        close_time = exit_quote_at or now
        selected_snapshot = price_snapshot if quote is jupiter_quote else fallback_price_snapshot
        snapshot = selected_snapshot or self.build_price_snapshot(
            quote,
            native_symbol=self._native_symbol(),
            pricing_mode=pricing_mode,
            executable_quote=executable_quote,
            now=close_time,
        )
        closed = self.ledger.close_position(
            position_id,
            close_time,
            "max_hold_timeout",
            exit_quote_at=exit_quote_at,
            exit_price_snapshot=snapshot,
        )
        self.ledger.record_execution(
            ExecutionRecord(
                execution_id=f"{position_id}:exit:timeout:{uuid.uuid4().hex}",
                position_id=position_id,
                mode=self.mode,
                action="exit",
                reason="max_hold_timeout",
                quote_id=quote.quote_id if quote is not None else None,
                quote_age_ms=quote.age_ms if quote is not None else None,
                cost=decision.cost,
                quote_input_quantity=quote.input_quantity if quote is not None else None,
                quote_output_quantity=quote.output_quantity if quote is not None else None,
                price_impact_pct=quote.price_impact_pct if quote is not None else None,
                quote_quoted_at=quote.quoted_at if quote is not None else None,
                quote_source=(quote.quote_source or quote.provider) if quote is not None else None,
                quote_route=quote.route if quote is not None else (),
                recorded_at=now,
                pricing_mode=pricing_mode,
                executable_quote=executable_quote,
                exit_status=exit_status,
                pnl_status=pnl_status,
                price_snapshot=snapshot,
            )
        )
        self._record_loss_counters(position, decision, now)

        outcome: ShadowOutcome | None = None
        if self.mode == "shadow":
            outcome = ShadowOutcome(
                position_id=position_id,
                identity=position.identity,
                returns_after_exit_pct={
                    window: None for window in (5, 15, 30, 60, 120)
                },
                paper_tp_reached=(
                    decision.return_pct is not None
                    and decision.return_pct >= self.strategy.config.take_profit_pct
                ),
                avoided_loss_pct=None,
                missed_profit_pct=None,
                recorded_at=now,
            )
            self.ledger.record_shadow_outcome(outcome)
        return ExitResult(position, decision, closed, outcome)

    @staticmethod
    def _usable_timeout_quote(position: VirtualPosition, quote, now: datetime):
        if quote is None:
            return None
        if (
            quote.mint != position.mint
            or quote.side != "sell"
            or quote.input_quantity != position.active_quantity_token
            or quote.output_quantity <= 0
            or quote.unusable_reason(now) is not None
        ):
            return None
        # A local pool observation is useful for prompt trigger detection but
        # cannot value a timeout after five seconds. Binance is only a bounded
        # fallback and expires after ten seconds. Jupiter retains its own
        # quote-expiry validation above.
        if quote.quoted_at is not None:
            try:
                age_sec = max(0, (now - quote.quoted_at).total_seconds())
            except (TypeError, ValueError):
                return None
            if quote.provider == "pool_wss_indicative" and age_sec > 5:
                return None
            if quote.provider == "binance_web3" and age_sec > 10:
                return None
        return quote

    def process_live_exit(
        self,
        position_id: str,
        now: datetime,
        sell_quote,
        exit_holders: int | None = None,
    ) -> ExitResult:
        if self.mode != "live":
            raise ValueError("process_live_exit requires live mode")
        position = self.ledger.positions[position_id]
        self.ledger.record_observation(position_id, now, sell_quote)
        position = self.ledger.positions[position_id]
        decision = self.strategy.evaluate_paper_exit(position, now, sell_quote)
        if exit_holders is not None:
            self.ledger.set_exit_holders(position_id, exit_holders)
            position = self.ledger.positions[position_id]
        if not decision.triggered or decision.cost is None:
            return ExitResult(position, decision, None)
        if position_id in self._live_exit_attempted:
            return ExitResult(position, decision, None)
        self._live_exit_attempted.add(position_id)
        self._record_exit_attempt(position, decision, now)
        assert self.live_executor is not None
        try:
            execution = self.live_executor.sell(
                position.mint,
                position.active_quantity_token,
                expected_quote=sell_quote,
            )
        except Exception as exc:
            self.ledger.record_event(
                position_id,
                "LIVE_EXIT_FAILED",
                now,
                {
                    "mint": position.mint,
                    "token_name": position.token_name,
                    "strategy_name": position.identity.strategy_name,
                    "reason": decision.reason,
                    "error_class": type(exc).__name__,
                },
            )
            return ExitResult(position, decision, None)
        actual_quote = replace(
            sell_quote,
            quote_id=f"{sell_quote.quote_id}:receipt",
            output_quantity=execution.actual_received,
        )
        actual_cost = self.strategy._cost_breakdown(
            position,
            actual_quote,
            execution.gas_fee_native,
            None,
        )
        actual_decision = replace(
            decision,
            quote=actual_quote,
            return_pct=actual_cost.gross_pnl_pct,
            cost=actual_cost,
        )
        self.ledger.transition_position(
            position_id,
            "EXIT_TRIGGERED",
            now,
            {"reason": decision.reason, "tx_hash": execution.tx_hash},
        )
        closed = self.ledger.close_position(position_id, now, decision.reason)
        self.ledger.record_execution(
            ExecutionRecord(
                execution_id=f"{position_id}:exit:confirmed",
                position_id=position_id,
                mode=self.mode,
                action="exit",
                reason=decision.reason or "exit",
                quote_id=actual_quote.quote_id,
                quote_age_ms=actual_quote.age_ms,
                cost=actual_cost,
                quote_input_quantity=actual_quote.input_quantity,
                quote_output_quantity=actual_quote.output_quantity,
                price_impact_pct=actual_quote.price_impact_pct,
                quote_quoted_at=actual_quote.quoted_at,
                quote_source=actual_quote.quote_source or actual_quote.provider,
                quote_route=actual_quote.route,
                recorded_at=now,
                pricing_mode=self.pricing_mode,
                executable_quote=True,
            )
        )
        self.ledger.record_event(
            position_id,
            "LIVE_EXIT_CONFIRMED",
            now,
            {
                "mint": position.mint,
                "token_name": position.token_name,
                "strategy_name": position.identity.strategy_name,
                "reason": decision.reason,
                "tx_hash": execution.tx_hash,
                "actual_received": execution.actual_received,
                "return_pct": actual_decision.return_pct,
                "settlement_verified": execution.settlement_verified,
            },
        )
        self._record_loss_counters(position, actual_decision, now)
        return ExitResult(position, actual_decision, closed)

    def process_shadow_exit(
        self,
        position_id: str,
        features: ShadowExitFeatures,
        now: datetime,
        sell_quote,
        returns_after_exit_pct: Mapping[int, Decimal | None] | None = None,
        avoided_loss_pct: Decimal | None = None,
        missed_profit_pct: Decimal | None = None,
        estimated_network_fee_sol: Decimal | None = None,
        estimated_priority_fee_sol: Decimal | None = None,
        exit_holders: int | None = None,
        exit_holders_loader: Callable[[], int | None] | None = None,
        price_snapshot: PriceSnapshot | None = None,
        pricing_mode: str | None = None,
    ) -> ExitResult:
        if self.mode != "shadow":
            raise ValueError("process_shadow_exit requires shadow mode")
        resolved_pricing_mode = pricing_mode or self.pricing_mode
        position = self.ledger.positions[position_id]
        self.ledger.record_observation(position_id, now, sell_quote)
        position = self.ledger.positions[position_id]
        features = replace(features, mfe_pct=position.mfe_pct)
        decision = self.strategy.evaluate_shadow_exit(
            position,
            features,
            now,
            sell_quote,
            estimated_network_fee_sol,
            estimated_priority_fee_sol,
        )
        if (
            self._is_bsc_executable_mode()
            and sell_quote is None
            and features.position_age_sec >= self.strategy.config.max_hold_sec
        ):
            decision = ExitDecision(
                triggered=True,
                identity=position.identity,
                reason="max_hold_timeout",
                position_age_sec=features.position_age_sec,
                return_pct=None,
                quote=None,
                cost=None,
            )
        if not decision.triggered:
            self._record_exit_attempt(position, decision, now, pricing_mode=resolved_pricing_mode)
            return ExitResult(position, decision, None)
        if decision.cost is None:
            self._record_exit_attempt(position, decision, now, pricing_mode=resolved_pricing_mode)
            if self._is_bsc_executable_mode() and decision.reason not in {
                "sell_quote_unavailable",
                "quote_expired",
                "no_route",
                "no_liquidity",
                "sell_quote_quantity_mismatch",
            }:
                return self.process_bsc_unavailable_exit(
                    position_id,
                    now,
                    decision.reason or "exit_triggered",
                    exit_holders=exit_holders,
                    exit_holders_loader=exit_holders_loader,
                )
            return ExitResult(position, decision, None)
        if exit_holders is None and exit_holders_loader is not None:
            try:
                exit_holders = exit_holders_loader()
            except Exception:
                exit_holders = None
        if exit_holders is not None:
            self.ledger.set_exit_holders(position_id, exit_holders)
            position = self.ledger.positions[position_id]
        self.ledger.transition_position(
            position_id,
            "EXIT_TRIGGERED",
            now,
            {"reason": decision.reason},
        )
        exit_quote_at = self._quote_lifecycle_timestamp(decision.quote)
        close_time = exit_quote_at or now
        snapshot = price_snapshot or self.build_price_snapshot(
            decision.quote,
            native_symbol=self._native_symbol(),
            pricing_mode=resolved_pricing_mode,
            executable_quote=self.executable_quote,
            now=close_time,
        )
        self._record_exit_attempt(
            position,
            decision,
            now,
            price_snapshot=snapshot if self._native_symbol() == "SOL" else None,
            pricing_mode=resolved_pricing_mode,
        )
        closed = self.ledger.close_position(
            position_id,
            close_time,
            decision.reason,
            exit_quote_at=exit_quote_at,
            exit_price_snapshot=snapshot,
        )
        snapshot = self.ledger.closed_positions[position_id].exit_price_snapshot
        returns = {
            window: (returns_after_exit_pct or {}).get(window)
            for window in (5, 15, 30, 60, 120)
        }
        paper_tp_reached = any(
            value is not None and value >= self.strategy.config.take_profit_pct
            for value in returns.values()
        ) or (
            decision.return_pct is not None
            and decision.return_pct >= self.strategy.config.take_profit_pct
        )
        outcome = ShadowOutcome(
            position_id=position_id,
            identity=position.identity,
            returns_after_exit_pct=returns,
            paper_tp_reached=paper_tp_reached,
                avoided_loss_pct=avoided_loss_pct,
                missed_profit_pct=missed_profit_pct,
                recorded_at=now,
            )
        self.ledger.record_shadow_outcome(outcome)
        return ExitResult(position, decision, closed, outcome)

    def _lifecycle_checks(
        self,
        signal: Signal,
        features: EntryFeatures,
    ) -> list[RuleCheck]:
        config = self.strategy.config
        native_symbol = "BNB" if config.identity.strategy_name == "bsc_binance_indicative" else "SOL"
        daily_loss_check = "daily_full_loss_bnb" if native_symbol == "BNB" else "daily_full_loss_sol"
        checks = [
            self._check(
                "buy_quote_mint",
                features.buy_quote is not None
                and features.buy_quote.mint == signal.mint,
                features.buy_quote.mint if features.buy_quote is not None else None,
                signal.mint,
                "buy_quote_mint_mismatch",
                "买入报价 Mint 与信号不一致",
            ),
            self._check(
                "sell_quote_mint",
                features.sell_quote is not None
                and features.sell_quote.mint == signal.mint,
                features.sell_quote.mint if features.sell_quote is not None else None,
                signal.mint,
                "sell_quote_mint_mismatch",
                "卖出报价 Mint 与信号不一致",
            ),
            self._check(
                "max_open_positions",
                len(self.ledger.active_positions) < config.max_open_positions,
                len(self.ledger.active_positions),
                f"< {config.max_open_positions}",
                "max_open_positions_reached",
                "已达到最大持仓数",
            ),
            self._check(
                "one_trade_per_mint",
                not (
                    config.one_trade_per_mint
                    and self.ledger.has_mint_lifecycle(signal.mint, config.identity)
                ),
                signal.mint,
                "not_seen",
                "mint_lifecycle_exists",
                "该 Mint 已存在生命周期",
            ),
            self._check(
                "same_name_cooldown",
                not self.ledger.name_in_cooldown(
                    features.token_name,
                    features.evaluated_at,
                    config.same_name_cooldown_sec,
                ),
                features.token_name,
                config.same_name_cooldown_sec,
                "same_name_cooldown",
                "同名代币仍在冷却期",
            ),
            self._check(
                "large_loss_pause",
                self.ledger.large_loss_count < config.pause_new_entries_after_large_losses,
                self.ledger.large_loss_count,
                f"< {config.pause_new_entries_after_large_losses}",
                "large_loss_pause",
                "大亏损次数已触发暂停开仓",
            ),
            self._check(
                daily_loss_check,
                self.ledger.daily_full_loss_sol(features.evaluated_at) < config.daily_full_loss_sol_limit,
                self.ledger.daily_full_loss_sol(features.evaluated_at),
                f"< {config.daily_full_loss_sol_limit} {native_symbol}",
                "daily_full_loss_limit",
                "每日完整亏损金额已触发日限制",
            ),
        ]
        return checks

    def _native_symbol(self) -> str:
        return "BNB" if self.strategy.config.identity.strategy_name == "bsc_binance_indicative" else "SOL"

    @staticmethod
    def build_price_snapshot(
        quote,
        *,
        native_symbol: str,
        native_usd: Decimal | None = None,
        pricing_mode: str = "executable_quote",
        executable_quote: bool = True,
        now: datetime | None = None,
    ) -> PriceSnapshot | None:
        if quote is None or quote.input_quantity <= 0 or quote.output_quantity <= 0:
            return None
        if quote.side == "buy":
            price_native = quote.input_quantity / quote.output_quantity
        elif quote.side == "sell":
            price_native = quote.output_quantity / quote.input_quantity
        else:
            return None
        source = {
            "binance_indicative": "binance_indicative",
            "pool_wss_indicative": "pool_wss",
            "pump_bonding_curve_quote": "pump_bonding_curve_quote",
            "indicative_timeout_fallback": "timeout_fallback",
            "bsc_executable_quote": quote.quote_source or quote.provider,
        }.get(pricing_mode, "jupiter_quote" if executable_quote else pricing_mode)
        if quote.provider == "pool_wss_indicative":
            source = "pool_wss"
        elif quote.provider == "binance_web3":
            source = "binance_indicative"
        parsed_native_usd = native_usd if native_usd is not None and native_usd > 0 else None
        price_usd = price_native * parsed_native_usd if parsed_native_usd is not None else None
        age_ms: int | None = None
        if now is not None and quote.quoted_at is not None:
            try:
                age_ms = max(0, int((now - quote.quoted_at).total_seconds() * 1000))
            except (TypeError, ValueError):
                age_ms = quote.age_ms
        else:
            age_ms = quote.age_ms
        return PriceSnapshot(
            price_native=price_native,
            native_symbol=native_symbol,
            native_usd=parsed_native_usd,
            price_usd=price_usd,
            price_source=source,
            quoted_at=quote.quoted_at,
            executable_quote=bool(executable_quote),
            estimated=not bool(executable_quote),
            price_age_ms=age_ms,
        )

    def _uses_bsc_quote_lifecycle(self) -> bool:
        """Keep this compatibility predicate for callers that inspect it."""

        return self._is_bsc_executable_mode()

    def _is_bsc_executable_mode(self) -> bool:
        return self.mode in {"paper", "shadow"} and self.pricing_mode == "bsc_executable_quote"

    def _quote_lifecycle_timestamp(self, quote) -> datetime | None:
        if quote is None:
            return None
        return quote.quoted_at

    def _record_exit_attempt(
        self,
        position: VirtualPosition,
        decision: ExitDecision,
        recorded_at: datetime,
        price_snapshot: PriceSnapshot | None = None,
        pricing_mode: str | None = None,
    ) -> None:
        if decision.reason is None:
            return
        quote = decision.quote
        self.ledger.record_execution(
            ExecutionRecord(
                execution_id=f"{position.position_id}:exit:{recorded_at.isoformat()}:{uuid.uuid4().hex}",
                position_id=position.position_id,
                mode=self.mode,
                action=(
                    "exit_attempt"
                    if self.mode == "live"
                    else "exit" if decision.cost is not None else "exit_attempt"
                ),
                reason=decision.reason,
                quote_id=quote.quote_id if quote is not None else None,
                quote_age_ms=quote.age_ms if quote is not None else None,
                cost=decision.cost,
                quote_input_quantity=quote.input_quantity if quote is not None else None,
                quote_output_quantity=quote.output_quantity if quote is not None else None,
                price_impact_pct=quote.price_impact_pct if quote is not None else None,
                quote_quoted_at=quote.quoted_at if quote is not None else None,
                quote_source=(quote.quote_source or quote.provider) if quote is not None else None,
                quote_route=quote.route if quote is not None else (),
                recorded_at=recorded_at,
                pricing_mode=pricing_mode or self.pricing_mode,
                executable_quote=self.executable_quote,
                price_snapshot=price_snapshot,
            )
        )

    def _live_entry_count(self) -> int:
        if self.ledger.connection is None:
            return sum(1 for execution in self.ledger.executions if execution.action == "entry")
        row = self.ledger.connection.execute(
            "SELECT COUNT(*) FROM executions WHERE mode = 'live' AND action = 'entry'"
        ).fetchone()
        return int(row[0]) if row is not None else 0

    def _record_loss_counters(self, position: VirtualPosition, decision: ExitDecision, recorded_at: datetime) -> None:
        if decision.cost is None:
            return
        if decision.cost.gross_pnl_pct <= self.strategy.config.large_loss_threshold_pct:
            self.ledger.large_loss_count += 1
        if decision.cost.gross_pnl_pct <= Decimal("-1"):
            self.ledger.record_full_loss(recorded_at, position.quantity_sol)

    @staticmethod
    def _check(
        name: str,
        passed: bool,
        actual: object,
        threshold: object,
        reason_code: str,
        reason_zh: str,
    ) -> RuleCheck:
        return RuleCheck(
            name=name,
            passed=passed,
            actual=actual,
            threshold=threshold,
            reason_code=None if passed else reason_code,
            reason_zh=None if passed else reason_zh,
        )
