"""Deterministic Paper and Shadow lifecycle simulation."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from typing import Mapping

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
    ) -> None:
        if mode not in {"paper", "shadow"}:
            raise ValueError("mode must be paper or shadow")
        self.mode = mode
        self.strategy = strategy or BaselineStrategy()
        self.ledger = ledger or SimulationLedger(mode=mode)

    def process_entry(
        self,
        signal: Signal,
        features: EntryFeatures,
        candidate_id: str,
        position_id: str,
    ) -> EntryResult:
        decision = self.strategy.evaluate_entry(features)
        checks = list(decision.checks)
        if decision.accepted:
            checks.extend(self._lifecycle_checks(signal, features))
        accepted = all(check.passed for check in checks)
        final_decision = EntryDecision(
            accepted=accepted,
            identity=decision.identity,
            checks=tuple(checks),
            soft_features=decision.soft_features,
        )
        failed_reasons = final_decision.failed_reason_codes
        candidate = Candidate(
            candidate_id=candidate_id,
            signal_id=signal.signal_id,
            mint=signal.mint,
            identity=final_decision.identity,
            status="ACCEPTED" if accepted else "REJECTED",
            filter_reason=",".join(failed_reasons) if failed_reasons else None,
            checks=final_decision.checks,
            soft_features=final_decision.soft_features,
        )
        self.ledger.record_signal(signal)
        self.ledger.record_candidate(candidate)
        if not accepted:
            return EntryResult(candidate=candidate, decision=final_decision, position=None)

        assert features.buy_quote is not None
        position = VirtualPosition(
            position_id=position_id,
            mint=signal.mint,
            mode=self.mode,
            identity=final_decision.identity,
            quantity_sol=self.strategy.config.position_size_sol,
            opened_at=features.evaluated_at,
            entry_quantity_token=features.buy_quote.output_quantity,
            remaining_quantity_token=features.buy_quote.output_quantity,
            entry_quote_id=features.buy_quote.quote_id,
            token_name=features.token_name,
            status="ENTRY_PENDING",
        )
        self.ledger.open_position(position)
        position = self.ledger.transition_position(
            position_id,
            "OPEN",
            features.evaluated_at,
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
                recorded_at=features.evaluated_at,
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
    ) -> ExitResult:
        if self.mode != "paper":
            raise ValueError("process_paper_exit requires paper mode")
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
        self._record_exit_attempt(position, decision, now)
        if not decision.triggered or decision.cost is None:
            return ExitResult(position, decision, None)
        self.ledger.transition_position(
            position_id,
            "EXIT_TRIGGERED",
            now,
            {"reason": decision.reason},
        )
        closed = self.ledger.close_position(position_id, now, decision.reason)
        self._record_loss_counters(decision)
        return ExitResult(position, decision, closed)

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
    ) -> ExitResult:
        if self.mode != "shadow":
            raise ValueError("process_shadow_exit requires shadow mode")
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
        self._record_exit_attempt(position, decision, now)
        if not decision.triggered or decision.cost is None:
            return ExitResult(position, decision, None)
        self.ledger.transition_position(
            position_id,
            "EXIT_TRIGGERED",
            now,
            {"reason": decision.reason},
        )
        closed = self.ledger.close_position(position_id, now, decision.reason)
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
                "daily_full_loss_units",
                self.ledger.full_loss_units < config.daily_full_loss_units_limit,
                self.ledger.full_loss_units,
                f"< {config.daily_full_loss_units_limit}",
                "daily_full_loss_limit",
                "完整仓位亏损单位已触发日限制",
            ),
        ]
        return checks

    def _record_exit_attempt(
        self,
        position: VirtualPosition,
        decision: ExitDecision,
        recorded_at: datetime,
    ) -> None:
        if decision.reason is None:
            return
        quote = decision.quote
        self.ledger.record_execution(
            ExecutionRecord(
                execution_id=f"{position.position_id}:exit:{len(self.ledger.executions)}",
                position_id=position.position_id,
                mode=self.mode,
                action="exit" if decision.cost is not None else "exit_attempt",
                reason=decision.reason,
                quote_id=quote.quote_id if quote is not None else None,
                quote_age_ms=quote.age_ms if quote is not None else None,
                cost=decision.cost,
                quote_input_quantity=quote.input_quantity if quote is not None else None,
                quote_output_quantity=quote.output_quantity if quote is not None else None,
                price_impact_pct=quote.price_impact_pct if quote is not None else None,
                quote_quoted_at=quote.quoted_at if quote is not None else None,
                recorded_at=recorded_at,
            )
        )

    def _record_loss_counters(self, decision: ExitDecision) -> None:
        if decision.cost is None:
            return
        if decision.cost.gross_pnl_pct <= self.strategy.config.large_loss_threshold_pct:
            self.ledger.large_loss_count += 1
        if decision.cost.gross_pnl_pct <= Decimal("-1"):
            self.ledger.full_loss_units += 1

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
