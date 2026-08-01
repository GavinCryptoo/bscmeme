"""Mode-neutral domain models for deterministic Paper and Shadow simulation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Mapping

if TYPE_CHECKING:
    from meme_system.adapters.protocols import ExecutableQuote


@dataclass(frozen=True)
class StrategyIdentity:
    strategy_name: str
    ruleset_name: str
    ruleset_version: str
    config_version: str

    def as_dict(self) -> dict[str, str]:
        return {
            "strategy_name": self.strategy_name,
            "ruleset_name": self.ruleset_name,
            "ruleset_version": self.ruleset_version,
            "config_version": self.config_version,
        }

    def lifecycle_key(self, mint: str) -> tuple[str, str, str]:
        return (mint, self.strategy_name, self.ruleset_version)


BASELINE_IDENTITY = StrategyIdentity(
    strategy_name="sol_ultra_early_baseline",
    ruleset_name="ultra_early_minimal",
    ruleset_version="0.1.0",
    config_version="0.1.0",
)


@dataclass(frozen=True)
class Signal:
    signal_id: str
    mint: str
    observed_at: datetime
    source: str


@dataclass(frozen=True)
class Candidate:
    candidate_id: str
    signal_id: str
    mint: str
    identity: StrategyIdentity
    status: str
    filter_reason: str | None = None
    checks: tuple["RuleCheck", ...] = ()
    soft_features: Mapping[str, object] | None = None


@dataclass(frozen=True)
class VirtualPosition:
    position_id: str
    mint: str
    mode: str
    identity: StrategyIdentity
    quantity_sol: Decimal
    opened_at: datetime
    entry_quantity_token: Decimal = Decimal("0")
    remaining_quantity_token: Decimal = Decimal("0")
    entry_quote_id: str | None = None
    token_name: str | None = None
    status: str = "OPEN"
    mfe_pct: Decimal = Decimal("0")
    mae_pct: Decimal = Decimal("0")
    last_return_pct: Decimal | None = None
    last_observed_at: datetime | None = None
    last_quote_id: str | None = None
    closed_at: datetime | None = None
    closed_reason: str | None = None

    @property
    def active_quantity_token(self) -> Decimal:
        if self.remaining_quantity_token == Decimal("0"):
            return self.entry_quantity_token
        return self.remaining_quantity_token


@dataclass(frozen=True)
class ExitEvent:
    exit_id: str
    position_id: str
    mode: str
    identity: StrategyIdentity
    reason: str
    observed_at: datetime


@dataclass(frozen=True)
class RuleCheck:
    name: str
    passed: bool
    actual: object
    threshold: object
    reason_code: str | None = None
    reason_zh: str | None = None


@dataclass(frozen=True)
class EntryFeatures:
    token_age_sec: int
    unique_buyers_15s: int
    buy_sell_count_ratio_15s: Decimal
    net_buy_15s: Decimal
    flow_windows_non_negative: tuple[bool, bool]
    creator_confirmed_sold: bool
    buy_quote: "ExecutableQuote | None"
    sell_quote: "ExecutableQuote | None"
    evaluated_at: datetime
    token_name: str | None = None
    soft_features: Mapping[str, object] | None = None


@dataclass(frozen=True)
class EntryDecision:
    accepted: bool
    identity: StrategyIdentity
    checks: tuple[RuleCheck, ...]
    soft_features: Mapping[str, object]

    @property
    def failed_reason_codes(self) -> tuple[str, ...]:
        return tuple(
            check.reason_code
            for check in self.checks
            if not check.passed and check.reason_code is not None
        )


@dataclass(frozen=True)
class CostBreakdown:
    gross_pnl_sol: Decimal
    gross_pnl_pct: Decimal
    route_fee_sol: Decimal | None
    estimated_network_fee_sol: Decimal | None
    estimated_priority_fee_sol: Decimal | None
    net_pnl_estimated_sol: Decimal
    net_pnl_is_estimated: bool


@dataclass(frozen=True)
class ExitDecision:
    triggered: bool
    identity: StrategyIdentity
    reason: str | None
    position_age_sec: int
    return_pct: Decimal | None
    quote: "ExecutableQuote | None"
    cost: CostBreakdown | None


@dataclass(frozen=True)
class ShadowExitFeatures:
    position_age_sec: int
    return_pct: Decimal
    mfe_pct: Decimal
    recent_net_flow_negative: bool
    independent_buyer_growth_stopped: bool
    creator_sell_confident: bool
    buyer_growth_and_flow_slowed: bool


@dataclass(frozen=True)
class ShadowOutcome:
    position_id: str
    identity: StrategyIdentity
    returns_after_exit_pct: Mapping[int, Decimal | None]
    paper_tp_reached: bool
    avoided_loss_pct: Decimal | None
    missed_profit_pct: Decimal | None
    recorded_at: datetime | None = None


@dataclass(frozen=True)
class ExecutionRecord:
    execution_id: str
    position_id: str
    mode: str
    action: str
    reason: str
    quote_id: str | None
    quote_age_ms: int | None
    cost: CostBreakdown | None
    quote_input_quantity: Decimal | None = None
    quote_output_quantity: Decimal | None = None
    price_impact_pct: Decimal | None = None
    quote_quoted_at: datetime | None = None
    recorded_at: datetime | None = None


@dataclass(frozen=True)
class PositionObservation:
    position_id: str
    observed_at: datetime
    quote_id: str | None
    return_pct: Decimal | None
    mfe_pct: Decimal | None
    mae_pct: Decimal | None
    valid: bool
    reason: str | None = None


@dataclass(frozen=True)
class LifecycleEvent:
    event_id: str
    position_id: str
    mode: str
    event_type: str
    occurred_at: datetime
    payload: Mapping[str, object]
