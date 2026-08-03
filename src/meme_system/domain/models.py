"""Mode-neutral domain models for deterministic Paper and Shadow simulation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Mapping

if TYPE_CHECKING:
    from meme_system.adapters.protocols import ExecutableQuote


@dataclass(frozen=True)
class PriceSnapshot:
    """One atomic native/USD price observation used for a lifecycle boundary."""

    price_native: Decimal | None
    native_symbol: str | None
    native_usd: Decimal | None
    price_usd: Decimal | None
    price_source: str | None
    quoted_at: datetime | None
    executable_quote: bool
    estimated: bool
    price_age_ms: int | None

    def as_dict(self) -> dict[str, object]:
        return {
            "price_native": str(self.price_native) if self.price_native is not None else None,
            "native_symbol": self.native_symbol,
            "native_usd": str(self.native_usd) if self.native_usd is not None else None,
            "price_usd": str(self.price_usd) if self.price_usd is not None else None,
            "price_source": self.price_source,
            "quoted_at": self.quoted_at.isoformat() if self.quoted_at is not None else None,
            "executable_quote": self.executable_quote,
            "estimated": self.estimated,
            "price_age_ms": self.price_age_ms,
        }


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
    ruleset_version="0.1.1",
    config_version="0.1.1",
)


BSC_BASELINE_IDENTITY = StrategyIdentity(
    strategy_name="bsc_binance_indicative",
    ruleset_name="ultra_early_selective_bsc",
    ruleset_version="0.1.5",
    config_version="0.1.5",
)


@dataclass(frozen=True)
class Signal:
    signal_id: str
    mint: str
    observed_at: datetime
    source: str
    chain: str = "solana"


@dataclass(frozen=True)
class Candidate:
    candidate_id: str
    signal_id: str
    mint: str
    identity: StrategyIdentity
    status: str
    chain: str = "solana"
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
    raw_name: str | None = None
    display_name: str | None = None
    symbol: str | None = None
    entry_price_snapshot: PriceSnapshot | None = None
    exit_price_snapshot: PriceSnapshot | None = None
    entry_holders: int | None = None
    entry_liquidity_usd: Decimal | None = None
    exit_holders: int | None = None
    exit_holders_observed_at: datetime | None = None
    exit_holders_source: str | None = None
    exit_holders_status: str | None = None
    exit_market_cap_usd: Decimal | None = None
    exit_liquidity_usd: Decimal | None = None
    exit_market_observed_at: datetime | None = None
    exit_market_source: str | None = None
    exit_market_status: str | None = None
    status: str = "OPEN"
    mfe_pct: Decimal = Decimal("0")
    mae_pct: Decimal = Decimal("0")
    last_return_pct: Decimal | None = None
    last_observed_at: datetime | None = None
    last_quote_id: str | None = None
    local_price_sol_per_token: Decimal | None = None
    local_price_observed_at: datetime | None = None
    local_price_source: str | None = None
    local_return_pct: Decimal | None = None
    jupiter_price_sol_per_token: Decimal | None = None
    jupiter_price_observed_at: datetime | None = None
    jupiter_return_pct: Decimal | None = None
    closed_at: datetime | None = None
    closed_reason: str | None = None
    # BSC Paper/Shadow lifecycle timestamps are kept separate from the
    # evaluation and execution-record timestamps.
    signal_observed_at: datetime | None = None
    evaluated_at: datetime | None = None
    entry_quote_at: datetime | None = None
    exit_quote_at: datetime | None = None

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
    token_age_sec: int | None
    unique_buyers_15s: int | None
    buy_sell_count_ratio_15s: Decimal | None
    net_buy_15s: Decimal | None
    flow_windows_non_negative: tuple[bool | None, bool | None]
    creator_confirmed_sold: bool | None
    buy_quote: "ExecutableQuote | None"
    sell_quote: "ExecutableQuote | None"
    evaluated_at: datetime
    token_name: str | None = None
    soft_features: Mapping[str, object] | None = None
    holders: int | None = None
    market_cap_usd: Decimal | None = None
    liquidity_usd: Decimal | None = None
    pricing_mode: str = "executable_quote"
    executable_quote: bool = True
    pricing_error: str | None = None
    raw_name: str | None = None
    display_name: str | None = None
    symbol: str | None = None
    native_usd: Decimal | None = None


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

    @property
    def unavailable_reason_codes(self) -> tuple[str, ...]:
        """Reason codes retained for fields that were unavailable but optional."""

        return tuple(
            check.reason_code
            for check in self.checks
            if check.passed
            and check.reason_code is not None
            and check.reason_code.endswith("_unavailable")
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
    holders: int | None = None
    liquidity_usd: Decimal | None = None


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
    pricing_mode: str = "executable_quote"
    executable_quote: bool = True
    exit_status: str | None = None
    pnl_status: str | None = None
    price_snapshot: PriceSnapshot | None = None
    quote_source: str | None = None
    quote_route: tuple[str, ...] = ()


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
