"""MEME_SURVIVOR_REVERSAL_V1 on the existing BSC Paper runtime.

The engine is deliberately a companion strategy, not a second runner.  It
uses the existing Binance Meme Rush snapshot, the existing BSC pool resolver,
the existing Pair WSS monitor, and the existing read-only executable quote
provider.  It never signs or broadcasts a transaction.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from collections import Counter, defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from queue import Empty, Queue
from types import SimpleNamespace
from typing import Any, Callable, Mapping, Sequence
from uuid import uuid4

from eth_abi import decode as abi_decode

from meme_system.adapters.bsc_wss import (
    BSC_WBNB_ADDRESS,
    BSC_USDT_ADDRESS,
    BSC_USDC_ADDRESS,
    BONDING_CURVE_POOL_TYPE,
    FLAP_LAUNCHED_TO_DEX_TOPIC,
    FLAP_PORTAL_ADDRESS,
    FLAP_PORTAL_EVENT_TOPICS,
    FLAP_PORTAL_POOL_TYPE,
    FLAP_TOKEN_BOUGHT_TOPIC,
    FLAP_TOKEN_SOLD_TOPIC,
    FOUR_MEME_TOKEN_MANAGER,
    FOUR_TOKEN_PURCHASE_TOPIC,
    FOUR_TOKEN_SALE_TOPIC,
    PANCAKE_V2_FACTORY,
    PANCAKE_V3_FACTORY,
    SWAP_EVENT_TOPIC,
    SYNC_EVENT_TOPIC,
    BscPairEvent,
    BscPoolDescriptor,
    BscPoolResolution,
    BscPoolResolver,
    BscVenueInspection,
    V2_POOL_TYPE,
    V3_POOL_TYPE,
    _data_words,
    normalize_bsc_address,
)
from meme_system.adapters.binance_web3.models import BinanceNormalizedSignal
from meme_system.adapters.binance_agentic_wallet import (
    AsyncLiveExecutionBridge,
    LiveExecutionEvent,
    LiveSwapResult,
)
from meme_system.adapters.quote_asset_usd import QuoteAssetUsdResolution, QuoteAssetUsdResolver
from meme_system.adapters.protocols import ExecutableQuote
from meme_system.analytics.live_entry_outcomes import build_report
from meme_system.domain.models import BALANCED_SURVIVOR_REVERSAL_IDENTITY, SURVIVOR_REVERSAL_IDENTITY, StrategyIdentity
from meme_system.runtime_ops import HealthRegistry, JsonlAuditWriter, RuntimeControl
from meme_system.storage.runtime_store import RuntimeStore


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _field(record: BinanceNormalizedSignal, name: str) -> Any:
    observed = record.fields.get(name)
    return observed.value if observed is not None and observed.available else None


def _decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return result if result.is_finite() else None


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


FIRST_SEEN_PRICE_HISTORY_STATUS = "FIRST_SEEN_TRACKING"
COMPLETE_PRICE_HISTORY_STATUSES = frozenset({
    "VALID", "LIVE_COMPLETE", "BACKFILLED_COMPLETE", "LIVE_USABLE",
    FIRST_SEEN_PRICE_HISTORY_STATUS,
})
PRICE_HISTORY_BLOCKING_STATUSES = frozenset({"PENDING", "PRICE_HISTORY_INCOMPLETE", "BACKFILL_INSUFFICIENT"})
PRICE_HISTORY_QUALITY_FIRST_SEEN = "FIRST_SEEN"
PRICE_HISTORY_QUALITY_GOOD = "GOOD"
PRICE_HISTORY_QUALITY_SPARSE = "SPARSE"
PRICE_HISTORY_QUALITY_INSUFFICIENT = "INSUFFICIENT"
AUDIT_PREFETCH_TTL_SECONDS = 300


def _row_value(row: Any, name: str, default: Any = None) -> Any:
    try:
        return row[name]
    except (KeyError, IndexError):
        return default


def _field_from_mapping(value: Any, name: str) -> Any:
    if isinstance(value, Mapping):
        item = value.get(name)
        if hasattr(item, "available"):
            return item.value if item.available else None
        return item
    getter = getattr(value, "value", None)
    if callable(getter):
        try:
            return getter(name)
        except Exception:
            return None
    return getattr(value, name, None)


@dataclass(frozen=True)
class SurvivorReversalConfig:
    """The frozen runtime values from Meme_Survivor_Reversal_V1_Codex_Final."""

    enabled: bool = True
    execution_provider: str = "paper"
    # This is runtime identity, not a strategy threshold.  The live launcher
    # sets it only after its executor preflight has succeeded.
    live_execution_enabled: bool = False
    position_size_bnb: Decimal = Decimal("0.01")
    active_max: int = 150
    idle_ttl_sec: int = 1800
    min_age_sec: int = 180
    max_age_sec: int | None = None
    min_active_mc_usd: Decimal = Decimal("250000")
    min_active_liquidity_usd: Decimal = Decimal("30000")
    min_active_holders: int = 250
    max_active_holders: int | None = None
    universe_min_mc_usd: Decimal = Decimal("300000")
    universe_max_mc_usd: Decimal | None = Decimal("2000000")
    universe_min_liquidity_usd: Decimal = Decimal("50000")
    universe_min_holders: int = 350
    universe_max_holders: int | None = None
    min_lp_mc_pct: Decimal = Decimal("8")
    drawdown_min_pct: Decimal = Decimal("30")
    drawdown_max_pct: Decimal = Decimal("50")
    low_stable_sec: int = 60
    rebound_min_pct: Decimal = Decimal("10")
    rebound_max_pct: Decimal = Decimal("20")
    buy_sell_volume_ratio: Decimal = Decimal("1.30")
    buy_sell_count_ratio: Decimal = Decimal("1.00")
    min_swap_count: int = 5
    max_tax_pct: Decimal = Decimal("10")
    max_dev_pct: Decimal = Decimal("2")
    max_insider_pct: Decimal = Decimal("10")
    max_sniper_pct: Decimal = Decimal("15")
    max_top10_pct: Decimal = Decimal("30")
    hard_stop_pct: Decimal = Decimal("20")
    liquidity_drop_pct: Decimal = Decimal("15")
    dump_price_pct: Decimal = Decimal("15")
    dump_flow_ratio: Decimal = Decimal("3")
    no_trade_exit_sec: int = 120
    time_stop_sec: int = 3600
    pre_candidate_watch_max: int = 50
    require_pullback: bool = True
    require_audit_fields: bool = True
    # A profile may define Candidate eligibility itself as the strategy entry
    # signal.  Execution still has to pass the real two-sided quote check.
    candidate_is_entry: bool = False
    # Keep the legacy profile's once-per-day guard explicit.  Profiles that
    # intentionally allow every eligible Candidate to attempt an entry can
    # disable only this daily cap without weakening the open-position or quote
    # safety checks.
    one_trade_per_day: bool = True
    # Optional per-token cooldown measured from the last completed Paper exit.
    # A zero value keeps the legacy profile behavior unchanged.
    same_token_cooldown_sec: int = 0
    # Maximum number of simultaneous OPEN Paper positions for this profile.
    max_open_positions: int = 1
    min_entry_price_usd: Decimal | None = None
    max_entry_price_usd: Decimal | None = None
    data_quality_start_at: datetime = field(default_factory=_utc_now)

    @property
    def strategy_name(self) -> str:
        return self.identity.strategy_name

    @property
    def identity(self) -> StrategyIdentity:
        return SURVIVOR_REVERSAL_IDENTITY

    def effective_values(self) -> dict[str, object]:
        return {
            "SURVIVOR_ENABLED": self.enabled,
            "MIN_AGE_SECONDS": self.min_age_sec,
            "MAX_AGE_SECONDS": self.max_age_sec,
            "CANDIDATE_MIN_MC": str(self.min_active_mc_usd),
            "CANDIDATE_MIN_LIQUIDITY": str(self.min_active_liquidity_usd),
            "CANDIDATE_MIN_HOLDERS": self.min_active_holders,
            "CANDIDATE_MAX_HOLDERS": self.max_active_holders,
            "ENTRY_MIN_MC": str(self.universe_min_mc_usd),
            "ENTRY_MAX_MC": str(self.universe_max_mc_usd) if self.universe_max_mc_usd is not None else None,
            "ENTRY_MIN_LIQUIDITY": str(self.universe_min_liquidity_usd),
            "ENTRY_MIN_LP_MC_RATIO": str(self.min_lp_mc_pct / Decimal("100")),
            "ENTRY_MIN_HOLDERS": self.universe_min_holders,
            "ENTRY_MAX_HOLDERS": self.universe_max_holders,
            "PULLBACK_MIN": str(self.drawdown_min_pct / Decimal("100")),
            "PULLBACK_MAX": str(self.drawdown_max_pct / Decimal("100")),
            "STOP_CONFIRM_SECONDS": self.low_stable_sec,
            "MIN_REBOUND": str(self.rebound_min_pct / Decimal("100")),
            "MAX_REBOUND": str(self.rebound_max_pct / Decimal("100")),
            "MIN_BUY_SELL_VOLUME_RATIO_1M": str(self.buy_sell_volume_ratio),
            "MIN_BUY_SELL_COUNT_RATIO_1M": str(self.buy_sell_count_ratio),
            "MIN_SWAP_COUNT_1M": self.min_swap_count,
            "MAX_ACTIVE_CANDIDATES": self.active_max,
            "CANDIDATE_IDLE_TTL_SECONDS": self.idle_ttl_sec,
            "ALLOW_LIVE_TRADING": self.live_execution_enabled,
            "EXECUTION_PROVIDER": self.execution_provider,
            "REQUIRE_PULLBACK": self.require_pullback,
            "ENTRY_GATE": "CANDIDATE" if self.candidate_is_entry else "SURVIVOR",
            "ONE_TRADE_PER_DAY": self.one_trade_per_day,
            "SAME_TOKEN_COOLDOWN_SECONDS": self.same_token_cooldown_sec,
            "MAX_OPEN_POSITIONS": self.max_open_positions,
            "HARD_STOP_PCT": str(self.hard_stop_pct),
            "NO_TRADE_EXIT_SECONDS": self.no_trade_exit_sec,
            "TIME_STOP_SECONDS": self.time_stop_sec,
            "ENTRY_MIN_PRICE_USD": str(self.min_entry_price_usd) if self.min_entry_price_usd is not None else None,
            "ENTRY_MAX_PRICE_USD": str(self.max_entry_price_usd) if self.max_entry_price_usd is not None else None,
        }

    def diagnostic_values(self) -> dict[str, object]:
        return {
            "PRE_CANDIDATE_PRICE_WATCH_MAX": self.pre_candidate_watch_max,
            "SURVIVOR_DATA_QUALITY_V22_START_AT": self.data_quality_start_at.isoformat(),
        }

    def self_check(self) -> dict[str, object]:
        """Compare explicitly supplied core env values with the frozen V1 values."""
        import os

        expected = self.effective_values()
        drift: dict[str, dict[str, object]] = {}
        for name, value in expected.items():
            if name not in os.environ:
                continue
            raw = os.environ[name].strip()
            if isinstance(value, bool):
                actual: object = raw.lower() == "true"
            elif isinstance(value, int):
                try:
                    actual = int(raw)
                except ValueError:
                    actual = raw
            elif name in {"ENTRY_MIN_LP_MC_RATIO", "PULLBACK_MIN", "PULLBACK_MAX", "MIN_REBOUND", "MAX_REBOUND"}:
                try:
                    actual = str(Decimal(raw))
                except InvalidOperation:
                    actual = raw
            else:
                try:
                    actual = str(Decimal(raw))
                except InvalidOperation:
                    actual = raw.lower() if isinstance(value, str) else raw
            expected_value = str(value).lower() if isinstance(value, bool) else str(value)
            actual_value = str(actual).lower() if isinstance(value, bool) else str(actual)
            if actual_value != expected_value:
                drift[name] = {"expected": value, "actual": raw}
        return {
            "strategy": self.strategy_name,
            "effective": expected,
            "config_drift": drift,
            "status": "CONFIG_DRIFT" if drift else "OK",
        }

    @classmethod
    def from_env(cls) -> "SurvivorReversalConfig":
        # Core V1 thresholds are frozen.  The env is checked, not used to
        # silently override them; this prevents a stale 1K runtime gate.
        import os

        def d(name: str, default: Decimal) -> Decimal:
            return _decimal(os.environ.get(name, str(default))) or default

        try:
            pre_candidate_watch_max = int(os.environ.get("PRE_CANDIDATE_PRICE_WATCH_MAX", "50"))
        except ValueError:
            pre_candidate_watch_max = 50
        raw_start = os.environ.get("SURVIVOR_DATA_QUALITY_V22_START_AT", "").strip()
        try:
            data_quality_start_at = datetime.fromisoformat(raw_start) if raw_start else _utc_now()
            if data_quality_start_at.tzinfo is None:
                data_quality_start_at = data_quality_start_at.replace(tzinfo=timezone.utc)
        except ValueError:
            data_quality_start_at = _utc_now()
        return cls(
            enabled=os.environ.get("SURVIVOR_ENABLED", "true").lower() == "true",
            execution_provider=os.environ.get("EXECUTION_PROVIDER", "paper").strip().lower(),
            position_size_bnb=d("SURVIVOR_POSITION_SIZE_BNB", Decimal("0.01")),
            active_max=max(1, min(150, int(os.environ.get("SURVIVOR_ACTIVE_MAX", "150")))),
            pre_candidate_watch_max=max(1, min(200, pre_candidate_watch_max)),
            data_quality_start_at=data_quality_start_at,
        )

    def validate(self) -> None:
        if self.execution_provider != "paper" and not self.live_execution_enabled:
            raise ValueError("MEME_SURVIVOR_REVERSAL_V1 requires EXECUTION_PROVIDER=paper")
        if self.position_size_bnb <= 0:
            raise ValueError("SURVIVOR_POSITION_SIZE_BNB must be positive")
        if self.no_trade_exit_sec <= 0:
            raise ValueError("no_trade_exit_sec must be positive")


@dataclass(frozen=True)
class BalancedSurvivorConfig(SurvivorReversalConfig):
    """Independent Paper profile whose Candidate gate is the entry signal."""

    min_active_mc_usd: Decimal = Decimal("5000")
    min_active_liquidity_usd: Decimal = Decimal("1000")
    min_active_holders: int = 30
    max_active_holders: int | None = 300
    min_age_sec: int = 0
    max_age_sec: int | None = 30 * 60
    universe_min_mc_usd: Decimal = Decimal("5000")
    universe_max_mc_usd: Decimal | None = None
    universe_min_liquidity_usd: Decimal = Decimal("1000")
    universe_min_holders: int = 30
    universe_max_holders: int | None = 300
    require_pullback: bool = False
    candidate_is_entry: bool = True
    # Candidate eligibility is the entry signal for Balanced; do not impose a
    # separate daily entry quota.
    one_trade_per_day: bool = False
    # Prevent the same token from being re-entered repeatedly after a close.
    # This is intentionally scoped to Balanced and does not change the daily
    # quota or any other Candidate/quote rule.
    same_token_cooldown_sec: int = 24 * 60 * 60
    # Allow up to thirty simultaneous Paper positions in Balanced.
    max_open_positions: int = 30
    # Balanced uses a 30% hard stop and gives a new position thirty
    # minutes to reach a modest positive return before the time exit applies.
    hard_stop_pct: Decimal = Decimal("30")
    time_stop_sec: int = 30 * 60
    # Binance Meme Rush may omit audit fields. Record that condition, but do
    # not block this Paper profile solely for an unavailable source. Explicit
    # audit failures (for example honeypot/high risk) remain blocking.
    require_audit_fields: bool = False
    min_entry_price_usd: Decimal | None = Decimal("0.000005")
    max_entry_price_usd: Decimal | None = Decimal("0.00008")

    @property
    def identity(self) -> StrategyIdentity:
        return BALANCED_SURVIVOR_REVERSAL_IDENTITY

    @classmethod
    def from_env(cls) -> "BalancedSurvivorConfig":
        base = SurvivorReversalConfig.from_env()
        return cls(
            enabled=base.enabled,
            execution_provider=base.execution_provider,
            live_execution_enabled=base.live_execution_enabled,
            position_size_bnb=base.position_size_bnb,
            active_max=base.active_max,
            idle_ttl_sec=base.idle_ttl_sec,
            pre_candidate_watch_max=base.pre_candidate_watch_max,
            data_quality_start_at=base.data_quality_start_at,
        )


@dataclass
class FlowSample:
    observed_at: datetime
    buy_volume_bnb: Decimal = Decimal("0")
    sell_volume_bnb: Decimal = Decimal("0")
    buy_count: int = 0
    sell_count: int = 0
    price_native: Decimal | None = None
    liquidity_native: Decimal | None = None
    price_usd: Decimal | None = None
    liquidity_usd: Decimal | None = None
    price_source: str | None = None
    token_amount: Decimal | None = None
    quote_amount: Decimal | None = None
    fee_amount: Decimal | None = None
    tx_hash: str | None = None
    block_number: int | None = None
    event_type: str | None = None
    source: str = "WSS"


# Flow is low-latency over WSS, but absence of a WSS message is never proof
# that the chain was quiet.  The reconciliation worker only reads bounded
# recent log ranges and returns immutable events to the owner loop.
FLOW_RECONCILIATION_INTERVAL_SEC = 4.0
FLOW_RECONCILIATION_SAFE_CONFIRMATIONS = 2
FLOW_RECONCILIATION_CHUNK_BLOCKS = 250
FLOW_RECONCILIATION_MAX_LAG_BLOCKS = 240
FLOW_RECONCILIATION_WINDOW_SEC = 40
FLOW_RECONCILIATION_DEGRADED_RESUBSCRIBE_MISSES = 3


@dataclass(frozen=True)
class FlowReconciliationJob:
    source_key: str
    venue_address: str
    pool_type: str
    topics: tuple[str, ...]
    from_block: int
    to_block: int
    requested_at: datetime


@dataclass(frozen=True)
class FlowReconciliationResult:
    job: FlowReconciliationJob
    events: tuple[BscPairEvent, ...]
    rpc_event_keys: tuple[tuple[str, int | None, str], ...]
    latest_chain_block: int | None
    safe_reconciled_block: int | None
    completed_at: datetime
    error_class: str | None = None
    error_message: str | None = None
    rpc_calls: int = 0
    rpc_429: int = 0
    latency_ms: int | None = None


@dataclass(frozen=True)
class PreMigrationVenueJob:
    mint: str
    migrate_status: int | None
    generation: int
    protocol: str | None
    native_token_price_usd: Decimal | None
    requested_at: datetime


@dataclass(frozen=True)
class PreMigrationVenueResult:
    job: PreMigrationVenueJob
    context: object | None
    context_error: str | None
    token_inspection: BscVenueInspection | None
    venue_inspection: BscVenueInspection | None
    quote_asset_per_bnb: Decimal | None
    completed_at: datetime
    duration_ms: int


@dataclass(frozen=True)
class QuoteAssetUsdJob:
    chain_id: int
    quote_asset: str
    native_usd: Decimal | None
    requested_at: datetime


@dataclass(frozen=True)
class QuoteAssetUsdResult:
    job: QuoteAssetUsdJob
    resolution: QuoteAssetUsdResolution
    completed_at: datetime
    native_usd: Decimal | None = None


@dataclass(frozen=True)
class FactoryPoolResolutionJob:
    """Immutable Factory event facts handed to the venue/RPC worker."""

    event: BscPairEvent
    pool_type: str
    token0: str
    token1: str
    pool: str
    requested_at: datetime


@dataclass(frozen=True)
class FactoryPoolResolutionResult:
    job: FactoryPoolResolutionJob
    inspection: BscVenueInspection | None
    resolutions: tuple[tuple[str, BscPoolResolution], ...]
    completed_at: datetime
    duration_ms: int
    error: str | None = None


@dataclass(frozen=True)
class FactoryGapFillJob:
    pool_type: str
    from_block: int
    to_block: int
    requested_at: datetime


@dataclass(frozen=True)
class FactoryGapFillResult:
    job: FactoryGapFillJob
    logs: tuple[Mapping[str, object], ...]
    completed_at: datetime
    error: str | None = None


@dataclass
class SurvivorCandidate:
    mint: str
    symbol: str | None
    first_seen_at: datetime
    last_seen_at: datetime
    data_quality_cohort: str = "LEGACY"
    token_created_at: datetime | None = None
    discovery_delay_seconds: Decimal | None = None
    first_snapshot_at: datetime | None = None
    first_lifecycle: str | None = None
    first_rank_type: int | None = None
    first_market_cap_usd: Decimal | None = None
    first_liquidity_usd: Decimal | None = None
    first_holders: int | None = None
    first_price_usd: Decimal | None = None
    first_progress_pct: Decimal | None = None
    latest_lifecycle: str | None = None
    latest_rank_type: int | None = None
    latest_progress_pct: Decimal | None = None
    latest_migrate_status: int | None = None
    emit_count: int = 0
    state: str = "DISCOVERED"
    market_cap_usd: Decimal | None = None
    liquidity_usd: Decimal | None = None
    holders: int | None = None
    current_price_native: Decimal | None = None
    ath_price_native: Decimal | None = None
    local_low_native: Decimal | None = None
    low_started_at: datetime | None = None
    stable_since: datetime | None = None
    pair_address: str | None = None
    pool_type: str | None = None
    last_rejection: str | None = None
    first_seen_price_usd: Decimal | None = None
    first_seen_price_source: str | None = None
    current_price_usd: Decimal | None = None
    ath_price_usd: Decimal | None = None
    ath_at: datetime | None = None
    drawdown_pct: Decimal | None = None
    age_seconds: int = 0
    price_status: str = "PENDING"
    price_source: str | None = None
    price_updated_at: datetime | None = None
    price_age_ms: int | None = None
    candidate_eligible: bool = False
    active_candidate: bool = False
    audit_state: str = "NOT_REQUESTED"
    audit_requested: bool = False
    audit_failed: bool = False
    quote_state: str = "NOT_REQUESTED"
    quote_requested: bool = False
    ready_to_buy: bool = False
    paper_buy: bool = False
    native_token_price_usd: Decimal | None = None
    liquidity_current_usd: Decimal | None = None
    liquidity_1m_ago_usd: Decimal | None = None
    liquidity_2m_ago_usd: Decimal | None = None
    buy_volume_30s: Decimal = Decimal("0")
    sell_volume_30s: Decimal = Decimal("0")
    buy_volume_1m: Decimal = Decimal("0")
    sell_volume_1m: Decimal = Decimal("0")
    buy_volume_3m: Decimal = Decimal("0")
    sell_volume_3m: Decimal = Decimal("0")
    buy_count_1m: int = 0
    sell_count_1m: int = 0
    buy_sell_volume_ratio_1m: Decimal | None = None
    buy_sell_count_ratio_1m: Decimal | None = None
    price_change_1m: Decimal | None = None
    source_status: dict[str, str] = field(default_factory=dict)
    last_price_attempt_at: datetime | None = None
    pre_candidate_watch: bool = False
    pre_candidate_rank: int | None = None
    candidate_distance_score: Decimal | None = None
    first_price_at: datetime | None = None
    first_price_delay_seconds: Decimal | None = None
    price_samples_before_candidate: int = 0
    price_coverage_before_candidate_seconds: Decimal = Decimal("0")
    ath_before_candidate: bool = False
    ath_before_candidate_price_usd: Decimal | None = None
    ath_before_candidate_at: datetime | None = None
    price_history_quality: str = PRICE_HISTORY_QUALITY_INSUFFICIENT
    candidate_at: datetime | None = None
    price_history_status: str = "PENDING"
    history_source: str | None = None
    history_start_at: datetime | None = None
    history_end_at: datetime | None = None
    history_sample_count: int = 0
    history_interval: str | None = None
    max_history_gap_seconds: Decimal | None = None
    record: BinanceNormalizedSignal | None = None
    descriptor: BscPoolDescriptor | None = None
    flows: deque[FlowSample] = field(default_factory=lambda: deque(maxlen=240))


@dataclass
class SurvivorPosition:
    position_id: str
    mint: str
    symbol: str | None
    opened_at: datetime
    entry_price_native: Decimal
    current_price_native: Decimal | None
    quantity_token: Decimal
    remaining_quantity_token: Decimal
    invested_bnb: Decimal
    realized_bnb: Decimal = Decimal("0")
    tp1: bool = False
    tp2: bool = False
    # Price-trigger and confirmed-fill state deliberately remain separate.
    # ``tp1``/``tp2`` are retained as filled-state aliases for existing
    # break-even and trailing risk rules.
    tp1_triggered: bool = False
    tp1_triggered_at: datetime | None = None
    tp1_trigger_price_native: Decimal | None = None
    tp1_filled: bool = False
    tp1_filled_at: datetime | None = None
    tp2_triggered: bool = False
    tp2_triggered_at: datetime | None = None
    tp2_trigger_price_native: Decimal | None = None
    tp2_filled: bool = False
    tp2_filled_at: datetime | None = None
    tp3_triggered: bool = False
    tp3_triggered_at: datetime | None = None
    tp3_trigger_price_native: Decimal | None = None
    tp3_filled: bool = False
    tp3_filled_at: datetime | None = None
    high_since_tp1_native: Decimal | None = None
    trailing_active: bool = False
    exit_intent_reason: str | None = None
    exit_intent_quantity: Decimal | None = None
    high_after_tp2: Decimal | None = None
    last_trade_at: datetime | None = None
    status: str = "OPEN"
    # The trigger condition and its mark are intentionally separate from the
    # final executable sell quote.  They can differ when a quote is delayed
    # or the market moves before the Paper exit is filled.
    exit_trigger_reason: str | None = None
    exit_trigger_pnl_pct: Decimal | None = None
    exit_trigger_price_native: Decimal | None = None
    exit_triggered_at: datetime | None = None
    # Position marks are deliberately independent from Candidate price state.
    # A candidate may be price-pending after a venue migration while an owned
    # token is still sellable and therefore must retain a live executable mark.
    position_mark_price_native: Decimal | None = None
    position_mark_price_usd: Decimal | None = None
    position_mark_source: str | None = None
    position_price_updated_at: datetime | None = None
    position_price_freshness: str = "UNAVAILABLE"
    external_exit_unpriced: bool = False
    no_trade_profit_partial_done: bool = False


def parse_v2_swap_flow(descriptor: BscPoolDescriptor, event: BscPairEvent) -> FlowSample | None:
    """Parse native/token direction from the standard V2 Swap ABI only."""

    if descriptor.pool_type != V2_POOL_TYPE or event.event_type != "swap":
        return None
    words = _data_words(event.data, 4)
    if words is None or descriptor.token0 is None or descriptor.token1 is None:
        return None
    native_in: int
    native_out: int
    token_in: int
    token_out: int
    if descriptor.token0 == BSC_WBNB_ADDRESS:
        native_in, native_out = words[0], words[2]
        token_in, token_out = words[1], words[3]
    elif descriptor.token1 == BSC_WBNB_ADDRESS:
        native_in, native_out = words[1], words[3]
        token_in, token_out = words[0], words[2]
    else:
        return None
    scale = Decimal(10) ** 18
    buy = Decimal(native_in) / scale if native_in > 0 and token_out > 0 else Decimal("0")
    sell = Decimal(native_out) / scale if native_out > 0 and token_in > 0 else Decimal("0")
    if buy <= 0 and sell <= 0:
        return None
    return FlowSample(
        observed_at=event.observed_at,
        buy_volume_bnb=buy,
        sell_volume_bnb=sell,
        buy_count=1 if buy > 0 else 0,
        sell_count=1 if sell > 0 else 0,
    )


def _fourmeme_event_mint(event: BscPairEvent) -> str | None:
    """Read the token identifier from a verified Four.meme manager event.

    Live purchase/sale samples show the token contract as the first ABI data
    word.  We deliberately do not decode the remaining quantities without a
    published ABI, so this supports direction/activity only, never volume.
    """

    if event.pool_type != BONDING_CURVE_POOL_TYPE or event.event_type != "bonding_curve_event":
        return None
    words = _data_words(event.data, 1)
    if words is None:
        return None
    return normalize_bsc_address("0x" + f"{words[0]:040x}"[-40:])


def parse_fourmeme_bonding_curve_activity(descriptor: BscPoolDescriptor, event: BscPairEvent) -> FlowSample | None:
    """Record only ABI-verified buy/sell direction for a Four.meme event."""

    if descriptor.pool_type != BONDING_CURVE_POOL_TYPE:
        return None
    if _fourmeme_event_mint(event) != normalize_bsc_address(descriptor.mint):
        return None
    topic = event.topics[0].lower() if event.topics else ""
    if topic == FOUR_TOKEN_PURCHASE_TOPIC:
        return FlowSample(observed_at=event.observed_at, buy_count=1)
    if topic == FOUR_TOKEN_SALE_TOPIC:
        return FlowSample(observed_at=event.observed_at, sell_count=1)
    return None


class SurvivorReversalEngine:
    """A bounded, fail-closed state machine attached to one runtime DB."""

    WSS_CANDIDATE_STATES = frozenset({
        "ACTIVE_CANDIDATE", "PULLBACK_ZONE", "LOW_FORMING", "STOP_CONFIRMED",
        "REVERSAL_CONFIRMED", "READY_TO_BUY", "POSITION_OPEN",
    })
    MIGRATED_DISCOVERY_RETRY_SECONDS = (0, 2, 5, 10, 20, 40, 60, 120)
    # Operational retention only: strategy state (candidate ATH/local low,
    # positions, cursor and descriptors) lives in its own current-state rows.
    # These append-only diagnostic samples are never an entry/exit source once
    # their short rolling windows have elapsed.
    FLOW_RETENTION = timedelta(minutes=15)
    PRICE_SNAPSHOT_RETENTION = timedelta(hours=1)
    OBSERVABILITY_RETENTION = timedelta(hours=24)
    INACTIVE_CANDIDATE_RETENTION = timedelta(hours=1)
    REGISTRY_RETENTION = timedelta(hours=1)
    CLOSED_POSITION_RETENTION = timedelta(hours=24)
    # At the observed BSC WSS sample rate, a one-minute, 1k-row owner-loop
    # batch keeps the rolling one-hour window bounded without a maintenance
    # thread or a long deletion pause.
    RETENTION_INTERVAL = timedelta(minutes=1)
    RETENTION_BATCH_SIZE = 1000
    # Owner-loop budgets: completed venue facts are deliberately drained in
    # small slices so a backlog can never delay Position/Flow/Candidate work.
    FLOW_EVENT_APPLY_MAX_ITEMS = 500
    FLOW_EVENT_APPLY_BUDGET_MS = 40
    VENUE_RESULT_APPLY_MAX_ITEMS = 2
    VENUE_RESULT_APPLY_BUDGET_MS = 25
    CANDIDATE_EVALUATE_BUDGET_MS = 80
    CANDIDATE_EVALUATE_MAX_ITEMS = 120

    def __init__(
        self,
        *,
        connection: Any,
        store: RuntimeStore,
        health: HealthRegistry,
        audit: JsonlAuditWriter,
        quote_provider: Any,
        resolver: BscPoolResolver | None,
        entry_quote_provider: Any | None = None,
        exit_quote_provider: Any | None = None,
        market_data: Any | None = None,
        controls: RuntimeControl,
        mode: str = "paper",
        config: SurvivorReversalConfig | None = None,
        clock: Any = _utc_now,
    ) -> None:
        if mode not in {"paper", "live"}:
            raise ValueError("Survivor Reversal supports only isolated BSC Paper or Live")
        self.connection = connection
        self.store = store
        self.health = health
        self.audit = audit
        self.quote_provider = quote_provider
        # Entry routing may be independent from position settlement. Existing
        # positions retain the verified settlement provider until they close.
        self.entry_quote_provider = entry_quote_provider or quote_provider
        self.exit_quote_provider = exit_quote_provider or quote_provider
        self.resolver = resolver
        self.market_data = market_data
        self.controls = controls
        self.mode = mode
        self.config = config or SurvivorReversalConfig()
        self.config.validate()
        self.clock = clock
        self.data_quality_start_at = self.config.data_quality_start_at
        self._lock = threading.RLock()
        self._candidates: dict[str, SurvivorCandidate] = {}
        self._latest_native_token_price_usd: Decimal | None = None
        self._latest_native_token_price_observed_at: datetime | None = None
        self._loaded_candidate_mints: set[str] = set()
        self._venue_history_backfill_enabled = os.environ.get(
            "VENUE_HISTORY_BACKFILL_ENABLED", "false"
        ).strip().lower() == "true"
        self._excluded_mints: set[str] = set()
        self._positions: dict[str, SurvivorPosition] = {}
        self._position_mark_dirty: set[str] = set()
        self._pending_events: deque[BscPairEvent] = deque(maxlen=5000)
        self._pending_gap_events: deque[BscPairEvent] = deque(maxlen=5000)
        # WSS callbacks only mark transport receipt and enqueue.  The owner
        # loop records processed events; reconciliation compares these keys
        # with the authoritative Logs RPC before applying the same event
        # pipeline, so duplicate WSS/RPC delivery is harmless.
        self._wss_flow_event_keys: deque[tuple[str, int | None, str]] = deque(maxlen=50000)
        self._wss_flow_event_key_set: set[tuple[str, int | None, str]] = set()
        self._processed_flow_event_keys: deque[tuple[str, int | None, str]] = deque(maxlen=50000)
        self._processed_flow_event_key_set: set[tuple[str, int | None, str]] = set()
        # Events recovered from Logs RPC are remembered separately from WSS
        # receipt. This prevents every periodic reconciliation pass from
        # counting the same silent miss again while preserving the transport
        # health distinction in diagnostics.
        self._reconciled_flow_event_keys: deque[tuple[str, int | None, str]] = deque(maxlen=50000)
        self._reconciled_flow_event_key_set: set[tuple[str, int | None, str]] = set()
        self._flow_reconciliation_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="survivor-flow-reconciliation",
        )
        self._flow_reconciliation_results: Queue[FlowReconciliationResult] = Queue()
        self._flow_reconciliation_pending: set[str] = set()
        self._flow_reconciliation_state: dict[str, dict[str, object]] = {}
        self._flow_reconciliation_last_schedule_at: datetime | None = None
        self._flow_reconciliation_misses_total = 0
        self._flow_reconciliation_recovered_total = 0
        self._flow_reconciliation_rpc_calls = 0
        self._flow_reconciliation_rpc_429 = 0
        self._flow_reconciliation_latency_ms: deque[Decimal] = deque(maxlen=500)
        self._flow_reconciliation_lag_blocks: deque[Decimal] = deque(maxlen=500)
        self._flow_reconciliation_resubscribe_requested = False
        self._flow_reconciliation_resubscribe_callback: Callable[[], None] | None = None
        self._load_flow_reconciliation_states()
        # Flap's BNB Portal is one shared venue for all pre-migration tokens.
        # WSS callbacks only enqueue logs; this owner-loop state keeps the
        # subscription cursor and migration work single-writer safe.
        self._flap_portal_cursor_block: int | None = self._load_flap_portal_cursor()
        self._flap_portal_gap_required = False
        self._flap_portal_gap_target: int | None = None
        self._flap_portal_seen_healthy = False
        self._flap_event_keys: deque[tuple[str, int | None, str]] = deque(maxlen=8192)
        self._flap_event_key_set: set[tuple[str, int | None, str]] = set()
        self._pending_flap_migrations: deque[tuple[str, str, datetime]] = deque(maxlen=256)
        self._factory_gap_required = False
        self._factory_wss_seen_healthy = False
        self._factory_gap_target: int | None = None
        self._factory_cursor = {V2_POOL_TYPE: self._load_factory_cursor(V2_POOL_TYPE), V3_POOL_TYPE: self._load_factory_cursor(V3_POOL_TYPE)}
        self._ensure_factory_cursors()
        self._last_discovery_count = 0
        self._record_update_total = 0
        self._audit_checks = 0
        self._audit_requested_total = 0
        self._audit_failed_total = 0
        self._quote_requested_total = 0
        self._paper_buy_total = 0
        self._audit_attempt_at: dict[str, datetime] = {}
        self._audit_prefetch_pending: set[str] = set()
        self._completed_audit_prefetches: deque[tuple[str, str, str, datetime]] = deque()
        self._audit_result_lock = threading.Lock()
        self._db_commit_error_count = 0
        self._db_locked_error_count = 0
        self._db_write_error_count = 0
        self._last_db_write_at: datetime | None = None
        self._last_retention_at: datetime | None = None
        self._retention_deleted_rows = 0
        self._runtime_db_initial_bytes = self._runtime_db_size_bytes()
        self._runtime_db_initial_at = self.clock()
        self._audit_prefetch_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="survivor-audit-prefetch",
        )
        # Venue fingerprinting is network-bound.  It must never hold the
        # owner loop (the only SQLite writer) while a launchpad/RPC endpoint
        # is slow.  Workers return immutable facts; this loop applies them.
        self._venue_resolution_executor = ThreadPoolExecutor(
            max_workers=2,
            thread_name_prefix="survivor-venue-resolution",
        )
        # Current validated pools must not wait behind a historical venue
        # fingerprint backlog before their quote asset can be converted to
        # USD.  This bounded lane performs only RPC/quote reads and returns to
        # the existing owner-loop result queues; it is not a second price
        # system and never writes SQLite.
        self._live_pool_upgrade_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="survivor-live-pool-upgrade",
        )
        # Quote-asset USD resolution is also network-bound.  Reuse the
        # existing bounded venue worker pool: workers return facts only and
        # the owner loop remains the sole SQLite writer.
        self._quote_asset_usd_results: Queue[QuoteAssetUsdResult] = Queue()
        self._quote_asset_usd_cache: dict[tuple[int, str], QuoteAssetUsdResolution] = {}
        self._quote_asset_usd_pending: set[tuple[int, str]] = set()
        self._quote_asset_usd_refresh_seconds = 60
        self._venue_resolution_results: Queue[PreMigrationVenueResult] = Queue()
        self._venue_resolution_jobs: dict[str, PreMigrationVenueJob] = {}
        self._priority_venue_resolution_jobs: set[str] = set()
        self._venue_resolution_generation: dict[str, int] = defaultdict(int)
        self._venue_resolution_completed = 0
        self._venue_resolution_failed = 0
        self._venue_resolution_durations_ms: deque[int] = deque(maxlen=500)
        self._slow_venue_apply_count = 0
        self._last_slow_venue_apply: dict[str, object] | None = None
        self._factory_resolution_results: Queue[FactoryPoolResolutionResult] = Queue()
        self._factory_resolution_jobs: set[tuple[str, int | None, str]] = set()
        # A current migrated token can carry a persisted pre-migration job
        # marker across a migration update.  Keep the one-at-a-time recovery
        # handoff separate from Factory event de-duplication; workers only
        # return RPC facts and this owner loop remains the SQLite writer.
        self._migrated_pool_recovery_jobs: set[str] = set()
        self._factory_gap_results: Queue[FactoryGapFillResult] = Queue()
        self._factory_gap_jobs: set[str] = set()
        self._main_loop_durations_ms: deque[int] = deque(maxlen=500)
        self._candidate_evaluate_lags_ms: deque[int] = deque(maxlen=1000)
        self._stage_metrics: dict[str, dict[str, object]] = {}
        self._flow_event_queue_latencies_ms: deque[Decimal] = deque(maxlen=500)
        self._reconciliation_result_queue_latencies_ms: deque[Decimal] = deque(maxlen=500)
        self._db_dirty = False
        self._candidate_eval_cursor = 0
        self._wss_provider_state: str | None = None
        self._wss_subscribed_addresses: set[str] = set()
        # WSS callbacks only publish this in-memory marker; the owner loop
        # consumes it and persists last_trade_at.  This closes the small race
        # where a real Swap arrives just after the event-queue drain but
        # before the 40-second exit check.
        self._wss_trade_markers: dict[str, datetime] = {}
        self._wss_trade_markers_lock = threading.Lock()
        self._load_exclusions()
        self._load_candidates()
        self._loaded_candidate_mints = set(self._candidates)
        self._load_positions()
        self._hydrate_active_candidate_pools()
        self._publish(self.clock())

    @staticmethod
    def _mint_key(mint: str) -> str:
        """BSC addresses are case-insensitive; chain profiles may override."""
        return mint.lower()

    def _load_flow_reconciliation_states(self) -> None:
        """Load only current reconciliation cursors, never historical logs."""

        try:
            rows = self.connection.execute(
                "SELECT state_key,value_json FROM runtime_state WHERE mode=? AND state_key LIKE 'flow_reconciliation:%'",
                (self.mode,),
            ).fetchall()
        except Exception:
            rows = ()
        for row in rows:
            key = str(row[0])
            try:
                value = json.loads(str(row[1]))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if isinstance(value, dict):
                self._flow_reconciliation_state[key.removeprefix("flow_reconciliation:")] = dict(value)

    def _record_stage_metric(self, stage: str, elapsed_ms: float, calls: int = 1) -> None:
        """Keep bounded owner-loop stage timings without another writer."""
        metrics = getattr(self, "_stage_metrics", None)
        if metrics is None:
            self._stage_metrics = metrics = {}
        item = metrics.setdefault(stage, {"calls": 0, "total_ms": 0.0, "samples_ms": deque(maxlen=500)})
        item["calls"] = int(item.get("calls", 0)) + max(0, int(calls))
        item["total_ms"] = float(item.get("total_ms", 0.0)) + max(0.0, float(elapsed_ms))
        samples = item.setdefault("samples_ms", deque(maxlen=500))
        samples.append(Decimal(str(max(0.0, float(elapsed_ms)))))

    def _commit_owner(self) -> None:
        started = time.monotonic()
        try:
            self.connection.commit()
            self._db_dirty = False
            self._last_db_write_at = self.clock()
        except Exception as exc:
            self._db_commit_error_count += 1
            self._db_write_error_count += 1
            if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                self._db_locked_error_count += 1
            raise
        finally:
            self._record_stage_metric("db_commit", (time.monotonic() - started) * 1000)

    @staticmethod
    def _flow_event_key(event: BscPairEvent) -> tuple[str, int | None, str]:
        tx = str(event.transaction_hash or "").lower()
        if not tx:
            tx = f"block:{event.block_number}:{event.pair_address.lower()}"
        return tx, event.log_index, event.event_type

    @staticmethod
    def _flow_topics_for_descriptor(descriptor: BscPoolDescriptor) -> tuple[str, ...]:
        return tuple(str(topic).lower() for topic in descriptor.subscription_topics)

    def _remember_event_key(
        self,
        event: BscPairEvent,
        *,
        target: str,
        key_set: set[tuple[str, int | None, str]],
    ) -> None:
        key = self._flow_event_key(event)
        if key in key_set:
            return
        target_queue = getattr(self, target)
        if len(target_queue) >= target_queue.maxlen:
            key_set.discard(target_queue.popleft())
        key_set.add(key)
        target_queue.append(key)

    def set_flow_reconciliation_resubscribe_callback(self, callback: Callable[[], None] | None) -> None:
        """Attach the runtime-owned WSS reconnect hook without a DB worker."""

        self._flow_reconciliation_resubscribe_callback = callback

    def consume_flow_reconciliation_resubscribe_request(self) -> bool:
        requested = bool(self._flow_reconciliation_resubscribe_requested)
        self._flow_reconciliation_resubscribe_requested = False
        return requested

    def _load_exclusions(self) -> None:
        if self.config.identity == SURVIVOR_REVERSAL_IDENTITY:
            # Historical exclusions were created solely for the retired BSC
            # complete-ATH-history hard gate. Keep rows for auditability, but
            # never use them to suppress a new local first-price observation.
            self._excluded_mints = set()
            return
        try:
            rows = self.connection.execute("SELECT mint FROM survivor_exclusions").fetchall()
        except Exception:
            rows = ()
        self._excluded_mints = {self._mint_key(str(row[0])) for row in rows if row[0]}

    def _load_candidates(self) -> None:
        try:
            rows = self.connection.execute("SELECT * FROM survivor_candidates").fetchall()
        except Exception:
            rows = ()
        recomputed_candidates: list[SurvivorCandidate] = []
        for row in rows:
            try:
                candidate = SurvivorCandidate(
                    mint=str(_row_value(row, "mint")), symbol=_row_value(row, "symbol"),
                    first_seen_at=datetime.fromisoformat(str(_row_value(row, "first_seen_at"))),
                    last_seen_at=datetime.fromisoformat(str(_row_value(row, "last_seen_at"))),
                    data_quality_cohort=str(_row_value(row, "data_quality_cohort", "LEGACY") or "LEGACY"),
                    token_created_at=datetime.fromisoformat(str(_row_value(row, "token_created_at"))) if _row_value(row, "token_created_at") else None,
                    discovery_delay_seconds=_decimal(_row_value(row, "discovery_delay_seconds")),
                    first_snapshot_at=datetime.fromisoformat(str(_row_value(row, "first_snapshot_at"))) if _row_value(row, "first_snapshot_at") else None,
                    first_lifecycle=_row_value(row, "first_lifecycle"),
                    first_rank_type=int(_row_value(row, "first_rank_type")) if _row_value(row, "first_rank_type") is not None else None,
                    first_market_cap_usd=_decimal(_row_value(row, "first_market_cap_usd")),
                    first_liquidity_usd=_decimal(_row_value(row, "first_liquidity_usd")),
                    first_holders=int(_row_value(row, "first_holders")) if _row_value(row, "first_holders") is not None else None,
                    first_price_usd=_decimal(_row_value(row, "first_price_usd")),
                    first_progress_pct=_decimal(_row_value(row, "first_progress_pct")),
                    latest_lifecycle=_row_value(row, "latest_lifecycle"),
                    latest_rank_type=int(_row_value(row, "latest_rank_type")) if _row_value(row, "latest_rank_type") is not None else None,
                    latest_progress_pct=_decimal(_row_value(row, "latest_progress_pct")),
                    latest_migrate_status=int(_row_value(row, "latest_migrate_status")) if _row_value(row, "latest_migrate_status") is not None else None,
                    emit_count=int(_row_value(row, "emit_count", 0) or 0),
                    state=str(_row_value(row, "state", "DISCOVERED")),
                    market_cap_usd=_decimal(_row_value(row, "market_cap_usd")),
                    liquidity_usd=_decimal(_row_value(row, "liquidity_usd")),
                    holders=int(_row_value(row, "holders")) if _row_value(row, "holders") is not None else None,
                    current_price_native=_decimal(_row_value(row, "current_price_native")),
                    ath_price_native=_decimal(_row_value(row, "ath_price_native")),
                    local_low_native=_decimal(_row_value(row, "local_low_native")),
                    low_started_at=datetime.fromisoformat(str(_row_value(row, "low_started_at"))) if _row_value(row, "low_started_at") else None,
                    stable_since=datetime.fromisoformat(str(_row_value(row, "stable_since"))) if _row_value(row, "stable_since") else None,
                    pair_address=_row_value(row, "pair_address"), pool_type=_row_value(row, "pool_type"),
                    last_rejection=_row_value(row, "last_rejection"),
                    first_seen_price_usd=_decimal(_row_value(row, "first_seen_price_usd")),
                    first_seen_price_source=_row_value(row, "first_seen_price_source"),
                    current_price_usd=_decimal(_row_value(row, "current_price_usd")),
                    ath_price_usd=_decimal(_row_value(row, "ath_price_usd")),
                    ath_at=datetime.fromisoformat(str(_row_value(row, "ath_at"))) if _row_value(row, "ath_at") else None,
                    drawdown_pct=_decimal(_row_value(row, "drawdown_pct")),
                    age_seconds=int(_row_value(row, "age_seconds", 0) or 0),
                    price_status=str(_row_value(row, "price_status", "PENDING")),
                    price_source=_row_value(row, "price_source"),
                    price_updated_at=datetime.fromisoformat(str(_row_value(row, "price_updated_at"))) if _row_value(row, "price_updated_at") else None,
                    price_age_ms=int(_row_value(row, "price_age_ms")) if _row_value(row, "price_age_ms") is not None else None,
                    candidate_eligible=bool(_row_value(row, "candidate_eligible", 0)),
                    active_candidate=bool(_row_value(row, "active_candidate", 0)),
                    audit_state=str(_row_value(row, "audit_state", "NOT_REQUESTED")),
                    audit_requested=bool(_row_value(row, "audit_requested", 0)),
                    audit_failed=bool(_row_value(row, "audit_failed", 0)),
                    quote_state=str(_row_value(row, "quote_state", "NOT_REQUESTED")),
                    quote_requested=bool(_row_value(row, "quote_requested", 0)),
                    ready_to_buy=bool(_row_value(row, "ready_to_buy", 0)),
                    paper_buy=bool(_row_value(row, "paper_buy", 0)),
                    native_token_price_usd=_decimal(_row_value(row, "native_token_price_usd")),
                    liquidity_current_usd=_decimal(_row_value(row, "liquidity_current_usd")),
                    liquidity_1m_ago_usd=_decimal(_row_value(row, "liquidity_1m_ago_usd")),
                    liquidity_2m_ago_usd=_decimal(_row_value(row, "liquidity_2m_ago_usd")),
                    buy_volume_30s=_decimal(_row_value(row, "buy_volume_30s", "0")) or Decimal("0"),
                    sell_volume_30s=_decimal(_row_value(row, "sell_volume_30s", "0")) or Decimal("0"),
                    buy_volume_1m=_decimal(_row_value(row, "buy_volume_1m", "0")) or Decimal("0"),
                    sell_volume_1m=_decimal(_row_value(row, "sell_volume_1m", "0")) or Decimal("0"),
                    buy_volume_3m=_decimal(_row_value(row, "buy_volume_3m", "0")) or Decimal("0"),
                    sell_volume_3m=_decimal(_row_value(row, "sell_volume_3m", "0")) or Decimal("0"),
                    buy_count_1m=int(_row_value(row, "buy_count_1m", 0) or 0),
                    sell_count_1m=int(_row_value(row, "sell_count_1m", 0) or 0),
                    buy_sell_volume_ratio_1m=_decimal(_row_value(row, "buy_sell_volume_ratio_1m")),
                    buy_sell_count_ratio_1m=_decimal(_row_value(row, "buy_sell_count_ratio_1m")),
                    price_change_1m=_decimal(_row_value(row, "price_change_1m")),
                    source_status=json.loads(str(_row_value(row, "source_status_json", "{}") or "{}")),
                    last_price_attempt_at=datetime.fromisoformat(str(_row_value(row, "last_price_attempt_at"))) if _row_value(row, "last_price_attempt_at") else None,
                    pre_candidate_watch=bool(_row_value(row, "pre_candidate_watch", 0)),
                    pre_candidate_rank=int(_row_value(row, "pre_candidate_rank")) if _row_value(row, "pre_candidate_rank") is not None else None,
                    candidate_distance_score=_decimal(_row_value(row, "candidate_distance_score")),
                    first_price_at=datetime.fromisoformat(str(_row_value(row, "first_price_at"))) if _row_value(row, "first_price_at") else None,
                    first_price_delay_seconds=_decimal(_row_value(row, "first_price_delay_seconds")),
                    price_samples_before_candidate=int(_row_value(row, "price_samples_before_candidate", 0) or 0),
                    price_coverage_before_candidate_seconds=_decimal(_row_value(row, "price_coverage_before_candidate_seconds", "0")) or Decimal("0"),
                    ath_before_candidate=bool(_row_value(row, "ath_before_candidate", 0)),
                    ath_before_candidate_price_usd=_decimal(_row_value(row, "ath_before_candidate_price_usd")),
                    ath_before_candidate_at=datetime.fromisoformat(str(_row_value(row, "ath_before_candidate_at"))) if _row_value(row, "ath_before_candidate_at") else None,
                    price_history_quality=str(_row_value(row, "price_history_quality", PRICE_HISTORY_QUALITY_INSUFFICIENT) or PRICE_HISTORY_QUALITY_INSUFFICIENT),
                    candidate_at=datetime.fromisoformat(str(_row_value(row, "candidate_at"))) if _row_value(row, "candidate_at") else None,
                    price_history_status=str(_row_value(row, "price_history_status", "PENDING")),
                    history_source=_row_value(row, "history_source"),
                    history_start_at=datetime.fromisoformat(str(_row_value(row, "history_start_at"))) if _row_value(row, "history_start_at") else None,
                    history_end_at=datetime.fromisoformat(str(_row_value(row, "history_end_at"))) if _row_value(row, "history_end_at") else None,
                    history_sample_count=int(_row_value(row, "history_sample_count", 0) or 0),
                    history_interval=_row_value(row, "history_interval"),
                    max_history_gap_seconds=_decimal(_row_value(row, "max_history_gap_seconds")),
                )
                # Rows written before the Portal adapter was enabled may
                # still carry the old unsupported/not-requested marker.  A
                # restart normalizes only the already identified Flap family;
                # no historical RPC scan or strategy promotion is performed.
                if self._is_flap_candidate(candidate) and candidate.latest_migrate_status != 1:
                    candidate.source_status.update({
                        "strategy_support": "FLOW_SUPPORTED",
                        "flow_status": "SUPPORTED",
                        # Older rows were persisted before the Portal adapter
                        # exposed realtime WSS/Flow.  Keep the diagnostic
                        # capability matrix consistent with the live state;
                        # this is an in-memory startup normalization and does
                        # not alter any entry/exit rule.
                        "venue_capabilities": json.dumps({
                            "BUY_QUOTE": "SUPPORTED",
                            "DISCOVERY": "SUPPORTED",
                            "FLOW": "SUPPORTED",
                            "LIQUIDITY": "SUPPORTED",
                            "PAPER_FILL": "UNSUPPORTED",
                            "PRICE": "SUPPORTED",
                            "SELL_QUOTE": "SUPPORTED",
                            "WSS": "SUPPORTED",
                        }, sort_keys=True, separators=(",", ":")),
                        "wss_status": "READY",
                        "wss_subscription_status": "READY",
                        "wss_failure_reason": "",
                    })
                candidate.data_quality_cohort = self._data_quality_cohort(candidate.first_seen_at)
                if candidate.candidate_at is not None and candidate.candidate_eligible:
                    self._recompute_candidate_history(candidate)
                    recomputed_candidates.append(candidate)
                if (
                    candidate.first_rank_type is not None
                    and candidate.latest_rank_type is not None
                    and candidate.latest_rank_type < candidate.first_rank_type
                ):
                    # Repair pre-fix rows that recorded a reordered lower-rank
                    # snapshot as the latest lifecycle. Never persist a
                    # NEW regression after a token reached a later stage.
                    candidate.latest_rank_type = candidate.first_rank_type
                    candidate.latest_lifecycle = {
                        10: "MEME_NEW",
                        20: "MEME_FINALIZING",
                        30: "MEME_MIGRATED",
                    }.get(candidate.first_rank_type, candidate.latest_lifecycle)
                legacy_reason = str(candidate.last_rejection or "")
                if legacy_reason.startswith(("ACTIVE_", "MC_OUTSIDE_10K", "AGE_BELOW_3M", "BUY_FLOW_GATE", "OBSERVATION_", "PRICE_NOT_UP")):
                    candidate.state = "LIGHT_TRACKING"
                    candidate.last_rejection = None
                    candidate.candidate_eligible = False
                    candidate.active_candidate = False
                self._candidates[candidate.mint] = candidate
                if candidate.native_token_price_usd is not None and candidate.native_token_price_usd > 0:
                    if self._latest_native_token_price_usd is None or candidate.last_seen_at >= self._latest_native_token_price_observed_at:
                        self._latest_native_token_price_usd = candidate.native_token_price_usd
                        self._latest_native_token_price_observed_at = candidate.last_seen_at
            except (KeyError, TypeError, ValueError, InvalidOperation):
                continue
        try:
            flow_rows = self.connection.execute(
                "SELECT * FROM survivor_flow_samples WHERE observed_at >= ? ORDER BY observed_at ASC",
                ((self.clock() - timedelta(seconds=120)).isoformat(),),
            ).fetchall()
        except Exception:
            flow_rows = ()
        for row in flow_rows:
            candidate = self._candidates.get(str(row["mint"]))
            if candidate is None:
                continue
            candidate.flows.append(FlowSample(
                observed_at=datetime.fromisoformat(str(row["observed_at"])),
                buy_volume_bnb=Decimal(str(row["buy_volume_bnb"])),
                sell_volume_bnb=Decimal(str(row["sell_volume_bnb"])),
                buy_count=int(row["buy_count"]), sell_count=int(row["sell_count"]),
                price_native=_decimal(row["price_native"]),
                liquidity_native=_decimal(row["liquidity_native"]),
                price_usd=_decimal(_row_value(row, "price_usd")),
                liquidity_usd=_decimal(_row_value(row, "liquidity_usd")),
                price_source=_row_value(row, "price_source"),
            ))
        if recomputed_candidates:
            persist_at = self.clock()
            for candidate in recomputed_candidates:
                self._persist_candidate(candidate, persist_at)
            self.connection.commit()

    def _load_positions(self) -> None:
        try:
            rows = self.connection.execute(
                "SELECT * FROM survivor_positions WHERE status = 'OPEN'"
            ).fetchall()
        except Exception:
            rows = ()
        for row in rows:
            try:
                intent_row = self.connection.execute("SELECT value_json FROM runtime_state WHERE mode=? AND state_key=?", (self.mode, f"exit_intent:{row['position_id']}")).fetchone()
                intent = json.loads(intent_row[0]) if intent_row else {}
                active_intent = intent if isinstance(intent, dict) and intent.get("state") == "EXIT_TRIGGERED_WAITING_ROUTE" else {}
                self._positions[str(row["position_id"])] = SurvivorPosition(
                    position_id=str(row["position_id"]), mint=str(row["mint"]),
                    symbol=row["symbol"], opened_at=datetime.fromisoformat(str(row["opened_at"])),
                    entry_price_native=Decimal(str(row["entry_price_native"])),
                    current_price_native=_decimal(row["current_price_native"]),
                    quantity_token=Decimal(str(row["quantity_token"])),
                    remaining_quantity_token=Decimal(str(row["remaining_quantity_token"])),
                    invested_bnb=Decimal(str(row["invested_bnb"])),
                    realized_bnb=Decimal(str(row["realized_bnb"] or "0")),
                    tp1=bool(_row_value(row, "tp1_filled", row["tp1_at"] is not None)),
                    tp2=bool(_row_value(row, "tp2_filled", row["tp2_at"] is not None)),
                    tp1_triggered=bool(_row_value(row, "tp1_triggered", row["tp1_at"] is not None)),
                    tp1_triggered_at=(datetime.fromisoformat(str(_row_value(row, "tp1_triggered_at"))) if _row_value(row, "tp1_triggered_at") else None),
                    tp1_trigger_price_native=_decimal(_row_value(row, "tp1_trigger_price_native")),
                    tp1_filled=bool(_row_value(row, "tp1_filled", row["tp1_at"] is not None)),
                    tp1_filled_at=(datetime.fromisoformat(str(_row_value(row, "tp1_filled_at"))) if _row_value(row, "tp1_filled_at") else (datetime.fromisoformat(str(row["tp1_at"])) if row["tp1_at"] else None)),
                    tp2_triggered=bool(_row_value(row, "tp2_triggered", row["tp2_at"] is not None)),
                    tp2_triggered_at=(datetime.fromisoformat(str(_row_value(row, "tp2_triggered_at"))) if _row_value(row, "tp2_triggered_at") else None),
                    tp2_trigger_price_native=_decimal(_row_value(row, "tp2_trigger_price_native")),
                    tp2_filled=bool(_row_value(row, "tp2_filled", row["tp2_at"] is not None)),
                    tp2_filled_at=(datetime.fromisoformat(str(_row_value(row, "tp2_filled_at"))) if _row_value(row, "tp2_filled_at") else (datetime.fromisoformat(str(row["tp2_at"])) if row["tp2_at"] else None)),
                    tp3_triggered=bool(_row_value(row, "tp3_triggered", False)),
                    tp3_triggered_at=(datetime.fromisoformat(str(_row_value(row, "tp3_triggered_at"))) if _row_value(row, "tp3_triggered_at") else None),
                    tp3_trigger_price_native=_decimal(_row_value(row, "tp3_trigger_price_native")),
                    tp3_filled=bool(_row_value(row, "tp3_filled", False)),
                    tp3_filled_at=(datetime.fromisoformat(str(_row_value(row, "tp3_filled_at"))) if _row_value(row, "tp3_filled_at") else None),
                    high_since_tp1_native=_decimal(_row_value(row, "high_since_tp1_native")),
                    trailing_active=bool(row["trailing_active"]),
                    last_trade_at=datetime.fromisoformat(str(row["last_trade_at"])) if row["last_trade_at"] else datetime.fromisoformat(str(row["opened_at"])),
                    status="OPEN",
                    exit_intent_reason=str(active_intent["reason"]) if active_intent.get("reason") else None,
                    exit_intent_quantity=_decimal(active_intent.get("quantity")),
                    exit_trigger_reason=(
                        str(active_intent.get("trigger_reason"))
                        if active_intent.get("trigger_reason")
                        else (str(_row_value(row, "exit_trigger_reason")) if _row_value(row, "exit_trigger_reason") else None)
                    ),
                    exit_trigger_pnl_pct=(
                        _decimal(active_intent.get("trigger_pnl_pct"))
                        if active_intent.get("trigger_pnl_pct") is not None
                        else _decimal(_row_value(row, "exit_trigger_pnl_pct"))
                    ),
                    exit_trigger_price_native=(
                        _decimal(active_intent.get("trigger_price_native"))
                        if active_intent.get("trigger_price_native") is not None
                        else _decimal(_row_value(row, "exit_trigger_price_native"))
                    ),
                    exit_triggered_at=(
                        datetime.fromisoformat(str(active_intent["triggered_at"]))
                        if active_intent.get("triggered_at")
                        else (datetime.fromisoformat(str(_row_value(row, "exit_triggered_at"))) if _row_value(row, "exit_triggered_at") else None)
                    ),
                    position_mark_price_native=(
                        _decimal(_row_value(row, "position_mark_price_native"))
                        or _decimal(row["current_price_native"])
                    ),
                    position_mark_price_usd=_decimal(_row_value(row, "position_mark_price_usd")),
                    position_mark_source=_row_value(row, "position_mark_source"),
                    position_price_updated_at=(
                        datetime.fromisoformat(str(_row_value(row, "position_price_updated_at")))
                        if _row_value(row, "position_price_updated_at")
                        else None
                    ),
                    position_price_freshness=str(
                        _row_value(row, "position_price_freshness", "UNAVAILABLE") or "UNAVAILABLE"
                    ),
                    external_exit_unpriced=bool(_row_value(row, "external_exit_unpriced", 0)),
                    no_trade_profit_partial_done=bool(_row_value(row, "no_trade_profit_partial_done", 0)),
                )
            except (KeyError, TypeError, ValueError, InvalidOperation):
                continue

    def _hydrate_active_candidate_pools(self) -> None:
        """Queue only current migrated rows that lack live market data.

        This deliberately performs no RPC or database work on startup.  The
        existing bounded venue worker receives the recovery jobs, preserving
        the realtime startup path while ensuring a persisted migrated token
        cannot remain stranded after a restart.
        """

        now = self.clock()
        for candidate in self._candidates.values():
            self.ensure_migrated_market_data(candidate, now)

    @staticmethod
    def _pool_context_protocol(context: object | None) -> str | None:
        name = type(context).__name__ if context is not None else ""
        if name.endswith("Context"):
            name = name[:-7]
        return name or None

    @staticmethod
    def _is_fourmeme_venue(protocol: object | None, context_protocol: str | None) -> bool:
        """Identify FourMeme only from an explicit venue label/context.

        Numeric protocol identifiers are deliberately not used here: some
        Binance records overload them across launchpads.
        """

        return "fourmeme" in str(context_protocol or protocol or "").lower().replace(" ", "")

    @staticmethod
    def _migration_flag(value: object | None) -> bool | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        normalized = str(value).strip().lower()
        if normalized in {"1", "true", "migrated", "yes"}:
            return True
        if normalized in {"0", "false", "not_migrated", "no"}:
            return False
        return None

    @staticmethod
    def _fourmeme_venue_state(candidate: SurvivorCandidate) -> str:
        return "FOURMEME_BONDING_CURVE"

    def _migration_discovery_due(self, candidate: SurvivorCandidate, now: datetime, force: bool) -> bool:
        """Bounded infrastructure retry, independent from Strategy Candidate gates."""

        if force:
            return True
        raw = candidate.source_status.get("migration_discovery_started_at")
        if not raw:
            candidate.source_status["migration_discovery_started_at"] = now.isoformat()
            candidate.source_status["migration_discovery_attempts"] = "0"
            return True
        try:
            started = datetime.fromisoformat(str(raw))
            attempts = int(candidate.source_status.get("migration_discovery_attempts", "0"))
        except (ValueError, TypeError):
            candidate.source_status["migration_discovery_started_at"] = now.isoformat()
            candidate.source_status["migration_discovery_attempts"] = "0"
            return True
        if attempts >= len(self.MIGRATED_DISCOVERY_RETRY_SECONDS):
            # A migration index can lag the on-chain PoolCreated transaction.
            # After the short burst, retain the same infrastructure check at a
            # low rate for at most 30 minutes; this remains independent of all
            # strategy-entry gates and stops immediately on a verified pool.
            if (now - started).total_seconds() > 30 * 60:
                return False
            last_attempt_raw = candidate.source_status.get("wss_resolution_attempt_at")
            try:
                last_attempt = datetime.fromisoformat(str(last_attempt_raw))
            except (TypeError, ValueError):
                return True
            return (now - last_attempt).total_seconds() >= 5 * 60
        return (now - started).total_seconds() >= self.MIGRATED_DISCOVERY_RETRY_SECONDS[attempts]

    def _migrated_market_data_bound(self, candidate: SurvivorCandidate) -> bool:
        """Whether a migrated token has a supported pool and live binding."""

        descriptor = candidate.descriptor
        if descriptor is None or not self._pool_pair_is_valid(candidate, descriptor):
            return False
        status = candidate.source_status
        return (
            status.get("pool_status") == "VALID"
            and status.get("strategy_support") == "SUPPORTED_ADAPTER"
            and status.get("wss_subscription_status") in {"READY", "SUBSCRIBED"}
        )

    def _within_migrated_monitoring_lifecycle(self, candidate: SurvivorCandidate, now: datetime) -> bool:
        """Keep recovery bounded to the strategy's existing monitoring age."""

        max_age = self.config.max_age_sec
        return max_age is None or self._age_seconds(candidate, now) <= max_age

    def ensure_migrated_market_data(self, candidate: SurvivorCandidate, now: datetime) -> bool:
        """Idempotently ensure recovery for a monitored migrated token.

        This is the single owner-loop entry point for migrated pool recovery.
        It only queues existing worker work; it never performs RPC inline and
        therefore cannot block Candidate/Position evaluation.
        """

        if self._migration_flag(candidate.latest_migrate_status) is not True:
            return False
        if not self._within_migrated_monitoring_lifecycle(candidate, now):
            return False
        if self._migrated_market_data_bound(candidate):
            return False
        if candidate.mint in self._migrated_pool_recovery_jobs:
            return False
        if not self._migration_discovery_due(candidate, now, force=False):
            return False
        candidate.source_status.update({
            "venue_state": "MIGRATED_POOL_PENDING",
            "migrated_pool_recovery_state": "PENDING",
            "venue_readiness": "VENUE_UNRESOLVED",
            "pool_status": "MIGRATED_POOL_PENDING",
            "wss_status": "NOT_REQUESTED",
            "wss_subscription_status": "NOT_REQUESTED",
            "price_status": "PENDING",
            "canonical_strategy_price": "",
            "wss_resolution_attempt_at": now.isoformat(),
            "migration_discovery_attempts": str(
                int(candidate.source_status.get("migration_discovery_attempts", "0")) + 1
            ),
        })
        self._schedule_migrated_pool_recovery(candidate, now)
        return True

    @staticmethod
    def _pool_pair_is_valid(candidate: SurvivorCandidate, descriptor: BscPoolDescriptor) -> bool:
        if descriptor.pool_type not in {V2_POOL_TYPE, V3_POOL_TYPE}:
            return False
        mint = normalize_bsc_address(candidate.mint)
        token0 = normalize_bsc_address(descriptor.token0)
        token1 = normalize_bsc_address(descriptor.token1)
        if mint is None or mint not in {token0, token1}:
            return False
        quote = token1 if token0 == mint else token0
        # Quote-asset USD conversion is asynchronous and recoverable.  A
        # parseable V2/V3 pool with any ERC-20 quote is still a valid realtime
        # market-data venue.
        return quote is not None

    @staticmethod
    def _pool_raw_price_from_reserves(
        candidate: SurvivorCandidate,
        descriptor: BscPoolDescriptor,
        reserves: tuple[int, int] | None,
    ) -> Decimal | None:
        """Return target-token price in the pool's quote asset for a V2 pair.

        This is a raw market fact, not a USD price.  Its conversion remains
        delegated to ``QuoteAssetUsdResolver`` so a non-WBNB quote never gets
        mistaken for BNB.
        """
        if descriptor.pool_type != V2_POOL_TYPE or reserves is None:
            return None
        mint = normalize_bsc_address(candidate.mint)
        token0 = normalize_bsc_address(descriptor.token0)
        token1 = normalize_bsc_address(descriptor.token1)
        if mint is None or mint not in {token0, token1}:
            return None
        if descriptor.token0_decimals is None or descriptor.token1_decimals is None:
            return None
        reserve0, reserve1 = reserves
        if reserve0 <= 0 or reserve1 <= 0:
            return None
        target_reserve, target_decimals, quote_reserve, quote_decimals = (
            (reserve0, descriptor.token0_decimals, reserve1, descriptor.token1_decimals)
            if token0 == mint
            else (reserve1, descriptor.token1_decimals, reserve0, descriptor.token0_decimals)
        )
        if target_reserve <= 0:
            return None
        return (
            Decimal(quote_reserve) / (Decimal(10) ** quote_decimals)
        ) / (
            Decimal(target_reserve) / (Decimal(10) ** target_decimals)
        )

    @staticmethod
    def _venue_strategy_support(inspection: BscVenueInspection) -> str:
        if not inspection.is_contract:
            return inspection.error_class or "NOT_A_CONTRACT"
        if inspection.protocol_family in {"PANCAKE_V2", "PANCAKE_V3"}:
            return "SUPPORTED_ADAPTER"
        if inspection.protocol_family == "FLAP_CONTEXT":
            return "SUPPORTED_ADAPTER"
        return "UNSUPPORTED_PROTOCOL"

    @staticmethod
    def _is_flap_context(context: object | None) -> bool:
        return type(context).__name__ == "FlapContext"

    @staticmethod
    def _is_flap_candidate(candidate: SurvivorCandidate) -> bool:
        status = candidate.source_status
        return (
            str(status.get("protocol_family") or "").upper() == "FLAP_CONTEXT"
            or str(status.get("protocol") or "").lower() == "flap"
            or str(status.get("venue") or "").lower() == "flap"
        )

    def _persist_pre_migration_context(
        self,
        candidate: SurvivorCandidate,
        context: object,
        now: datetime,
    ) -> BscVenueInspection | None:
        """Persist token and manager fingerprints before adapter selection.

        No Binance pair field is trusted here.  The token proxy is retained as
        a fingerprint record and the actual Portal/manager returned by the
        read-only Context is retained as the venue record.  Both writes occur
        only in the owner loop through the existing connection.
        """

        if self.resolver is None or not hasattr(self, "connection"):
            return None
        token_inspection = None
        if hasattr(self.resolver, "inspect_venue"):
            try:
                token_inspection = self.resolver.inspect_venue(candidate.mint)
            except Exception:
                token_inspection = None
        if token_inspection is not None:
            self._persist_venue_registry(candidate.mint, token_inspection, now, source="PRE_MIGRATION_TOKEN_FINGERPRINT")
        launchpad = normalize_bsc_address(getattr(context, "launchpad", None))
        if launchpad is None:
            return None
        inspection = None
        if self._is_flap_context(context) and hasattr(self.resolver, "inspect_flap_context"):
            try:
                inspection = self.resolver.inspect_flap_context(launchpad)
            except Exception:
                inspection = None
        if inspection is None and hasattr(self.resolver, "inspect_venue"):
            try:
                inspection = self.resolver.inspect_venue(launchpad)
            except Exception:
                inspection = None
        if inspection is not None:
            self._persist_venue_registry(candidate.mint, inspection, now, source="PRE_MIGRATION_CONTEXT")
        return inspection

    def _apply_flap_pre_migration_context(
        self,
        candidate: SurvivorCandidate,
        context: object,
        now: datetime,
    ) -> None:
        """Route a verified Flap bonding curve without inventing a WSS fill path."""

        inspection = self._persist_pre_migration_context(candidate, context, now)
        launchpad = normalize_bsc_address(getattr(context, "launchpad", None))
        implementation = normalize_bsc_address(getattr(context, "token_implementation", None))
        quote_asset = normalize_bsc_address(getattr(context, "fundraising_currency", None))
        capabilities = inspection.capabilities_json if inspection is not None else json.dumps({
            "DISCOVERY": "SUPPORTED", "PRICE": "SUPPORTED", "LIQUIDITY": "SUPPORTED",
            "WSS": "SUPPORTED", "FLOW": "SUPPORTED", "BUY_QUOTE": "SUPPORTED",
            "SELL_QUOTE": "SUPPORTED", "PAPER_FILL": "UNSUPPORTED",
        }, sort_keys=True, separators=(",", ":"))
        token_decimals = getattr(context, "token_decimals", None)
        quote_decimals = getattr(context, "fundraising_decimals", 18)
        reconciliation_source = getattr(context, "reconciliation_source", "GET_TOKEN_V6")
        candidate.descriptor = None
        candidate.pair_address = None
        candidate.pool_type = None
        candidate.source_status.update({
            "venue": "Flap",
            "venue_address": launchpad or "",
            "venue_state": "PRE_MIGRATION",
            "venue_readiness": "VENUE_READY",
            "pool_source": "FLAP_PORTAL_CONTEXT",
            "pool_status": "VALID",
            "pool_valid": "VALID",
            "protocol": "Flap",
            "protocol_family": "FLAP_CONTEXT",
            "protocol_fingerprint": inspection.protocol_fingerprint if inspection is not None else "FLAP_PORTAL_CONTEXT_V1",
            "selector_fingerprint": inspection.selector_bitmap if inspection is not None else "flap_portal_context=1",
            "venue_capabilities": capabilities,
            "strategy_support": "FLOW_SUPPORTED",
            "flow_status": "SUPPORTED",
            "buy_quote_capability": "SUPPORTED",
            "sell_quote_capability": "SUPPORTED",
            "paper_fill_capability": "UNSUPPORTED",
            "pre_migration_resolution_status": "RESOLVED",
            "token_proxy_implementation": implementation or "",
            "fundraising_quote_asset": quote_asset or "NATIVE_BNB",
            "wss_status": "READY",
            "wss_subscription_status": "READY",
            "wss_failure_reason": "",
            "flap_portal_address": FLAP_PORTAL_ADDRESS,
            "flap_token_decimals": str(token_decimals) if token_decimals is not None else "18",
            "fundraising_quote_decimals": str(quote_decimals),
            "flap_state_reconciliation_source": reconciliation_source,
        })
        self._apply_flap_curve_liquidity(candidate, context)
        self._apply_flap_reconciliation_price(candidate, context, now)

    def _apply_flap_curve_liquidity(self, candidate: SurvivorCandidate, context: object) -> None:
        """Use Flap's verified quote-side reserve for the current liquidity.

        Binance's Meme Rush ``liquidity`` field is an indicative source metric
        and is not the bonding-curve reserve.  Once ``getTokenV6`` supplies a
        reserve, the strategy keeps the Binance value for diagnostics but uses
        the on-chain reserve for the current liquidity gate/dashboard.  USD is
        only derived from an observed native-token price.
        """

        raw = getattr(context, "curve_reserve_raw", None)
        if raw is None:
            raw = candidate.source_status.get("curve_reserve_raw")
        try:
            reserve_raw = int(raw) if raw is not None else 0
        except (TypeError, ValueError):
            reserve_raw = 0
        if reserve_raw <= 0:
            return
        reserve_bnb = Decimal(reserve_raw) / (Decimal(10) ** 18)
        native_usd = candidate.native_token_price_usd or self._latest_native_token_price_usd
        candidate.source_status.update({
            "liquidity_source": "FLAP_CURVE_ONCHAIN",
            "liquidity_usd_source": "FLAP_CURVE_ONCHAIN" if native_usd and native_usd > 0 else "NATIVE_PRICE_UNAVAILABLE",
            "curve_reserve_raw": str(reserve_raw),
            "curve_reserve_bnb": str(reserve_bnb),
            "binance_liquidity_usd": str(candidate.liquidity_usd) if candidate.liquidity_usd is not None else "",
        })
        if native_usd is None or native_usd <= 0:
            return
        # Flap reports the quote-side reserve.  A balanced curve liquidity
        # value is twice that reserve, matching the pool convention used by
        # the Pancake resolver.
        onchain_liquidity_usd = Decimal(2) * reserve_bnb * native_usd
        candidate.liquidity_usd = onchain_liquidity_usd
        candidate.liquidity_current_usd = onchain_liquidity_usd

    def _fresh_bnb_usd(self, now: datetime) -> Decimal | None:
        """Return the shared BNB/USD mark only while it remains fresh."""
        observed_at = getattr(self, "_latest_native_token_price_observed_at", None)
        value = getattr(self, "_latest_native_token_price_usd", None)
        if value is None or value <= 0 or observed_at is None:
            return None
        if now - observed_at > timedelta(seconds=90):
            return None
        return value

    @staticmethod
    def _is_native_quote_asset(quote_asset: str) -> bool:
        return quote_asset in {"", "native_bnb", "bnb", BSC_WBNB_ADDRESS, "0x0000000000000000000000000000000000000000"}

    def _fresh_quote_asset_usd(self, quote_asset: str, now: datetime) -> QuoteAssetUsdResolution | None:
        normalized = normalize_bsc_address(quote_asset)
        if normalized is None:
            return None
        result = self._quote_asset_usd_cache.get((56, normalized))
        if result is None or not result.fresh or result.price_usd is None or result.price_usd <= 0:
            return None
        if now - result.updated_at > timedelta(seconds=self._quote_asset_usd_refresh_seconds):
            return None
        return result

    def _schedule_quote_asset_usd_resolution(self, quote_asset: str, now: datetime, bnb_usd: Decimal | None) -> None:
        normalized = normalize_bsc_address(quote_asset)
        if normalized is None:
            return
        key = (56, normalized)
        cached = self._quote_asset_usd_cache.get(key)
        if cached is not None and now - cached.updated_at <= timedelta(seconds=self._quote_asset_usd_refresh_seconds):
            return
        if key in self._quote_asset_usd_pending:
            return
        self._quote_asset_usd_pending.add(key)
        job = QuoteAssetUsdJob(56, normalized, bnb_usd, now)
        self._live_pool_upgrade_executor.submit(self._run_quote_asset_usd_job, job)

    def _run_quote_asset_usd_job(self, job: QuoteAssetUsdJob) -> None:
        """Worker-only quote I/O. It never reads or writes the runtime DB."""
        rpc = getattr(self.resolver, "rpc", None)
        if rpc is None:
            self._quote_asset_usd_results.put(QuoteAssetUsdResult(
                job,
                QuoteAssetUsdResolution(job.quote_asset, None, None, None, None, self.clock(), False, "RPC_UNAVAILABLE"),
                self.clock(), None,
            ))
            return
        # Keep the concrete GMGN adapter, not its provider-name string: the
        # shared QuoteAssetUsdResolver reuses its trusted token metadata and
        # quote capabilities as fallbacks after RPC reads.
        entry = self.entry_quote_provider
        if not callable(getattr(entry, "quote", None)):
            entry = getattr(entry, "provider", entry)
        resolver = QuoteAssetUsdResolver(rpc, gmgn_provider=entry)
        native_usd = job.native_usd
        if native_usd is None or native_usd <= 0:
            native = resolver.resolve_native_usd(job.chain_id)
            native_usd = native.price_usd if native.fresh else None
        result = resolver.resolve(job.chain_id, job.quote_asset, native_usd=native_usd)
        self._quote_asset_usd_results.put(QuoteAssetUsdResult(job, result, self.clock(), native_usd))

    def _fresh_binance_direct_usd(self, candidate: SurvivorCandidate, now: datetime) -> Decimal | None:
        value = _decimal(candidate.source_status.get("binance_price_usd"))
        if value is None or value <= 0:
            return None
        observed = candidate.source_status.get("binance_price_updated_at")
        try:
            updated_at = datetime.fromisoformat(str(observed)) if observed else candidate.last_seen_at
        except (TypeError, ValueError):
            updated_at = candidate.last_seen_at
        return value if now - updated_at <= timedelta(seconds=90) else None

    @staticmethod
    def _flap_raw_price_is_fresh(candidate: SurvivorCandidate, now: datetime) -> bool:
        for key in ("flap_last_post_price_at", "flap_reconciliation_price_at", "last_chain_trade_at"):
            raw = candidate.source_status.get(key)
            if not raw:
                continue
            try:
                return now - datetime.fromisoformat(str(raw)) <= timedelta(seconds=90)
            except (TypeError, ValueError):
                continue
        return False

    def _apply_quote_asset_usd_metadata(self, candidate: SurvivorCandidate, result: QuoteAssetUsdResolution, now: datetime) -> None:
        candidate.source_status.update({
            "quote_asset_price": str(result.price_usd) if result.price_usd is not None else "",
            "quote_asset_price_source": result.source or "",
            "quote_asset_price_at": result.updated_at.isoformat(),
            "quote_asset_price_age": str(max(0, int((now - result.updated_at).total_seconds()))),
            "quote_asset_decimals": str(result.decimals) if result.decimals is not None else "",
            "quote_asset_symbol": result.symbol or "",
            "quote_asset_price_failure": result.failure_reason or "",
        })

    def _recompute_pool_canonical_price(self, candidate: SurvivorCandidate, now: datetime) -> bool:
        """Convert a V2/V3 raw pool price through the shared quote-asset mark.

        Pool parsing and USD conversion are intentionally separate.  A valid
        Pancake pool remains WSS/Flow-capable while its quote asset is being
        resolved, and only the USD Candidate price stays pending.
        """
        raw_price = _decimal(candidate.source_status.get("pool_last_raw_price"))
        quote_asset = normalize_bsc_address(candidate.source_status.get("pool_quote_asset"))
        bnb_usd = self._fresh_bnb_usd(now)
        if raw_price is None or raw_price <= 0 or quote_asset is None:
            return False
        if self._is_native_quote_asset(quote_asset):
            quote_usd = bnb_usd
            conversion_source = "BNB_USD"
        else:
            cached = self._fresh_quote_asset_usd(quote_asset, now)
            quote_usd = cached.price_usd if cached is not None else None
            conversion_source = cached.source if cached is not None else None
            if quote_usd is None:
                self._schedule_quote_asset_usd_resolution(quote_asset, now, bnb_usd)
        canonical = raw_price * quote_usd if quote_usd is not None and quote_usd > 0 else None
        candidate.source_status.update({
            "pool_raw_price": str(raw_price),
            "pool_raw_quote_asset": quote_asset,
            "quote_asset_usd": str(quote_usd) if quote_usd is not None else "",
            "canonical_strategy_price": str(canonical) if canonical is not None else "",
            "canonical_price_source": conversion_source or "QUOTE_ASSET_PRICE_PENDING",
            "price_conversion_source": conversion_source or "QUOTE_ASSET_PRICE_PENDING",
            "price_conversion_updated_at": now.isoformat(),
        })
        if canonical is None or canonical <= 0:
            candidate.current_price_usd = None
            candidate.price_status = "QUOTE_ASSET_PRICE_PENDING"
            candidate.price_source = "BSC_POOL_WSS"
            candidate.price_updated_at = now
            candidate.source_status["price"] = "QUOTE_ASSET_PRICE_PENDING"
            return False
        # Position marks are denominated in BNB even when the venue's quote
        # asset is another ERC-20.  Derive that mark only from the fresh USD
        # conversion; never treat the raw quote-asset price as BNB.
        native_price = canonical / bnb_usd if bnb_usd is not None and bnb_usd > 0 else None
        self._update_price(candidate, canonical, now, "BSC_WSS", native_price=native_price, record_sample=False)
        return True

    def _schedule_pool_quote_asset_resolution(self, candidate: SurvivorCandidate, now: datetime) -> None:
        quote_asset = normalize_bsc_address(candidate.source_status.get("pool_quote_asset"))
        if quote_asset is None or self._is_native_quote_asset(quote_asset):
            return
        self._schedule_quote_asset_usd_resolution(
            quote_asset,
            now,
            self._fresh_bnb_usd(now) or candidate.native_token_price_usd,
        )

    def _drain_quote_asset_usd_results(self, now: datetime, *, max_items: int = 2, budget_ms: int = 25) -> int:
        started = time.monotonic()
        applied = 0
        while applied < max_items and (not applied or (time.monotonic() - started) * 1000 < budget_ms):
            try:
                item = self._quote_asset_usd_results.get_nowait()
            except Empty:
                break
            applied += 1
            key = (item.job.chain_id, item.job.quote_asset)
            self._quote_asset_usd_pending.discard(key)
            self._quote_asset_usd_cache[key] = item.resolution
            if item.native_usd is not None and item.native_usd > 0:
                self._latest_native_token_price_usd = item.native_usd
                self._latest_native_token_price_observed_at = item.completed_at
            for candidate in self._candidates.values():
                is_flap = self._is_flap_candidate(candidate)
                raw_quote_asset = (
                    candidate.source_status.get("fundraising_quote_asset")
                    if is_flap else candidate.source_status.get("pool_quote_asset")
                )
                quote_asset = normalize_bsc_address(raw_quote_asset)
                if quote_asset != item.job.quote_asset:
                    continue
                self._apply_quote_asset_usd_metadata(candidate, item.resolution, now)
                was_candidate_eligible = candidate.candidate_eligible
                if is_flap:
                    self._recompute_flap_canonical_price(candidate, now)
                else:
                    self._recompute_pool_canonical_price(candidate, now)
                candidate.candidate_eligible = self._candidate_eligible(candidate, now)
                if candidate.candidate_eligible and not was_candidate_eligible and candidate.candidate_at is None:
                    self._mark_candidate_entry(candidate, now)
                candidate.state = self._discovery_state(candidate, now)
                candidate.active_candidate = candidate.candidate_eligible
                if not candidate.candidate_eligible:
                    candidate.ready_to_buy = False
                self._update_rollups(candidate, now)
                self._persist_candidate(candidate, now)
        self._record_stage_metric("quote_asset_usd_result_apply", (time.monotonic() - started) * 1000, applied)
        return applied

    def _recompute_flap_canonical_price(self, candidate: SurvivorCandidate, now: datetime) -> bool:
        """Convert the latest verified Flap quote-side mark into canonical USD.

        This method never does I/O.  The quote-asset-per-BNB factor is fetched
        by the existing venue worker and the owner loop only applies the
        completed fact.  That preserves both realtime recovery and SQLite's
        single-writer boundary.
        """
        raw = _decimal(candidate.source_status.get("flap_last_post_price"))
        if raw is None or raw <= 0:
            return False
        quote_asset = str(candidate.source_status.get("fundraising_quote_asset") or "NATIVE_BNB").lower()
        bnb_usd = self._fresh_bnb_usd(now)
        if (
            bnb_usd is None
            and candidate.native_token_price_usd is not None
            and candidate.native_token_price_usd > 0
            and now - candidate.last_seen_at <= timedelta(seconds=90)
        ):
            # The owning Binance record carries the same BNB/USD mark.  It is
            # valid only while that record itself is fresh.
            bnb_usd = candidate.native_token_price_usd
        # Binance token USD is retained exclusively as a reference mark.  It
        # must never replace an unavailable/stale Live venue price.
        reference_usd = self._fresh_binance_direct_usd(candidate, now)
        live_canonical_only = (
            getattr(self, "mode", "paper") == "live"
            and self.config.identity == BALANCED_SURVIVOR_REVERSAL_IDENTITY
        )
        # A Portal mark is authoritative only while it is actually current.
        # A Live strategy cannot substitute a fresh Binance reference for a
        # stale Portal mark.  Keep the historical Paper fallback below, but
        # fail closed in Live until Flap supplies another canonical mark.
        if not self._flap_raw_price_is_fresh(candidate, now):
            if not live_canonical_only and reference_usd is not None:
                candidate.source_status.update({
                    "raw_price": str(raw),
                    "raw_quote_asset": quote_asset,
                    "canonical_price_usd": str(reference_usd),
                    "price_conversion_source": "DIRECT_USD_FALLBACK",
                    "price_conversion_updated_at": now.isoformat(),
                })
                self._update_price(candidate, reference_usd, now, "BINANCE_DIRECT_USD_FALLBACK", record_sample=False)
                return True
            candidate.source_status.update({
                "canonical_strategy_price": "",
                "canonical_price_source": "FLAP_RAW_PRICE_STALE",
                "price_conversion_source": "FLAP_RAW_PRICE_STALE",
            })
            if live_canonical_only:
                candidate.current_price_usd = None
                candidate.price_status = "CANONICAL_PRICE_STALE"
                candidate.price_source = "FLAP_PORTAL_EVENT"
                candidate.price_updated_at = now
            return False
        quote_usd: Decimal | None = None
        conversion_source: str | None = None
        native_price: Decimal | None = None
        if self._is_native_quote_asset(quote_asset):
            quote_usd = bnb_usd
            conversion_source = "BNB_USD"
            native_price = raw
        elif quote_asset in {BSC_USDT_ADDRESS, BSC_USDC_ADDRESS}:
            cached = self._fresh_quote_asset_usd(quote_asset, now)
            if cached is not None:
                quote_usd = cached.price_usd
                conversion_source = cached.source
        else:
            per_bnb = _decimal(candidate.source_status.get("flap_quote_asset_per_bnb"))
            if bnb_usd is not None and per_bnb is not None and per_bnb > 0:
                quote_usd = bnb_usd / per_bnb
                conversion_source = "FLAP_PORTAL_QUOTE_EXACT_INPUT"
            else:
                cached = self._fresh_quote_asset_usd(quote_asset, now)
                if cached is not None:
                    quote_usd = cached.price_usd
                    conversion_source = cached.source
                else:
                    self._schedule_quote_asset_usd_resolution(quote_asset, now, bnb_usd)
        if quote_usd is None and not self._is_native_quote_asset(quote_asset):
            self._schedule_quote_asset_usd_resolution(quote_asset, now, bnb_usd)
        computed_usd = raw * quote_usd if quote_usd is not None else None
        if computed_usd is not None and reference_usd is not None and reference_usd > 0:
            gap = abs(computed_usd / reference_usd - Decimal("1"))
            candidate.source_status["flap_binance_price_gap_pct"] = str(gap * Decimal("100"))
            if gap > Decimal("0.15") and not live_canonical_only:
                candidate.source_status.update({
                    "price": "PRICE_SOURCE_CONFLICT",
                    "price_conversion_source": "PRICE_SOURCE_CONFLICT",
                    "canonical_price_usd": "",
                })
                candidate.current_price_usd = None
                candidate.price_status = "PRICE_SOURCE_CONFLICT"
                candidate.price_source = "FLAP_CANONICAL_PRICE"
                candidate.price_updated_at = now
                return False
        if computed_usd is None and not live_canonical_only and reference_usd is not None:
            candidate.source_status.update({
                "raw_price": str(raw),
                "raw_quote_asset": quote_asset,
                "quote_asset_usd": "",
                "canonical_price_usd": str(reference_usd),
                "price_conversion_source": "DIRECT_USD_FALLBACK",
                "price_conversion_updated_at": now.isoformat(),
            })
            self._update_price(candidate, reference_usd, now, "BINANCE_DIRECT_USD_FALLBACK", record_sample=False)
            return True
        candidate.source_status.update({
            "raw_price": str(raw),
            "raw_quote_asset": quote_asset,
            "quote_asset_usd": str(quote_usd) if quote_usd is not None else "",
            "canonical_price_usd": str(computed_usd) if computed_usd is not None else "",
            "price_conversion_source": conversion_source or "PRICE_CONVERSION_PENDING",
            "price_conversion_updated_at": now.isoformat(),
            "canonical_strategy_price": str(computed_usd) if computed_usd is not None else "",
            "canonical_price_source": conversion_source or "PRICE_CONVERSION_PENDING",
        })
        if quote_usd is None or quote_usd <= 0:
            # A raw on-chain Flap price is useful diagnostics, but cannot pass
            # the USD Candidate gate until its quote asset has a fresh value.
            candidate.current_price_native = raw if native_price is not None else candidate.current_price_native
            candidate.current_price_usd = None
            candidate.price_status = "PRICE_CONVERSION_PENDING"
            candidate.price_source = "FLAP_PORTAL_EVENT"
            candidate.price_updated_at = now
            candidate.price_age_ms = 0
            candidate.source_status["price"] = "PRICE_CONVERSION_PENDING"
            return False
        self._update_price(
            candidate,
            computed_usd,
            now,
            "FLAP_CANONICAL_PRICE",
            native_price=native_price,
            record_sample=False,
        )
        return True

    def _apply_flap_reconciliation_price(self, candidate: SurvivorCandidate, context: object, now: datetime) -> None:
        """Use getTokenV7/V6 only as a bounded price reconciliation fallback."""
        raw = getattr(context, "reconciliation_price_raw", None)
        try:
            raw_int = int(raw) if raw is not None else 0
        except (TypeError, ValueError):
            raw_int = 0
        if raw_int <= 0:
            return
        last_event_raw = candidate.source_status.get("last_chain_trade_at")
        if last_event_raw:
            try:
                if now - datetime.fromisoformat(last_event_raw) < timedelta(seconds=90):
                    return
            except (TypeError, ValueError):
                pass
        quote_price = Decimal(raw_int) / (Decimal(10) ** 18)
        if quote_price <= 0:
            return
        candidate.source_status["flap_reconciliation_price_raw"] = str(raw_int)
        candidate.source_status["flap_reconciliation_price_source"] = "GET_TOKEN_V7"
        candidate.source_status["flap_reconciliation_price_at"] = now.isoformat()
        candidate.source_status["flap_last_post_price"] = str(quote_price)
        candidate.source_status["flap_last_post_price_at"] = now.isoformat()
        self._recompute_flap_canonical_price(candidate, now)

    def _apply_unknown_pre_migration_state(
        self,
        candidate: SurvivorCandidate,
        *,
        context_error: str | None,
    ) -> None:
        """Keep an unrecognized launchpad explicit and fail closed.

        ``migrateStatus=0`` is evidence that Pancake discovery is premature,
        not evidence that the token belongs to Four.meme.  The result remains
        diagnosable and can be retried on later realtime observations.
        """

        candidate.descriptor = None
        candidate.pair_address = None
        candidate.pool_type = None
        candidate.source_status.update({
            "venue": "UNKNOWN",
            "venue_state": "PRE_MIGRATION_VENUE_RESOLUTION",
            "venue_readiness": "VENUE_PENDING",
            "pool_source": "PRE_MIGRATION_CHAIN_FINGERPRINT",
            "pool_status": "VENUE_UNKNOWN",
            "pool_valid": "UNKNOWN",
            "protocol_family": "UNKNOWN_PREMIGRATION_FAMILY_PENDING",
            "strategy_support": "UNSUPPORTED_PROTOCOL",
            "flow_status": "UNSUPPORTED",
            "buy_quote_capability": "UNSUPPORTED",
            "sell_quote_capability": "UNSUPPORTED",
            "paper_fill_capability": "UNSUPPORTED",
            "pre_migration_resolution_status": "PENDING",
            "wss_status": "NOT_REQUESTED",
            "wss_subscription_status": "NOT_REQUESTED",
            "wss_failure_reason": context_error or "VENUE_UNKNOWN",
        })

    @staticmethod
    def _mark_pre_migration_resolution_pending(candidate: SurvivorCandidate) -> None:
        """Expose a queued live venue read without inventing a protocol.

        A fresh Meme Rush page can contain many unmigrated tokens.  Reads are
        deliberately bounded so Factory WSS stays responsive, but a token
        waiting for that bounded read must not look as if it was either a
        Four.meme curve or an invalid Pancake pool.
        """

        if candidate.source_status.get("pre_migration_resolution_status") == "RESOLVED":
            return
        candidate.source_status.update({
            "venue": "PENDING",
            "venue_state": "PRE_MIGRATION_VENUE_RESOLUTION",
            "venue_readiness": "VENUE_PENDING",
            "pool_source": "PRE_MIGRATION_CHAIN_FINGERPRINT",
            "pool_status": "PENDING",
            "pool_valid": "UNKNOWN",
            "protocol_family": "UNKNOWN_PREMIGRATION_FAMILY_PENDING",
            "strategy_support": "UNSUPPORTED_PROTOCOL",
            "flow_status": "UNSUPPORTED",
            "buy_quote_capability": "UNKNOWN",
            "sell_quote_capability": "UNKNOWN",
            "paper_fill_capability": "UNSUPPORTED",
            "pre_migration_resolution_status": "PENDING",
            "wss_status": "NOT_REQUESTED",
            "wss_subscription_status": "NOT_REQUESTED",
            "wss_failure_reason": "PRE_MIGRATION_VENUE_RESOLUTION",
        })

    def _pre_migration_registry_needs_upgrade(self, candidate: SurvivorCandidate) -> bool:
        """Upgrade an older generic Portal record to its verified Family."""

        if candidate.source_status.get("protocol_family") != "FLAP_CONTEXT":
            return False
        venue = normalize_bsc_address(candidate.source_status.get("venue_address"))
        if venue is None or not hasattr(self, "connection"):
            return False
        try:
            row = self.connection.execute(
                "SELECT protocol_family FROM bsc_venue_registry WHERE token_address=? AND venue_address=?",
                (candidate.mint, venue),
            ).fetchone()
        except Exception:
            return False
        # Older persisted Flap rows were resolved before the curve reserve was
        # exposed.  Refresh those live records once so the dashboard/strategy
        # can use the verified on-chain liquidity instead of the Binance
        # indicative field.  This is a bounded live refresh, not a historical
        # scanner.
        return (
            row is None
            or _row_value(row, "protocol_family") != "FLAP_CONTEXT"
            or not candidate.source_status.get("curve_reserve_raw")
        )

    def _schedule_pre_migration_venue_resolution(
        self,
        candidate: SurvivorCandidate,
        *,
        protocol: object | None,
        migrate_status: object | None,
        now: datetime,
    ) -> None:
        """Queue one pre-migration inspection; never perform RPC in on_records."""

        mint = candidate.mint
        # Discovery may learn ``migrateStatus=1`` while an earlier snapshot
        # still tries to enqueue the generic pre-migration resolver.  Do not
        # let that stale path overwrite/consume migrated-pool discovery.
        if self._migration_flag(migrate_status if migrate_status is not None else candidate.latest_migrate_status) is True:
            candidate.source_status["venue_state"] = "MIGRATED_POOL_DISCOVERY"
            return
        if mint in self._venue_resolution_jobs:
            # A current token that already clears every non-price Candidate
            # condition must not remain behind historical fingerprint work.
            # Reuse the bounded live resolver lane; the completed result still
            # returns through the sole owner-loop/SQLite application path.
            if (
                mint not in self._priority_venue_resolution_jobs
                and hasattr(self, "config")
                and self._candidate_non_price_eligible(candidate, now)
            ):
                self._priority_venue_resolution_jobs.add(mint)
                candidate.source_status["venue_resolution_job_state"] = "PRIORITY_REQUEUED"
                getattr(self, "_live_pool_upgrade_executor", self._venue_resolution_executor).submit(
                    self._run_pre_migration_venue_job,
                    self._venue_resolution_jobs[mint],
                )
            return
        generation = self._venue_resolution_generation[mint] + 1
        self._venue_resolution_generation[mint] = generation
        try:
            normalized_status = int(migrate_status) if migrate_status is not None else candidate.latest_migrate_status
        except (TypeError, ValueError):
            normalized_status = candidate.latest_migrate_status
        job = PreMigrationVenueJob(
            mint=mint,
            migrate_status=normalized_status,
            generation=generation,
            protocol=str(protocol) if protocol is not None else None,
            native_token_price_usd=candidate.native_token_price_usd or getattr(self, "_latest_native_token_price_usd", None),
            requested_at=now,
        )
        self._venue_resolution_jobs[mint] = job
        candidate.source_status.update({
            "venue_resolution_job_state": "QUEUED",
            "venue_resolution_requested_at": now.isoformat(),
            "venue_resolution_generation": str(generation),
            "venue_state": "VENUE_RESOLVING",
        })
        priority_executor = getattr(self, "_live_pool_upgrade_executor", None)
        executor = priority_executor if (priority_executor is not None and hasattr(self, "config") and self._candidate_non_price_eligible(candidate, now)) else self._venue_resolution_executor
        if executor is priority_executor:
            self._priority_venue_resolution_jobs.add(mint)
        executor.submit(self._run_pre_migration_venue_job, job)

    def _schedule_one_stale_pre_migration_resolution(self, now: datetime) -> None:
        """Restore one current price-ready Venue job left queued across restart.

        A persisted ``QUEUED`` marker is not a live Future.  This bounded
        owner-loop handoff recreates only a fresh, non-price-eligible runtime
        job; it is not a historical venue scan and still performs all RPC in
        the existing worker/result-queue path.
        """
        for candidate in sorted(self._candidates.values(), key=lambda item: (item.last_seen_at, item.mint), reverse=True):
            if now - candidate.last_seen_at > timedelta(seconds=self.config.idle_ttl_sec):
                return
            # A migrate update can arrive while a pre-migration job is still
            # persisted as QUEUED.  That job is intentionally ignored by its
            # result consumer, so it must not be resubmitted indefinitely.
            if self._migration_flag(candidate.latest_migrate_status) is True:
                continue
            if candidate.source_status.get("venue_state") != "VENUE_RESOLVING":
                continue
            if candidate.mint in self._venue_resolution_jobs:
                continue
            if not self._candidate_non_price_eligible(candidate, now):
                continue
            requested = candidate.source_status.get("venue_resolution_requested_at")
            try:
                requested_at = datetime.fromisoformat(str(requested))
            except (TypeError, ValueError):
                requested_at = candidate.first_seen_at
            if now - requested_at < timedelta(seconds=10):
                continue
            self._schedule_pre_migration_venue_resolution(
                candidate,
                protocol=candidate.source_status.get("protocol"),
                migrate_status=candidate.latest_migrate_status,
                now=now,
            )
            return

    def _schedule_migrated_pool_recovery(self, candidate: SurvivorCandidate, now: datetime) -> None:
        """Submit one current-token V2/V3 recovery through an existing lane."""

        if candidate.mint in self._migrated_pool_recovery_jobs:
            return
        self._migrated_pool_recovery_jobs.add(candidate.mint)
        candidate.source_status.update({
            "venue_state": "MIGRATED_POOL_DISCOVERY",
            "migrated_pool_recovery_state": "QUEUED",
            "migrated_pool_recovery_requested_at": now.isoformat(),
        })
        event = BscPairEvent(
            pair_address=candidate.mint,
            event_type="factory_pair_created",
            block_number=None,
            transaction_hash=None,
            log_index=None,
            observed_at=now,
            pool_type=V2_POOL_TYPE,
            source="MIGRATED_RUNTIME_DISCOVERY",
        )
        # Recovery of a current migrated token is P1 live market-data work.
        # Reuse the existing current-pool lane so old venue fingerprint jobs
        # cannot strand it behind a long backlog.  The completed result still
        # returns through the same owner-loop Factory queue.
        getattr(self, "_live_pool_upgrade_executor", self._venue_resolution_executor).submit(
            self._run_migrated_pool_recovery,
            FactoryPoolResolutionJob(event, V2_POOL_TYPE, candidate.mint, candidate.mint, candidate.mint, now),
        )

    def _run_migrated_pool_recovery(self, job: FactoryPoolResolutionJob) -> None:
        """I/O worker for one migrated runtime token; never writes SQLite."""

        started = time.monotonic()
        resolutions: list[tuple[str, BscPoolResolution]] = []
        error: str | None = None
        try:
            v2 = self.resolver.resolve_pancake_v2(job.token0, ()) if self.resolver is not None else None
            v3 = self.resolver.resolve_pancake_v3(job.token0, ()) if self.resolver is not None else None
            available = [item for item in (v2, v3) if item is not None and item.descriptor is not None]
            if available:
                for item in available:
                    resolutions.append((job.token0, item))
            else:
                # Preserve the exact, fail-closed discovery outcome so the
                # owner loop can make the stalled state explicit without
                # inventing a Binance-derived strategy price.
                outcome = v3 if v3 is not None and v3.status not in {"NO_PANCAKE_V3_POOL", "NO_PANCAKE_PAIR"} else v2
                resolutions.append((job.token0, outcome or BscPoolResolution(None, "RPC_READ_FAILED", "MIGRATED_RUNTIME_DISCOVERY")))
        except Exception as exc:  # pragma: no cover - provider boundary
            error = str(exc)[:500] or type(exc).__name__
            resolutions.append((job.token0, BscPoolResolution(None, "RPC_READ_FAILED", "MIGRATED_RUNTIME_DISCOVERY")))
        self._factory_resolution_results.put(FactoryPoolResolutionResult(
            job=job,
            inspection=None,
            resolutions=tuple(resolutions),
            completed_at=self.clock(),
            duration_ms=int((time.monotonic() - started) * 1000),
            error=error,
        ))

    def _run_pre_migration_venue_job(self, job: PreMigrationVenueJob) -> None:
        """I/O worker: read/parse only.  It must not touch SQLite or state."""

        started = time.monotonic()
        context: object | None = None
        context_error: str | None = None
        token_inspection: BscVenueInspection | None = None
        venue_inspection: BscVenueInspection | None = None
        quote_asset_per_bnb: Decimal | None = None
        try:
            if self.quote_provider is not None and hasattr(self.quote_provider, "cached_context"):
                context = self.quote_provider.cached_context(job.mint)
            if context is None and self.quote_provider is not None and hasattr(self.quote_provider, "resolve_venue"):
                context = self.quote_provider.resolve_venue(job.mint)
            if self._is_flap_context(context) and self.quote_provider is not None and hasattr(self.quote_provider, "flap_quote_asset_per_bnb"):
                quote_asset_per_bnb = self.quote_provider.flap_quote_asset_per_bnb(context)
        except Exception as exc:
            context_error = str(exc) or type(exc).__name__
        try:
            if self.resolver is not None and hasattr(self.resolver, "inspect_venue"):
                token_inspection = self.resolver.inspect_venue(job.mint)
                launchpad = normalize_bsc_address(getattr(context, "launchpad", None))
                if launchpad is not None:
                    if self._is_flap_context(context) and hasattr(self.resolver, "inspect_flap_context"):
                        venue_inspection = self.resolver.inspect_flap_context(launchpad)
                    if venue_inspection is None:
                        venue_inspection = self.resolver.inspect_venue(launchpad)
        except Exception as exc:
            context_error = context_error or (str(exc) or type(exc).__name__)
        self._venue_resolution_results.put(PreMigrationVenueResult(
            job=job,
            context=context,
            context_error=context_error,
            token_inspection=token_inspection,
            venue_inspection=venue_inspection,
            quote_asset_per_bnb=quote_asset_per_bnb,
            completed_at=self.clock(),
            duration_ms=int((time.monotonic() - started) * 1000),
        ))

    def _drain_pre_migration_venue_results(
        self,
        now: datetime,
        *,
        max_items: int | None = None,
        budget_ms: int | None = None,
    ) -> int:
        """Owner-loop side of venue resolution, including all SQLite writes."""
        started = time.monotonic()
        applied = 0
        while True:
            if max_items is not None and applied >= max_items:
                break
            if budget_ms is not None and applied and (time.monotonic() - started) * 1000 >= budget_ms:
                break
            try:
                result = self._venue_resolution_results.get_nowait()
            except Empty:
                break
            applied += 1
            apply_started = time.monotonic()
            job = self._venue_resolution_jobs.get(result.job.mint)
            candidate = self._candidates.get(result.job.mint)
            if job != result.job or candidate is None or candidate.latest_migrate_status == 1:
                if candidate is not None:
                    candidate.source_status["venue_resolution_job_state"] = "STALE_RESULT_DISCARDED"
                self._venue_resolution_jobs.pop(result.job.mint, None)
                getattr(self, "_priority_venue_resolution_jobs", set()).discard(result.job.mint)
                continue
            self._venue_resolution_jobs.pop(result.job.mint, None)
            getattr(self, "_priority_venue_resolution_jobs", set()).discard(result.job.mint)
            self._venue_resolution_completed += 1
            self._venue_resolution_durations_ms.append(result.duration_ms)
            candidate.source_status.update({
                "venue_resolution_job_state": "DONE" if result.context_error is None else "RETRY_WAIT",
                "venue_resolution_completed_at": result.completed_at.isoformat(),
                "venue_resolution_duration_ms": str(result.duration_ms),
            })
            if result.token_inspection is not None:
                self._persist_venue_registry(candidate.mint, result.token_inspection, now, source="PRE_MIGRATION_TOKEN_FINGERPRINT")
            if result.venue_inspection is not None:
                self._persist_venue_registry(candidate.mint, result.venue_inspection, now, source="PRE_MIGRATION_CONTEXT")
            context = result.context
            if self._is_flap_context(context) and getattr(context, "migrated", None) is False:
                inspection = result.venue_inspection
                launchpad = normalize_bsc_address(getattr(context, "launchpad", None))
                implementation = normalize_bsc_address(getattr(context, "token_implementation", None))
                quote_asset = normalize_bsc_address(getattr(context, "fundraising_currency", None))
                candidate.descriptor = None
                candidate.pair_address = None
                candidate.pool_type = None
                token_decimals = getattr(context, "token_decimals", None)
                quote_decimals = getattr(context, "fundraising_decimals", 18)
                candidate.source_status.update({
                    "venue": "Flap", "venue_address": launchpad or "", "venue_state": "PRE_MIGRATION",
                    "venue_readiness": "VENUE_READY", "pool_source": "FLAP_PORTAL_CONTEXT", "pool_status": "VALID",
                    "pool_valid": "VALID", "protocol": "Flap", "protocol_family": "FLAP_CONTEXT",
                    "protocol_fingerprint": inspection.protocol_fingerprint if inspection is not None else "FLAP_PORTAL_CONTEXT_V1",
                    "selector_fingerprint": inspection.selector_bitmap if inspection is not None else "flap_portal_context=1",
                    "venue_capabilities": inspection.capabilities_json if inspection is not None else "",
                    "strategy_support": "FLOW_SUPPORTED", "flow_status": "SUPPORTED",
                    "buy_quote_capability": "SUPPORTED", "sell_quote_capability": "SUPPORTED", "paper_fill_capability": "UNSUPPORTED",
                    "pre_migration_resolution_status": "RESOLVED", "token_proxy_implementation": implementation or "",
                    "fundraising_quote_asset": quote_asset or "NATIVE_BNB", "wss_status": "READY",
                    "wss_subscription_status": "READY", "wss_failure_reason": "",
                    "flap_portal_address": FLAP_PORTAL_ADDRESS,
                    "flap_token_decimals": str(token_decimals) if token_decimals is not None else "18",
                    "fundraising_quote_decimals": str(quote_decimals),
                    "flap_state_reconciliation_source": getattr(context, "reconciliation_source", "GET_TOKEN_V6"),
                })
                if result.quote_asset_per_bnb is not None and result.quote_asset_per_bnb > 0:
                    candidate.source_status["flap_quote_asset_per_bnb"] = str(result.quote_asset_per_bnb)
                    candidate.source_status["flap_quote_conversion_source"] = "FLAP_PORTAL_QUOTE_EXACT_INPUT"
                self._apply_flap_curve_liquidity(candidate, context)
                self._apply_flap_reconciliation_price(candidate, context, now)
                was_candidate_eligible = candidate.candidate_eligible
                self._recompute_flap_canonical_price(candidate, now)
                candidate.candidate_eligible = self._candidate_eligible(candidate, now)
                if candidate.candidate_eligible and not was_candidate_eligible and candidate.candidate_at is None:
                    self._mark_candidate_entry(candidate, now)
                candidate.state = self._discovery_state(candidate, now)
                candidate.active_candidate = candidate.candidate_eligible
                if not candidate.candidate_eligible:
                    candidate.ready_to_buy = False
            else:
                self._apply_unknown_pre_migration_state(candidate, context_error=result.context_error)
                if result.context_error:
                    self._venue_resolution_failed += 1
            self._persist_candidate(candidate, now)
            apply_ms = (time.monotonic() - apply_started) * 1000
            if apply_ms > 100:
                self._slow_venue_apply_count += 1
                self._last_slow_venue_apply = {
                    "token": result.job.mint,
                    "venue": candidate.source_status.get("venue_address") if candidate is not None else None,
                    "stage": "venue_result_apply",
                    "elapsed_ms": round(apply_ms, 3),
                }
                print("SLOW_VENUE_RESULT_APPLY", json.dumps(self._last_slow_venue_apply, ensure_ascii=False), flush=True)
        self._record_stage_metric("venue_result_apply", (time.monotonic() - started) * 1000, applied)
        return applied

    def _persist_venue_registry(
        self,
        mint: str,
        inspection: BscVenueInspection,
        now: datetime,
        *,
        source: str,
    ) -> None:
        """Persist any source-discovered contract before strategy support.

        This is only called by the Balanced owner loop.  It deliberately does
        not depend on a V2/V3 descriptor, quote asset, or WSS subscription.
        """

        token = normalize_bsc_address(mint)
        if token is None:
            return
        reserves = inspection.reserves or (None, None)
        self.connection.execute(
            "INSERT INTO bsc_venue_registry("
            "token_address,venue_address,chain,is_contract,code_size,bytecode_hash,implementation_address,factory_address,"
            "token0,token1,reserve0,reserve1,slot0_supported,liquidity_value,fee,selector_bitmap,protocol_fingerprint,"
            "protocol_family,capabilities_json,strategy_support,discovery_source,discovered_at,updated_at"
            ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(token_address,venue_address) DO UPDATE SET "
            "chain=excluded.chain,is_contract=excluded.is_contract,code_size=excluded.code_size,bytecode_hash=excluded.bytecode_hash,"
            "implementation_address=excluded.implementation_address,factory_address=excluded.factory_address,token0=excluded.token0,"
            "token1=excluded.token1,reserve0=excluded.reserve0,reserve1=excluded.reserve1,slot0_supported=excluded.slot0_supported,"
            "liquidity_value=excluded.liquidity_value,fee=excluded.fee,selector_bitmap=excluded.selector_bitmap,"
            "protocol_fingerprint=excluded.protocol_fingerprint,protocol_family=excluded.protocol_family,"
            "capabilities_json=excluded.capabilities_json,strategy_support=excluded.strategy_support,"
            "discovery_source=excluded.discovery_source,updated_at=excluded.updated_at",
            (
                token, inspection.address, inspection.chain, int(inspection.is_contract), inspection.code_size,
                inspection.bytecode_hash, inspection.implementation, inspection.factory, inspection.token0, inspection.token1,
                str(reserves[0]) if reserves[0] is not None else None,
                str(reserves[1]) if reserves[1] is not None else None,
                int(inspection.slot0), str(inspection.liquidity) if inspection.liquidity is not None else None,
                inspection.fee, inspection.selector_bitmap, inspection.protocol_fingerprint, inspection.protocol_family,
                inspection.capabilities_json, self._venue_strategy_support(inspection), source, now.isoformat(), now.isoformat(),
            ),
        )

    def _resolve_candidate_pool(
        self,
        candidate: SurvivorCandidate,
        *,
        pair: object | None = None,
        pool: object | None = None,
        curve: object | None = None,
        protocol: object | None = None,
        migrate_status: object | None = None,
        now: datetime | None = None,
        force: bool = False,
    ) -> None:
        """Resolve one Active Candidate through the existing venue/WSS path."""

        now = now or self.clock()
        pair_address = normalize_bsc_address(pair) or normalize_bsc_address(candidate.pair_address)
        pool_address = normalize_bsc_address(pool)
        existing = candidate.descriptor
        if not force and existing is not None and (pair_address is None or existing.address == pair_address):
            return
        last_attempt_raw = candidate.source_status.get("wss_resolution_attempt_at")
        migrated_fourmeme = self._is_fourmeme_venue(protocol, None) and self._migration_flag(migrate_status if migrate_status is not None else candidate.latest_migrate_status) is True
        if migrated_fourmeme and existing is None and not self._migration_discovery_due(candidate, now, force):
            return
        if not force and not migrated_fourmeme and pair_address is None and last_attempt_raw:
            try:
                last_attempt = datetime.fromisoformat(last_attempt_raw)
                if now - last_attempt < timedelta(seconds=30):
                    return
            except ValueError:
                pass
        candidate.source_status["wss_resolution_attempt_at"] = now.isoformat()
        if migrated_fourmeme and existing is None:
            candidate.source_status["migration_discovery_attempts"] = str(int(candidate.source_status.get("migration_discovery_attempts", "0")) + 1)
            candidate.source_status["venue_state"] = "MIGRATED_POOL_DISCOVERY"
        context: object | None = None
        context_error: str | None = None
        if self.quote_provider is not None and hasattr(self.quote_provider, "cached_context"):
            try:
                context = self.quote_provider.cached_context(candidate.mint)
            except Exception:
                context = None
        if context is None and self.quote_provider is not None and hasattr(self.quote_provider, "resolve_venue"):
            try:
                context = self.quote_provider.resolve_venue(candidate.mint)
            except Exception as exc:
                context_error = str(exc) or type(exc).__name__

        context_protocol = self._pool_context_protocol(context)
        protocol_value = context_protocol or (str(protocol) if protocol is not None else candidate.source_status.get("protocol") or "UNKNOWN")
        context_migrated = getattr(context, "migrated", None)
        if context_migrated is not None:
            migration_value = "1" if bool(context_migrated) else "0"
        elif migrate_status is not None:
            migration_value = str(migrate_status)
        elif candidate.latest_migrate_status is not None:
            migration_value = str(candidate.latest_migrate_status)
        else:
            migration_value = "UNKNOWN"
        context_pair = normalize_bsc_address(getattr(context, "pancake_pair", None))
        binance_addresses = tuple(dict.fromkeys(value for value in (pair_address, pool_address) if value is not None))
        candidate.source_status.update({
            "protocol": protocol_value,
            "migration_status": migration_value,
        })

        is_fourmeme = self._is_fourmeme_venue(protocol_value, context_protocol)
        signal_migrated = self._migration_flag(migrate_status if migrate_status is not None else candidate.latest_migrate_status)
        # A fresh on-chain FourMeme context is authoritative; without one, an
        # explicit Binance FourMeme migration signal determines whether this is
        # still a bonding-curve token. Never search Pancake before migration.
        fourmeme_unmigrated = is_fourmeme and (
            context_migrated is False or (context_migrated is None and signal_migrated is False)
        )
        if fourmeme_unmigrated:
            # Four.meme's TokenManager is the active bonding-curve venue
            # before Pancake migration.  It is not a Pancake pair, but it is
            # a real read-only discovery/market-data venue and must remain in
            # the same Candidate lifecycle.  Quotes stay fail-closed until
            # the existing provider verifies both directions at buy time.
            descriptor = (
                self.resolver.resolve_fourmeme_bonding_curve(candidate.mint)
                if self.resolver is not None and hasattr(self.resolver, "resolve_fourmeme_bonding_curve")
                else BscPoolDescriptor(
                    address=FOUR_MEME_TOKEN_MANAGER,
                    pool_type=BONDING_CURVE_POOL_TYPE,
                    mint=candidate.mint,
                    event_topics=(FOUR_TOKEN_PURCHASE_TOPIC, FOUR_TOKEN_SALE_TOPIC),
                )
            )
            candidate.descriptor = descriptor
            candidate.pair_address = descriptor.address
            candidate.pool_type = descriptor.pool_type
            inspection = None
            if self.resolver is not None and hasattr(self.resolver, "inspect_fourmeme_bonding_curve"):
                try:
                    inspection = self.resolver.inspect_fourmeme_bonding_curve()
                except Exception:
                    inspection = None
            if inspection is not None and hasattr(self, "connection"):
                self._persist_venue_registry(candidate.mint, inspection, now, source="FOURMEME_BONDING_CURVE")
            candidate.source_status.update({
                "venue": "FourMeme",
                "venue_state": self._fourmeme_venue_state(candidate),
                "venue_readiness": "VENUE_READY",
                "pool_source": "FOURMEME_TOKEN_MANAGER",
                "pool_status": "VALID",
                "pool_valid": "VALID",
                "protocol_family": "FOURMEME_BONDING_CURVE",
                "strategy_support": "FLOW_UNSUPPORTED",
                "venue_capabilities": inspection.capabilities_json if inspection is not None else "",
                "flow_status": "UNSUPPORTED",
                "wss_status": "READY",
                "wss_subscription_status": "READY",
                "wss_failure_reason": "",
            })
            return
        # A pre-migration status must be resolved through on-chain Context
        # evidence first.  Do not send it through Pancake discovery or assume
        # that it is Four.meme merely because Binance reported ``0``.
        if signal_migrated is False:
            if self._is_flap_context(context) and context_migrated is False:
                self._apply_flap_pre_migration_context(candidate, context, now)
            else:
                self._apply_unknown_pre_migration_state(candidate, context_error=context_error)
            return
        # A venue-supplied migrated pair is useful discovery input, but is
        # still validated exactly like every Binance address.  Anchor, token,
        # protocol and bonding-curve addresses are intentionally excluded.
        discovery_addresses = (*binance_addresses, *( (context_pair,) if context_pair is not None else () ))
        venue_inspections: list[BscVenueInspection] = []
        if self.resolver is not None and hasattr(self.resolver, "inspect_venue"):
            for address in discovery_addresses:
                try:
                    inspection = self.resolver.inspect_venue(address)
                except Exception:
                    inspection = None
                if inspection is None:
                    continue
                venue_inspections.append(inspection)
                # Registry-first: a contract is persisted before selecting an
                # execution adapter.  Non-contract/RPC facts are retained too
                # for diagnosis, but never treated as a pool.
                if hasattr(self, "connection"):
                    self._persist_venue_registry(
                        candidate.mint,
                        inspection,
                        now,
                        source="BINANCE_VENUE_ADDRESS" if inspection.address in binance_addresses else "CONTEXT_VENUE_ADDRESS",
                    )
        v2_resolution = self.resolver.resolve_pancake_v2(
            candidate.mint,
            discovery_addresses,
            quote_asset_usd={BSC_WBNB_ADDRESS: candidate.native_token_price_usd} if candidate.native_token_price_usd else None,
        ) if self.resolver is not None else None
        resolution = v2_resolution
        if (resolution is None or resolution.descriptor is None) and self.resolver is not None and hasattr(self.resolver, "resolve_pancake_v3"):
            v3_resolution = self.resolver.resolve_pancake_v3(candidate.mint, discovery_addresses)
            if v3_resolution.descriptor is not None:
                resolution = v3_resolution
        # A migrated FourMeme token may pair against an asset outside the
        # small direct-query quote set.  Ask the Factory for this token only;
        # this is a bounded fallback, never a chain-wide scan.
        if getattr(self, "_venue_history_backfill_enabled", True) and migrated_fourmeme and (resolution is None or resolution.descriptor is None) and hasattr(self.resolver, "discover_factory_pools"):
            latest = self.resolver.rpc.call("eth_blockNumber", ()) if getattr(self.resolver, "rpc", None) is not None else None
            try:
                latest_block = int(str(latest), 16)
            except (TypeError, ValueError):
                latest_block = None
            if latest_block is not None:
                observed_at = candidate.candidate_at or candidate.token_created_at or candidate.first_seen_at
                start_block = self.resolver.block_at_or_before(observed_at - timedelta(minutes=10))
                scan_start = start_block if start_block is not None else max(0, latest_block - 20_000)
                self._checkpoint_pool_scan(candidate, "SCANNING", scan_start, latest_block, None, now)
                discovered = self.resolver.discover_factory_pools(
                    candidate.mint, from_block=scan_start, to_block=latest_block,
                    quote_asset_usd={BSC_WBNB_ADDRESS: candidate.native_token_price_usd} if candidate.native_token_price_usd else None,
                )
                self._checkpoint_pool_scan(candidate, "COMPLETE", scan_start, latest_block, latest_block, now, discovered)
                if discovered:
                    self._persist_factory_registry_results(candidate.mint, discovered, now)
                    supported = [item for item in discovered if item.descriptor is not None and self._pool_pair_is_valid(candidate, item.descriptor)]
                    resolution = (max(supported, key=lambda item: item.liquidity_usd or Decimal("-1")) if supported else discovered[0])
        descriptor = resolution.descriptor if resolution is not None else None
        if descriptor is not None and self.resolver is not None and hasattr(self.resolver, "inspect_venue") and hasattr(self, "connection"):
            # Factory/direct discovery is also a venue discovery.  Keeping it
            # in the generic registry makes all future selection logic use a
            # single source of truth instead of a Pancake-only side table.
            try:
                resolved_inspection = self.resolver.inspect_venue(descriptor.address)
            except Exception:
                resolved_inspection = None
            if resolved_inspection is not None:
                venue_inspections.append(resolved_inspection)
                self._persist_venue_registry(candidate.mint, resolved_inspection, now, source=resolution.pool_source)
        if descriptor is None:
            contract_inspection = next((item for item in venue_inspections if item.is_contract), None)
            if contract_inspection is not None:
                mint = normalize_bsc_address(candidate.mint)
                quote_asset = (
                    contract_inspection.token1 if contract_inspection.token0 == mint
                    else contract_inspection.token0 if contract_inspection.token1 == mint
                    else None
                )
                known_shape = contract_inspection.protocol_family in {"PANCAKE_V2", "PANCAKE_V3"}
                support = "SUPPORTED_ADAPTER" if known_shape else "UNSUPPORTED_PROTOCOL"
                candidate.descriptor = None
                candidate.pair_address = contract_inspection.address
                candidate.pool_type = None
                candidate.source_status.update({
                    "venue": "BSC Venue",
                    "venue_address": contract_inspection.address,
                    "venue_readiness": "VENUE_READY",
                    "binance_pair_address": pair_address or "",
                    "binance_pool_address": pool_address or "",
                    "pool_source": "VENUE_FINGERPRINT",
                    "pool_status": "VALID" if known_shape else "VALID_UNKNOWN_PROTOCOL",
                    "pool_valid": "VALID",
                    "venue_state": "MIGRATED_TO_PANCAKE" if known_shape else "POOL_FOUND_BUT_UNSUPPORTED",
                    "protocol_family": contract_inspection.protocol_family,
                    "protocol_fingerprint": contract_inspection.protocol_fingerprint,
                    "selector_fingerprint": contract_inspection.selector_bitmap,
                    "strategy_support": support,
                    "venue_capabilities": contract_inspection.capabilities_json,
                    "pool_token0": contract_inspection.token0 or "",
                    "pool_token1": contract_inspection.token1 or "",
                    "pool_quote_asset": quote_asset or "",
                    "pool_factory": contract_inspection.factory or "",
                    "pool_fee": str(contract_inspection.fee) if contract_inspection.fee is not None else "",
                    "wss_subscription_status": "READY" if known_shape else "NOT_REQUESTED",
                    "wss_failure_reason": "" if known_shape else support,
                })
                return
            candidate.descriptor = None
            candidate.pair_address = pair_address or context_pair
            candidate.source_status.update({
                "venue": context_protocol or "UNKNOWN",
                "venue_readiness": "VENUE_READY",
                "binance_pair_address": pair_address or "",
                "binance_pool_address": pool_address or "",
                "pool_source": resolution.pool_source if resolution is not None else "NONE",
                "pool_status": resolution.status if resolution is not None else "RPC_READ_FAILED",
                "pool_valid": "INVALID",
                "venue_state": "POOL_DISCOVERY_SCANNING" if migrated_fourmeme and candidate.source_status.get("scan_status") != "COMPLETE" else ("POOL_DISCOVERY_RETRY" if migrated_fourmeme else "MIGRATED_POOL_UNRESOLVED"),
                "wss_subscription_status": "UNRESOLVED",
                "wss_failure_reason": resolution.status if resolution is not None else "RPC_READ_FAILED",
            })
            return
        if not self._pool_pair_is_valid(candidate, descriptor):
            candidate.descriptor = None
            candidate.pair_address = descriptor.address
            candidate.source_status.update({
                "venue": f"PancakeSwap {descriptor.pool_type.upper()}",
                "venue_readiness": "VENUE_READY",
                "pool_source": resolution.pool_source if resolution is not None else "NONE",
                "pool_status": "INVALID",
                "pool_valid": "INVALID",
                "strategy_support": "UNSUPPORTED_POOL_TYPE",
                "pool_token0": descriptor.token0 or "",
                "pool_token1": descriptor.token1 or "",
                "pool_quote_asset": resolution.quote_asset if resolution is not None and resolution.quote_asset is not None else "",
                "wss_subscription_status": "UNRESOLVED",
                "wss_failure_reason": "UNSUPPORTED_POOL_TYPE" if descriptor.pool_type not in {V2_POOL_TYPE, V3_POOL_TYPE} else "POOL_INVALID",
            })
            return
        candidate.descriptor = descriptor
        candidate.pair_address = descriptor.address
        candidate.pool_type = descriptor.pool_type
        candidate.source_status.update({
            "venue": f"PancakeSwap {descriptor.pool_type.upper()}",
            "venue_state": "MIGRATED_TO_PANCAKE",
            "venue_readiness": "VENUE_READY",
            "pool_status": "VALID",
            "wss_status": "READY",
            "protocol": protocol_value,
            "binance_pair_address": pair_address or "",
            "binance_pool_address": pool_address or "",
            "pool_source": resolution.pool_source if resolution is not None else "NONE",
            "pool_valid": "VALID",
            "pool_token0": descriptor.token0 or "",
            "pool_token1": descriptor.token1 or "",
            "pool_reserve0": str(resolution.reserves[0]) if resolution is not None and resolution.reserves is not None else "",
            "pool_reserve1": str(resolution.reserves[1]) if resolution is not None and resolution.reserves is not None else "",
            "pool_quote_asset": resolution.quote_asset if resolution is not None and resolution.quote_asset is not None else "",
            "pool_liquidity_usd": str(resolution.liquidity_usd) if resolution is not None and resolution.liquidity_usd is not None else "UNAVAILABLE",
            "pool_factory": resolution.factory if resolution is not None and resolution.factory is not None else "",
            "pool_fee": str(resolution.fee) if resolution is not None and resolution.fee is not None else "",
            "discovered_at": now.isoformat(),
            "discovery_source": resolution.pool_source if resolution is not None else "NONE",
            "wss_subscription_status": "READY",
            "wss_failure_reason": "",
        })
        self._schedule_pool_quote_asset_resolution(candidate, now)
        if context_error and context is None:
            candidate.source_status["venue_resolution"] = "SOURCE_UNAVAILABLE"

    def _persist_factory_registry_results(
        self,
        mint: str,
        results: Sequence[BscPoolResolution],
        now: datetime,
    ) -> None:
        """Owner-loop persistence for Factory-discovered pools (never worker SQL)."""

        token = normalize_bsc_address(mint)
        if token is None:
            return
        for result in results:
            descriptor = result.descriptor
            if descriptor is None or descriptor.token0 is None or descriptor.token1 is None:
                continue
            reserves = result.reserves or (None, None)
            self.connection.execute(
                "INSERT OR REPLACE INTO bsc_pool_registry("
                "token_address,pool_address,pool_type,token0,token1,quote_asset,fee,factory,created_block,discovered_at,source,validation_status,reserve0,reserve1,liquidity_value"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    token, descriptor.address, descriptor.pool_type, descriptor.token0, descriptor.token1,
                    result.quote_asset, result.fee, result.factory or "", None, now.isoformat(), result.pool_source,
                    result.status, str(reserves[0]) if reserves[0] is not None else None,
                    str(reserves[1]) if reserves[1] is not None else None,
                    str(result.liquidity_usd) if result.liquidity_usd is not None else None,
                ),
            )

    def _checkpoint_pool_scan(self, candidate: SurvivorCandidate, status: str, start: int, target: int, last: int | None, now: datetime, results: Sequence[BscPoolResolution] = ()) -> None:
        """Persist resumable scan facts from the owner loop only."""
        v2 = sum(item.descriptor is not None and item.descriptor.pool_type == V2_POOL_TYPE for item in results)
        v3 = sum(item.descriptor is not None and item.descriptor.pool_type == V3_POOL_TYPE for item in results)
        self.connection.execute(
            "INSERT INTO bsc_pool_scan_checkpoint(token_address,scan_status,scan_start_block,scan_target_block,last_scanned_block,v2_logs_found,v3_logs_found,v2_valid_pools,v3_valid_pools,scan_started_at,scan_completed_at,last_scan_at,last_error) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(token_address) DO UPDATE SET scan_status=excluded.scan_status,scan_target_block=excluded.scan_target_block,last_scanned_block=excluded.last_scanned_block,v2_logs_found=excluded.v2_logs_found,v3_logs_found=excluded.v3_logs_found,v2_valid_pools=excluded.v2_valid_pools,v3_valid_pools=excluded.v3_valid_pools,scan_completed_at=excluded.scan_completed_at,last_scan_at=excluded.last_scan_at,last_error=excluded.last_error",
            (candidate.mint, status, start, target, last, v2, v3, v2, v3, now.isoformat(), now.isoformat() if status == "COMPLETE" else None, now.isoformat(), None),
        )
        candidate.source_status.update({"scan_status": status, "scan_start_block": str(start), "scan_target_block": str(target), "last_scanned_block": str(last) if last is not None else "", "v2_valid_pools": str(v2), "v3_valid_pools": str(v3), "scan_completed_at": now.isoformat() if status == "COMPLETE" else ""})

    @staticmethod
    def _age_seconds(candidate: SurvivorCandidate, now: datetime) -> int:
        """Use Binance createTime when valid; keep local discovery as fallback."""

        origin = candidate.token_created_at or candidate.first_seen_at
        return max(0, int((now - origin).total_seconds()))

    @staticmethod
    def _merge_discovery_source(candidate: SurvivorCandidate, record: BinanceNormalizedSignal, now: datetime) -> None:
        """Keep discovery provenance without making it an entry signal.

        `source_status_json` is deliberately the only persistence needed:
        token identity remains the existing mint-keyed candidate, and no
        duplicate source-specific Candidate or Position lifecycle is created.
        """

        source = "OKX" if record.endpoint_type == "okx_signal" else "BINANCE"
        raw_sources = str(candidate.source_status.get("discovery_sources") or "")
        sources = {item for item in raw_sources.split(",") if item}
        sources.add(source)
        candidate.source_status["discovery_sources"] = ",".join(sorted(sources))
        if source != "OKX":
            return
        signal_at = _field(record, "okx_signal_at")
        wallet_type = str(_field(record, "okx_wallet_type") or "UNKNOWN")
        candidate.source_status["okx_signal_count"] = str(int(candidate.source_status.get("okx_signal_count", "0")) + 1)
        if wallet_type == "SMART_MONEY":
            key = "okx_smart_money_count"
        elif wallet_type == "INFLUENCER":
            key = "okx_kol_count"
        elif wallet_type == "WHALE":
            key = "okx_whale_count"
        else:
            key = "okx_unknown_wallet_type_count"
        candidate.source_status[key] = str(int(candidate.source_status.get(key, "0")) + 1)
        candidate.source_status.update({
            "latest_okx_signal_at": _iso(signal_at) if isinstance(signal_at, datetime) else now.isoformat(),
            "latest_okx_wallet_type": wallet_type,
            "latest_okx_amount_usd": str(_field(record, "okx_amount_usd")) if _field(record, "okx_amount_usd") is not None else "",
            "latest_okx_trigger_wallet_count": str(_field(record, "okx_trigger_wallet_count")) if _field(record, "okx_trigger_wallet_count") is not None else "",
            "latest_okx_sold_ratio_percent": str(_field(record, "okx_sold_ratio_percent")) if _field(record, "okx_sold_ratio_percent") is not None else "",
            "okx_signal_price_reference": str(_field(record, "okx_signal_price_reference")) if _field(record, "okx_signal_price_reference") is not None else "",
        })

    def _data_quality_cohort(self, first_seen_at: datetime) -> str:
        return "FRESH_V22" if first_seen_at >= self.data_quality_start_at else "LEGACY"

    def _fresh_cohort_name(self) -> str:
        return "FRESH_V22"

    def on_records(self, records: Sequence[BinanceNormalizedSignal], now: datetime | None = None) -> None:
        now = now or self.clock()
        discovery_started = time.monotonic()
        # Venue reads are network-bound. A newly fetched Binance snapshot can
        # contain a burst of unseen tokens, so direct inspection must not
        # starve Factory WSS event consumption or the next realtime cycle.
        # Factory events remain the primary discovery path; this is only a
        # bounded migration/address supplement.
        venue_resolution_budget = 1
        with self._lock:
            native_price_refreshed = False
            for record in records:
                mint = self._mint_key(record.signal.mint)
                if mint in self._excluded_mints:
                    continue
                is_okx_signal = record.endpoint_type == "okx_signal"
                candidate = self._candidates.get(mint)
                first_seen_this_process = mint not in self._loaded_candidate_mints
                if candidate is None:
                    candidate = SurvivorCandidate(
                        mint=mint,
                        symbol=_field(record, "symbol"),
                        first_seen_at=now,
                        last_seen_at=now,
                        data_quality_cohort=self._data_quality_cohort(now),
                    )
                    self._candidates[mint] = candidate
                    candidate.source_status["first_discovery_source"] = "OKX" if is_okx_signal else "BINANCE"
                    candidate.source_status["first_discovered_at"] = now.isoformat()
                    # Persist discovery time immediately.  A later process
                    # restart must not recreate age from Binance fields.
                    self._persist_candidate(candidate, now)
                self._merge_discovery_source(candidate, record, now)
                was_candidate_eligible = candidate.candidate_eligible
                candidate.last_seen_at = now
                candidate.emit_count += 1
                market_cap = _decimal(_field(record, "market_cap_usd"))
                liquidity = _decimal(_field(record, "liquidity_usd"))
                lifecycle = _field(record, "lifecycle")
                rank_type = _field(record, "rank_type")
                progress = _decimal(_field(record, "progress_pct"))
                migrate_status = _field(record, "migrate_status")
                try:
                    incoming_rank = int(rank_type) if rank_type is not None else None
                except (TypeError, ValueError):
                    incoming_rank = None
                    candidate.source_status["rank_type"] = "INVALID"
                lifecycle_regression = (
                    incoming_rank is not None
                    and candidate.latest_rank_type is not None
                    and incoming_rank < candidate.latest_rank_type
                )
                previous_migrate_status = candidate.latest_migrate_status
                if lifecycle_regression:
                    # A lower-rank response after a higher lifecycle is a
                    # stale/reordered feed observation. Keep the monotonic
                    # Survivor lifecycle and its latest snapshot identity.
                    candidate.source_status["lifecycle"] = "VALID"
                    pair = None
                    pool = None
                    curve = None
                else:
                    # An OKX signal must enrich the existing token lifecycle,
                    # never replace a richer Binance snapshot with a sparse
                    # signal record.  For an OKX-only token it remains the
                    # discovery record until regular metadata enrichment has
                    # produced a venue-backed snapshot.
                    if not is_okx_signal or candidate.record is None:
                        candidate.record = record
                    candidate.symbol = _field(record, "symbol") or candidate.symbol
                    if lifecycle is not None:
                        candidate.latest_lifecycle = str(lifecycle)
                    if incoming_rank is not None:
                        candidate.latest_rank_type = incoming_rank
                    candidate.latest_progress_pct = progress if progress is not None else candidate.latest_progress_pct
                    if migrate_status is not None:
                        try:
                            incoming_migrate = int(migrate_status)
                            # Migration is monotonic.  A delayed Binance
                            # snapshot must not move an already migrated Flap
                            # token back to pre-migration state.
                            if candidate.latest_migrate_status != 1 or incoming_migrate == 1:
                                candidate.latest_migrate_status = incoming_migrate
                        except (TypeError, ValueError):
                            candidate.source_status["migrate_status"] = "INVALID"
                    if candidate.first_snapshot_at is None:
                        candidate.first_snapshot_at = now
                        candidate.first_lifecycle = str(lifecycle) if lifecycle is not None else None
                        candidate.first_rank_type = candidate.latest_rank_type
                        candidate.first_market_cap_usd = market_cap
                        candidate.first_liquidity_usd = liquidity
                        candidate.first_holders = int(_field(record, "holders")) if _field(record, "holders") is not None else None
                        candidate.first_progress_pct = progress
                    created_at = _field(record, "token_created_at")
                    if isinstance(created_at, datetime):
                        candidate.token_created_at = created_at
                        candidate.discovery_delay_seconds = Decimal(str(max(0, (candidate.first_seen_at - created_at).total_seconds())))
                    if market_cap is not None and market_cap >= 0:
                        candidate.market_cap_usd = market_cap
                    if liquidity is not None and liquidity >= 0:
                        # Keep the Binance value for diagnostics.  A verified
                        # Flap context may replace the current value below
                        # with its on-chain bonding-curve reserve.
                        candidate.source_status["binance_liquidity_usd"] = str(liquidity)
                        if candidate.source_status.get("liquidity_source") != "FLAP_CURVE_ONCHAIN":
                            candidate.liquidity_usd = liquidity
                            candidate.source_status["liquidity_source"] = "BINANCE_MEME_RUSH"
                    try:
                        holders = _field(record, "holders")
                        if holders is not None:
                            candidate.holders = int(holders)
                    except (TypeError, ValueError):
                        candidate.source_status["holders"] = "INVALID"
                    incoming_native_price = _decimal(_field(record, "native_token_price"))
                    candidate.native_token_price_usd = incoming_native_price or candidate.native_token_price_usd
                    if candidate.native_token_price_usd is not None and candidate.native_token_price_usd > 0:
                        if self._latest_native_token_price_observed_at is None or now >= self._latest_native_token_price_observed_at:
                            self._latest_native_token_price_usd = candidate.native_token_price_usd
                            self._latest_native_token_price_observed_at = now
                            native_price_refreshed = True
                        if candidate.source_status.get("liquidity_source") == "FLAP_CURVE_ONCHAIN":
                            self._apply_flap_curve_liquidity(candidate, candidate.record or record)
                    pair = _field(record, "pair_address")
                    pool = _field(record, "pool_address")
                    curve = _field(record, "bonding_curve_address")
                candidate.source_status["creator_sold"] = "VALID" if _field(record, "dev_sold_percent") is not None or _field(record, "creator_sold") is not None else "SOURCE_UNAVAILABLE"
                # OKX signal price is intentionally stored only as diagnostic
                # metadata.  It must never become strategy/position price.
                price = None if is_okx_signal else _decimal(_field(record, "price_usd"))
                if price is not None and price > 0:
                    candidate.source_status.update({
                        "binance_price_usd": str(price),
                        "binance_price_updated_at": now.isoformat(),
                        "discovery_reference_price": str(price),
                        "discovery_reference_price_at": now.isoformat(),
                        "discovery_reference_price_source": "BINANCE_MEME_RUSH",
                    })
                    flap_event_fresh = False
                    if self._is_flap_candidate(candidate):
                        raw_event_at = candidate.source_status.get("last_chain_trade_at")
                        if raw_event_at:
                            try:
                                flap_event_fresh = now - datetime.fromisoformat(raw_event_at) < timedelta(seconds=90)
                            except (TypeError, ValueError):
                                flap_event_fresh = False
                    live_canonical_only = self.mode == "live" and self.config.identity == BALANCED_SURVIVOR_REVERSAL_IDENTITY
                    if not flap_event_fresh and not live_canonical_only:
                        self._update_price(candidate, price, now, "MEME_RUSH")
                    else:
                        # In Live, Binance remains a reference/diagnostic mark
                        # for every venue. Portal or Pool/WSS owns the
                        # canonical strategy price.
                        candidate.source_status["price_source_priority"] = "CANONICAL_VENUE_PRICE"
                    candidate.source_status["price"] = "REFERENCE_ONLY" if live_canonical_only else "VALID"
                    candidate.source_status["token_info_price"] = "NOT_APPLICABLE"
                else:
                    candidate.source_status["price"] = "PENDING"
                    candidate.source_status["token_info_price"] = "NOT_REQUESTED"
                    if candidate.current_price_usd is None:
                        candidate.price_status = "PENDING"
                # The Balanced profile has a strict one-way entry ceiling.
                # Once the source has observed a price above that ceiling, this
                # token is no longer an opportunity for this runtime.  Keep a
                # durable terminal marker so later source updates (including a
                # price pullback) cannot recreate its Candidate/WSS lifecycle.
                if self._is_balanced_price_ceiling_terminal(candidate):
                    self._reject_balanced_price_ceiling(candidate, now)
                    self._persist_candidate(candidate, now)
                    continue
                candidate.age_seconds = self._age_seconds(candidate, now)
                candidate.candidate_eligible = self._candidate_eligible(candidate, now)
                if candidate.candidate_eligible and not was_candidate_eligible and candidate.candidate_at is None:
                    self._mark_candidate_entry(candidate, now)
                record_migrate_status = _field(record, "migrate_status")
                effective_migrate_status = (
                    record_migrate_status
                    if record_migrate_status is not None
                    else candidate.latest_migrate_status
                )
                signal_migrated = self._migration_flag(effective_migrate_status)
                record_or_context_protocol = _field(record, "protocol") or candidate.source_status.get("protocol")
                migrated_fourmeme = self._is_fourmeme_venue(record_or_context_protocol, None) and signal_migrated is True
                unmigrated_fourmeme = self._is_fourmeme_venue(record_or_context_protocol, None) and signal_migrated is False
                migration_transition = signal_migrated is True and previous_migrate_status != 1
                # Existing rows from the former WAITING_MIGRATION policy are
                # normalized lazily when they next arrive from the realtime
                # source.  This is not a historical scan and remains bounded
                # to one venue resolution per source cycle.
                # A first-seen Binance burst is not allowed to synchronously
                # resolve arbitrary venues before the next realtime cycle.
                # Factory WSS owns generic Pool discovery; Four.meme curves
                # are normalized by the bounded owner-loop task below.
                registry_upgrade_needed = self._pre_migration_registry_needs_upgrade(candidate)
                pre_migration_resolution = (
                    signal_migrated is False
                    and not unmigrated_fourmeme
                    and (
                        candidate.source_status.get("pre_migration_resolution_status") != "RESOLVED"
                        or registry_upgrade_needed
                    )
                )
                if pre_migration_resolution and (
                    candidate.candidate_eligible
                    or candidate.state in {"PULLBACK_ZONE", "LOW_FORMING", "STOP_CONFIRMED", "REVERSAL_CONFIRMED", "READY_TO_BUY", "POSITION_OPEN"}
                ) and (first_seen_this_process or candidate.candidate_at == now):
                    self._mark_pre_migration_resolution_pending(candidate)
                realtime_venue_work = migration_transition or pre_migration_resolution
                if pre_migration_resolution:
                    self._schedule_pre_migration_venue_resolution(
                        candidate,
                        protocol=_field(record, "protocol"),
                        migrate_status=effective_migrate_status,
                        now=now,
                    )
                elif signal_migrated is True:
                    # Both the 0→1 transition and every normal Token refresh
                    # use the same idempotent recovery invariant.  This keeps
                    # a token recoverable even when the bounded Candidate
                    # evaluator has deferred it behind higher-priority work.
                    self.ensure_migrated_market_data(candidate, now)
                elif realtime_venue_work and not lifecycle_regression:
                    # Pool/venue resolution is network-bound.  Never execute
                    # the resolver from the Binance callback/owner path: a
                    # single RPC timeout here used to stall discovery,
                    # Candidate evaluation, and Position evaluation together.
                    # Factory WSS and the bounded resolver workers own the
                    # actual I/O; this marker lets those paths retry without
                    # making the current realtime snapshot wait.
                    candidate.source_status.setdefault("venue_resolution_job_state", "DEFERRED_WORKER")
                    candidate.source_status["venue_resolution_deferred_at"] = now.isoformat()
                if candidate.descriptor is not None:
                    candidate.pair_address = candidate.descriptor.address
                    candidate.pool_type = candidate.descriptor.pool_type
                self._schedule_recent_unknown_quote_pool_upgrade(candidate, now)
                candidate.state = self._discovery_state(candidate, now)
                candidate.active_candidate = candidate.candidate_eligible
                if not candidate.candidate_eligible:
                    candidate.ready_to_buy = False
                self._update_rollups(candidate, now)
                self._persist_candidate(candidate, now)
            if native_price_refreshed:
                # A fresh BNB/USD mark can unblock any Flap price that was
                # waiting only for quote-asset conversion.  Re-evaluate in
                # the owner loop without waiting for another Binance token
                # snapshot or rediscovery.
                for candidate in self._candidates.values():
                    if not self._is_flap_candidate(candidate) or not candidate.source_status.get("flap_last_post_price"):
                        continue
                    was_candidate_eligible = candidate.candidate_eligible
                    self._recompute_flap_canonical_price(candidate, now)
                    candidate.candidate_eligible = self._candidate_eligible(candidate, now)
                    if candidate.candidate_eligible and not was_candidate_eligible and candidate.candidate_at is None:
                        self._mark_candidate_entry(candidate, now)
                    candidate.state = self._discovery_state(candidate, now)
                    candidate.active_candidate = candidate.candidate_eligible
                    if not candidate.candidate_eligible:
                        candidate.ready_to_buy = False
                    self._update_rollups(candidate, now)
                    self._persist_candidate(candidate, now)
            watch = self._refresh_pre_candidate_watch(now)
            # A token cannot be promoted to Candidate until it has a real
            # strategy price.  Prioritize tokens that already meet the other
            # lightweight Candidate gates, then use the existing pre-candidate
            # watch as the bounded fallback.  This preserves Meme Rush as the
            # primary feed and avoids fabricating a price when Token Info is
            # unavailable.
            price_pending_candidates = sorted(
                (
                    candidate for candidate in self._candidates.values()
                    if self._candidate_non_price_eligible(candidate, now)
                    and (candidate.price_status != "VALID" or candidate.current_price_usd is None)
                ),
                key=lambda candidate: (candidate.last_seen_at, candidate.mint),
                reverse=True,
            )
            price_enrichment_targets = price_pending_candidates or [
                candidate for candidate in watch
                if candidate.price_status == "PENDING" and candidate.age_seconds >= 4
            ]
            if price_enrichment_targets:
                candidate = price_enrichment_targets[0]
                self._supplement_pending_price(candidate, now)
                was_candidate_eligible = candidate.candidate_eligible
                candidate.candidate_eligible = self._candidate_eligible(candidate, now)
                if candidate.candidate_eligible and not was_candidate_eligible and candidate.candidate_at is None:
                    self._mark_candidate_entry(candidate, now)
                candidate.state = self._discovery_state(candidate, now)
                candidate.active_candidate = candidate.candidate_eligible
                self._update_rollups(candidate, now)
                self._persist_candidate(candidate, now)
            self._last_discovery_count = len(records)
            self._record_update_total += len(records)
            self.connection.commit()
            self._publish(now)
            self._record_stage_metric("discovery_apply", (time.monotonic() - discovery_started) * 1000, len(records))

    def _supplement_pending_price(self, candidate: SurvivorCandidate, now: datetime) -> None:
        """Fetch Token Info as a reference mark for a price-pending token.

        Live Balanced may retain this mark for diagnostics, but it must wait
        for the venue-owned Portal/Pool price before entering the Candidate
        lifecycle.  Paper keeps the previous enrichment behavior.
        """
        if self.market_data is None:
            candidate.source_status["token_info_price"] = "NOT_REQUESTED"
            return
        if candidate.last_price_attempt_at is not None and now - candidate.last_price_attempt_at < timedelta(seconds=30):
            return
        candidate.last_price_attempt_at = now
        try:
            snapshot = self.market_data.snapshot(candidate.mint)
        except Exception:
            candidate.source_status["token_info_price"] = "SOURCE_UNAVAILABLE"
            return
        price = _decimal(_field_from_mapping(snapshot, "price_usd"))
        if price is None or price <= 0:
            candidate.source_status["token_info_price"] = "SOURCE_UNAVAILABLE"
            return
        candidate.native_token_price_usd = _decimal(_field_from_mapping(snapshot, "native_token_price")) or candidate.native_token_price_usd
        candidate.source_status["token_info_price"] = "VALID"
        candidate.source_status.update({
            "token_info_reference_price": str(price),
            "token_info_reference_price_at": now.isoformat(),
        })
        if (
            getattr(self, "mode", "paper") == "live"
            and self.config.identity == BALANCED_SURVIVOR_REVERSAL_IDENTITY
        ):
            candidate.source_status["price_source_priority"] = "CANONICAL_VENUE_PRICE"
            candidate.source_status["price"] = "REFERENCE_ONLY"
            candidate.price_status = "CANONICAL_PRICE_PENDING"
            return
        self._update_price(candidate, price, now, "TOKEN_INFO")

    def _candidate_distance(self, candidate: SurvivorCandidate, now: datetime) -> Decimal:
        """Return a diagnostic-only normalized distance to the active gate."""

        def deficit(value: Decimal | int | None, threshold: Decimal) -> Decimal:
            if value is None:
                return Decimal("1")
            current = Decimal(value)
            if current >= threshold:
                return Decimal("0")
            return min(Decimal("1"), max(Decimal("0"), (threshold - current) / threshold))

        age = Decimal(str(self._age_seconds(candidate, now)))
        return (
            deficit(age, Decimal(self.config.min_age_sec))
            + deficit(candidate.market_cap_usd, self.config.min_active_mc_usd)
            + deficit(candidate.liquidity_usd, self.config.min_active_liquidity_usd)
            + deficit(candidate.holders, Decimal(self.config.min_active_holders))
        )

    def _refresh_pre_candidate_watch(self, now: datetime) -> list[SurvivorCandidate]:
        pool = [
            candidate for candidate in self._candidates.values()
            if not candidate.candidate_eligible
            and candidate.state != "EXPIRED"
            and (now - candidate.last_seen_at).total_seconds() <= self.config.idle_ttl_sec
        ]
        distances = {candidate.mint: self._candidate_distance(candidate, now) for candidate in pool}
        pool.sort(key=lambda candidate: (distances[candidate.mint], -candidate.last_seen_at.timestamp(), candidate.mint))
        selected = pool[: self.config.pre_candidate_watch_max]
        selected_mints = {candidate.mint for candidate in selected}
        for rank, candidate in enumerate(selected, start=1):
            candidate.pre_candidate_watch = True
            candidate.pre_candidate_rank = rank
            candidate.candidate_distance_score = distances[candidate.mint]
            candidate.source_status["pre_candidate_price_watch"] = "VALID"
        for candidate in pool[self.config.pre_candidate_watch_max:]:
            candidate.pre_candidate_watch = False
            candidate.pre_candidate_rank = None
            candidate.candidate_distance_score = distances[candidate.mint]
            candidate.source_status["pre_candidate_price_watch"] = "NOT_APPLICABLE"
        for candidate in self._candidates.values():
            if candidate.mint not in selected_mints and candidate.mint not in distances:
                candidate.pre_candidate_watch = False
                candidate.pre_candidate_rank = None
                candidate.candidate_distance_score = None
                candidate.source_status["pre_candidate_price_watch"] = "NOT_APPLICABLE"
        return selected

    def _price_samples(self, candidate: SurvivorCandidate, *, before: datetime | None = None) -> list[FlowSample]:
        samples = [sample for sample in candidate.flows if sample.price_usd is not None or sample.price_native is not None]
        if before is not None:
            samples = [sample for sample in samples if sample.observed_at <= before]
        return samples

    def _history_samples(self, candidate: SurvivorCandidate, end_at: datetime) -> list[FlowSample]:
        """Load only the pre-candidate price window; never use post-candidate data."""
        try:
            rows = self.connection.execute(
                "SELECT observed_at,price_usd,price_native,price_source "
                "FROM survivor_price_snapshots WHERE mint=? AND observed_at>=? AND observed_at<=? "
                "ORDER BY observed_at ASC",
                (candidate.mint, candidate.first_seen_at.isoformat(), end_at.isoformat()),
            ).fetchall()
        except Exception:
            rows = ()
        if rows:
            return [
                FlowSample(
                    observed_at=datetime.fromisoformat(str(row[0])),
                    price_usd=_decimal(row[1]),
                    price_native=_decimal(row[2]),
                    price_source=row[3],
                )
                for row in rows
                if _decimal(row[1]) is not None or _decimal(row[2]) is not None
            ]
        return self._price_samples(candidate, before=end_at)

    def _set_history_metadata(self, candidate: SurvivorCandidate, samples: Sequence[FlowSample]) -> None:
        samples = [sample for sample in samples if (sample.price_usd or sample.price_native) is not None]
        candidate.history_sample_count = len(samples)
        candidate.history_start_at = samples[0].observed_at if samples else None
        candidate.history_end_at = samples[-1].observed_at if samples else None
        sources = sorted({sample.price_source for sample in samples if sample.price_source})
        candidate.history_source = "+".join(sources) if sources else None
        gaps = [
            Decimal(str((current.observed_at - previous.observed_at).total_seconds()))
            for previous, current in zip(samples, samples[1:])
        ]
        candidate.max_history_gap_seconds = max(gaps) if gaps else Decimal("0")
        if gaps:
            candidate.history_interval = f"{(sum(gaps) / Decimal(len(gaps))):.2f}s"
        elif samples:
            candidate.history_interval = "single_sample"
        else:
            candidate.history_interval = None

    def _freeze_candidate_history(self, candidate: SurvivorCandidate, samples: Sequence[FlowSample], candidate_at: datetime) -> None:
        """Record available pre-candidate samples without making completeness a gate."""
        samples = sorted(
            [sample for sample in samples if (sample.price_usd or sample.price_native) is not None and sample.observed_at <= candidate_at],
            key=lambda sample: sample.observed_at,
        )
        if not samples and candidate.first_seen_price_usd is not None:
            # A local first-price observation is enough to start the strategy
            # trajectory.  It is real observed data, not a reconstructed or
            # post-candidate substitute.
            samples = [FlowSample(
                observed_at=candidate.first_price_at or candidate.first_seen_at,
                price_usd=candidate.first_seen_price_usd,
                price_source=candidate.first_seen_price_source,
            )]
        self._set_history_metadata(candidate, samples)
        candidate.price_samples_before_candidate = len(samples)
        if len(samples) >= 2:
            candidate.price_coverage_before_candidate_seconds = Decimal(str(max(0, (samples[-1].observed_at - samples[0].observed_at).total_seconds())))
        else:
            candidate.price_coverage_before_candidate_seconds = Decimal("0")
        priced = [
            (sample.price_usd or sample.price_native, sample)
            for sample in samples
            if (sample.price_usd or sample.price_native) is not None
        ]
        first_valid_at = samples[0].observed_at if samples else None
        if priced:
            ath_price, ath_sample = max(priced, key=lambda item: item[0])
            candidate.ath_before_candidate_price_usd = ath_price
            candidate.ath_before_candidate_at = ath_sample.observed_at
            candidate.ath_before_candidate = first_valid_at is not None
            if ath_sample.price_native is not None:
                candidate.ath_price_native = ath_sample.price_native
        else:
            candidate.ath_before_candidate_price_usd = None
            candidate.ath_before_candidate_at = None
            candidate.ath_before_candidate = False
        # Survivor V1 now starts its ATH trajectory at the first locally
        # observed valid price.  Sample count, coverage and gaps remain
        # diagnostic only; they must not prevent Pullback or Paper entry.
        if priced:
            candidate.price_history_quality = PRICE_HISTORY_QUALITY_FIRST_SEEN
            candidate.price_history_status = FIRST_SEEN_PRICE_HISTORY_STATUS
            candidate.source_status["price_history"] = "VALID"
            if candidate.last_rejection in {"PRICE_HISTORY_INCOMPLETE", "PRICE_HISTORY_SPARSE", "BACKFILL_INSUFFICIENT"}:
                candidate.last_rejection = None
        else:
            candidate.price_history_quality = PRICE_HISTORY_QUALITY_INSUFFICIENT
            candidate.price_history_status = "PENDING"
            candidate.source_status["price_history"] = "PENDING"

    def _recompute_candidate_history(self, candidate: SurvivorCandidate) -> None:
        """Recompute frozen history from stored real snapshots, never post-candidate data."""
        if candidate.candidate_at is None:
            return
        samples = self._history_samples(candidate, candidate.candidate_at)
        self._freeze_candidate_history(candidate, samples, candidate.candidate_at)

    def _attempt_legacy_history_backfill(self, candidate: SurvivorCandidate) -> None:
        """Fail closed when the existing Binance Kline adapter cannot serve BSC history."""
        if candidate.data_quality_cohort != "LEGACY" or candidate.history_source is not None:
            return
        # The existing adapter is explicitly CT_501/Solana-only and the
        # project has no documented Binance BSC Token Kline mapping.  Do not
        # invent a platform, endpoint, or third-party fallback.
        candidate.history_source = "BINANCE_KLINE_UNAVAILABLE_BSC"
        candidate.history_sample_count = 0
        candidate.history_start_at = None
        candidate.history_end_at = None
        candidate.history_interval = None
        candidate.max_history_gap_seconds = None
        candidate.price_history_quality = PRICE_HISTORY_QUALITY_INSUFFICIENT
        candidate.price_history_status = "BACKFILL_INSUFFICIENT"
        candidate.source_status["price_history"] = "SOURCE_UNAVAILABLE"

    def _mark_candidate_entry(self, candidate: SurvivorCandidate, now: datetime) -> None:
        candidate.candidate_at = now
        before = self._history_samples(candidate, now)
        self._freeze_candidate_history(candidate, before, now)

    def _candidate_eligible(self, candidate: SurvivorCandidate, now: datetime) -> bool:
        if not (
            self._candidate_non_price_eligible(candidate, now)
            and candidate.price_status == "VALID"
            and candidate.current_price_usd is not None
            and candidate.current_price_usd > 0
        ):
            return False
        # The Balanced profile uses Candidate eligibility as its complete
        # strategy signal, so the same configured price range must apply here
        # instead of becoming a second, later entry-only gate.
        if self.config.candidate_is_entry:
            if self.config.min_entry_price_usd is not None and candidate.current_price_usd < self.config.min_entry_price_usd:
                return False
            if self.config.max_entry_price_usd is not None and candidate.current_price_usd > self.config.max_entry_price_usd:
                return False
        return True

    def _candidate_non_price_eligible(self, candidate: SurvivorCandidate, now: datetime) -> bool:
        age_seconds = self._age_seconds(candidate, now)
        return (
            age_seconds >= self.config.min_age_sec
            and (self.config.max_age_sec is None or age_seconds <= self.config.max_age_sec)
            and candidate.market_cap_usd is not None
            and candidate.market_cap_usd >= self.config.min_active_mc_usd
            and candidate.liquidity_usd is not None
            and candidate.liquidity_usd >= self.config.min_active_liquidity_usd
            and candidate.holders is not None
            and candidate.holders >= self.config.min_active_holders
            and (
                self.config.max_active_holders is None
                or candidate.holders <= self.config.max_active_holders
            )
        )

    @staticmethod
    def _history_allows_pullback(candidate: SurvivorCandidate) -> bool:
        # No reconstructed-history quality requirement: only a first real
        # locally observed price and a current valid price are required.
        return (
            candidate.price_status == "VALID"
            and candidate.first_seen_price_usd is not None
            and candidate.ath_price_usd is not None
            and candidate.source_status.get("price_source_switch") != "PRICE_SOURCE_SWITCH_PENDING"
        )

    def _history_block_reason(self, candidate: SurvivorCandidate) -> str | None:
        if not self.config.require_pullback:
            return None
        if not SurvivorReversalEngine._history_allows_pullback(candidate):
            return "PRICE_PENDING"
        return None

    def on_wss_event(self, event: BscPairEvent) -> None:
        # A Flap Portal is shared by every pre-migration token.  Its address
        # cannot be used as a position-level activity marker: an event for
        # token A must not keep token B's no-trade timer alive.  The owner
        # loop decodes the event token and updates only the matching position
        # in ``_apply_flap_trade_event``.
        if event.event_type in {"swap", "bonding_curve_event"}:
            with self._wss_trade_markers_lock:
                previous = self._wss_trade_markers.get(event.pair_address.lower())
                if previous is None or event.observed_at > previous:
                    self._wss_trade_markers[event.pair_address.lower()] = event.observed_at
        if event.event_type in {"swap", "sync", "bonding_curve_event", "flap_token_bought", "flap_token_sold"}:
            # A few isolated replay fixtures construct the engine without the
            # full constructor. Keep the callback side-effect free and lazy in
            # those cases; production instances initialize these collections.
            if not hasattr(self, "_wss_flow_event_keys"):
                self._wss_flow_event_keys = deque(maxlen=50000)
            if not hasattr(self, "_wss_flow_event_key_set"):
                self._wss_flow_event_key_set = set()
            self._remember_event_key(
                event,
                target="_wss_flow_event_keys",
                key_set=self._wss_flow_event_key_set,
            )
        # deque append is atomic under CPython and this callback performs no
        # SQLite or strategy mutation; the owner loop drains it under _lock.
        self._pending_events.append(event)

    def _drain_flow_event_queue(self, queue: deque[BscPairEvent], now: datetime) -> int:
        """Apply a bounded P1 event slice; remaining events stay queued."""
        started = time.monotonic()
        applied = 0
        while queue and applied < self.FLOW_EVENT_APPLY_MAX_ITEMS:
            if applied and (time.monotonic() - started) * 1000 >= self.FLOW_EVENT_APPLY_BUDGET_MS:
                break
            event = queue.popleft()
            try:
                self._flow_event_queue_latencies_ms.append(
                    Decimal(str(max(0.0, (now - event.observed_at).total_seconds() * 1000)))
                )
            except (AttributeError, TypeError, ValueError):
                pass
            self._apply_event(event)
            applied += 1
        self._record_stage_metric("flow_wss_result_apply", (time.monotonic() - started) * 1000, applied)
        return applied

    def evaluate(self, now: datetime | None = None) -> None:
        now = now or self.clock()
        started = time.monotonic()
        with self._lock:
            # Consume realtime venue events before evaluating exits.  WSS
            # callbacks enqueue events asynchronously; if the owner loop
            # evaluated the 40-second rule first, an event already waiting in
            # this queue could be misclassified as "no trade" and close a
            # position one cycle too early.
            self._drain_flow_event_queue(self._pending_events, now)
            recon_started = time.monotonic()
            self._drain_flow_reconciliation_results(now)
            self._record_stage_metric("reconciliation_result_apply", (time.monotonic() - recon_started) * 1000)
            self._drain_flow_event_queue(self._pending_gap_events, now)
            # Factory validation is worker-owned; only a tiny completed-result
            # slice is applied here so new-pool discovery cannot starve P0/P1.
            self._drain_factory_pool_resolution_results(now)
            # Upgrade at most one still-fresh runtime row written by the old
            # quote whitelist.  This is a compatibility handoff for current
            # Live observations, not a historical pool scan.
            self._schedule_one_recent_unknown_quote_pool_upgrade(now)
            self._schedule_one_stale_pre_migration_resolution(now)
            # Positions and asynchronous venue facts are handled only by this
            # owner loop; no network I/O is performed here.
            position_started = time.monotonic()
            self._evaluate_positions(now)
            self._record_stage_metric("position_evaluate", (time.monotonic() - position_started) * 1000)
            exit_started = time.monotonic()
            self._drain_audit_prefetch_results(now)
            self._record_stage_metric("exit_result_apply", (time.monotonic() - exit_started) * 1000)
            # Startup/reconnect gap events are important, but they never get
            # to delay a real Factory WSS event or the current Binance cycle.
            self._drain_factory_gap_results(now)
            self._schedule_factory_gap_fill(now)
            self._schedule_flow_reconciliation(now)
            self._process_one_flap_migration(now)
            self._drain_pre_migration_venue_results(
                now,
                max_items=self.VENUE_RESULT_APPLY_MAX_ITEMS,
                budget_ms=self.VENUE_RESULT_APPLY_BUDGET_MS,
            )
            self._drain_quote_asset_usd_results(
                now,
                max_items=self.VENUE_RESULT_APPLY_MAX_ITEMS,
                budget_ms=self.VENUE_RESULT_APPLY_BUDGET_MS,
            )
            # Do not run legacy/one-off venue resolvers from the owner loop.
            # Their RPC/fingerprint work belongs in the existing executor;
            # realtime Position/Flow/Candidate work must remain schedulable
            # even when a provider is slow or unavailable.
            candidate_started = time.monotonic()
            priority_states = {"POSITION_OPEN", "PULLBACK_ZONE", "LOW_FORMING", "STOP_CONFIRMED", "REVERSAL_CONFIRMED", "READY_TO_BUY", "ACTIVE_CANDIDATE"}
            open_mints = {position.mint for position in self._positions.values() if position.status == "OPEN"}
            ordered_candidates = sorted(
                self._candidates.values(),
                key=lambda item: (
                    item.mint in open_mints,
                    item.state in priority_states,
                    item.last_seen_at,
                    item.mint,
                ),
                reverse=True,
            )
            evaluated_candidates = 0
            for candidate in ordered_candidates:
                if evaluated_candidates >= self.CANDIDATE_EVALUATE_MAX_ITEMS:
                    break
                if evaluated_candidates and (time.monotonic() - candidate_started) * 1000 >= self.CANDIDATE_EVALUATE_BUDGET_MS:
                    break
                self._evaluate_candidate(candidate, now)
                evaluated_candidates += 1
                # This timestamp is a runtime watchdog fact.  Persist it from
                # the owner loop so an ACTIVE_CANDIDATE cannot silently miss
                # evaluation behind a slow venue worker.
                if candidate.active_candidate:
                    self._persist_candidate(candidate, now)
            self._record_stage_metric("candidate_evaluate", (time.monotonic() - candidate_started) * 1000, evaluated_candidates)
            maintenance_started = time.monotonic()
            self._prune_runtime_history(now)
            self._publish(now)
            self._record_stage_metric("maintenance", (time.monotonic() - maintenance_started) * 1000)
            if getattr(self, "_db_dirty", False):
                self._commit_owner()
        self._main_loop_durations_ms.append(int((time.monotonic() - started) * 1000))

    def _activate_one_fourmeme_bonding_candidate(self, now: datetime) -> None:
        """Lazily normalize one current Four.meme curve candidate per loop.

        This repairs only live in-memory candidates that still carry the old
        WAITING_MIGRATION marker. It runs after Factory WSS is healthy and
        consumes one item, so it cannot occupy startup or starve realtime
        Factory events.
        """

        candidate = next(
            (
                item for item in sorted(self._candidates.values(), key=lambda value: (value.last_seen_at, value.mint), reverse=True)
                if self._is_fourmeme_venue(item.source_status.get("protocol"), None)
                and self._migration_flag(item.latest_migrate_status) is False
                and item.source_status.get("venue_readiness") != "VENUE_READY"
            ),
            None,
        )
        if candidate is None:
            return
        self._resolve_candidate_pool(
            candidate,
            protocol="FourMeme",
            migrate_status=0,
            now=now,
        )
        candidate.state = self._discovery_state(candidate, now)
        candidate.active_candidate = candidate.candidate_eligible
        self._persist_candidate(candidate, now)
        self.connection.commit()

    def _activate_one_pre_migration_candidate(self, now: datetime) -> None:
        """Resolve one current non-Four pre-migration token per owner loop.

        This is a live recovery path, not a historical backfill: only fresh
        candidates seen during the normal idle window are considered, and it
        runs after Factory WSS is healthy with all realtime events drained.
        It ensures a token which drops out of the next small Meme Rush page is
        still fingerprinted once from its already-recorded live observation.
        """

        candidate = next(
            (
                item for item in sorted(
                    self._candidates.values(),
                    key=lambda value: (int(value.candidate_eligible), value.last_seen_at, value.mint),
                    reverse=True,
                )
                if self._migration_flag(item.latest_migrate_status) is False
                and (
                    item.source_status.get("pre_migration_resolution_status") not in {"RESOLVED", "PENDING"}
                    or self._pre_migration_registry_needs_upgrade(item)
                )
                and not self._is_fourmeme_venue(item.source_status.get("protocol"), None)
                and item.source_status.get("venue_state") != "FOURMEME_BONDING_CURVE"
                and (now - item.last_seen_at).total_seconds() <= self.config.idle_ttl_sec
            ),
            None,
        )
        if candidate is None:
            return
        self._resolve_candidate_pool(
            candidate,
            protocol=candidate.source_status.get("protocol"),
            migrate_status=0,
            now=now,
            force=self._pre_migration_registry_needs_upgrade(candidate),
        )
        candidate.state = self._discovery_state(candidate, now)
        candidate.active_candidate = candidate.candidate_eligible
        self._persist_candidate(candidate, now)
        self.connection.commit()

    def _hydrate_legacy_venue_registry(self, now: datetime) -> None:
        """Owner-loop migration of one legacy Binance venue per evaluation.

        This is intentionally not a chain-history scanner: it only fingerprints
        the exact addresses already stored in this runtime as
        ``UNSUPPORTED_POOL_TYPE``.  One item per main-loop evaluation keeps the
        live signal path bounded and preserves the single SQLite writer rule.
        """

        if self.resolver is None or not hasattr(self.resolver, "inspect_venue"):
            return
        for candidate in sorted(self._candidates.values(), key=lambda item: (item.updated_at if hasattr(item, "updated_at") else item.last_seen_at, item.mint)):
            if candidate.source_status.get("pool_status") != "UNSUPPORTED_POOL_TYPE":
                continue
            venue = normalize_bsc_address(candidate.pair_address)
            if venue is None:
                continue
            present = self.connection.execute(
                "SELECT 1 FROM bsc_venue_registry WHERE token_address=? AND venue_address=?",
                (candidate.mint, venue),
            ).fetchone()
            if present is not None:
                continue
            try:
                inspection = self.resolver.inspect_venue(venue)
            except Exception:
                return
            if inspection is None:
                continue
            self._persist_venue_registry(candidate.mint, inspection, now, source="LEGACY_BINANCE_VENUE_AUDIT")
            if inspection.is_contract:
                mint = normalize_bsc_address(candidate.mint)
                quote = inspection.token1 if inspection.token0 == mint else inspection.token0 if inspection.token1 == mint else None
                known_shape = inspection.protocol_family in {"PANCAKE_V2", "PANCAKE_V3"}
                support = "SUPPORTED_ADAPTER" if known_shape else "UNSUPPORTED_PROTOCOL"
                candidate.source_status.update({
                    "venue_address": inspection.address,
                    "pool_status": "VALID" if known_shape else "VALID_UNKNOWN_PROTOCOL",
                    "pool_valid": "VALID",
                    "venue_state": "MIGRATED_TO_PANCAKE" if known_shape else "POOL_FOUND_BUT_UNSUPPORTED",
                    "protocol_family": inspection.protocol_family,
                    "protocol_fingerprint": inspection.protocol_fingerprint,
                    "selector_fingerprint": inspection.selector_bitmap,
                    "strategy_support": support,
                    "venue_capabilities": inspection.capabilities_json,
                    "pool_token0": inspection.token0 or "",
                    "pool_token1": inspection.token1 or "",
                    "pool_quote_asset": quote or "",
                    "pool_factory": inspection.factory or "",
                    "wss_subscription_status": "READY" if known_shape else "NOT_REQUESTED",
                    "wss_failure_reason": "" if known_shape else support,
                })
                self._persist_candidate(candidate, now)
            return

    def _load_factory_cursor(self, pool_type: str) -> int | None:
        key = f"factory_{pool_type}_cursor_block"
        row = self.connection.execute("SELECT value_json FROM runtime_state WHERE mode=? AND state_key=?", (self.mode, key)).fetchone()
        try:
            return int(json.loads(row[0])) if row else None
        except (TypeError, ValueError, json.JSONDecodeError):
            return None

    def _load_flap_portal_cursor(self) -> int | None:
        row = self.connection.execute(
            "SELECT value_json FROM runtime_state WHERE mode=? AND state_key=?",
            (self.mode, "flap_portal_cursor_block"),
        ).fetchone()
        try:
            return int(json.loads(row[0])) if row else None
        except (TypeError, ValueError, json.JSONDecodeError):
            return None

    def _save_flap_portal_cursor(self, block: int) -> None:
        self._flap_portal_cursor_block = int(block)
        self.store.set_state("flap_portal_cursor_block", int(block))

    def _ensure_factory_cursors(self) -> None:
        """Production-start bootstrap; runs on the Balanced owner thread."""
        print("FACTORY_CURSOR_BOOTSTRAP_ENTER", flush=True)
        print(f"FACTORY_CURSOR_DB_READ v2={self._factory_cursor[V2_POOL_TYPE]} v3={self._factory_cursor[V3_POOL_TYPE]}", flush=True)
        if self._factory_cursor[V2_POOL_TYPE] is not None and self._factory_cursor[V3_POOL_TYPE] is not None:
            self._factory_gap_required = True
            print("FACTORY_CURSOR_BOOTSTRAP_DONE existing", flush=True)
            return
        if self.resolver is None:
            print("FACTORY_CURSOR_BOOTSTRAP_DONE resolver_unconfigured", flush=True)
            return
        head = self.resolver.logs_rpc.call("eth_blockNumber", ())
        try:
            block = int(str(head), 16)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("FACTORY_CURSOR_BOOTSTRAP head_block_failed") from exc
        print(f"FACTORY_CURSOR_INITIALIZE block={block}", flush=True)
        for pool_type in (V2_POOL_TYPE, V3_POOL_TYPE):
            if self._factory_cursor[pool_type] is None:
                self._save_factory_cursor(pool_type, block)
        self.connection.commit()
        self._factory_cursor = {V2_POOL_TYPE: self._load_factory_cursor(V2_POOL_TYPE), V3_POOL_TYPE: self._load_factory_cursor(V3_POOL_TYPE)}
        if self._factory_cursor[V2_POOL_TYPE] is None or self._factory_cursor[V3_POOL_TYPE] is None:
            raise RuntimeError("FACTORY_CURSOR_BOOTSTRAP persistence_failed")
        print(f"FACTORY_CURSOR_PERSISTED v2={self._factory_cursor[V2_POOL_TYPE]} v3={self._factory_cursor[V3_POOL_TYPE]}", flush=True)
        self._factory_gap_required = True
        print("FACTORY_CURSOR_BOOTSTRAP_DONE initialized", flush=True)

    def _save_factory_cursor(self, pool_type: str, block: int) -> None:
        self._factory_cursor[pool_type] = block
        self.store.set_state(f"factory_{pool_type}_cursor_block", block)

    def _run_factory_gap_fill(self, now: datetime) -> None:
        if not self._factory_gap_required or self.resolver is None or self._factory_gap_target is None:
            return
        done = True
        for pool_type in (V2_POOL_TYPE, V3_POOL_TYPE):
            cursor = self._factory_cursor[pool_type]
            if cursor is None:
                self._save_factory_cursor(pool_type, self._factory_gap_target)
                continue
            if cursor >= self._factory_gap_target:
                continue
            done = False
            end = min(self._factory_gap_target, cursor + 999)
            logs = self.resolver.factory_logs(pool_type, cursor + 1, end)
            if logs is None:
                self.store.set_state("factory_gap_fill", {"state": "FACTORY_GAP_FILL_FAILED", "from_block": cursor + 1, "to_block": end})
                return
            for log in logs:
                topics = tuple(str(x).lower() for x in log.get("topics", ()) if isinstance(x, str))
                # Gap fill only restores the Factory event stream and cursor.
                # Validation/fingerprinting stays on the normal owner-loop
                # event path, after realtime WSS has already acknowledged.
                self._pending_gap_events.append(BscPairEvent(
                    PANCAKE_V2_FACTORY if pool_type == V2_POOL_TYPE else PANCAKE_V3_FACTORY,
                    "factory_pair_created" if pool_type == V2_POOL_TYPE else "factory_pool_created",
                    int(str(log.get("blockNumber")), 16), log.get("transactionHash") if isinstance(log.get("transactionHash"), str) else None,
                    int(str(log.get("logIndex")), 16), now, pool_type=pool_type,
                    data=log.get("data") if isinstance(log.get("data"), str) else None, topics=topics,
                ))
            self._save_factory_cursor(pool_type, end)
            self.store.set_state("factory_gap_fill", {"state": "RUNNING", "from_block": cursor + 1, "to_block": end, "events_found": len(logs)})
            break
        if done or all((self._factory_cursor[item] or 0) >= self._factory_gap_target for item in (V2_POOL_TYPE, V3_POOL_TYPE)):
            self._factory_gap_required = False
            self.store.set_state("factory_gap_fill", {"state": "HEALTHY", "target_block": self._factory_gap_target})

    def _schedule_factory_gap_fill(self, now: datetime) -> None:
        """Submit one bounded Factory gap chunk without RPC on the owner loop."""
        if not self._factory_gap_required or self.resolver is None or self._factory_gap_target is None:
            return
        for pool_type in (V2_POOL_TYPE, V3_POOL_TYPE):
            cursor = self._factory_cursor.get(pool_type)
            if cursor is None or cursor >= self._factory_gap_target or pool_type in self._factory_gap_jobs:
                continue
            end = min(self._factory_gap_target, cursor + 999)
            job = FactoryGapFillJob(pool_type, cursor + 1, end, now)
            self._factory_gap_jobs.add(pool_type)
            self._venue_resolution_executor.submit(self._run_factory_gap_fill_job, job)
            # One RPC range at a time keeps the shared provider bounded.
            break

    def _run_factory_gap_fill_job(self, job: FactoryGapFillJob) -> None:
        logs: tuple[Mapping[str, object], ...] = ()
        error: str | None = None
        try:
            raw = self.resolver.factory_logs(job.pool_type, job.from_block, job.to_block)
            if isinstance(raw, list):
                logs = tuple(item for item in raw if isinstance(item, Mapping))
            else:
                error = "RPC_READ_FAILED"
        except Exception as exc:  # pragma: no cover - provider boundary
            error = str(exc)[:500] or type(exc).__name__
        self._factory_gap_results.put(FactoryGapFillResult(job, logs, self.clock(), error))

    def _drain_factory_gap_results(self, now: datetime) -> int:
        applied = 0
        while applied < 1:
            try:
                result = self._factory_gap_results.get_nowait()
            except Empty:
                break
            applied += 1
            self._factory_gap_jobs.discard(result.job.pool_type)
            if result.error is not None:
                self.store.set_state("factory_gap_fill", {
                    "state": "FACTORY_GAP_FILL_FAILED",
                    "from_block": result.job.from_block,
                    "to_block": result.job.to_block,
                    "error": result.error,
                })
                continue
            for log in result.logs:
                topics = tuple(str(x).lower() for x in log.get("topics", ()) if isinstance(x, str))
                try:
                    block_number = int(str(log.get("blockNumber")), 16)
                    log_index = int(str(log.get("logIndex")), 16)
                except (TypeError, ValueError):
                    continue
                self._pending_gap_events.append(BscPairEvent(
                    PANCAKE_V2_FACTORY if result.job.pool_type == V2_POOL_TYPE else PANCAKE_V3_FACTORY,
                    "factory_pair_created" if result.job.pool_type == V2_POOL_TYPE else "factory_pool_created",
                    block_number,
                    log.get("transactionHash") if isinstance(log.get("transactionHash"), str) else None,
                    log_index, result.completed_at, pool_type=result.job.pool_type,
                    data=log.get("data") if isinstance(log.get("data"), str) else None,
                    topics=topics, source="RPC_GAP_FILL",
                ))
            self._save_factory_cursor(result.job.pool_type, result.job.to_block)
            self.store.set_state("factory_gap_fill", {
                "state": "RUNNING" if result.job.to_block < (self._factory_gap_target or result.job.to_block) else "HEALTHY",
                "from_block": result.job.from_block,
                "to_block": result.job.to_block,
                "events_found": len(result.logs),
            })
            if all((self._factory_cursor.get(item) or 0) >= (self._factory_gap_target or 0) for item in (V2_POOL_TYPE, V3_POOL_TYPE)):
                self._factory_gap_required = False
                self.store.set_state("factory_gap_fill", {"state": "HEALTHY", "target_block": self._factory_gap_target})
        return applied

    def _flow_source_descriptors(self) -> dict[str, tuple[BscPoolDescriptor, set[str]]]:
        """Return only venues needed by current Candidates or OPEN positions."""

        open_mints = {position.mint.lower() for position in self._positions.values() if position.status == "OPEN"}
        sources: dict[str, tuple[BscPoolDescriptor, set[str]]] = {}
        for candidate in self._candidates.values():
            relevant = (
                candidate.mint.lower() in open_mints
                or candidate.candidate_eligible
                or candidate.state in self.WSS_CANDIDATE_STATES
            )
            if not relevant:
                continue
            descriptor = candidate.descriptor
            if descriptor is None and self._is_flap_candidate(candidate):
                descriptor = BscPoolDescriptor(
                    address=FLAP_PORTAL_ADDRESS,
                    pool_type=FLAP_PORTAL_POOL_TYPE,
                    mint=None,
                    event_topics=FLAP_PORTAL_EVENT_TOPICS,
                )
            if descriptor is None or not descriptor.subscription_topics:
                continue
            address = normalize_bsc_address(descriptor.address)
            if address is None:
                continue
            source_key = f"{descriptor.pool_type}:{address}"
            previous = sources.get(source_key)
            if previous is None:
                sources[source_key] = (descriptor, {candidate.mint.lower()})
            else:
                previous[1].add(candidate.mint.lower())
        return sources

    def _flow_state(self, source_key: str) -> dict[str, object]:
        if not hasattr(self, "_flow_reconciliation_state"):
            self._flow_reconciliation_state = {}
        state = self._flow_reconciliation_state.setdefault(source_key, {})
        state.setdefault("source_key", source_key)
        state.setdefault("transport_health", getattr(self, "_wss_provider_state", None) or "UNKNOWN")
        state.setdefault("subscription_health", "UNKNOWN")
        state.setdefault("data_completeness_health", "PENDING")
        state.setdefault("scan_status", "PENDING")
        state.setdefault("last_reconciled_block", None)
        state.setdefault("latest_chain_block", None)
        state.setdefault("safe_reconciled_block", None)
        state.setdefault("last_wss_block", None)
        state.setdefault("last_flow_event_block", None)
        state.setdefault("last_flow_event_at", None)
        state.setdefault("coverage_start_at", None)
        state.setdefault("last_reconciled_at", None)
        state.setdefault("last_error", None)
        state.setdefault("silent_miss_count", 0)
        state.setdefault("wss_event_count", 0)
        state.setdefault("event_count", 0)
        state.setdefault("rpc_reconciled_event_count", 0)
        state.setdefault("reconciled_event_count", 0)
        state.setdefault("recovered_event_count", 0)
        state.setdefault("reconciliation_lag_blocks", None)
        state.setdefault("window_30s_complete", False)
        state.setdefault("window_40s_complete", False)
        state.setdefault("window_1m_complete", False)
        state.setdefault("window_3m_complete", False)
        state.setdefault("window_start_at", None)
        state.setdefault("window_end_at", None)
        state.setdefault("reconciled_through_block", None)
        state.setdefault("reconciled_through_time", None)
        return state

    def _persist_flow_state(self, source_key: str, state: Mapping[str, object]) -> None:
        self.store.set_state(f"flow_reconciliation:{source_key}", dict(state))

    def _schedule_flow_reconciliation(self, now: datetime) -> None:
        """Schedule bounded Logs RPC work; never call the network in evaluate()."""

        if self.resolver is None or not getattr(self.resolver.logs_rpc, "configured", False):
            return
        if self._flow_reconciliation_last_schedule_at is not None and (
            now - self._flow_reconciliation_last_schedule_at
        ).total_seconds() < FLOW_RECONCILIATION_INTERVAL_SEC:
            return
        sources = self._flow_source_descriptors()
        if not sources:
            return
        self._flow_reconciliation_last_schedule_at = now
        for source_key, (descriptor, _mints) in sources.items():
            if source_key in self._flow_reconciliation_pending:
                continue
            state = self._flow_state(source_key)
            last = state.get("last_reconciled_block")
            try:
                last_block = int(last) if last is not None else None
            except (TypeError, ValueError):
                last_block = None
            # A newly observed source is initialized at the current safe head,
            # not scanned from genesis.  The worker obtains that head.
            from_block = -1 if last_block is None else last_block + 1
            if last_block is not None:
                state_head = state.get("safe_reconciled_block")
                try:
                    safe_head = int(state_head) if state_head is not None else None
                except (TypeError, ValueError):
                    safe_head = None
                if safe_head is not None and from_block <= safe_head - FLOW_RECONCILIATION_MAX_LAG_BLOCKS:
                    from_block = safe_head - FLOW_RECONCILIATION_MAX_LAG_BLOCKS + 1
                    state["coverage_start_at"] = now.isoformat()
                    state["scan_status"] = "SCANNING"
            job = FlowReconciliationJob(
                source_key=source_key,
                venue_address=descriptor.address,
                pool_type=descriptor.pool_type,
                topics=self._flow_topics_for_descriptor(descriptor),
                from_block=from_block,
                to_block=-1,
                requested_at=now,
            )
            self._flow_reconciliation_pending.add(source_key)
            self._flow_reconciliation_executor.submit(self._run_flow_reconciliation_job, job)

    def _run_flow_reconciliation_job(self, job: FlowReconciliationJob) -> None:
        started = time.monotonic()
        completed_at = self.clock()
        logs_rpc = self.resolver.logs_rpc if self.resolver is not None else None
        if logs_rpc is None or not logs_rpc.configured:
            self._flow_reconciliation_results.put(FlowReconciliationResult(
                job, (), (), None, None, completed_at, "RPC_LOGS_UNSUPPORTED", "logs_rpc_not_configured",
                latency_ms=int((time.monotonic() - started) * 1000),
            ))
            return
        logs_calls_before = int(getattr(logs_rpc, "get_logs_call_count", 0))
        logs_429_before = int(getattr(logs_rpc, "get_logs_429_count", 0))
        try:
            raw_head = logs_rpc.call("eth_blockNumber", ())
            latest = int(str(raw_head), 16)
        except (TypeError, ValueError, RuntimeError):
            self._flow_reconciliation_results.put(FlowReconciliationResult(
                job, (), (), None, None, completed_at, "RPC_READ_FAILED", "eth_blockNumber_failed",
                rpc_calls=1, latency_ms=int((time.monotonic() - started) * 1000),
            ))
            return
        safe_head = max(0, latest - FLOW_RECONCILIATION_SAFE_CONFIRMATIONS)
        if job.from_block < 0:
            start = safe_head
        else:
            start = min(max(0, job.from_block), safe_head + 1)
        if start > safe_head:
            self._flow_reconciliation_results.put(FlowReconciliationResult(
                replace(job, from_block=start, to_block=safe_head), (), (), latest, safe_head,
                completed_at, rpc_calls=1, rpc_429=0,
                latency_ms=int((time.monotonic() - started) * 1000),
            ))
            return
        events: list[BscPairEvent] = []
        event_keys: list[tuple[str, int | None, str]] = []
        cursor = start
        error_class: str | None = None
        error_message: str | None = None
        chunk = FLOW_RECONCILIATION_CHUNK_BLOCKS
        while cursor <= safe_head:
            end = min(safe_head, cursor + chunk - 1)
            try:
                raw_logs = logs_rpc.call("eth_getLogs", [{
                    "address": job.venue_address,
                    "fromBlock": hex(cursor),
                    "toBlock": hex(end),
                    "topics": [list(job.topics)],
                }])
            except Exception as exc:  # pragma: no cover - defensive RPC boundary
                raw_logs = None
                error_class = "RPC_READ_FAILED"
                error_message = str(exc)[:500] or type(exc).__name__
            if not isinstance(raw_logs, list):
                if chunk > 25:
                    chunk = max(25, chunk // 2)
                    continue
                error_class = error_class or "RPC_READ_FAILED"
                error_message = error_message or "eth_getLogs_failed"
                break
            for log in raw_logs:
                if not isinstance(log, Mapping):
                    continue
                topics = tuple(str(topic).lower() for topic in log.get("topics", ()) if isinstance(topic, str))
                if not topics or topics[0] not in job.topics:
                    continue
                try:
                    block_number = int(str(log.get("blockNumber")), 16)
                    log_index = int(str(log.get("logIndex")), 16)
                except (TypeError, ValueError):
                    continue
                if job.pool_type == FLAP_PORTAL_POOL_TYPE:
                    event_type = {
                        FLAP_TOKEN_BOUGHT_TOPIC: "flap_token_bought",
                        FLAP_TOKEN_SOLD_TOPIC: "flap_token_sold",
                        FLAP_LAUNCHED_TO_DEX_TOPIC: "flap_launched_to_dex",
                    }.get(topics[0], "bonding_curve_event")
                elif job.pool_type == BONDING_CURVE_POOL_TYPE:
                    event_type = "bonding_curve_event"
                elif topics[0] == SWAP_EVENT_TOPIC:
                    event_type = "swap"
                elif topics[0] == SYNC_EVENT_TOPIC:
                    event_type = "sync"
                else:
                    continue
                event = BscPairEvent(
                    pair_address=job.venue_address.lower(), event_type=event_type,
                    block_number=block_number,
                    transaction_hash=log.get("transactionHash") if isinstance(log.get("transactionHash"), str) else None,
                    log_index=log_index, observed_at=completed_at,
                    pool_type=job.pool_type, data=log.get("data") if isinstance(log.get("data"), str) else None,
                    topics=topics, source="RPC_RECONCILIATION",
                )
                events.append(event)
                event_keys.append(self._flow_event_key(event))
            cursor = end + 1
        rpc_calls = max(0, int(getattr(logs_rpc, "get_logs_call_count", 0)) - logs_calls_before)
        rpc_429 = max(0, int(getattr(logs_rpc, "get_logs_429_count", 0)) - logs_429_before)
        self._flow_reconciliation_results.put(FlowReconciliationResult(
            replace(job, from_block=start, to_block=safe_head), tuple(events), tuple(event_keys),
            latest, safe_head, completed_at, error_class, error_message,
            rpc_calls=rpc_calls, rpc_429=rpc_429,
            latency_ms=int((time.monotonic() - started) * 1000),
        ))

    def _drain_flow_reconciliation_results(self, now: datetime) -> None:
        while True:
            try:
                result = self._flow_reconciliation_results.get_nowait()
            except Empty:
                break
            try:
                self._reconciliation_result_queue_latencies_ms.append(
                    Decimal(str(max(0.0, (now - result.completed_at).total_seconds() * 1000)))
                )
            except (AttributeError, TypeError, ValueError):
                pass
            self._flow_reconciliation_pending.discard(result.job.source_key)
            state = self._flow_state(result.job.source_key)
            state["latest_chain_block"] = result.latest_chain_block
            state["safe_reconciled_block"] = result.safe_reconciled_block
            state["last_scan_at"] = result.completed_at.isoformat()
            state["last_latency_ms"] = result.latency_ms
            self._flow_reconciliation_rpc_calls += result.rpc_calls
            self._flow_reconciliation_rpc_429 += result.rpc_429
            if result.latency_ms is not None:
                if not hasattr(self, "_flow_reconciliation_latency_ms"):
                    self._flow_reconciliation_latency_ms = deque(maxlen=500)
                self._flow_reconciliation_latency_ms.append(Decimal(result.latency_ms))
            if result.error_class is not None:
                state.update({
                    "scan_status": "FAILED",
                    "data_completeness_health": "DEGRADED",
                    "last_error": result.error_message or result.error_class,
                    "reconciliation_status": "FLOW_RECONCILIATION_DEGRADED",
                    "window_30s_complete": False,
                    "window_40s_complete": False,
                    "window_1m_complete": False,
                    "window_3m_complete": False,
                })
                self._persist_flow_state(result.job.source_key, state)
                self._apply_flow_state_to_candidates(result.job.source_key, state, now)
                continue
            wss_keys = self._wss_flow_event_key_set
            reconciled_keys = getattr(self, "_reconciled_flow_event_key_set", set())
            missing = [
                event for event in result.events
                if self._flow_event_key(event) not in wss_keys
                and self._flow_event_key(event) not in reconciled_keys
            ]
            if missing:
                self._flow_reconciliation_misses_total += len(missing)
                self._flow_reconciliation_recovered_total += len(missing)
                state["silent_miss_count"] = int(state.get("silent_miss_count") or 0) + len(missing)
                state["recovered_event_count"] = int(state.get("recovered_event_count") or 0) + len(missing)
                state["last_silent_miss_at"] = now.isoformat()
                state["last_silent_miss_tx_hashes"] = [event.transaction_hash for event in missing[:20]]
                self._audit_event("WSS_SILENT_MISS", None, {
                    "venue": result.job.venue_address,
                    "pool_type": result.job.pool_type,
                    "source_key": result.job.source_key,
                    "from_block": result.job.from_block,
                    "to_block": result.job.to_block,
                    "missing_count": len(missing),
                    "missing_tx_hashes": [event.transaction_hash for event in missing[:20]],
                })
                for event in missing:
                    if not hasattr(self, "_reconciled_flow_event_keys"):
                        self._reconciled_flow_event_keys = deque(maxlen=50000)
                    if not hasattr(self, "_reconciled_flow_event_key_set"):
                        self._reconciled_flow_event_key_set = set()
                    self._remember_event_key(
                        event,
                        target="_reconciled_flow_event_keys",
                        key_set=self._reconciled_flow_event_key_set,
                    )
                    self._pending_gap_events.append(event)
                state["data_completeness_health"] = "DEGRADED"
                state["reconciliation_status"] = "FLOW_DATA_UNVERIFIED"
                state["consecutive_silent_miss_ranges"] = int(state.get("consecutive_silent_miss_ranges") or 0) + 1
                if (
                    len(missing) >= FLOW_RECONCILIATION_DEGRADED_RESUBSCRIBE_MISSES
                    or int(state["consecutive_silent_miss_ranges"]) >= FLOW_RECONCILIATION_DEGRADED_RESUBSCRIBE_MISSES
                ):
                    self._flow_reconciliation_resubscribe_requested = True
            else:
                state["consecutive_silent_miss_ranges"] = 0
            state["wss_event_count"] = len(result.events) - len(missing)
            state["event_count"] = len(result.events)
            state["rpc_reconciled_event_count"] = len(result.events)
            state["reconciled_event_count"] = len(result.events)
            state["last_reconciled_block"] = result.safe_reconciled_block
            state["reconciled_through_block"] = result.safe_reconciled_block
            state["reconciled_through_time"] = result.completed_at.isoformat()
            state["last_reconciled_at"] = result.completed_at.isoformat()
            state["window_start_at"] = (now - timedelta(seconds=FLOW_RECONCILIATION_WINDOW_SEC)).isoformat()
            state["window_end_at"] = now.isoformat()
            state["last_error"] = None
            state["scan_status"] = "COMPLETE"
            if state.get("coverage_start_at") is None:
                state["coverage_start_at"] = result.completed_at.isoformat()
            try:
                coverage_start = datetime.fromisoformat(str(state["coverage_start_at"]))
                coverage_age = max(0.0, (now - coverage_start).total_seconds())
            except (TypeError, ValueError):
                coverage_age = 0.0
            state["window_30s_complete"] = coverage_age >= 30 and not result.error_class
            state["window_40s_complete"] = coverage_age >= FLOW_RECONCILIATION_WINDOW_SEC and not result.error_class
            state["window_1m_complete"] = coverage_age >= 60 and not result.error_class
            state["window_3m_complete"] = coverage_age >= 180 and not result.error_class
            lag = None
            if result.latest_chain_block is not None and result.safe_reconciled_block is not None:
                lag = max(0, result.latest_chain_block - result.safe_reconciled_block)
            state["reconciliation_lag_blocks"] = lag
            if lag is not None:
                if not hasattr(self, "_flow_reconciliation_lag_blocks"):
                    self._flow_reconciliation_lag_blocks = deque(maxlen=500)
                self._flow_reconciliation_lag_blocks.append(Decimal(lag))
            if not missing and state["window_30s_complete"] and lag is not None and lag <= FLOW_RECONCILIATION_SAFE_CONFIRMATIONS:
                state["data_completeness_health"] = "HEALTHY"
                state["reconciliation_status"] = "HEALTHY"
            elif not missing:
                state["data_completeness_health"] = "PENDING"
                state["reconciliation_status"] = "FLOW_DATA_UNVERIFIED"
            self._persist_flow_state(result.job.source_key, state)
            self._apply_flow_state_to_candidates(result.job.source_key, state, now)
        if self._flow_reconciliation_resubscribe_requested and self._flow_reconciliation_resubscribe_callback is not None:
            try:
                self._flow_reconciliation_resubscribe_callback()
            except Exception:
                pass
            self._flow_reconciliation_resubscribe_requested = False

    def _apply_flow_state_to_candidates(self, source_key: str, state: Mapping[str, object], now: datetime) -> None:
        for candidate in self._candidates.values():
            candidate_source = self._candidate_flow_source_key(candidate)
            if candidate_source != source_key:
                continue
            candidate.source_status.update({
                "transport_health": str(state.get("transport_health") or self._wss_provider_state or "UNKNOWN"),
                "subscription_health": str(state.get("subscription_health") or "UNKNOWN"),
                "data_completeness_health": str(state.get("data_completeness_health") or "PENDING"),
                "flow_reconciliation_status": str(state.get("reconciliation_status") or "FLOW_DATA_UNVERIFIED"),
                "last_reconciled_block": str(state.get("last_reconciled_block") or ""),
                "last_wss_block": str(state.get("last_wss_block") or ""),
                "latest_chain_block": str(state.get("latest_chain_block") or ""),
                "flow_event_count": str(state.get("event_count") or 0),
                "wss_event_count": str(state.get("wss_event_count") or 0),
                "reconciled_event_count": str(state.get("reconciled_event_count") or 0),
                "flow_window_start": str(state.get("window_start_at") or ""),
                "flow_window_end": str(state.get("window_end_at") or ""),
                "reconciliation_lag_blocks": str(state.get("reconciliation_lag_blocks") or ""),
                "flow_window_30s_complete": "true" if state.get("window_30s_complete") else "false",
                "flow_window_40s_complete": "true" if state.get("window_40s_complete") else "false",
                "flow_window_1m_complete": "true" if state.get("window_1m_complete") else "false",
                "flow_window_3m_complete": "true" if state.get("window_3m_complete") else "false",
                "last_reconciled_at": str(state.get("last_reconciled_at") or ""),
            })
            if candidate.candidate_eligible:
                self._persist_candidate(candidate, now)

    def _candidate_flow_source_key(self, candidate: SurvivorCandidate) -> str | None:
        descriptor = candidate.descriptor
        if descriptor is None and self._is_flap_candidate(candidate):
            return f"{FLAP_PORTAL_POOL_TYPE}:{FLAP_PORTAL_ADDRESS}"
        if descriptor is None or not descriptor.address:
            return None
        return f"{descriptor.pool_type}:{descriptor.address.lower()}"

    def _no_trade_reason(self, suffix: str) -> str:
        """Build a duration-accurate no-trade state/reason identifier."""

        return f"NO_TRADE_{int(self.config.no_trade_exit_sec)}S_{suffix}"

    def _no_trade_reconciliation_window_key(self) -> str:
        """Select the smallest complete reconciliation window covering the rule."""

        seconds = self.config.no_trade_exit_sec
        if seconds <= 30:
            return "window_30s_complete"
        if seconds <= 40:
            return "window_40s_complete"
        if seconds <= 60:
            return "window_1m_complete"
        return "window_3m_complete"

    def _flow_data_completeness_ready(self, candidate: SurvivorCandidate, now: datetime) -> bool:
        source_key = self._candidate_flow_source_key(candidate)
        if source_key is None:
            return False
        if candidate.source_status.get("flow_status") not in {"SUPPORTED", "SUPPORTED_DIRECTION_ONLY"}:
            candidate.source_status["no_trade_state"] = self._no_trade_reason("UNVERIFIED")
            candidate.source_status["flow_reconciliation_status"] = "FLOW_DATA_UNVERIFIED"
            return False
        state = self._flow_state(source_key)
        completeness = state.get("data_completeness_health")
        if completeness != "HEALTHY" or not state.get(self._no_trade_reconciliation_window_key()):
            if completeness in {"DEGRADED", "FAILED"} or state.get("scan_status") == "FAILED":
                candidate.source_status["no_trade_state"] = self._no_trade_reason("UNVERIFIED")
                candidate.source_status["flow_reconciliation_status"] = "FLOW_DATA_UNVERIFIED"
            else:
                candidate.source_status["no_trade_state"] = self._no_trade_reason("PENDING_CONFIRMATION")
                candidate.source_status["flow_reconciliation_status"] = "FLOW_DATA_PENDING"
            return False
        try:
            last_at = datetime.fromisoformat(str(state.get("last_reconciled_at")))
            lag = int(state.get("reconciliation_lag_blocks") or 999999)
        except (TypeError, ValueError):
            candidate.source_status["no_trade_state"] = self._no_trade_reason("UNVERIFIED")
            candidate.source_status["flow_reconciliation_status"] = "FLOW_DATA_UNVERIFIED"
            return False
        if (now - last_at).total_seconds() > FLOW_RECONCILIATION_INTERVAL_SEC * 3 or lag > FLOW_RECONCILIATION_SAFE_CONFIRMATIONS:
            candidate.source_status["no_trade_state"] = self._no_trade_reason("UNVERIFIED")
            candidate.source_status["flow_reconciliation_status"] = "FLOW_DATA_UNVERIFIED"
            return False
        candidate.source_status["no_trade_state"] = self._no_trade_reason("CONFIRMED_READY")
        return True

    def _flow_window_complete(self, candidate: SurvivorCandidate, now: datetime, seconds: int) -> bool:
        source_key = self._candidate_flow_source_key(candidate)
        if source_key is None:
            return False
        state = self._flow_state(source_key)
        if state.get("data_completeness_health") != "HEALTHY":
            return False
        key = "window_30s_complete" if seconds <= 30 else "window_1m_complete" if seconds <= 60 else "window_3m_complete"
        return bool(state.get(key))

    def _run_flap_portal_gap_fill(self, now: datetime) -> None:
        """Fill only the Portal disconnect window through the Logs RPC.

        Results are converted into the same ``BscPairEvent`` queue consumed by
        live WSS.  The cursor advances only after a successful RPC response.
        """
        if not self._flap_portal_gap_required or self.resolver is None or self._flap_portal_gap_target is None:
            return
        if self._flap_portal_cursor_block is None:
            self._save_flap_portal_cursor(self._flap_portal_gap_target)
            self.store.set_state("flap_portal_gap_fill", {"state": "HEALTHY", "target_block": self._flap_portal_gap_target})
            self._flap_portal_gap_required = False
            return
        if self._flap_portal_cursor_block >= self._flap_portal_gap_target:
            self._flap_portal_gap_required = False
            self.store.set_state("flap_portal_gap_fill", {"state": "HEALTHY", "target_block": self._flap_portal_gap_target})
            return
        start = self._flap_portal_cursor_block + 1
        end = min(self._flap_portal_gap_target, start + 999)
        try:
            raw_logs = self.resolver.logs_rpc.call("eth_getLogs", [{
                "address": FLAP_PORTAL_ADDRESS,
                "fromBlock": hex(start),
                "toBlock": hex(end),
                "topics": [list(FLAP_PORTAL_EVENT_TOPICS)],
            }])
        except Exception as exc:
            raw_logs = None
            error = str(exc) or type(exc).__name__
        else:
            error = None
        if not isinstance(raw_logs, list):
            self.store.set_state("flap_portal_gap_fill", {
                "state": "FLAP_PORTAL_GAP_FILL_FAILED",
                "from_block": start,
                "to_block": end,
                "error": error or "RPC_READ_FAILED",
            })
            return
        events_found = 0
        for log in raw_logs:
            if not isinstance(log, Mapping):
                continue
            topics = tuple(str(value).lower() for value in log.get("topics", ()) if isinstance(value, str))
            if not topics or topics[0] not in FLAP_PORTAL_EVENT_TOPICS:
                continue
            event_type = {
                FLAP_TOKEN_BOUGHT_TOPIC: "flap_token_bought",
                FLAP_TOKEN_SOLD_TOPIC: "flap_token_sold",
                FLAP_LAUNCHED_TO_DEX_TOPIC: "flap_launched_to_dex",
            }[topics[0]]
            try:
                block_number = int(str(log.get("blockNumber")), 16)
                log_index = int(str(log.get("logIndex")), 16)
            except (TypeError, ValueError):
                continue
            self._pending_gap_events.append(BscPairEvent(
                pair_address=FLAP_PORTAL_ADDRESS,
                event_type=event_type,
                block_number=block_number,
                transaction_hash=log.get("transactionHash") if isinstance(log.get("transactionHash"), str) else None,
                log_index=log_index,
                observed_at=now,
                pool_type=FLAP_PORTAL_POOL_TYPE,
                data=log.get("data") if isinstance(log.get("data"), str) else None,
                topics=topics, source="RPC_GAP_FILL",
            ))
            events_found += 1
        self._save_flap_portal_cursor(end)
        self.store.set_state("flap_portal_gap_fill", {
            "state": "RUNNING" if end < self._flap_portal_gap_target else "HEALTHY",
            "from_block": start,
            "to_block": end,
            "events_found": events_found,
            "target_block": self._flap_portal_gap_target,
        })
        if end >= self._flap_portal_gap_target:
            self._flap_portal_gap_required = False

    def complete_factory_startup_gap_fill(self, *, timeout_sec: float = 20.0) -> bool:
        """Finish the cursor-defined startup gap before Binance discovery.

        It reuses the existing Factory cursor and BSC_LOGS_RPC only; no
        historical candidate or Venue backfill is permitted on this path.
        """

        deadline = time.monotonic() + max(1.0, float(timeout_sec))
        with self._lock:
            while self._factory_gap_required and self._factory_gap_target is not None:
                before = tuple(self._factory_cursor[item] for item in (V2_POOL_TYPE, V3_POOL_TYPE))
                self._run_factory_gap_fill(self.clock())
                after = tuple(self._factory_cursor[item] for item in (V2_POOL_TYPE, V3_POOL_TYPE))
                if not self._factory_gap_required:
                    self.connection.commit()
                    return True
                if after == before or time.monotonic() >= deadline:
                    return False
            self.connection.commit()
            return not self._factory_gap_required

    def subscription_details(self) -> tuple[dict[str, object], ...]:
        with self._lock:
            now = self.clock()
            positions = {position.mint for position in self._positions.values() if position.status == "OPEN"}
            transition_states = {"PULLBACK_ZONE", "LOW_FORMING", "STOP_CONFIRMED", "REVERSAL_CONFIRMED", "READY_TO_BUY"}
            details: dict[str, dict[str, object]] = {}
            ordered = sorted(self._candidates.values(), key=lambda item: (-item.age_seconds, item.mint))
            active_count = 0
            for candidate in ordered:
                is_open_position = candidate.mint in positions
                descriptor = candidate.descriptor
                # On restart the persisted Candidate row intentionally does
                # not carry the in-memory descriptor object.  Rehydrate a
                # verified V2/V3 descriptor from its stored registry fields
                # for OPEN positions immediately; this keeps exit monitoring
                # subscribed without a synchronous historical resolver pass.
                if descriptor is None and is_open_position:
                    pool_address = normalize_bsc_address(candidate.pair_address)
                    pool_type = str(candidate.pool_type or "").lower()
                    if pool_address is not None and pool_type in {V2_POOL_TYPE, V3_POOL_TYPE}:
                        status = candidate.source_status
                        descriptor = BscPoolDescriptor(
                            address=pool_address,
                            pool_type=pool_type,
                            mint=candidate.mint,
                            token0=normalize_bsc_address(status.get("pool_token0")),
                            token1=normalize_bsc_address(status.get("pool_token1")),
                        )
                        candidate.descriptor = descriptor
                    elif pool_type == BONDING_CURVE_POOL_TYPE:
                        # Four.meme positions use the shared TokenManager as
                        # their event venue.  Rehydrate that descriptor on
                        # restart just like a persisted V2/V3 pool; otherwise
                        # an OPEN position can spend its first monitoring
                        # window unsubscribed and be misclassified by the
                        # no-trade timeout before the resolver catches up.
                        protocol = str(candidate.source_status.get("protocol") or "").lower()
                        pool_source = str(candidate.source_status.get("pool_source") or "").upper()
                        if protocol == "fourmeme" or "FOURMEME" in pool_source or pool_address == FOUR_MEME_TOKEN_MANAGER:
                            descriptor = BscPoolDescriptor(
                                address=pool_address or FOUR_MEME_TOKEN_MANAGER,
                                pool_type=BONDING_CURVE_POOL_TYPE,
                                mint=candidate.mint,
                                event_topics=(FOUR_TOKEN_PURCHASE_TOPIC, FOUR_TOKEN_SALE_TOPIC),
                            )
                            candidate.descriptor = descriptor
                if descriptor is None:
                    continue
                if not is_open_position and (not candidate.candidate_eligible or candidate.state == "EXPIRED"):
                    continue
                stale = (now - candidate.last_seen_at).total_seconds() > self.config.idle_ttl_sec
                if is_open_position:
                    reason = "OPEN_POSITION"
                elif candidate.state in transition_states and not stale:
                    reason = "TRANSITION_GRACE"
                elif candidate.state == "ACTIVE_CANDIDATE" and not stale:
                    if active_count >= self.config.active_max:
                        continue
                    reason = "ACTIVE_CANDIDATE"
                    active_count += 1
                else:
                    continue
                address = descriptor.address
                previous = details.get(address)
                priority = {"ACTIVE_CANDIDATE": 1, "TRANSITION_GRACE": 2, "OPEN_POSITION": 3}
                if previous is not None and priority[str(previous["reason"])] >= priority[reason]:
                    continue
                details[address] = {
                    "contract": candidate.mint,
                    "symbol": candidate.symbol,
                    "reason": reason,
                    "pool_address": address,
                    "descriptor": descriptor,
                }
            # All pre-migration Flap tokens share the official Portal log
            # stream.  Register one descriptor, regardless of token count;
            # event.token is decoded and routed by the owner loop.
            flap_needed = any(
                self._is_flap_candidate(candidate)
                and (
                    candidate.mint in positions
                    or (
                        candidate.candidate_eligible
                        and candidate.state in self.WSS_CANDIDATE_STATES
                        and (now - candidate.last_seen_at).total_seconds() <= self.config.idle_ttl_sec
                    )
                )
                for candidate in self._candidates.values()
            )
            if flap_needed:
                flap_descriptor = BscPoolDescriptor(
                    address=FLAP_PORTAL_ADDRESS,
                    pool_type=FLAP_PORTAL_POOL_TYPE,
                    mint=None,
                    event_topics=FLAP_PORTAL_EVENT_TOPICS,
                )
                details[FLAP_PORTAL_ADDRESS] = {
                    "contract": FLAP_PORTAL_ADDRESS,
                    "symbol": "Flap Portal",
                    "reason": "FLAP_PORTAL",
                    "pool_address": FLAP_PORTAL_ADDRESS,
                    "descriptor": flap_descriptor,
                }
            return tuple(sorted(details.values(), key=lambda item: str(item["pool_address"])))

    def subscription_descriptors(self) -> tuple[BscPoolDescriptor, ...]:
        return tuple(detail["descriptor"] for detail in self.subscription_details())  # type: ignore[misc]

    def _wss_ready_for_candidate(self, candidate: SurvivorCandidate) -> bool:
        descriptor = candidate.descriptor
        if descriptor is None and self._is_flap_candidate(candidate):
            if self._wss_provider_state in {"DEGRADED", "UNAVAILABLE", "STOPPED", "DISCONNECTED", "CONNECTING", "NO_POOL_ADDRESS"}:
                return False
            return not self._wss_subscribed_addresses or FLAP_PORTAL_ADDRESS in self._wss_subscribed_addresses
        if descriptor is None:
            return False
        if self._wss_provider_state in {"DEGRADED", "UNAVAILABLE", "STOPPED", "DISCONNECTED", "CONNECTING", "NO_POOL_ADDRESS"}:
            return False
        if self._wss_provider_state in {"HEALTHY", "READY"} and self._wss_subscribed_addresses:
            return descriptor.address in self._wss_subscribed_addresses
        return True

    def _is_flap_subscription_configured(self) -> bool:
        return any(self._is_flap_candidate(candidate) for candidate in self._candidates.values())

    def update_wss_status(self, state: str, subscribed_addresses: Sequence[object]) -> None:
        """Publish provider/subscription facts back to the state machine."""

        with self._lock:
            if state in {"DEGRADED", "UNAVAILABLE", "STOPPED", "DISCONNECTED"} and self._factory_wss_seen_healthy:
                self._factory_gap_required = True
                self.store.set_state("factory_gap_fill", {"state": "PENDING", "disconnect_at": self.clock().isoformat()})
            if state in {"DEGRADED", "UNAVAILABLE", "STOPPED", "DISCONNECTED"} and self._flap_portal_seen_healthy:
                self._flap_portal_gap_required = True
                self.store.set_state("flap_portal_gap_fill", {
                    "state": "PENDING",
                    "disconnect_at": self.clock().isoformat(),
                    "last_block": self._flap_portal_cursor_block,
                })
            if state in {"HEALTHY", "READY"}:
                latest = self.resolver.logs_rpc.call("eth_blockNumber", ()) if self.resolver is not None else None
                try:
                    current = int(str(latest), 16)
                except (TypeError, ValueError):
                    current = None
                if not self._factory_wss_seen_healthy:
                    self._factory_wss_seen_healthy = True
                    if current is not None:
                        for pool_type in (V2_POOL_TYPE, V3_POOL_TYPE):
                            if self._factory_cursor[pool_type] is None:
                                self._save_factory_cursor(pool_type, current)
                        if self._factory_gap_required:
                            self._factory_gap_target = current
                elif self._factory_gap_required and current is not None:
                    self._factory_gap_target = current
                if self._is_flap_subscription_configured() and current is not None:
                    if self._flap_portal_cursor_block is None:
                        # From-now guarantee only: do not scan old Portal logs.
                        self._save_flap_portal_cursor(current)
                    elif not self._flap_portal_seen_healthy:
                        # A process restart is also a reconnect window.  The
                        # persisted cursor is the last owner-processed Portal
                        # block; fill through the post-ACK head before live
                        # events continue, using the same queue path.
                        self._flap_portal_gap_required = True
                        self._flap_portal_gap_target = current
                    if self._flap_portal_gap_required:
                        self._flap_portal_gap_target = current
                    self._flap_portal_seen_healthy = True
                    self.store.set_state("flap_portal_wss", {
                        "state": "HEALTHY",
                        "cursor_block": self._flap_portal_cursor_block,
                    })
            self._wss_provider_state = state
            self._wss_subscribed_addresses = {
                address
                for value in subscribed_addresses
                if (address := normalize_bsc_address(value)) is not None
            }
            for source_key, (descriptor, _mints) in self._flow_source_descriptors().items():
                flow_state = self._flow_state(source_key)
                flow_state["transport_health"] = state
                watched = normalize_bsc_address(descriptor.address)
                flow_state["subscription_health"] = (
                    "HEALTHY" if state in {"HEALTHY", "READY"} and watched in self._wss_subscribed_addresses else "DEGRADED"
                )
                if state in {"DEGRADED", "UNAVAILABLE", "STOPPED", "DISCONNECTED"}:
                    flow_state["data_completeness_health"] = "DEGRADED"
                    flow_state["reconciliation_status"] = "FLOW_RECONCILIATION_DEGRADED"
            open_mints = {
                position.mint
                for position in self._positions.values()
                if position.status == "OPEN"
            }
            changed = False
            for candidate in self._candidates.values():
                # An open position remains a live market-data obligation even
                # after its Candidate leaves ACTIVE_CANDIDATE (for example
                # after an age/price refresh).  Do not let that state change
                # silently remove the pool from WSS or retain a stale
                # SUBSCRIBED marker.
                if (
                    candidate.mint not in open_mints
                    and (not candidate.candidate_eligible or candidate.state not in self.WSS_CANDIDATE_STATES)
                ) or (candidate.descriptor is None and not self._is_flap_candidate(candidate)):
                    continue
                previous = candidate.source_status.get("wss_subscription_status")
                subscribed = (
                    FLAP_PORTAL_ADDRESS in self._wss_subscribed_addresses
                    if candidate.descriptor is None and self._is_flap_candidate(candidate)
                    else candidate.descriptor.address in self._wss_subscribed_addresses
                )
                if subscribed and state in {"HEALTHY", "READY"}:
                    candidate.source_status["wss_subscription_status"] = "SUBSCRIBED"
                    candidate.source_status["wss_status"] = "SUBSCRIBED"
                    if candidate.descriptor is None or candidate.descriptor.pool_type in {V2_POOL_TYPE, FLAP_PORTAL_POOL_TYPE}:
                        candidate.source_status["flow_status"] = "SUPPORTED"
                    elif candidate.descriptor.pool_type == BONDING_CURVE_POOL_TYPE:
                        candidate.source_status["flow_status"] = "SUPPORTED_DIRECTION_ONLY"
                elif state in {"DEGRADED", "UNAVAILABLE", "STOPPED"}:
                    candidate.source_status["wss_subscription_status"] = "WSS_SUBSCRIBE_FAILED"
                else:
                    candidate.source_status["wss_subscription_status"] = "READY"
                changed = changed or previous != candidate.source_status.get("wss_subscription_status")
            if changed:
                now = self.clock()
                for candidate in self._candidates.values():
                    if candidate.candidate_eligible:
                        self._persist_candidate(candidate, now)
                self.connection.commit()
                self._publish(now)

    def _no_trade_monitoring_ready(self, candidate: SurvivorCandidate | None) -> bool:
        """Whether a position has a verified venue flow channel for the 40s rule.

        Binance Meme Rush activity is not a substitute for a subscribed,
        ABI-parsed venue event stream.  Pre-migration Flap and any venue with
        no supported flow parser therefore remain open for the normal hard
        stop/time-stop rules instead of being falsely labelled NO_TRADE_40S.
        """

        if candidate is None or (candidate.descriptor is None and not self._is_flap_candidate(candidate)):
            return False
        if candidate.source_status.get("wss_subscription_status") != "SUBSCRIBED":
            return False
        provider_state = getattr(self, "_wss_provider_state", None)
        subscribed = getattr(self, "_wss_subscribed_addresses", set())
        # A persisted SUBSCRIBED value is not enough after a restart or a
        # candidate state transition.  Require the current provider to be
        # healthy and, when it reports its active set, require this exact pool
        # to be present in that set.
        if provider_state is not None and provider_state not in {"HEALTHY", "READY"}:
            return False
        watched_address = FLAP_PORTAL_ADDRESS if candidate.descriptor is None else candidate.descriptor.address
        if subscribed and watched_address not in subscribed:
            return False
        return candidate.source_status.get("flow_status") in {"SUPPORTED", "SUPPORTED_DIRECTION_ONLY"}

    def active_candidate_wss_status(self) -> tuple[dict[str, object], ...]:
        """Return explainable pool/subscription state for every live Candidate."""

        with self._lock:
            subscribed = self._wss_subscribed_addresses
            result: list[dict[str, object]] = []
            for candidate in sorted(
                (item for item in self._candidates.values() if item.candidate_eligible and item.state in self.WSS_CANDIDATE_STATES),
                key=lambda item: (item.symbol or "", item.mint),
            ):
                descriptor = candidate.descriptor
                watched_address = FLAP_PORTAL_ADDRESS if descriptor is None and self._is_flap_candidate(candidate) else (descriptor.address if descriptor is not None else None)
                if watched_address is not None and watched_address in subscribed and self._wss_provider_state in {"HEALTHY", "READY"}:
                    subscription = "SUBSCRIBED"
                    failure = ""
                elif descriptor is None:
                    subscription = candidate.source_status.get("wss_subscription_status", "UNRESOLVED")
                    failure = candidate.source_status.get("wss_failure_reason", "VENUE_UNKNOWN")
                elif self._wss_provider_state in {"DEGRADED", "UNAVAILABLE", "STOPPED"}:
                    subscription = "WSS_SUBSCRIBE_FAILED"
                    failure = "WSS_SUBSCRIBE_FAILED"
                else:
                    subscription = candidate.source_status.get("wss_subscription_status", "READY")
                    failure = candidate.source_status.get("wss_failure_reason", "")
                result.append({
                    "contract_address": candidate.mint,
                    "symbol": candidate.symbol,
                    "pair_address": candidate.pair_address,
                    "venue": candidate.source_status.get("venue", "UNKNOWN"),
                    "protocol": candidate.source_status.get("protocol", "UNKNOWN"),
                    "migration_status": candidate.source_status.get("migration_status", "UNKNOWN"),
                    "pool_source": candidate.source_status.get("pool_source", "NONE"),
                    "pool_valid": candidate.source_status.get("pool_valid", "INVALID"),
                    "wss_subscription_status": subscription,
                    "failure_reason": failure,
                    "last_price_event": candidate.source_status.get("last_price_event_at"),
                    "last_swap_event": candidate.source_status.get("last_swap_event_at"),
                    "last_sync_event": candidate.source_status.get("last_sync_event_at"),
                })
            return tuple(result)

    @staticmethod
    def _percentile(values: Sequence[Decimal], percentile: int) -> str | None:
        if not values:
            return None
        ordered = sorted(values)
        index = max(0, min(len(ordered) - 1, (percentile * len(ordered) + 99) // 100 - 1))
        return str(ordered[index])

    def _distribution(self, values: Sequence[Decimal], total: int) -> dict[str, object]:
        return {
            "valid": len(values),
            "missing": max(0, total - len(values)),
            "p50": self._percentile(values, 50),
            "p90": self._percentile(values, 90),
            "p95": self._percentile(values, 95),
            "p99": self._percentile(values, 99),
            "max": str(max(values)) if values else None,
        }

    def _diagnostics(self, recent: Sequence[SurvivorCandidate], now: datetime) -> tuple[dict[str, object], dict[str, object]]:
        def age(candidate: SurvivorCandidate) -> bool:
            return self._age_seconds(candidate, now) >= self.config.min_age_sec

        def mc(candidate: SurvivorCandidate) -> bool:
            return candidate.market_cap_usd is not None and candidate.market_cap_usd >= self.config.min_active_mc_usd

        def liquidity(candidate: SurvivorCandidate) -> bool:
            return candidate.liquidity_usd is not None and candidate.liquidity_usd >= self.config.min_active_liquidity_usd

        def holders(candidate: SurvivorCandidate) -> bool:
            return candidate.holders is not None and candidate.holders >= self.config.min_active_holders

        age_pass = [age(candidate) for candidate in recent]
        mc_pass = [mc(candidate) for candidate in recent]
        liquidity_pass = [liquidity(candidate) for candidate in recent]
        holder_pass = [holders(candidate) for candidate in recent]
        gate_distribution = {
            "as_of_timestamp": now.isoformat(),
            "window": "last_1000_by_last_seen_at",
            "definition": "The most recent 1000 tracked contracts ordered by last_seen_at; gate counts are independent and cumulative counts are row-wise intersections.",
            "size": len(recent),
            f"age_gte_{self.config.min_age_sec}s": sum(age_pass),
            f"mc_gte_{self.config.min_active_mc_usd}": sum(mc_pass),
            f"liquidity_gte_{self.config.min_active_liquidity_usd}": sum(liquidity_pass),
            f"holders_gte_{self.config.min_active_holders}": sum(holder_pass),
            "cumulative": {
                "age_pass": sum(age_pass),
                "age_mc_pass": sum(a and m for a, m in zip(age_pass, mc_pass)),
                "age_mc_liquidity_pass": sum(a and m and l for a, m, l in zip(age_pass, mc_pass, liquidity_pass)),
                "age_mc_liquidity_holder_pass": sum(a and m and l and h for a, m, l, h in zip(age_pass, mc_pass, liquidity_pass, holder_pass)),
            },
            "market_cap_usd": self._distribution([c.market_cap_usd for c in recent if c.market_cap_usd is not None], len(recent)),
            "liquidity_usd": self._distribution([c.liquidity_usd for c in recent if c.liquidity_usd is not None], len(recent)),
            "holders": self._distribution([Decimal(c.holders) for c in recent if c.holders is not None], len(recent)),
        }
        top20 = sorted(
            [candidate for candidate in recent if candidate.state not in {"EXPIRED", "REJECTED"}],
            key=lambda candidate: (self._candidate_distance(candidate, now), -candidate.last_seen_at.timestamp(), candidate.mint),
        )[:20]
        gate_distribution["top20_closest"] = [
            {
                "rank": index,
                "mint": candidate.mint,
                "symbol": candidate.symbol,
                "age_seconds": self._age_seconds(candidate, now),
                "market_cap_usd": str(candidate.market_cap_usd) if candidate.market_cap_usd is not None else None,
                "liquidity_usd": str(candidate.liquidity_usd) if candidate.liquidity_usd is not None else None,
                "holders": candidate.holders,
                "distance_score": str(self._candidate_distance(candidate, now)),
                "pre_candidate_watch": candidate.pre_candidate_watch,
                "price_status": candidate.price_status,
                "last_rejection": candidate.last_rejection,
            }
            for index, candidate in enumerate(top20, start=1)
        ]

        candidates = [candidate for candidate in recent if candidate.candidate_eligible]
        candidate_delays = [candidate.first_price_delay_seconds for candidate in candidates if candidate.first_price_delay_seconds is not None]
        all_delays = [candidate.first_price_delay_seconds for candidate in recent if candidate.first_price_delay_seconds is not None]
        coverage = {
            "as_of_timestamp": now.isoformat(),
            "window": "candidate_rows_in_last_1000",
            "definition": "Price history is evaluated strictly before candidate_at; post-candidate prices are excluded.",
            "candidate_count": len(candidates),
            "candidate_with_pre_candidate_price_history": sum(candidate.price_samples_before_candidate > 0 for candidate in candidates),
            "candidate_with_at_least_3_price_samples_before_candidate": sum(candidate.price_samples_before_candidate >= 3 for candidate in candidates),
            "candidate_with_at_least_30s_price_coverage_before_candidate": sum(candidate.price_coverage_before_candidate_seconds >= 30 for candidate in candidates),
            "candidate_with_at_least_20_price_samples_before_candidate": sum(candidate.price_samples_before_candidate >= 20 for candidate in candidates),
            "candidate_with_at_least_10m_price_coverage_before_candidate": sum(candidate.price_coverage_before_candidate_seconds >= 600 for candidate in candidates),
            "first_price_delay_seconds": {
                "valid": len(candidate_delays),
                "p50": self._percentile(candidate_delays, 50),
                "p90": self._percentile(candidate_delays, 90),
                "p95": self._percentile(candidate_delays, 95),
            },
            "all_recent_first_price_delay_seconds": {
                "valid": len(all_delays),
                "p50": self._percentile(all_delays, 50),
                "p90": self._percentile(all_delays, 90),
                "p95": self._percentile(all_delays, 95),
            },
            "candidate_ath_before_candidate": sum(candidate.ath_before_candidate for candidate in candidates),
            "candidate_ath_before_candidate_total": len(candidates),
            "live_usable": sum(candidate.price_history_status == "LIVE_USABLE" for candidate in candidates),
            "history_quality_good": sum(candidate.price_history_quality == PRICE_HISTORY_QUALITY_GOOD for candidate in candidates),
            "history_quality_sparse": sum(candidate.price_history_quality == PRICE_HISTORY_QUALITY_SPARSE for candidate in candidates),
            "history_quality_insufficient": sum(candidate.price_history_quality == PRICE_HISTORY_QUALITY_INSUFFICIENT for candidate in candidates),
            "live_complete": sum(candidate.price_history_status == "LIVE_COMPLETE" for candidate in candidates),
            "backfilled_complete": sum(candidate.price_history_status == "BACKFILLED_COMPLETE" for candidate in candidates),
            "backfill_insufficient": sum(candidate.price_history_status == "BACKFILL_INSUFFICIENT" for candidate in candidates),
            "price_history_incomplete": sum(candidate.price_history_status == "PRICE_HISTORY_INCOMPLETE" for candidate in candidates),
        }
        return gate_distribution, coverage

    def status(self) -> dict[str, object]:
        with self._lock:
            now = self.clock()
            all_candidates = tuple(self._candidates.values())
            recent = sorted(self._candidates.values(), key=lambda item: (item.last_seen_at, item.first_seen_at), reverse=True)[:1000]
            counts = defaultdict(int)
            rejection_reasons = defaultdict(int)
            source_unavailable = defaultdict(int)
            for candidate in recent:
                counts[candidate.state] += 1
                if candidate.last_rejection:
                    reason = candidate.last_rejection
                    parts = reason.removeprefix("FILTERED:").split("|") if reason.startswith("FILTERED:") else [reason]
                    for part in parts:
                        if part:
                            rejection_reasons[part] += 1
                for field_name, state in candidate.source_status.items():
                    if state == "SOURCE_UNAVAILABLE":
                        source_unavailable[field_name] += 1
            open_positions = [self._position_payload(position) for position in self._positions.values() if position.status == "OPEN"]
            recent_status = {
                "as_of_timestamp": now.isoformat(),
                "window": "last_1000_by_last_seen_at",
                "definition": "Recent discovery snapshot, not the current all-tracked runtime state.",
                "size": len(recent),
                "price_valid": sum(candidate.price_status == "VALID" for candidate in recent),
                "price_pending": sum(candidate.price_status == "PENDING" for candidate in recent),
                "candidate_eligible": sum(candidate.candidate_eligible for candidate in recent),
                "active_candidate": sum(candidate.state == "ACTIVE_CANDIDATE" for candidate in recent),
                "pullback_zone": sum(candidate.state in {"PULLBACK_ZONE", "LOW_FORMING", "STOP_CONFIRMED"} for candidate in recent),
                "audit_requested": sum(candidate.audit_requested for candidate in recent),
                "audit_failed": sum(candidate.audit_failed for candidate in recent),
                "quote_requested": sum(candidate.quote_requested for candidate in recent),
                "ready_to_buy": sum(candidate.ready_to_buy for candidate in recent),
                "paper_buy": sum(candidate.paper_buy for candidate in recent),
            }
            gate_distribution, price_coverage = self._diagnostics(recent, now)
            current_candidates = [candidate for candidate in all_candidates if candidate.candidate_eligible]
            active_now = [candidate for candidate in current_candidates if candidate.state == "ACTIVE_CANDIDATE"]
            wss_details = self.subscription_details()
            wss_candidate_status = self.active_candidate_wss_status()
            cohort_counts = Counter(candidate.data_quality_cohort for candidate in all_candidates)
            recent_cohort_counts = Counter(candidate.data_quality_cohort for candidate in recent)
            history_status_counts = Counter(candidate.price_history_status for candidate in current_candidates)
            loop_ms = [Decimal(value) for value in self._main_loop_durations_ms]
            evaluate_lag_ms = [Decimal(value) for value in self._candidate_evaluate_lags_ms]
            venue_ms = [Decimal(value) for value in self._venue_resolution_durations_ms]
            runtime_db_size = self._runtime_db_size_bytes()
            candidate_history_items = [
                {
                    "contract": candidate.mint,
                    "symbol": candidate.symbol,
                    "first_seen_at": _iso(candidate.first_seen_at),
                    "candidate_at": _iso(candidate.candidate_at),
                    "data_quality_cohort": candidate.data_quality_cohort,
                    "history_status": candidate.price_history_status,
                    "history_source": candidate.history_source,
                    "history_sample_count": candidate.history_sample_count,
                    "history_interval": candidate.history_interval,
                    "max_history_gap_seconds": str(candidate.max_history_gap_seconds) if candidate.max_history_gap_seconds is not None else None,
                    "price_history_quality": candidate.price_history_quality,
                    "history_start_at": _iso(candidate.history_start_at),
                    "history_end_at": _iso(candidate.history_end_at),
                    "ath_before_candidate_price_usd": str(candidate.ath_before_candidate_price_usd) if candidate.ath_before_candidate_price_usd is not None else None,
                    "ath_before_candidate_at": _iso(candidate.ath_before_candidate_at),
                    "ath_price_usd": str(candidate.ath_price_usd) if candidate.ath_price_usd is not None else None,
                    "current_price_usd": str(candidate.current_price_usd) if candidate.current_price_usd is not None else None,
                    "current_drawdown_pct": str(candidate.drawdown_pct) if candidate.drawdown_pct is not None else None,
                    "audit_prefetch_at": candidate.source_status.get("audit_prefetch_at"),
                    "audit_status": candidate.source_status.get("audit_status", candidate.audit_state),
                    "audit_age_seconds": self._audit_age_seconds(candidate, now),
                    "state": candidate.state,
                    "last_rejection": candidate.last_rejection,
                }
                for candidate in sorted(current_candidates, key=lambda item: (item.candidate_at or item.first_seen_at, item.mint))
            ]
            lifecycle_counts = Counter(candidate.latest_lifecycle or "UNKNOWN" for candidate in recent)
            lifecycle_transitions = Counter(
                f"{candidate.first_lifecycle or 'UNKNOWN'}->{candidate.latest_lifecycle or 'UNKNOWN'}"
                for candidate in recent
                if candidate.first_lifecycle is not None or candidate.latest_lifecycle is not None
            )
            discovery_delays = [candidate.discovery_delay_seconds for candidate in recent if candidate.discovery_delay_seconds is not None]
            return {
                "identity": self.config.identity.as_dict(),
                "as_of_timestamp": now.isoformat(),
                "strategy_enabled": self.config.enabled,
                "execution_provider": self.config.execution_provider,
                "paper_only": True,
                "discovered": len(recent),
                "tracked_total": len(self._candidates),
                "active": counts["ACTIVE_CANDIDATE"],
                "candidate_eligible_now": len(current_candidates),
                "active_candidate_now": len(active_now),
                "wss_subscriptions_now": len(wss_details),
                "wss_provider_state": self._wss_provider_state,
                "wss_candidate_coverage": wss_candidate_status,
                "candidate_gate_pass_last_1000": gate_distribution,
                "definitions": {
                    "candidate_eligible_now": "All currently tracked contracts satisfying the unchanged Age/MC/Liquidity/Holder Candidate gate at as_of_timestamp.",
                    "active_candidate_now": "Current all-tracked contracts whose state is ACTIVE_CANDIDATE.",
                    "wss_subscriptions_now": "Current resolved WSS pool subscriptions after stale cleanup; one entry per pool.",
                },
                "pullback": counts["PULLBACK_ZONE"] + counts["LOW_FORMING"],
                "stop_confirmed": counts["STOP_CONFIRMED"],
                "reversal_confirmed": counts["REVERSAL_CONFIRMED"] + counts["READY_TO_BUY"],
                "open_positions": len(open_positions),
                "states": dict(counts),
                "active_max": self.config.active_max,
                "active_min_mc_usd": str(self.config.min_active_mc_usd),
                "entry_min_mc_usd": str(self.config.universe_min_mc_usd),
                "entry_max_mc_usd": (
                    str(self.config.universe_max_mc_usd)
                    if self.config.universe_max_mc_usd is not None
                    else None
                ),
                "no_trade_exit_sec": self.config.no_trade_exit_sec,
                "config_self_check": self.config.self_check(),
                "diagnostic_config": self.config.diagnostic_values(),
                "recent_1000": recent_status,
                "candidate_gate_diagnostics": gate_distribution,
                "price_coverage": price_coverage,
                "data_quality": {
                    "cohort_start_at": self.data_quality_start_at.isoformat(),
                    "cohort_counts_all_tracked": dict(cohort_counts),
                    "cohort_counts_last_1000": dict(recent_cohort_counts),
                    "candidate_history_status_counts_now": dict(history_status_counts),
                    "candidate_history_items_now": candidate_history_items,
                    "legacy_backfill_capability": "UNAVAILABLE_FOR_BSC_EXISTING_BINANCE_KLINE_ADAPTER",
                    "legacy_backfill_definition": "No BSC Kline request was made; the existing adapter only supports CT_501/Solana.",
                },
                "lifecycle_diagnostics": {
                    "latest_counts": dict(lifecycle_counts),
                    "first_to_latest": dict(lifecycle_transitions),
                    "create_time_valid": sum(candidate.token_created_at is not None for candidate in recent),
                    "discovery_delay_seconds": {
                        "valid": len(discovery_delays),
                        "p50": self._percentile(discovery_delays, 50),
                        "p90": self._percentile(discovery_delays, 90),
                        "p95": self._percentile(discovery_delays, 95),
                    },
                },
                "pre_candidate_price_watch_count": sum(candidate.pre_candidate_watch for candidate in recent),
                "pre_candidate_price_watch_max": self.config.pre_candidate_watch_max,
                "last_discovery_count": self._last_discovery_count,
                "real_token_updates": self._record_update_total,
                "wss_flow_samples": sum(len(candidate.flows) for candidate in recent),
                "survivor_wss_pool_count": len(wss_details),
                "wss_subscriptions": [
                    {key: value for key, value in detail.items() if key != "descriptor"}
                    for detail in wss_details
                ],
                "active_candidate_wss_status": wss_candidate_status,
                "flow_reconciliation": {
                    "transport_health": self._wss_provider_state,
                    "subscription_health": "HEALTHY" if self._wss_provider_state in {"HEALTHY", "READY"} else "DEGRADED",
                    "data_completeness_health": (
                        "DEGRADED" if any(item.get("data_completeness_health") in {"DEGRADED", "FAILED"} for item in self._flow_reconciliation_state.values())
                        else ("HEALTHY" if self._flow_reconciliation_state and all(item.get("data_completeness_health") == "HEALTHY" for item in self._flow_reconciliation_state.values()) else "PENDING")
                    ),
                    "sources": {key: dict(value) for key, value in self._flow_reconciliation_state.items()},
                    "silent_miss_count": self._flow_reconciliation_misses_total,
                    "recovered_event_count": self._flow_reconciliation_recovered_total,
                    "rpc_calls": self._flow_reconciliation_rpc_calls,
                    "rpc_429": self._flow_reconciliation_rpc_429,
                    "latency_ms": self._distribution(getattr(self, "_flow_reconciliation_latency_ms", ()), len(getattr(self, "_flow_reconciliation_latency_ms", ()))),
                    "lag_blocks": self._distribution(getattr(self, "_flow_reconciliation_lag_blocks", ()), len(getattr(self, "_flow_reconciliation_lag_blocks", ()))),
                },
                "audit_checks": self._audit_checks,
                "audit_requested_total": self._audit_requested_total,
                "audit_failed_total": self._audit_failed_total,
                "db_commit_error_count": self._db_commit_error_count,
                "db_locked_error_count": self._db_locked_error_count,
                "db_write_error_count": self._db_write_error_count,
                "last_db_write_at": _iso(self._last_db_write_at),
                "runtime_db_size": runtime_db_size,
                "db_growth_bytes_per_hour": (
                    None
                    if now <= self._runtime_db_initial_at
                    else int((runtime_db_size - self._runtime_db_initial_bytes) * 3600 / (now - self._runtime_db_initial_at).total_seconds())
                ),
                "retention": {
                    "last_run_at": _iso(self._last_retention_at),
                    "deleted_rows": self._retention_deleted_rows,
                    "flow_seconds": int(self.FLOW_RETENTION.total_seconds()),
                    "price_snapshot_seconds": int(self.PRICE_SNAPSHOT_RETENTION.total_seconds()),
                    "observability_seconds": int(self.OBSERVABILITY_RETENTION.total_seconds()),
                    "inactive_candidate_seconds": int(self.INACTIVE_CANDIDATE_RETENTION.total_seconds()),
                    "registry_seconds": int(self.REGISTRY_RETENTION.total_seconds()),
                    "closed_position_seconds": int(self.CLOSED_POSITION_RETENTION.total_seconds()),
                },
                "quote_requested_total": self._quote_requested_total,
                "paper_buy_total": self._paper_buy_total,
                "scheduler": {
                    "main_loop_last_tick_at": _iso(now),
                    "main_loop_duration_ms": self._distribution(loop_ms, len(loop_ms)),
                    "candidate_evaluate_lag_ms": self._distribution(evaluate_lag_ms, len(evaluate_lag_ms)),
                    "venue_resolution_ms": self._distribution(venue_ms, len(venue_ms)),
                    "venue_jobs_queued_or_running": len(self._venue_resolution_jobs),
                    "venue_result_queue_size": self._venue_resolution_results.qsize(),
                    "venue_resolution_completed": self._venue_resolution_completed,
                    "venue_resolution_failed": self._venue_resolution_failed,
                    "slow_venue_apply_count": self._slow_venue_apply_count,
                    "last_slow_venue_apply": self._last_slow_venue_apply,
                    "stages": self._stage_metrics_payload(),
                    "venue_queue_depth": self._venue_resolution_results.qsize() + len(self._venue_resolution_jobs),
                    "oldest_venue_job_age_ms": self._oldest_venue_job_age_ms(now),
                    "flow_queue_depth": len(self._pending_events) + len(self._pending_gap_events),
                    "position_queue_depth": len(self._position_mark_dirty),
                    "flow_event_queue_latency_ms": self._distribution(
                        getattr(self, "_flow_event_queue_latencies_ms", ()),
                        len(getattr(self, "_flow_event_queue_latencies_ms", ())),
                    ),
                    "reconciliation_result_queue_latency_ms": self._distribution(
                        getattr(self, "_reconciliation_result_queue_latencies_ms", ()),
                        len(getattr(self, "_reconciliation_result_queue_latencies_ms", ())),
                    ),
                },
                "audit_source": "Binance Token Audit/Meme Rush fields only",
                "audit_prefetch_ttl_seconds": AUDIT_PREFETCH_TTL_SECONDS,
                "audit_prefetch_items": [
                    {
                        "contract": candidate.mint,
                        "symbol": candidate.symbol,
                        "drawdown": str(candidate.drawdown_pct) if candidate.drawdown_pct is not None else None,
                        "audit_prefetch_at": candidate.source_status.get("audit_prefetch_at"),
                        "audit_status": candidate.source_status.get("audit_status", candidate.audit_state),
                        "audit_age_seconds": self._audit_age_seconds(candidate, now),
                    }
                    for candidate in sorted(all_candidates, key=lambda item: (item.last_seen_at, item.mint), reverse=True)
                    if candidate.source_status.get("audit_prefetch_at") is not None
                ],
                "rejection_reasons": dict(rejection_reasons),
                "source_unavailable_fields": dict(source_unavailable),
                "field_status_values": ["VALID", "PENDING", "NOT_REQUESTED", "NOT_APPLICABLE", "SOURCE_UNAVAILABLE", "INVALID"],
                "open_position_items": open_positions,
                "missing_audit": sorted(candidate.mint for candidate in recent if candidate.audit_requested and candidate.audit_failed),
            }

    @staticmethod
    def _usd_gate_label(value: Decimal) -> str:
        """Render the configured USD gate in stable, machine-readable reasons."""
        if value >= Decimal("1000000") and value % Decimal("1000000") == 0:
            return f"{int(value / Decimal('1000000'))}M"
        if value >= Decimal("1000") and value % Decimal("1000") == 0:
            return f"{int(value / Decimal('1000'))}K"
        return str(value).replace(".", "_")

    def _discovery_state(self, candidate: SurvivorCandidate, now: datetime) -> str:
        if self._is_balanced_price_ceiling_terminal(candidate):
            self._reject_balanced_price_ceiling(candidate, now)
            return "REJECTED"
        candidate.age_seconds = self._age_seconds(candidate, now)
        candidate.candidate_eligible = self._candidate_eligible(candidate, now)
        candidate.active_candidate = candidate.candidate_eligible
        if candidate.price_status != "VALID":
            candidate.last_rejection = None
        if candidate.age_seconds < self.config.min_age_sec:
            candidate.source_status["age"] = "VALID"
            return "LIGHT_TRACKING"
        if self.config.max_age_sec is not None and candidate.age_seconds > self.config.max_age_sec:
            candidate.source_status["age"] = "VALID"
            candidate.last_rejection = f"AGE_ABOVE_{self.config.max_age_sec}S"
            return "LIGHT_TRACKING"
        filtered: list[str] = []
        if candidate.market_cap_usd is None:
            candidate.source_status["market_cap"] = "SOURCE_UNAVAILABLE"
        elif candidate.market_cap_usd < self.config.min_active_mc_usd:
            candidate.source_status["market_cap"] = "VALID"
            filtered.append(f"MC_BELOW_{self._usd_gate_label(self.config.min_active_mc_usd)}")
        else:
            candidate.source_status["market_cap"] = "VALID"
        if candidate.liquidity_usd is None:
            candidate.source_status["liquidity"] = "SOURCE_UNAVAILABLE"
        elif candidate.liquidity_usd < self.config.min_active_liquidity_usd:
            candidate.source_status["liquidity"] = "VALID"
            filtered.append(f"LIQUIDITY_BELOW_{self._usd_gate_label(self.config.min_active_liquidity_usd)}")
        else:
            candidate.source_status["liquidity"] = "VALID"
        if candidate.holders is None:
            candidate.source_status["holders"] = "SOURCE_UNAVAILABLE"
        elif candidate.holders < self.config.min_active_holders:
            candidate.source_status["holders"] = "VALID"
            filtered.append(f"HOLDERS_BELOW_{self.config.min_active_holders}")
        else:
            candidate.source_status["holders"] = "VALID"
        if self._candidate_non_price_eligible(candidate, now) and (
            candidate.price_status != "VALID" or candidate.current_price_usd is None
        ):
            candidate.source_status["price"] = "PENDING"
            candidate.last_rejection = "PRICE_PENDING"
            return "PRE_CANDIDATE_PRICE_PENDING"
        if not candidate.candidate_eligible:
            candidate.last_rejection = "FILTERED:" + "|".join(filtered) if filtered else None
            return "LIGHT_TRACKING"
        if candidate.source_status.get("venue_readiness") == "VENUE_NOT_READY":
            candidate.last_rejection = "VENUE_NOT_READY"
            candidate.ready_to_buy = False
            return "WAITING_MIGRATION"
        history_block = self._history_block_reason(candidate) if candidate.candidate_eligible else None
        if history_block is not None:
            if candidate.state in {"PULLBACK_ZONE", "LOW_FORMING", "STOP_CONFIRMED", "REVERSAL_CONFIRMED", "READY_TO_BUY"}:
                candidate.state = "ACTIVE_CANDIDATE"
                candidate.ready_to_buy = False
            candidate.last_rejection = history_block
            return "ACTIVE_CANDIDATE"
        candidate.last_rejection = None
        if candidate.state in {"PULLBACK_ZONE", "LOW_FORMING", "STOP_CONFIRMED", "REVERSAL_CONFIRMED", "READY_TO_BUY", "POSITION_OPEN"}:
            return candidate.state
        return "ACTIVE_CANDIDATE"

    def _is_balanced_price_ceiling_terminal(self, candidate: SurvivorCandidate) -> bool:
        """Whether the Balanced-only price ceiling has permanently retired a token."""

        if self.config.identity != BALANCED_SURVIVOR_REVERSAL_IDENTITY:
            return False
        upper = self.config.max_entry_price_usd
        price = candidate.current_price_usd
        if upper is None or price is None:
            return False
        if candidate.source_status.get("price_ceiling_terminal") == "TRUE" and price <= upper:
            # The stored marker records a former configuration decision.  A
            # later explicit ceiling increase must re-evaluate the token
            # against the active rule instead of keeping it permanently out.
            candidate.source_status.pop("price_ceiling_terminal", None)
            candidate.source_status.pop("price_ceiling_rejected_at", None)
        return price > upper

    def _price_above_entry_ceiling_reason(self) -> str:
        upper = self.config.max_entry_price_usd
        if upper is None:
            return "PRICE_ABOVE_ENTRY_CEILING"
        return f"PRICE_ABOVE_{str(upper).replace('.', '_')}"

    def _reject_balanced_price_ceiling(self, candidate: SurvivorCandidate, now: datetime) -> None:
        """Persist the Balanced-only terminal price decision without changing rules."""

        candidate.source_status["price_ceiling_terminal"] = "TRUE"
        candidate.source_status["price_ceiling_rejected_at"] = now.isoformat()
        candidate.state = "REJECTED"
        candidate.candidate_eligible = False
        candidate.active_candidate = False
        candidate.ready_to_buy = False
        candidate.last_rejection = self._price_above_entry_ceiling_reason()

    def _flap_event_seen(self, event: BscPairEvent) -> bool:
        key = self._flow_event_key(event)
        if key in self._flap_event_key_set or key in getattr(self, "_processed_flow_event_key_set", set()):
            return True
        if len(self._flap_event_keys) >= self._flap_event_keys.maxlen:
            old = self._flap_event_keys.popleft()
            self._flap_event_key_set.discard(old)
        self._flap_event_keys.append(key)
        self._flap_event_key_set.add(key)
        if hasattr(self, "_processed_flow_event_key_set"):
            self._remember_event_key(
                event,
                target="_processed_flow_event_keys",
                key_set=self._processed_flow_event_key_set,
            )
        return False

    @staticmethod
    def _flap_event_time(event: BscPairEvent, ts: int) -> datetime:
        try:
            value = datetime.fromtimestamp(int(ts), timezone.utc)
            # Reject malformed/future timestamps without discarding the log.
            if value.year >= 2020 and value <= datetime.now(timezone.utc) + timedelta(minutes=5):
                return value
        except (TypeError, ValueError, OverflowError, OSError):
            pass
        return event.observed_at

    def _apply_flap_trade_event(self, event: BscPairEvent) -> None:
        if self._flap_event_seen(event):
            return
        words = _data_words(event.data, 7)
        if words is None:
            return
        try:
            ts, token_raw, _trader_raw, amount_raw, quote_raw, fee_raw, post_price_raw = abi_decode(
                ["uint256", "address", "address", "uint256", "uint256", "uint256", "uint256"],
                bytes.fromhex(str(event.data)[2:]),
            )
        except Exception:
            return
        token = normalize_bsc_address(token_raw)
        if token is None:
            return
        candidate = self._candidates.get(token)
        if candidate is None or not self._is_flap_candidate(candidate):
            return
        try:
            token_decimals = int(candidate.source_status.get("flap_token_decimals", "18"))
        except (TypeError, ValueError):
            token_decimals = 18
        try:
            quote_decimals = int(candidate.source_status.get("fundraising_quote_decimals", "18"))
        except (TypeError, ValueError):
            quote_decimals = 18
        scale_token = Decimal(10) ** max(0, min(token_decimals, 36))
        scale_quote = Decimal(10) ** max(0, min(quote_decimals, 36))
        token_amount = Decimal(int(amount_raw)) / scale_token
        quote_amount = Decimal(int(quote_raw)) / scale_quote
        fee_amount = Decimal(int(fee_raw)) / scale_quote
        post_price = Decimal(int(post_price_raw)) / (Decimal(10) ** 18)
        observed_at = self._flap_event_time(event, int(ts))
        side_buy = event.event_type == "flap_token_bought"
        quote_asset = str(candidate.source_status.get("fundraising_quote_asset") or "NATIVE_BNB").lower()
        is_native = quote_asset in {"", "native_bnb", "bnb", BSC_WBNB_ADDRESS, "0x0000000000000000000000000000000000000000"}
        price_native = post_price if is_native else None
        sample = FlowSample(
            observed_at=observed_at,
            buy_volume_bnb=quote_amount if side_buy and is_native else Decimal("0"),
            sell_volume_bnb=quote_amount if not side_buy and is_native else Decimal("0"),
            buy_count=1 if side_buy else 0,
            sell_count=0 if side_buy else 1,
            price_native=price_native,
            price_usd=None,
            price_source="FLAP_PORTAL_EVENT",
            token_amount=token_amount,
            quote_amount=quote_amount,
            fee_amount=fee_amount,
            tx_hash=event.transaction_hash,
            block_number=event.block_number,
            event_type=event.event_type,
            source=event.source,
        )
        candidate.source_status.update({
            "flow_status": "SUPPORTED",
            "strategy_support": "FLOW_SUPPORTED",
            "wss_status": "SUBSCRIBED",
            "wss_subscription_status": "SUBSCRIBED",
            "last_chain_trade_at": observed_at.isoformat(),
            "last_price_event_at": observed_at.isoformat(),
            "price_source": "FLAP_PORTAL_EVENT",
            "flap_last_post_price": str(post_price),
            "flap_last_post_price_at": observed_at.isoformat(),
            "flap_last_token_amount": str(token_amount),
            "flap_last_quote_amount": str(quote_amount),
            "flap_last_fee_amount": str(fee_amount),
            "flap_last_post_price": str(post_price),
        })
        counter = "flap_buy_count" if side_buy else "flap_sell_count"
        try:
            candidate.source_status[counter] = str(int(candidate.source_status.get(counter, "0")) + 1)
        except (TypeError, ValueError):
            candidate.source_status[counter] = "1"
        candidate.flows.append(sample)
        for position in self._positions.values():
            if (
                position.status == "OPEN"
                and position.mint == candidate.mint
                and observed_at >= position.opened_at
            ):
                position.last_trade_at = observed_at
                if price_native is not None:
                    self._set_position_mark(
                        position,
                        native_price=price_native,
                        usd_price=self._position_mark_usd(price_native, candidate),
                        source="FLAP_WSS",
                        observed_at=observed_at,
                    )
                self._persist_trade_activity(position, observed_at)
        if not self._recompute_flap_canonical_price(candidate, observed_at):
            # Keep the raw venue mark for diagnostics while making its lack of
            # a fresh USD conversion explicit to the Candidate state machine.
            candidate.source_status["raw_price_status"] = "VALID_NATIVE"
            if candidate.ath_price_native is None or post_price > candidate.ath_price_native:
                candidate.ath_price_native = post_price
                candidate.ath_at = observed_at
            if candidate.local_low_native is None or post_price < candidate.local_low_native:
                candidate.local_low_native = post_price
        self._persist_flow(candidate, sample)
        self._update_rollups(candidate, observed_at)
        self._persist_candidate(candidate, observed_at)

    def _apply_flap_migration_event(self, event: BscPairEvent) -> None:
        if self._flap_event_seen(event):
            return
        try:
            token_raw, pool_raw, _amount_raw, _eth_raw = abi_decode(
                ["address", "address", "uint256", "uint256"],
                bytes.fromhex(str(event.data)[2:]),
            )
        except Exception:
            return
        token = normalize_bsc_address(token_raw)
        pool = normalize_bsc_address(pool_raw)
        if token is None:
            return
        candidate = self._candidates.get(token)
        if candidate is None or not self._is_flap_candidate(candidate):
            return
        candidate.latest_migrate_status = 1
        candidate.source_status.update({
            "migrate_status": "1",
            "migration_status": "1",
            "venue_state": "MIGRATED_POOL_DISCOVERY",
            "migration_observed_at": event.observed_at.isoformat(),
            "migration_pool": pool or "",
        })
        if pool is not None:
            self._pending_flap_migrations.append((candidate.mint, pool, event.observed_at))
        self.ensure_migrated_market_data(candidate, event.observed_at)
        self._persist_candidate(candidate, event.observed_at)

    def _process_one_flap_migration(self, now: datetime) -> None:
        if not self._pending_flap_migrations:
            return
        mint, pool, observed_at = self._pending_flap_migrations.popleft()
        candidate = self._candidates.get(mint)
        if candidate is None:
            return
        # The migration callback is P1 and must not perform RPC/fingerprint
        # work inline.  Factory WSS/registry will resolve the pool; retain the
        # live migration state and let the bounded worker path consume it.
        candidate.pair_address = normalize_bsc_address(pool) or candidate.pair_address
        candidate.source_status.update({
            "venue_state": "MIGRATED_POOL_DISCOVERY",
            "venue_resolution_job_state": "DEFERRED_WORKER",
            "venue_resolution_deferred_at": (observed_at or now).isoformat(),
            "migration_pool": normalize_bsc_address(pool) or "",
        })
        self._persist_candidate(candidate, now)

    def _apply_event(self, event: BscPairEvent) -> None:
        is_flap_event = event.pool_type == FLAP_PORTAL_POOL_TYPE or event.pair_address.lower() == FLAP_PORTAL_ADDRESS
        if event.event_type in {"swap", "sync", "bonding_curve_event"} and not is_flap_event:
            key = self._flow_event_key(event)
            if key in getattr(self, "_processed_flow_event_key_set", set()):
                return
            self._remember_event_key(
                event,
                target="_processed_flow_event_keys",
                key_set=self._processed_flow_event_key_set,
            )
        if event.event_type in {"swap", "sync", "bonding_curve_event", "flap_token_bought", "flap_token_sold"}:
            source_key = f"{event.pool_type}:{event.pair_address.lower()}"
            state = self._flow_state(source_key)
            if event.block_number is not None:
                previous_block = state.get("last_flow_event_block")
                try:
                    if previous_block is None or int(event.block_number) > int(previous_block):
                        state["last_flow_event_block"] = int(event.block_number)
                except (TypeError, ValueError):
                    state["last_flow_event_block"] = int(event.block_number)
                if event.source == "WSS":
                    previous_wss_block = state.get("last_wss_block")
                    try:
                        if previous_wss_block is None or int(event.block_number) > int(previous_wss_block):
                            state["last_wss_block"] = int(event.block_number)
                    except (TypeError, ValueError):
                        state["last_wss_block"] = int(event.block_number)
            state["last_flow_event_at"] = event.observed_at.isoformat()
        if event.event_type in {"factory_pair_created", "factory_pool_created"}:
            self._apply_factory_pool_event(event)
            return
        if event.pool_type == FLAP_PORTAL_POOL_TYPE or event.pair_address.lower() == FLAP_PORTAL_ADDRESS:
            if event.block_number is not None:
                if self._flap_portal_cursor_block is None or event.block_number > self._flap_portal_cursor_block:
                    self._save_flap_portal_cursor(event.block_number)
            if event.event_type in {"flap_token_bought", "flap_token_sold"}:
                self._apply_flap_trade_event(event)
            elif event.event_type == "flap_launched_to_dex":
                self._apply_flap_migration_event(event)
            self.store.set_state("flap_portal_wss", {
                "state": "HEALTHY",
                "last_event_block": self._flap_portal_cursor_block,
                "last_event_at": event.observed_at.isoformat(),
            })
            return
        pool = event.pair_address.lower()
        event_mint = _fourmeme_event_mint(event)
        candidates = [
            candidate for candidate in self._candidates.values()
            if candidate.descriptor is not None
            and candidate.descriptor.address == pool
            and (candidate.descriptor.pool_type != BONDING_CURVE_POOL_TYPE or candidate.mint == event_mint)
        ]
        for candidate in candidates:
            candidate.source_status[f"last_{event.event_type}_event_at"] = event.observed_at.isoformat()
            sample = parse_v2_swap_flow(candidate.descriptor, event)
            if sample is not None:
                candidate.source_status["flow_status"] = "SUPPORTED"
            if sample is None:
                sample = parse_fourmeme_bonding_curve_activity(candidate.descriptor, event)
                if sample is not None:
                    candidate.source_status["flow_status"] = "SUPPORTED_DIRECTION_ONLY"
            if sample is None:
                sample = FlowSample(observed_at=event.observed_at)
            sample.source = event.source
            if self.resolver is not None and candidate.descriptor is not None:
                price = self.resolver.price_from_event(candidate.descriptor, event)
                if price is not None:
                    sample.price_native = price.native_token_price
                    candidate.source_status["last_price_event_at"] = event.observed_at.isoformat()
                    candidate.source_status.update({
                        "pool_last_raw_price": str(price.native_token_price),
                        "pool_last_raw_price_at": event.observed_at.isoformat(),
                    })
                    if self._recompute_pool_canonical_price(candidate, event.observed_at):
                        sample.price_usd = candidate.current_price_usd
            if event.event_type == "sync":
                words = _data_words(event.data, 2)
                if words is not None and candidate.descriptor.token0 == BSC_WBNB_ADDRESS:
                    sample.liquidity_native = Decimal(words[0]) / (Decimal(10) ** 18)
                elif words is not None and candidate.descriptor.token1 == BSC_WBNB_ADDRESS:
                    sample.liquidity_native = Decimal(words[1]) / (Decimal(10) ** 18)
                if sample.liquidity_native is not None and candidate.native_token_price_usd is not None:
                    sample.liquidity_usd = sample.liquidity_native * candidate.native_token_price_usd
            candidate.flows.append(sample)
            if sample.buy_count > 0 or sample.sell_count > 0:
                for position in self._positions.values():
                    # A reconciliation batch can include trades from before a
                    # newly-created position.  Those events are useful for
                    # candidate history, but must never make a new position
                    # look already idle and trigger its no-trade exit early.
                    if (
                        position.status == "OPEN"
                        and position.mint == candidate.mint
                        and sample.observed_at >= position.opened_at
                    ):
                        position.last_trade_at = sample.observed_at
                        self._persist_trade_activity(position, sample.observed_at)
            if sample.price_native is not None:
                sample.price_source = "BSC_WSS"
            self._persist_flow(candidate, sample)
            self._update_rollups(candidate, sample.observed_at)
            self._persist_candidate(candidate, sample.observed_at)

    def _apply_factory_pool_event(self, event: BscPairEvent) -> None:
        """Queue Factory validation; the owner loop only decodes/cursors."""

        if self.resolver is None:
            return
        pool_type = V2_POOL_TYPE if event.event_type == "factory_pair_created" else V3_POOL_TYPE
        if event.block_number is not None:
            previous = self._factory_cursor.get(pool_type)
            if previous is None or event.block_number > previous:
                self._save_factory_cursor(pool_type, event.block_number)
        # The common parser deliberately requires a target mint; Factory WSS
        # events carry two mints, so decode them locally without SQL in WSS.
        if len(event.topics) < 3:
            return
        token0 = normalize_bsc_address("0x" + event.topics[1][-40:])
        token1 = normalize_bsc_address("0x" + event.topics[2][-40:])
        if token0 is None or token1 is None:
            return
        words = _data_words(event.data, 2)
        if words is None:
            return
        pool = normalize_bsc_address("0x" + f"{words[0 if pool_type == V2_POOL_TYPE else 1]:064x}"[-40:])
        if pool is None:
            return
        job_key = (pool_type, event.block_number, pool)
        if job_key in self._factory_resolution_jobs:
            return
        self._factory_resolution_jobs.add(job_key)
        self._venue_resolution_executor.submit(
            self._run_factory_pool_resolution,
            FactoryPoolResolutionJob(event, pool_type, token0, token1, pool, event.observed_at),
        )

    def _schedule_recent_unknown_quote_pool_upgrade(self, candidate: SurvivorCandidate, now: datetime) -> None:
        """Revalidate a current Live row written by the retired quote whitelist.

        This is deliberately not a historical scanner: it is limited to a
        currently refreshed candidate whose only recorded failure was the old
        unknown-quote classification.  Validation reuses the existing Factory
        worker/result queue, so all RPC happens off the owner loop and the
        normal registry/WSS attachment path remains the single implementation.
        """
        retired_whitelist_state = candidate.source_status.get("wss_failure_reason") == "UNSUPPORTED_QUOTE_ASSET"
        incomplete_rehydration = (
            candidate.source_status.get("pool_status") == "VALID"
            and bool(normalize_bsc_address(candidate.source_status.get("pool_quote_asset")))
            and _decimal(candidate.source_status.get("pool_last_raw_price")) is None
        )
        quote_conversion_pending = (
            candidate.source_status.get("pool_status") == "VALID"
            and bool(normalize_bsc_address(candidate.source_status.get("pool_quote_asset")))
            and _decimal(candidate.source_status.get("pool_last_raw_price")) is not None
            and self._fresh_quote_asset_usd(str(candidate.source_status.get("pool_quote_asset")), now) is None
        )
        if quote_conversion_pending:
            self._schedule_pool_quote_asset_resolution(candidate, now)
            return
        if not retired_whitelist_state and not incomplete_rehydration:
            return
        if now - candidate.last_seen_at > timedelta(seconds=self.config.idle_ttl_sec):
            return
        row = self.connection.execute(
            "SELECT pool_address,pool_type,token0,token1 FROM bsc_pool_registry "
            "WHERE token_address=? AND validation_status IN ('UNSUPPORTED_QUOTE_ASSET','VALID') "
            "ORDER BY discovered_at DESC LIMIT 1",
            (candidate.mint,),
        ).fetchone()
        if row is None:
            return
        pool = normalize_bsc_address(row["pool_address"])
        token0 = normalize_bsc_address(row["token0"])
        token1 = normalize_bsc_address(row["token1"])
        pool_type = str(row["pool_type"] or "").lower()
        if pool is None or token0 is None or token1 is None or pool_type not in {V2_POOL_TYPE, V3_POOL_TYPE}:
            return
        job_key = (pool_type, None, pool)
        if job_key in self._factory_resolution_jobs:
            return
        self._factory_resolution_jobs.add(job_key)
        event = BscPairEvent(
            pair_address=pool,
            event_type="factory_pair_created" if pool_type == V2_POOL_TYPE else "factory_pool_created",
            block_number=None,
            transaction_hash=None,
            log_index=None,
            observed_at=now,
            pool_type=pool_type,
            source="RUNTIME_REVALIDATION",
        )
        # Revalidation of a current candidate must not sit behind historical
        # venue fingerprint jobs.  It returns through the normal Factory
        # result queue and does not write SQLite in the worker.
        self._live_pool_upgrade_executor.submit(
            self._run_factory_pool_resolution,
            FactoryPoolResolutionJob(event, pool_type, token0, token1, pool, now),
        )

    def _schedule_one_recent_unknown_quote_pool_upgrade(self, now: datetime) -> None:
        for candidate in sorted(self._candidates.values(), key=lambda item: (item.last_seen_at, item.mint), reverse=True):
            if now - candidate.last_seen_at > timedelta(seconds=self.config.idle_ttl_sec):
                return
            if candidate.source_status.get("pool_status") == "VALID" and bool(normalize_bsc_address(candidate.source_status.get("pool_quote_asset"))) and _decimal(candidate.source_status.get("pool_last_raw_price")) is not None and self._fresh_quote_asset_usd(str(candidate.source_status.get("pool_quote_asset")), now) is None:
                self._schedule_pool_quote_asset_resolution(candidate, now)
                return
            if not (candidate.source_status.get("wss_failure_reason") == "UNSUPPORTED_QUOTE_ASSET" or (candidate.source_status.get("pool_status") == "VALID" and bool(normalize_bsc_address(candidate.source_status.get("pool_quote_asset"))) and _decimal(candidate.source_status.get("pool_last_raw_price")) is None)):
                continue
            self._schedule_recent_unknown_quote_pool_upgrade(candidate, now)
            return

    def _run_factory_pool_resolution(self, job: FactoryPoolResolutionJob) -> None:
        """RPC/fingerprint/Pool validation worker; never touches SQLite."""
        started = time.monotonic()
        inspection: BscVenueInspection | None = None
        resolutions: list[tuple[str, BscPoolResolution]] = []
        error: str | None = None
        try:
            if self.resolver is not None and hasattr(self.resolver, "inspect_venue"):
                inspection = self.resolver.inspect_venue(job.pool)
            for mint in (job.token0, job.token1):
                checked = (
                    self.resolver._validate_v2_pair(mint, job.pool, quote_asset_usd=None, allow_unsupported_quote=True)
                    if job.pool_type == V2_POOL_TYPE
                    else self.resolver._validate_v3_pool(mint, job.pool, allow_unsupported_quote=True)
                )
                if checked.descriptor is None:
                    continue
                # Pool protocol validation must finish before quote-asset USD
                # conversion.  An unfamiliar quote asset is not a malformed
                # pool and must remain eligible for WSS/Flow attachment.
                status = "VALID"
                resolutions.append((mint, BscPoolResolution(
                    checked.descriptor, status, "PANCAKE_FACTORY_WSS", job.pool,
                    checked.token0, checked.token1, checked.reserves, checked.quote_asset,
                    checked.liquidity_usd, checked.factory, checked.fee,
                )))
        except Exception as exc:  # pragma: no cover - defensive provider boundary
            error = str(exc)[:500] or type(exc).__name__
        self._factory_resolution_results.put(FactoryPoolResolutionResult(
            job=job,
            inspection=inspection,
            resolutions=tuple(resolutions),
            completed_at=self.clock(),
            duration_ms=int((time.monotonic() - started) * 1000),
            error=error,
        ))

    def _drain_factory_pool_resolution_results(self, now: datetime) -> int:
        """Apply a small Factory result slice in the single owner writer."""
        started = time.monotonic()
        applied = 0
        while applied < self.VENUE_RESULT_APPLY_MAX_ITEMS:
            if applied and (time.monotonic() - started) * 1000 >= self.VENUE_RESULT_APPLY_BUDGET_MS:
                break
            try:
                result = self._factory_resolution_results.get_nowait()
            except Empty:
                break
            applied += 1
            apply_started = time.monotonic()
            job = result.job
            self._factory_resolution_jobs.discard((job.pool_type, job.event.block_number, job.pool))
            if job.event.source == "MIGRATED_RUNTIME_DISCOVERY":
                self._migrated_pool_recovery_jobs.discard(job.token0)
            if result.inspection is not None:
                for mint in (job.token0, job.token1):
                    self._persist_venue_registry(mint, result.inspection, job.event.observed_at, source="PANCAKE_FACTORY_WSS")
            for mint, resolution in result.resolutions:
                self._persist_factory_registry_results(mint, (resolution,), job.event.observed_at)
                candidate = self._candidates.get(mint)
                descriptor = resolution.descriptor
                if candidate is None:
                    continue
                if descriptor is None:
                    if job.event.source == "MIGRATED_RUNTIME_DISCOVERY":
                        no_pool = resolution.status in {"NO_PANCAKE_PAIR", "NO_PANCAKE_V3_POOL"}
                        candidate.source_status.update({
                            # A direct Factory lookup is a point-in-time
                            # fact, never a terminal strategy state.  The
                            # normal Candidate evaluator will requeue this
                            # token through ensure_migrated_market_data().
                            "venue_state": "MIGRATED_POOL_PENDING" if no_pool else "MIGRATED_POOL_DISCOVERY_RETRY",
                            "migrated_pool_recovery_state": "PENDING" if no_pool else "RETRY",
                            "venue_readiness": "VENUE_UNRESOLVED",
                            "pool_status": "MIGRATED_POOL_PENDING" if no_pool else resolution.status,
                            "pool_source": resolution.pool_source,
                            "wss_status": "NOT_REQUESTED",
                            "wss_subscription_status": "NOT_REQUESTED",
                            "wss_failure_reason": resolution.status,
                            "migrated_pool_recovery_last_result": resolution.status,
                            "price_status": "PENDING",
                            "canonical_strategy_price": "",
                        })
                        self._persist_candidate(candidate, job.event.observed_at)
                    continue
                # Registry is authoritative.  Quote-asset USD conversion is a
                # separate asynchronous capability, so every valid V2/V3
                # descriptor joins the realtime WSS/Flow path immediately.
                if resolution.status == "VALID" and self._pool_pair_is_valid(candidate, descriptor):
                    candidate.descriptor = descriptor
                    candidate.pair_address = descriptor.address
                    candidate.pool_type = descriptor.pool_type
                    candidate.source_status.update({
                        "venue_state": "MIGRATED_TO_PANCAKE",
                        "migrated_pool_recovery_state": "RESOLVED" if job.event.source == "MIGRATED_RUNTIME_DISCOVERY" else candidate.source_status.get("migrated_pool_recovery_state", ""),
                        "venue_readiness": "VENUE_READY",
                        "pool_status": "VALID",
                        "pool_valid": "VALID",
                        "strategy_support": "SUPPORTED_ADAPTER",
                        "pool_source": resolution.pool_source,
                        "pool_token0": descriptor.token0 or "",
                        "pool_token1": descriptor.token1 or "",
                        "pool_quote_asset": resolution.quote_asset or "",
                        "pool_factory": resolution.factory or "",
                        "pool_liquidity_usd": str(resolution.liquidity_usd) if resolution.liquidity_usd is not None else "UNAVAILABLE",
                        "wss_status": "READY",
                        "wss_subscription_status": "READY",
                        "wss_failure_reason": "",
                    })
                    raw_price = self._pool_raw_price_from_reserves(candidate, descriptor, resolution.reserves)
                    if raw_price is not None:
                        candidate.source_status.update({
                            "pool_last_raw_price": str(raw_price),
                            "pool_last_raw_price_at": job.event.observed_at.isoformat(),
                        })
                    self._schedule_pool_quote_asset_resolution(candidate, now)
                    self._recompute_pool_canonical_price(candidate, now)
                    # The descriptor and its live binding are now available.
                    # Re-run the lightweight Candidate facts immediately;
                    # quote-asset USD completion performs the same refresh if
                    # canonical price is still pending.
                    was_candidate_eligible = candidate.candidate_eligible
                    candidate.candidate_eligible = self._candidate_eligible(candidate, now)
                    if candidate.candidate_eligible and not was_candidate_eligible and candidate.candidate_at is None:
                        self._mark_candidate_entry(candidate, now)
                    candidate.state = self._discovery_state(candidate, now)
                    candidate.active_candidate = candidate.candidate_eligible
                    if not candidate.candidate_eligible:
                        candidate.ready_to_buy = False
                self._persist_candidate(candidate, job.event.observed_at)
            if result.error:
                self.store.set_state("factory_resolution", {"state": "DEGRADED", "last_error": result.error, "last_at": result.completed_at.isoformat()})
            apply_ms = (time.monotonic() - apply_started) * 1000
            if apply_ms > 100:
                self._slow_venue_apply_count += 1
                self._last_slow_venue_apply = {
                    "token": job.token0,
                    "venue": job.pool,
                    "stage": "venue_result_apply",
                    "elapsed_ms": round(apply_ms, 3),
                }
                print("SLOW_VENUE_RESULT_APPLY", json.dumps(self._last_slow_venue_apply, ensure_ascii=False), flush=True)
        self._record_stage_metric("venue_result_apply", (time.monotonic() - started) * 1000, applied)
        return applied

    def _update_price(
        self,
        candidate: SurvivorCandidate,
        price: Decimal,
        now: datetime,
        source: str = "BSC_WSS",
        *,
        native_price: Decimal | None = None,
        record_sample: bool = True,
        persist_snapshot: bool = True,
    ) -> None:
        if price <= 0:
            return
        # Binance token prices remain useful discovery diagnostics, but a
        # Live Balanced decision must be driven by its venue price.  Keep this
        # guard at the common update boundary so a future enrichment caller
        # cannot silently make a reference mark canonical again.
        if (
            getattr(self, "mode", "paper") == "live"
            and self.config.identity == BALANCED_SURVIVOR_REVERSAL_IDENTITY
            and source in {"MEME_RUSH", "TOKEN_INFO", "BINANCE_DIRECT_USD_FALLBACK"}
        ):
            candidate.source_status.update({
                "discovery_reference_price": str(price),
                "discovery_reference_price_at": now.isoformat(),
                "discovery_reference_price_source": source,
                "price_source_priority": "CANONICAL_VENUE_PRICE",
                "price": "REFERENCE_ONLY",
            })
            if candidate.current_price_usd is None:
                candidate.price_status = "CANONICAL_PRICE_PENDING"
            return
        previous_price = candidate.current_price_usd
        previous_source = candidate.price_source
        source_switch_pending = False
        # A migration changes venue and potentially quote conventions.  Do
        # not let the first Pancake observation manufacture an ATH/drawdown
        # transition against a bonding-curve price.  The next consistent
        # source observation can resume the normal lifecycle.
        if (
            source == "BSC_WSS"
            and previous_source in {"MEME_RUSH", "TOKEN_INFO", "FOURMEME_BONDING_CURVE"}
            and previous_price is not None
            and previous_price > 0
        ):
            gap = abs(price / previous_price - Decimal("1"))
            source_switch_pending = gap > Decimal("0.15")
            candidate.source_status["price_source_switch"] = (
                "PRICE_SOURCE_SWITCH_PENDING" if source_switch_pending else "VALID"
            )
            candidate.source_status["price_source_switch_gap_pct"] = str(gap * Decimal("100"))
            if source_switch_pending:
                candidate.price_history_status = "PRICE_SOURCE_SWITCH_PENDING"
        if source == "BSC_WSS" and native_price is None:
            native_price = price
        if native_price is not None:
            candidate.current_price_native = native_price
        elif source != "BSC_WSS":
            candidate.current_price_usd = price
        candidate.current_price_usd = price
        if persist_snapshot:
            try:
                self.connection.execute(
                    "INSERT OR IGNORE INTO survivor_price_snapshots(mint,observed_at,price_usd,price_native,price_source) VALUES(?,?,?,?,?)",
                    (candidate.mint, now.isoformat(), str(price), str(native_price) if native_price is not None else None, source),
                )
            except Exception:
                # Candidate state remains fail-closed if the optional snapshot
                # table is unavailable during an older isolated test/runtime.
                candidate.source_status["price_history"] = "SOURCE_UNAVAILABLE"
        candidate.price_status = "VALID"
        candidate.price_source = source
        candidate.price_updated_at = now
        candidate.price_age_ms = 0
        candidate.source_status["price"] = "VALID"
        # A position must not retain the pre-entry mark forever.  The owner
        # loop later persists this mark once per evaluation; closed positions
        # deliberately use realized executable quotes instead of this value.
        for position in getattr(self, "_positions", {}).values():
            if position.status != "OPEN" or position.mint != candidate.mint:
                continue
            position_native, position_usd = self._candidate_position_mark(candidate, now)
            if position_native is not None:
                self._set_position_mark(
                    position,
                    native_price=position_native,
                    usd_price=position_usd,
                    source=source,
                    observed_at=now,
                )
        if record_sample and persist_snapshot:
            last = candidate.flows[-1] if candidate.flows else None
            if not (
                last is not None
                and last.observed_at == now
                and (last.price_usd or last.price_native) == price
                and last.price_source == source
            ):
                candidate.flows.append(FlowSample(
                    observed_at=now,
                    price_native=native_price,
                    price_usd=price,
                    price_source=source,
                ))
        if candidate.first_seen_price_usd is None:
            candidate.first_seen_price_usd = price
            candidate.first_seen_price_source = source
            candidate.first_price_at = now
            candidate.first_price_delay_seconds = Decimal(str(max(0, (now - candidate.first_seen_at).total_seconds())))
            # The first valid price is always the initial ATH.  This also
            # repairs rows created by the earlier runtime that had a stale
            # native ATH but no valid USD price.
            candidate.ath_price_usd = price
            candidate.ath_at = now
            candidate.ath_price_native = native_price
            candidate.drawdown_pct = Decimal("0")
            candidate.price_history_quality = PRICE_HISTORY_QUALITY_FIRST_SEEN
            candidate.price_history_status = FIRST_SEEN_PRICE_HISTORY_STATUS
            candidate.source_status["price_history"] = "VALID"
            return
        if source_switch_pending:
            return
        if source == "BSC_WSS" and candidate.source_status.get("price_source_switch") == "PRICE_SOURCE_SWITCH_PENDING":
            # A second source-local observation resumes ATH/drawdown tracking;
            # it does not reinterpret the first cross-venue jump.
            candidate.source_status["price_source_switch"] = "VALID"
            candidate.price_history_status = "VALID"
        if candidate.ath_price_usd is None or price > candidate.ath_price_usd:
            candidate.ath_price_usd = price
            candidate.ath_at = now
            candidate.drawdown_pct = Decimal("0")
            candidate.ath_price_native = native_price or candidate.ath_price_native or price
            candidate.local_low_native = None
            candidate.low_started_at = None
            candidate.stable_since = None
            if candidate.state not in {"ACTIVE_CANDIDATE", "LIGHT_TRACKING"}:
                candidate.state = "ACTIVE_CANDIDATE"
            return
        if candidate.ath_price_usd is None:
            return
        drawdown = (price / candidate.ath_price_usd - Decimal("1")) * Decimal("100")
        candidate.drawdown_pct = drawdown
        if native_price is not None:
            candidate.ath_price_native = candidate.ath_price_native or native_price
        if not self.config.require_pullback:
            return
        if drawdown < -self.config.drawdown_max_pct:
            candidate.state = "REJECTED"
            candidate.last_rejection = "DRAWDOWN_TOO_DEEP"
            return
        if drawdown <= -self.config.drawdown_min_pct:
            if candidate.candidate_eligible and self._history_allows_pullback(candidate) and candidate.state in {"ACTIVE_CANDIDATE", "LIGHT_TRACKING"}:
                candidate.state = "PULLBACK_ZONE"
            low_price = native_price or price
            if candidate.local_low_native is None or low_price < candidate.local_low_native:
                candidate.local_low_native = low_price
                candidate.low_started_at = now
                candidate.stable_since = None
            elif candidate.low_started_at is not None and low_price >= candidate.local_low_native:
                candidate.stable_since = candidate.stable_since or now
                if (now - candidate.stable_since).total_seconds() >= self.config.low_stable_sec:
                    candidate.state = "STOP_CONFIRMED"
                    self._audit_event("SURVIVOR_STOP_CONFIRMED", candidate, {"stable_sec": self.config.low_stable_sec})

    @staticmethod
    def _ratio(numerator: Decimal, denominator: Decimal) -> Decimal | None:
        return numerator / denominator if denominator > 0 else None

    def _update_rollups(self, candidate: SurvivorCandidate, now: datetime) -> None:
        candidate.age_seconds = self._age_seconds(candidate, now)
        candidate.price_age_ms = int((now - candidate.price_updated_at).total_seconds() * 1000) if candidate.price_updated_at is not None else None
        def window(seconds: int) -> list[FlowSample]:
            return [sample for sample in candidate.flows if sample.observed_at >= now - timedelta(seconds=seconds)]

        def volumes(samples: Sequence[FlowSample]) -> tuple[Decimal, Decimal]:
            return (
                sum((sample.buy_volume_bnb for sample in samples), Decimal("0")),
                sum((sample.sell_volume_bnb for sample in samples), Decimal("0")),
            )

        samples_30s = window(30)
        samples_1m = window(60)
        samples_3m = window(180)
        candidate.buy_volume_30s, candidate.sell_volume_30s = volumes(samples_30s)
        candidate.buy_volume_1m, candidate.sell_volume_1m = volumes(samples_1m)
        candidate.buy_volume_3m, candidate.sell_volume_3m = volumes(samples_3m)
        candidate.buy_count_1m = sum(sample.buy_count for sample in samples_1m)
        candidate.sell_count_1m = sum(sample.sell_count for sample in samples_1m)
        candidate.buy_sell_volume_ratio_1m = self._ratio(candidate.buy_volume_1m, candidate.sell_volume_1m)
        candidate.buy_sell_count_ratio_1m = self._ratio(Decimal(candidate.buy_count_1m), Decimal(candidate.sell_count_1m))
        price_samples = [sample for sample in candidate.flows if sample.price_usd is not None or sample.price_native is not None]
        if price_samples:
            latest = price_samples[-1]
            one_minute_ago = [sample for sample in price_samples if sample.observed_at <= now - timedelta(seconds=60)]
            if one_minute_ago:
                old_price = one_minute_ago[-1].price_usd or one_minute_ago[-1].price_native
                new_price = latest.price_usd or latest.price_native
                if old_price and new_price and old_price > 0:
                    candidate.price_change_1m = new_price / old_price - Decimal("1")
        liquidity_samples = [sample for sample in candidate.flows if sample.liquidity_usd is not None]
        if liquidity_samples:
            candidate.liquidity_current_usd = liquidity_samples[-1].liquidity_usd
            prior_1m = [sample for sample in liquidity_samples if sample.observed_at <= now - timedelta(seconds=60)]
            prior_2m = [sample for sample in liquidity_samples if sample.observed_at <= now - timedelta(seconds=120)]
            candidate.liquidity_1m_ago_usd = prior_1m[-1].liquidity_usd if prior_1m else None
            candidate.liquidity_2m_ago_usd = prior_2m[-1].liquidity_usd if prior_2m else None

    def _last_valid_market_data_at(self, candidate: SurvivorCandidate) -> datetime:
        """Return the newest locally observed market-data timestamp."""

        timestamps = [candidate.last_seen_at]
        if candidate.price_updated_at is not None:
            timestamps.append(candidate.price_updated_at)
        for name in ("last_price_event_at", "last_swap_event_at", "last_sync_event_at"):
            raw = candidate.source_status.get(name)
            if raw:
                try:
                    timestamps.append(datetime.fromisoformat(raw))
                except ValueError:
                    continue
        return max(timestamps)

    def _should_retain_candidate(self, candidate: SurvivorCandidate, now: datetime) -> bool:
        """Apply Idle TTL only to genuinely stale, non-opportunity candidates.

        The 30-minute value is an inactivity TTL, not a candidate lifetime.  A
        candidate near the configured pullback zone remains retained even when
        the discovery feed temporarily stops returning it, so the existing
        price/WSS path can continue the state machine.
        """

        transition_states = {
            "PULLBACK_ZONE", "LOW_FORMING", "STOP_CONFIRMED",
            "REVERSAL_CONFIRMED", "READY_TO_BUY", "POSITION_OPEN",
        }
        if candidate.state in transition_states:
            return True
        if not candidate.candidate_eligible:
            return False
        if candidate.price_status != "VALID" or not self._history_allows_pullback(candidate):
            return False
        if candidate.drawdown_pct is not None and candidate.drawdown_pct <= Decimal("-20"):
            return True
        last_market_data_at = self._last_valid_market_data_at(candidate)
        return (now - last_market_data_at).total_seconds() <= self.config.idle_ttl_sec

    def _evaluate_candidate(self, candidate: SurvivorCandidate, now: datetime) -> None:
        # This is scheduler wait time, not token age: candidate_at can be
        # hours old and would otherwise make a healthy bounded evaluator look
        # permanently backlogged.  Record the interval since this candidate's
        # previous owner-loop evaluation.
        previous_evaluate_raw = candidate.source_status.get("last_evaluate_at")
        if previous_evaluate_raw:
            try:
                previous_evaluate_at = datetime.fromisoformat(str(previous_evaluate_raw))
                self._candidate_evaluate_lags_ms.append(max(0, int((now - previous_evaluate_at).total_seconds() * 1000)))
            except (TypeError, ValueError):
                pass
        candidate.source_status["last_evaluate_at"] = now.isoformat()
        # ``READY_TO_BUY`` is a final entry state, never a stale sub-gate.
        # A fresh discovery/price evaluation that removes Candidate eligibility
        # must clear it before quote or execution work can be requested.
        if not candidate.candidate_eligible:
            candidate.ready_to_buy = False
        if candidate.state == "EXPIRED" and self._should_retain_candidate(candidate, now):
            candidate.state = "ACTIVE_CANDIDATE"
            candidate.active_candidate = True
            candidate.last_rejection = None
        if (now - candidate.last_seen_at).total_seconds() > self.config.idle_ttl_sec and not self._should_retain_candidate(candidate, now):
            candidate.state = "EXPIRED"
            candidate.last_rejection = "IDLE_TTL_30M"
            self._persist_candidate(candidate, now)
            return
        candidate.age_seconds = self._age_seconds(candidate, now)
        candidate.price_age_ms = int((now - candidate.price_updated_at).total_seconds() * 1000) if candidate.price_updated_at is not None else None
        if candidate.price_status != "VALID" or candidate.current_price_usd is None or candidate.ath_price_usd is None:
            return
        # Balanced intentionally has no second strategy-entry filter.  Once
        # Candidate has the complete configured MC/liquidity/holder/price
        # signal, proceed directly to the executable Quote/Paper path.  WSS,
        # Audit and Flow stay observable, but they no longer veto this profile.
        if self.config.candidate_is_entry:
            if not candidate.candidate_eligible:
                return
            candidate.ready_to_buy = True
            candidate.state = "READY_TO_BUY"
            self._try_buy(candidate, now)
            return
        if candidate.source_status.get("venue_readiness") == "VENUE_NOT_READY":
            candidate.state = "WAITING_MIGRATION"
            candidate.ready_to_buy = False
            candidate.last_rejection = "VENUE_NOT_READY"
            return
        if candidate.descriptor is not None and candidate.descriptor.pool_type == V3_POOL_TYPE:
            # Price/ATH can be tracked from V3 Swap events, but this runtime has
            # no validated V3 Swap-flow direction/volume parser yet.
            candidate.source_status["flow_status"] = "UNSUPPORTED"
            candidate.ready_to_buy = False
            candidate.last_rejection = "FLOW_UNSUPPORTED_V3"
            return
        if candidate.candidate_eligible and not self._wss_ready_for_candidate(candidate):
            candidate.ready_to_buy = False
            if candidate.state in {"PULLBACK_ZONE", "LOW_FORMING", "STOP_CONFIRMED", "REVERSAL_CONFIRMED", "READY_TO_BUY"}:
                candidate.state = "ACTIVE_CANDIDATE"
            candidate.last_rejection = candidate.source_status.get("wss_failure_reason") or "WSS_SUBSCRIBE_FAILED"
            return
        history_block = self._history_block_reason(candidate) if candidate.candidate_eligible else None
        if history_block is not None:
            candidate.last_rejection = history_block
            candidate.ready_to_buy = False
            return
        self._maybe_schedule_audit_prefetch(candidate, now)
        if candidate.local_low_native and candidate.local_low_native > 0:
            comparison_price = candidate.current_price_native or candidate.current_price_usd
            rebound = (comparison_price / candidate.local_low_native - Decimal("1")) * Decimal("100")
            if rebound > self.config.rebound_max_pct:
                candidate.state = "REJECTED"
                candidate.last_rejection = "REBOUND_TOO_HIGH"
                return
            if candidate.state == "STOP_CONFIRMED" and rebound >= self.config.rebound_min_pct:
                candidate.state = "REVERSAL_CONFIRMED"
                self._audit_event("SURVIVOR_REVERSAL_CONFIRMED", candidate, {"rebound_pct": rebound})
        if self.config.require_pullback and candidate.state not in {"REVERSAL_CONFIRMED", "READY_TO_BUY"}:
            return
        reason = self._universe_reason(candidate, now)
        if reason is not None:
            candidate.last_rejection = reason
            return
        if self.config.require_pullback and (candidate.drawdown_pct is None or candidate.drawdown_pct > -self.config.drawdown_min_pct):
            candidate.last_rejection = "AUDIT_NOT_IN_PULLBACK_ZONE"
            return
        # A prefetch is only a cache warm-up.  The formal pre-buy gate always
        # performs a fresh full audit before READY_TO_BUY/Quote.
        audit_state, audit_reason = self._request_audit(candidate, now, True)
        if audit_state != "PASS" and (self.config.require_audit_fields or audit_state == "FAIL"):
            candidate.last_rejection = audit_reason
            candidate.ready_to_buy = False
            return
        # Test/replay engines without a Logs RPC retain the existing local
        # flow gate; production BSC instances with an authoritative Logs RPC
        # must prove a complete recent window before allowing a buy.
        logs_configured = bool(
            getattr(self, "resolver", None) is not None
            and getattr(getattr(getattr(self, "resolver", None), "logs_rpc", None), "configured", False)
        )
        if logs_configured and not self._flow_window_complete(candidate, now, 60):
            candidate.last_rejection = "FLOW_DATA_UNVERIFIED"
            candidate.ready_to_buy = False
            return
        volume_buy, volume_sell, count_buy, count_sell = self._flow_1m(candidate, now)
        if volume_sell <= 0 or volume_buy / volume_sell < self.config.buy_sell_volume_ratio or count_sell <= 0 or Decimal(count_buy) / Decimal(count_sell) < self.config.buy_sell_count_ratio or count_buy + count_sell < self.config.min_swap_count:
            candidate.last_rejection = "BUY_FLOW_GATE_1M"
            candidate.ready_to_buy = False
            return
        candidate.ready_to_buy = True
        candidate.state = "READY_TO_BUY"
        self._try_buy(candidate, now)

    def _universe_reason(self, candidate: SurvivorCandidate, now: datetime) -> str | None:
        age_seconds = self._age_seconds(candidate, now)
        if age_seconds < self.config.min_age_sec:
            return f"AGE_BELOW_{self.config.min_age_sec}S"
        if self.config.max_age_sec is not None and age_seconds > self.config.max_age_sec:
            return f"AGE_ABOVE_{self.config.max_age_sec}S"
        if candidate.market_cap_usd is None or candidate.market_cap_usd < self.config.universe_min_mc_usd or (
            self.config.universe_max_mc_usd is not None
            and candidate.market_cap_usd > self.config.universe_max_mc_usd
        ):
            if self.config.identity == BALANCED_SURVIVOR_REVERSAL_IDENTITY:
                return "MC_BELOW_5K"
            return "MC_OUTSIDE_300K_2M"
        if candidate.liquidity_usd is None or candidate.liquidity_usd < self.config.universe_min_liquidity_usd:
            if self.config.identity == BALANCED_SURVIVOR_REVERSAL_IDENTITY:
                return "LIQUIDITY_BELOW_1K"
            return "LIQUIDITY_BELOW_50K"
        if candidate.market_cap_usd <= 0 or candidate.liquidity_usd / candidate.market_cap_usd * 100 < self.config.min_lp_mc_pct:
            return "LP_MC_BELOW_8_PERCENT"
        if candidate.holders is None:
            return "HOLDERS_REQUIRED_BEFORE_BUY"
        if candidate.holders < self.config.universe_min_holders:
            return f"HOLDERS_BELOW_{self.config.universe_min_holders}"
        if self.config.universe_max_holders is not None and candidate.holders > self.config.universe_max_holders:
            return f"HOLDERS_ABOVE_{self.config.universe_max_holders}"
        if candidate.current_price_usd is None:
            return "PRICE_REQUIRED_BEFORE_BUY"
        if self.config.min_entry_price_usd is not None and candidate.current_price_usd < self.config.min_entry_price_usd:
            return f"PRICE_BELOW_{str(self.config.min_entry_price_usd).replace('.', '_')}"
        if self.config.max_entry_price_usd is not None and candidate.current_price_usd > self.config.max_entry_price_usd:
            return self._price_above_entry_ceiling_reason()
        return None

    def _audit_result(self, candidate: SurvivorCandidate) -> tuple[str, str]:
        self._audit_checks += 1
        return self._audit_result_for_record(candidate.record)

    def _audit_result_for_record(self, record: BinanceNormalizedSignal | None) -> tuple[str, str]:
        if record is None:
            return "UNKNOWN", "AUDIT_UNAVAILABLE"
        values: dict[str, Any] = {}
        for key in ("risk_level", "honeypot", "dev_percent", "insider_percent", "sniper_percent", "top10_percent", "tax_rate_buy", "tax_rate_sell"):
            value = _field(record, key)
            if value is not None:
                values[key] = value
        raw = _field(record, "audit_info_json")
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                raw = None
        if isinstance(raw, Mapping):
            normalized = {"".join(ch for ch in str(key).lower() if ch.isalnum()): value for key, value in raw.items()}
            aliases = {
                "risk_level": ("risklevel", "risk"), "honeypot": ("honeypot",),
                "dev_percent": ("devpercent", "developerpercent"), "insider_percent": ("insiderpercent",),
                "sniper_percent": ("sniperpercent",), "top10_percent": ("top10percent", "top10holderspercent"),
                "tax_rate_buy": ("taxratebuy", "buytax"), "tax_rate_sell": ("taxratesell", "selltax"),
            }
            for name, keys in aliases.items():
                if name not in values:
                    for key in keys:
                        if key in normalized:
                            values[name] = normalized[key]
                            break
        required = ("risk_level", "honeypot", "dev_percent", "insider_percent", "sniper_percent", "tax_rate_buy", "tax_rate_sell")
        if any(name not in values for name in required):
            return "UNKNOWN", "AUDIT_FIELDS_MISSING"
        if str(values["risk_level"]).lower() == "high":
            return "FAIL", "AUDIT_RISK_HIGH"
        if str(values["honeypot"]).lower() in {"true", "1", "yes"}:
            return "FAIL", "AUDIT_HONEYPOT"
        limits = (("tax_rate_buy", self.config.max_tax_pct), ("tax_rate_sell", self.config.max_tax_pct), ("dev_percent", self.config.max_dev_pct), ("insider_percent", self.config.max_insider_pct), ("sniper_percent", self.config.max_sniper_pct))
        for name, maximum in limits:
            value = _decimal(values[name])
            if value is None:
                return "UNKNOWN", "AUDIT_FIELD_INVALID"
            if value > maximum:
                return "FAIL", f"AUDIT_{name.upper()}_ABOVE_LIMIT"
        top10 = _decimal(values.get("top10_percent"))
        if top10 is not None and top10 > self.config.max_top10_pct:
            return "FAIL", "AUDIT_TOP10_ABOVE_30_PERCENT"
        return "PASS", "AUDIT_PASS"

    @staticmethod
    def _audit_timestamp(candidate: SurvivorCandidate, key: str) -> datetime | None:
        raw = candidate.source_status.get(key)
        if not raw:
            return None
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            return None

    def _audit_age_seconds(self, candidate: SurvivorCandidate, now: datetime) -> int | None:
        completed_at = self._audit_timestamp(candidate, "audit_completed_at")
        if completed_at is None:
            return None
        return max(0, int((now - completed_at).total_seconds()))

    def _audit_cache_is_fresh(self, candidate: SurvivorCandidate, now: datetime) -> bool:
        completed_at = self._audit_timestamp(candidate, "audit_completed_at")
        return (
            candidate.audit_state == "VALID"
            and completed_at is not None
            and (now - completed_at).total_seconds() < AUDIT_PREFETCH_TTL_SECONDS
        )

    def _audit_prefetch_is_in_cooldown(self, candidate: SurvivorCandidate, now: datetime) -> bool:
        completed_at = self._audit_timestamp(candidate, "audit_completed_at")
        return completed_at is not None and (now - completed_at).total_seconds() < AUDIT_PREFETCH_TTL_SECONDS

    def _maybe_schedule_audit_prefetch(self, candidate: SurvivorCandidate, now: datetime) -> None:
        """Warm the existing Meme Rush audit fields without blocking WSS."""

        if not candidate.candidate_eligible or candidate.drawdown_pct is None:
            return
        if candidate.drawdown_pct > Decimal("-25") or not self._history_allows_pullback(candidate):
            return
        if (
            self._audit_cache_is_fresh(candidate, now)
            or self._audit_prefetch_is_in_cooldown(candidate, now)
            or candidate.mint in self._audit_prefetch_pending
        ):
            return
        record = candidate.record
        self._audit_prefetch_pending.add(candidate.mint)
        candidate.audit_requested = True
        candidate.audit_failed = False
        candidate.audit_state = "PENDING"
        candidate.source_status.update({
            "audit_prefetch_at": now.isoformat(),
            "audit_status": "PENDING",
            "audit_source": "BINANCE_MEME_RUSH_AUDIT_FIELDS",
            "audit_age_seconds": "0",
        })
        self._audit_requested_total += 1
        future = self._audit_prefetch_executor.submit(self._audit_result_for_record, record)
        future.add_done_callback(
            lambda completed, mint=candidate.mint: self._complete_audit_prefetch(mint, completed)
        )

    def _complete_audit_prefetch(self, mint: str, future: object) -> None:
        """Worker callback: enqueue only; SQLite is owned by the main loop."""
        now = self.clock()
        try:
            state, reason = future.result()  # type: ignore[attr-defined]
        except Exception:
            state, reason = "UNKNOWN", "AUDIT_PREFETCH_FAILED"
        with self._audit_result_lock:
            self._completed_audit_prefetches.append((mint, state, reason, now))

    def _drain_audit_prefetch_results(self, now: datetime) -> None:
        """Persist completed audit results on the strategy main thread only."""
        results: list[tuple[str, str, str, datetime]] = []
        with self._audit_result_lock:
            while self._completed_audit_prefetches:
                results.append(self._completed_audit_prefetches.popleft())
        wrote = False
        for mint, state, reason, completed_at in results:
            self._audit_prefetch_pending.discard(mint)
            candidate = self._candidates.get(mint)
            if candidate is None:
                continue
            candidate.audit_state = "VALID" if state == "PASS" else ("SOURCE_UNAVAILABLE" if state == "UNKNOWN" else "INVALID")
            candidate.audit_failed = state != "PASS"
            candidate.source_status.update({
                "audit_completed_at": completed_at.isoformat(),
                "audit_status": candidate.audit_state,
                "audit_age_seconds": "0",
                "audit_result": reason,
            })
            if candidate.audit_failed:
                self._audit_failed_total += 1
            try:
                self._persist_candidate(candidate, completed_at)
                wrote = True
            except Exception as exc:
                self._db_write_error_count += 1
                if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                    self._db_locked_error_count += 1
                continue
            self._audit_event("SURVIVOR_AUDIT_PREFETCH_COMPLETED", candidate, {
                "status": candidate.audit_state,
                "reason": reason,
                "audit_age_seconds": 0,
            })
        if wrote:
            try:
                self.connection.commit()
                self._last_db_write_at = now
            except Exception as exc:
                self._db_commit_error_count += 1
                self._db_write_error_count += 1
                if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                    self._db_locked_error_count += 1

    def _request_audit(self, candidate: SurvivorCandidate, now: datetime, force: bool = False) -> tuple[str, str]:
        """Audit only a pullback/pre-buy candidate, never the Layer A universe."""
        attempted_at = self._audit_attempt_at.get(candidate.mint)
        if not force and attempted_at is not None and now - attempted_at < timedelta(seconds=60):
            if candidate.audit_state == "VALID":
                return "PASS", "AUDIT_PASS"
            return "UNKNOWN", candidate.last_rejection or "AUDIT_FIELDS_MISSING"
        self._audit_attempt_at[candidate.mint] = now
        candidate.audit_requested = True
        candidate.audit_state = "PENDING"
        self._audit_requested_total += 1
        state, reason = self._audit_result(candidate)
        if state == "PASS":
            candidate.audit_state = "VALID"
            candidate.audit_failed = False
            candidate.source_status.update({
                "audit_completed_at": now.isoformat(),
                "audit_status": "VALID",
                "audit_age_seconds": "0",
                "audit_result": reason,
            })
            return state, reason
        candidate.audit_state = "SOURCE_UNAVAILABLE" if state == "UNKNOWN" else "INVALID"
        candidate.audit_failed = True
        self._audit_failed_total += 1
        candidate.source_status.update({
            "audit_completed_at": now.isoformat(),
            "audit_status": candidate.audit_state,
            "audit_age_seconds": "0",
            "audit_result": reason,
        })
        # Binance does not currently provide a reliable creator_sold field in
        # this path. UNKNOWN is recorded and is deliberately non-blocking; the
        # actual audit limits above remain fail-closed.
        return state, reason

    def _flow_1m(self, candidate: SurvivorCandidate, now: datetime) -> tuple[Decimal, Decimal, int, int]:
        cutoff = now - timedelta(seconds=60)
        samples = [sample for sample in candidate.flows if sample.observed_at >= cutoff]
        return (sum((sample.buy_volume_bnb for sample in samples), Decimal("0")), sum((sample.sell_volume_bnb for sample in samples), Decimal("0")), sum(sample.buy_count for sample in samples), sum(sample.sell_count for sample in samples))

    def _try_buy(self, candidate: SurvivorCandidate, now: datetime) -> None:
        if self.controls.paused("paper"):
            candidate.last_rejection = "ENTRY_PAUSED"
            return
        max_open_positions = max(1, int(getattr(self.config, "max_open_positions", 1)))
        open_positions = sum(position.status == "OPEN" for position in self._positions.values())
        if open_positions >= max_open_positions:
            candidate.last_rejection = "MAX_OPEN_POSITIONS_REACHED"
            return
        # The cooldown below starts after a completed exit, so it does not
        # protect against repeated entries while the first position is still
        # open. Enforce one live Paper position per token first.
        same_token_open = any(
            position.status == "OPEN"
            and getattr(position, "mint", None) is not None
            and self._mint_key(position.mint) == self._mint_key(candidate.mint)
            for position in getattr(self, "_positions", {}).values()
        )
        if not same_token_open:
            try:
                same_token_open = self.connection.execute(
                    "SELECT 1 FROM survivor_positions WHERE lower(mint)=lower(?) AND status='OPEN' LIMIT 1",
                    (candidate.mint,),
                ).fetchone() is not None
            except (AttributeError, sqlite3.OperationalError):
                same_token_open = False
        if same_token_open:
            candidate.last_rejection = "SAME_TOKEN_ALREADY_OPEN"
            candidate.source_status["same_token_open"] = True
            return
        if self._same_token_cooldown_active(candidate, now):
            return
        if self.config.one_trade_per_day:
            today = now.date().isoformat()
            row = self.connection.execute("SELECT 1 FROM survivor_positions WHERE opened_at LIKE ? LIMIT 1", (today + "%",)).fetchone()
            if row is not None:
                candidate.last_rejection = "ONE_TRADE_PER_DAY"
                return
        # Some deterministic tests construct a minimal engine without calling
        # __init__; retain the original provider in that compatibility path.
        entry_provider = getattr(self, "entry_quote_provider", self.quote_provider)
        if entry_provider is None or (
            candidate.current_price_native is None and not self.config.candidate_is_entry
        ):
            candidate.last_rejection = "QUOTE_GATE_FAILED"
            candidate.quote_state = "NOT_REQUESTED" if entry_provider is None else "SOURCE_UNAVAILABLE"
            candidate.source_status.update({"quote_gate": "QUOTE_GATE_FAILED", "paper_buy": "PAPER_BUY_SKIPPED"})
            return
        candidate.quote_requested = True
        candidate.quote_state = "PENDING"
        self._quote_requested_total += 1
        buy, sell, error = entry_provider.quote_candidate(candidate.mint, self.config.position_size_bnb, {})
        if error == "QUOTE_PENDING":
            candidate.last_rejection = "QUOTE_PENDING"
            candidate.quote_state = "PENDING"
            candidate.source_status.update({"quote_gate": "QUOTE_PENDING", "paper_buy": "PAPER_BUY_PENDING"})
            return
        usable = buy is not None and sell is not None and buy.output_quantity > 0 and sell.output_quantity > 0 and self._mint_key(buy.mint) == candidate.mint and self._mint_key(sell.mint) == candidate.mint
        if not usable:
            candidate.last_rejection = "QUOTE_GATE_FAILED"
            candidate.quote_state = "SOURCE_UNAVAILABLE" if error else "INVALID"
            candidate.source_status.update({
                "quote_gate": "QUOTE_GATE_FAILED",
                "paper_buy": "PAPER_BUY_SKIPPED",
                "quote_error": error or "BUY_OR_SELL_QUOTE_UNAVAILABLE",
            })
            self._audit_event("SURVIVOR_BUY_QUOTE_UNAVAILABLE", candidate, {"error": error or "BUY_OR_SELL_QUOTE_UNAVAILABLE"})
            return
        candidate.quote_state = "VALID"
        entry_price = buy.input_quantity / buy.output_quantity
        # A Candidate-direct profile can legitimately use a bonding-curve or
        # router quote before an independent WSS mark exists.  The executable
        # entry quote is the only safe initial Paper mark in that case.
        if candidate.current_price_native is None:
            candidate.current_price_native = entry_price
        position = SurvivorPosition(
            position_id=f"survivor:{candidate.mint}:{uuid4().hex[:12]}", mint=candidate.mint,
            symbol=candidate.symbol, opened_at=now, entry_price_native=entry_price,
            current_price_native=candidate.current_price_native, quantity_token=buy.output_quantity,
            remaining_quantity_token=buy.output_quantity, invested_bnb=buy.input_quantity,
        )
        self._positions[position.position_id] = position
        # Freeze the entry observation with the Paper fill.  The candidate
        # row remains live and will change on every source update, so it must
        # never be used later to render an entry-time market snapshot.
        self.connection.execute(
            "INSERT INTO survivor_positions("
            "position_id,mint,symbol,opened_at,status,entry_price_native,entry_price_usd,"
            "current_price_native,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,"
            "last_trade_at,quote_source,updated_at,entry_holders,entry_market_cap_usd,entry_liquidity_usd"
            ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                position.position_id, position.mint, position.symbol, now.isoformat(), position.status,
                str(position.entry_price_native),
                str(candidate.current_price_usd) if candidate.current_price_usd is not None else None,
                str(position.current_price_native), str(position.quantity_token), str(position.remaining_quantity_token),
                str(position.invested_bnb), "0", now.isoformat(), buy.quote_source or buy.provider, now.isoformat(),
                candidate.holders,
                str(candidate.market_cap_usd) if candidate.market_cap_usd is not None else None,
                str(candidate.liquidity_usd) if candidate.liquidity_usd is not None else None,
            ),
        )
        self.connection.commit()
        candidate.state = "POSITION_OPEN"
        candidate.paper_buy = True
        self._paper_buy_total += 1
        self._audit_event("SURVIVOR_PAPER_ENTRY", candidate, {"position_id": position.position_id, "quote_source": buy.quote_source or buy.provider, "executable_quote": True})

    def _position_mark_usd(
        self,
        native_price: Decimal | None,
        candidate: SurvivorCandidate | None,
    ) -> Decimal | None:
        """Convert a BNB-denominated mark only when a fresh native USD rate exists."""

        if native_price is None or native_price <= 0:
            return None
        native_usd = (
            candidate.native_token_price_usd
            if candidate is not None and candidate.native_token_price_usd is not None
            else getattr(self, "_latest_native_token_price_usd", None)
        )
        return native_price * native_usd if native_usd is not None and native_usd > 0 else None

    def _same_token_cooldown_active(self, candidate: SurvivorCandidate, now: datetime) -> bool:
        """Return whether a recently closed position still blocks this mint."""

        cooldown_sec = int(getattr(self.config, "same_token_cooldown_sec", 0) or 0)
        if cooldown_sec <= 0:
            return False
        try:
            row = self.connection.execute(
                "SELECT closed_at FROM survivor_positions "
                "WHERE lower(mint)=lower(?) AND status='CLOSED' AND closed_at IS NOT NULL "
                "ORDER BY closed_at DESC LIMIT 1",
                (candidate.mint,),
            ).fetchone()
        except sqlite3.OperationalError:
            # Keep minimal deterministic engines that omit the full runtime
            # schema compatible; production Balanced always has closed_at.
            row = None
        if row is None:
            return False
        closed_at_raw = row[0] if len(row) else None
        try:
            closed_at = datetime.fromisoformat(str(closed_at_raw))
            if closed_at.tzinfo is None:
                closed_at = closed_at.replace(tzinfo=timezone.utc)
            cooldown_until = closed_at + timedelta(seconds=cooldown_sec)
        except (TypeError, ValueError):
            return False
        if now >= cooldown_until:
            return False
        candidate.last_rejection = "SAME_TOKEN_COOLDOWN"
        candidate.source_status["same_token_cooldown_until"] = cooldown_until.isoformat()
        return True

    def _candidate_position_mark(
        self,
        candidate: SurvivorCandidate,
        now: datetime | None = None,
    ) -> tuple[Decimal | None, Decimal | None]:
        """Return the Candidate mark in the Position's native denomination.

        Paper positions enter with the Candidate's native venue denomination,
        so the base engine may use its live venue mark directly.  Live
        execution overrides this for non-BNB quote assets.
        """

        native_price = candidate.current_price_native
        if native_price is None or native_price <= 0:
            return None, None
        return native_price, self._position_mark_usd(native_price, candidate)

    def _set_position_mark(
        self,
        position: SurvivorPosition,
        *,
        native_price: Decimal | None,
        usd_price: Decimal | None,
        source: str,
        observed_at: datetime,
    ) -> bool:
        """Apply an independently sourced open-position mark in the owner loop."""

        if native_price is not None and native_price <= 0:
            native_price = None
        if usd_price is not None and usd_price <= 0:
            usd_price = None
        if native_price is None and usd_price is None:
            return False
        changed = (
            position.position_mark_price_native != native_price
            or position.position_mark_price_usd != usd_price
            or position.position_mark_source != source
            or position.position_price_freshness != "FRESH"
        )
        if native_price is not None:
            # Existing TP/SL logic uses this native BNB/token mark. It is now
            # explicitly a position mark rather than a Candidate lifecycle mark.
            position.current_price_native = native_price
            position.position_mark_price_native = native_price
        if usd_price is not None:
            position.position_mark_price_usd = usd_price
        position.position_mark_source = source
        position.position_price_updated_at = observed_at
        position.position_price_freshness = "FRESH"
        if changed:
            getattr(self, "_position_mark_dirty", set()).add(position.position_id)
        return changed

    def _hard_stop_is_confirmed(self, position: SurvivorPosition, pnl: Decimal) -> bool:
        """Return whether a stop mark is sufficient to commit a hard-stop exit.

        Paper marks are executable quotes, so their normal mark is enough.
        Live execution overrides this: a venue/WSS observation may wake an
        urgent sell-quote check, but must not by itself lock an irreversible
        HARD_STOP intent.
        """

        return pnl <= -self.config.hard_stop_pct

    def _evaluate_positions(self, now: datetime) -> None:
        for position in tuple(self._positions.values()):
            if position.status != "OPEN":
                continue
            # A pending TP intent is an execution concern, not a price-state
            # gate: keep consuming fresh marks so TP2/TP3 crossings are never
            # lost while TP1 is waiting for the provider.  Non-TP exits keep
            # their existing immediate priority.
            if position.exit_intent_reason is not None and not position.exit_intent_reason.startswith("TP"):
                if position.exit_intent_quantity is not None and position.exit_intent_quantity < position.remaining_quantity_token:
                    self._partial_exit(position, position.exit_intent_quantity / position.remaining_quantity_token, position.exit_intent_reason, now)
                else:
                    self._close_position(position, position.exit_intent_reason, now)
                continue
            candidate = self._candidates.get(position.mint)
            dirty_marks = getattr(self, "_position_mark_dirty", set())
            mark_changed = position.position_id in dirty_marks
            if candidate is not None:
                candidate_native, candidate_usd = self._candidate_position_mark(candidate, now)
            else:
                candidate_native, candidate_usd = (None, None)
            if candidate_native is not None:
                mark_changed = self._set_position_mark(
                    position,
                    native_price=candidate_native,
                    usd_price=candidate_usd,
                    source=candidate.price_source or "VENUE_WSS",
                    observed_at=candidate.price_updated_at or now,
                ) or mark_changed
            last_trade_at = position.last_trade_at or position.opened_at
            if candidate is not None and candidate.descriptor is not None:
                marker_lock = getattr(self, "_wss_trade_markers_lock", None)
                markers = getattr(self, "_wss_trade_markers", {})
                marker_address = candidate.descriptor.address
                if marker_lock is None:
                    marker = markers.get(marker_address.lower())
                else:
                    with marker_lock:
                        marker = markers.get(marker_address.lower())
                if marker is not None and marker > last_trade_at:
                    position.last_trade_at = marker
                    last_trade_at = marker
                    self._persist_trade_activity(position, marker)
            if position.current_price_native is None or position.entry_price_native <= 0:
                if mark_changed:
                    self._persist_position(position, now, reason=None)
                continue
            pnl = (position.current_price_native / position.entry_price_native - Decimal("1")) * Decimal("100")
            if self._no_trade_monitoring_ready(candidate) and (now - last_trade_at).total_seconds() >= self.config.no_trade_exit_sec:
                # A quiet local WSS queue is not chain evidence. Only a
                # complete, recent Logs-RPC reconciliation may confirm the
                # negative condition. The confirmed action is PnL-aware:
                # below +10% closes all; above +10% sells half once.
                if not self._flow_data_completeness_ready(candidate, now):
                    candidate.last_rejection = (
                        "FLOW_DATA_UNVERIFIED"
                        if candidate.source_status.get("no_trade_state") == self._no_trade_reason("UNVERIFIED")
                        else "FLOW_DATA_PENDING"
                    )
                    continue
                if pnl < Decimal("10"):
                    reason = self._no_trade_reason("PNL_LT_10")
                    candidate.source_status["no_trade_state"] = self._no_trade_reason("PNL_LT_10_FULL_EXIT")
                    self._record_exit_trigger(position, reason, now, pnl=pnl)
                    self._close_position(position, reason, now)
                    continue
                if pnl > Decimal("10") and not position.no_trade_profit_partial_done:
                    reason = self._no_trade_reason("PNL_GT_10_PARTIAL")
                    candidate.source_status["no_trade_state"] = self._no_trade_reason("PNL_GT_10_PARTIAL_EXIT")
                    self._record_exit_trigger(position, reason, now, pnl=pnl)
                    # Live execution keeps its exit intent until an order is
                    # confirmed. Paper execution marks this done atomically
                    # with the simulated fill below.
                    self._partial_exit(position, Decimal("0.50"), reason, now)
                    continue
                candidate.source_status["no_trade_state"] = (
                    self._no_trade_reason("PNL_GT_10_PARTIAL_DONE")
                    if position.no_trade_profit_partial_done
                    else self._no_trade_reason("PNL_EQ_10_HOLD")
                )
            samples = list(candidate.flows) if candidate else []
            recent = [sample for sample in samples if sample.observed_at >= now - timedelta(seconds=120)]
            liq_values = [sample.liquidity_native for sample in recent if sample.liquidity_native is not None]
            # TP1-to-TP2 protection tracks its own high, and begins only
            # after the TP1 SELL is confirmed.  TP2 retains its existing,
            # independent trailing-high behavior.
            if position.tp1 and not position.tp2 and position.current_price_native > (position.high_since_tp1_native or Decimal("0")):
                position.high_since_tp1_native = position.current_price_native
            reason: str | None = None
            if self._hard_stop_is_confirmed(position, pnl):
                reason = "HARD_STOP_PNL"
            elif position.tp2 and position.high_after_tp2 and position.current_price_native <= position.high_after_tp2 * Decimal("0.75"):
                reason = "TRAILING_25_PERCENT"
            elif position.tp1 and not position.tp2 and position.high_since_tp1_native and position.current_price_native <= position.high_since_tp1_native * Decimal("0.65"):
                reason = "TP1_HIGH_RETRACE_35_PERCENT"
            elif position.tp1 and position.current_price_native <= position.entry_price_native * Decimal("0.99"):
                reason = "TP1_BREAK_EVEN"
            elif len(liq_values) >= 2 and liq_values[-1] <= liq_values[0] * (Decimal("1") - self.config.liquidity_drop_pct / Decimal("100")):
                reason = "EMERGENCY_LIQUIDITY_DROP"
            elif len(recent) >= 2:
                one_min = [sample for sample in recent if sample.observed_at >= now - timedelta(seconds=60)]
                buy, sell = sum((s.buy_volume_bnb for s in one_min), Decimal("0")), sum((s.sell_volume_bnb for s in one_min), Decimal("0"))
                prices = [s.price_native for s in one_min if s.price_native is not None]
                if len(prices) >= 2 and prices[-1] <= prices[0] * (Decimal("1") - self.config.dump_price_pct / Decimal("100")) and sell > buy * self.config.dump_flow_ratio:
                    reason = "EMERGENCY_DUMP"
            if reason is None and (now - position.opened_at).total_seconds() >= self.config.time_stop_sec and pnl < Decimal("10"):
                reason = "TIME_STOP"
            if reason is not None:
                self._record_exit_trigger(position, reason, now, pnl=pnl)
                self._close_position(position, reason, now)
                continue
            # Latch every crossed TP from a fresh position mark before any
            # quote/executor work.  A pending TP1 must never hide a TP2/TP3
            # crossing that occurred in the same or a later price update.
            latched = self._latch_tp_triggers(position, now, pnl) if mark_changed else False
            persisted_by_exit = self._execute_latched_tp(position, now)
            if position.tp2 and position.current_price_native > (position.high_after_tp2 or Decimal("0")):
                position.high_after_tp2 = position.current_price_native
            if (mark_changed or latched) and not persisted_by_exit:
                self._persist_position(position, now, reason=None)

    _TP_STAGES = (
        ("tp1", Decimal("50"), "TP1_PLUS_50", Decimal("0.30")),
        ("tp2", Decimal("100"), "TP2_PLUS_100", Decimal("0.30")),
        ("tp3", Decimal("200"), "TP3_PLUS_200", None),
    )

    def _latch_tp_triggers(self, position: SurvivorPosition, now: datetime, pnl: Decimal) -> bool:
        """Persist all historical TP crossings; never tie this to a fill."""

        changed = False
        for stage, threshold, _reason, _fraction in self._TP_STAGES:
            if pnl < threshold or getattr(position, f"{stage}_triggered"):
                continue
            setattr(position, f"{stage}_triggered", True)
            setattr(position, f"{stage}_triggered_at", now)
            setattr(position, f"{stage}_trigger_price_native", position.current_price_native)
            changed = True
        return changed

    def _next_latched_tp(self, position: SurvivorPosition) -> tuple[str, Decimal | None] | None:
        """Return the next unfilled stage in original TP1 -> TP2 -> TP3 order."""

        for stage, _threshold, reason, fraction in self._TP_STAGES:
            if getattr(position, f"{stage}_triggered") and not getattr(position, f"{stage}_filled"):
                return reason, fraction
        return None

    def _execute_latched_tp(self, position: SurvivorPosition, now: datetime) -> bool:
        """Execute at most one latched stage; subclasses retain single-flight IO."""

        next_stage = self._next_latched_tp(position)
        if next_stage is None:
            return False
        reason, fraction = next_stage
        self._record_exit_trigger(position, reason, now)
        if fraction is None:
            self._close_position(position, reason, now)
            return True
        return self._partial_exit(position, fraction, reason, now)

    def _mark_tp_filled(self, position: SurvivorPosition, reason: str, now: datetime) -> None:
        """Apply fill-driven risk state only after a real/simulated sell fills."""

        stage = "tp1" if reason.startswith("TP1") else ("tp2" if reason.startswith("TP2") else ("tp3" if reason.startswith("TP3") else None))
        if stage is None:
            return
        setattr(position, f"{stage}_filled", True)
        if getattr(position, f"{stage}_filled_at") is None:
            setattr(position, f"{stage}_filled_at", now)
        if stage == "tp1":
            position.tp1 = True
            position.high_since_tp1_native = position.current_price_native
        elif stage == "tp2":
            position.tp2 = True
            position.trailing_active = True
            position.high_after_tp2 = position.current_price_native

    def _current_mark_pnl_pct(self, position: SurvivorPosition) -> Decimal | None:
        if position.entry_price_native <= 0 or position.current_price_native is None or position.current_price_native <= 0:
            return None
        return (position.current_price_native / position.entry_price_native - Decimal("1")) * Decimal("100")

    def _record_exit_trigger(
        self,
        position: SurvivorPosition,
        reason: str,
        now: datetime,
        *,
        pnl: Decimal | None = None,
    ) -> None:
        """Persist the latest trigger once, without changing exit behavior.

        The same intent is retried while a provider is unavailable.  Do not
        move its timestamp on every retry: that would make the dashboard look
        as if the trigger happened at the eventual fill time.
        """

        if position.exit_trigger_reason == reason and position.exit_triggered_at is not None:
            return
        position.exit_trigger_reason = reason
        position.exit_trigger_pnl_pct = pnl if pnl is not None else self._current_mark_pnl_pct(position)
        position.exit_trigger_price_native = position.current_price_native
        position.exit_triggered_at = now
        try:
            self.connection.execute(
                "UPDATE survivor_positions SET exit_trigger_reason=?,exit_trigger_pnl_pct=?,"
                "exit_trigger_price_native=?,exit_triggered_at=?,updated_at=? WHERE position_id=?",
                (
                    reason,
                    str(position.exit_trigger_pnl_pct) if position.exit_trigger_pnl_pct is not None else None,
                    str(position.exit_trigger_price_native) if position.exit_trigger_price_native is not None else None,
                    now.isoformat(),
                    now.isoformat(),
                    position.position_id,
                ),
            )
            self.connection.commit()
        except Exception:
            # The owner loop's normal write path will retry persistence.  A
            # trigger record must never make the strategy stop evaluating.
            try:
                self.connection.rollback()
            except Exception:
                pass

    def _partial_exit(self, position: SurvivorPosition, fraction: Decimal, reason: str, now: datetime) -> bool:
        self._record_exit_trigger(position, reason, now)
        quantity = position.remaining_quantity_token * fraction
        provider = getattr(self, "exit_quote_provider", self.quote_provider)
        if hasattr(provider, "sell_quote"):
            quote, error = provider.sell_quote(position.mint, quantity)
        else:
            quote, error = (provider.quote(position.mint, "sell", quantity) if provider is not None else None), None
        if quote is None or quote.output_quantity <= 0:
            self._set_exit_intent(position, reason, quantity, now, error)
            self._audit_event("SURVIVOR_EXIT_QUOTE_UNAVAILABLE", self._candidates.get(position.mint), {"position_id": position.position_id, "reason": reason})
            return False
        position.remaining_quantity_token -= quantity
        position.realized_bnb += quote.output_quantity
        self._mark_tp_filled(position, reason, now)
        if reason.endswith("_PNL_GT_10_PARTIAL") and reason.startswith("NO_TRADE_"):
            position.no_trade_profit_partial_done = True
        self._persist_position(position, now, reason=None)
        self._clear_exit_intent(position)
        self._audit_event("SURVIVOR_PARTIAL_EXIT", self._candidates.get(position.mint), {
            "position_id": position.position_id,
            "reason": reason,
            "trigger_reason": position.exit_trigger_reason,
            "trigger_pnl_pct": str(position.exit_trigger_pnl_pct) if position.exit_trigger_pnl_pct is not None else None,
            "trigger_price_native": str(position.exit_trigger_price_native) if position.exit_trigger_price_native is not None else None,
            "triggered_at": _iso(position.exit_triggered_at),
            "sell_quote_id": quote.quote_id,
            "sell_input_quantity": str(quote.input_quantity),
            "sell_output_bnb": str(quote.output_quantity),
            "quote_source": quote.quote_source or quote.provider,
            "executable_quote": True,
        })
        return True

    def _close_position(self, position: SurvivorPosition, reason: str, now: datetime) -> None:
        self._record_exit_trigger(position, reason, now)
        provider = getattr(self, "exit_quote_provider", self.quote_provider)
        if hasattr(provider, "sell_quote"):
            quote, error = provider.sell_quote(position.mint, position.remaining_quantity_token)
        else:
            quote, error = (provider.quote(position.mint, "sell", position.remaining_quantity_token) if provider is not None else None), None
        if quote is None or quote.output_quantity <= 0:
            self._set_exit_intent(position, reason, position.remaining_quantity_token, now, error)
            self._audit_event("SURVIVOR_EXIT_QUOTE_UNAVAILABLE", self._candidates.get(position.mint), {"position_id": position.position_id, "reason": reason})
            return
        position.realized_bnb += quote.output_quantity
        position.remaining_quantity_token = Decimal("0")
        position.status = "CLOSED"
        self._mark_tp_filled(position, reason, now)
        # The executable sell quote is the authoritative native exit price.
        # Keep the contemporaneous source observation separately as the USD,
        # holders, market-cap and liquidity exit snapshot below.
        position.current_price_native = quote.output_quantity / quote.input_quantity
        self._clear_exit_intent(position)
        self._persist_position(position, now, reason=reason)
        if self._candidates.get(position.mint) is not None:
            self._candidates[position.mint].state = "POSITION_CLOSED"
        self._audit_event("SURVIVOR_PAPER_EXIT", self._candidates.get(position.mint), {
            "position_id": position.position_id,
            "reason": reason,
            "trigger_reason": position.exit_trigger_reason,
            "trigger_pnl_pct": str(position.exit_trigger_pnl_pct) if position.exit_trigger_pnl_pct is not None else None,
            "trigger_price_native": str(position.exit_trigger_price_native) if position.exit_trigger_price_native is not None else None,
            "triggered_at": _iso(position.exit_triggered_at),
            "sell_quote_id": quote.quote_id,
            "sell_input_quantity": str(quote.input_quantity),
            "sell_output_bnb": str(quote.output_quantity),
            "quote_source": quote.quote_source or quote.provider,
            "realized_pnl_pct": str(self._position_pnl_pct(position)),
            "executable_quote": True,
        })

    def _set_exit_intent(self, position: SurvivorPosition, reason: str, quantity: Decimal, now: datetime, error: str | None) -> None:
        self._record_exit_trigger(position, reason, now)
        position.exit_intent_reason = reason
        position.exit_intent_quantity = quantity
        store = getattr(self, "store", None)
        if store is not None:
            store.set_state(f"exit_intent:{position.position_id}", {
                "reason": reason,
                "quantity": str(quantity),
                "at": now.isoformat(),
                "error": error,
                "state": "EXIT_TRIGGERED_WAITING_ROUTE",
                "trigger_reason": position.exit_trigger_reason,
                "trigger_pnl_pct": str(position.exit_trigger_pnl_pct) if position.exit_trigger_pnl_pct is not None else None,
                "trigger_price_native": str(position.exit_trigger_price_native) if position.exit_trigger_price_native is not None else None,
                "triggered_at": _iso(position.exit_triggered_at),
            })

    def _clear_exit_intent(self, position: SurvivorPosition) -> None:
        position.exit_intent_reason = None
        position.exit_intent_quantity = None
        store = getattr(self, "store", None)
        if store is not None:
            store.set_state(f"exit_intent:{position.position_id}", {"state": "COMPLETED"})

    def _persist_candidate(self, candidate: SurvivorCandidate, now: datetime) -> None:
        columns = (
            "mint,symbol,first_seen_at,last_seen_at,data_quality_cohort,token_created_at,discovery_delay_seconds,first_snapshot_at,first_lifecycle,first_rank_type,first_market_cap_usd,first_liquidity_usd,first_holders,first_price_usd,first_progress_pct,latest_lifecycle,latest_rank_type,latest_progress_pct,latest_migrate_status,emit_count,state,market_cap_usd,liquidity_usd,holders,current_price_native,ath_price_native,local_low_native,low_started_at,stable_since,pair_address,pool_type,last_rejection,updated_at,"
            "first_seen_price_usd,first_seen_price_source,current_price_usd,ath_price_usd,ath_at,drawdown_pct,age_seconds,price_status,price_source,price_updated_at,price_age_ms,candidate_eligible,active_candidate,audit_state,audit_requested,audit_failed,quote_state,quote_requested,ready_to_buy,paper_buy,native_token_price_usd,liquidity_current_usd,liquidity_1m_ago_usd,liquidity_2m_ago_usd,buy_volume_30s,sell_volume_30s,buy_volume_1m,sell_volume_1m,buy_volume_3m,sell_volume_3m,buy_count_1m,sell_count_1m,buy_sell_volume_ratio_1m,buy_sell_count_ratio_1m,price_change_1m,source_status_json,last_price_attempt_at,pre_candidate_watch,pre_candidate_rank,candidate_distance_score,first_price_at,first_price_delay_seconds,price_samples_before_candidate,price_coverage_before_candidate_seconds,ath_before_candidate,ath_before_candidate_price_usd,ath_before_candidate_at,price_history_quality,candidate_at,price_history_status,history_source,history_start_at,history_end_at,history_sample_count,history_interval,max_history_gap_seconds,canonical_strategy_price,canonical_price_source"
        )
        values = (
            candidate.mint, candidate.symbol, candidate.first_seen_at.isoformat(), candidate.last_seen_at.isoformat(), candidate.data_quality_cohort,
            _iso(candidate.token_created_at), str(candidate.discovery_delay_seconds) if candidate.discovery_delay_seconds is not None else None,
            _iso(candidate.first_snapshot_at), candidate.first_lifecycle, candidate.first_rank_type,
            str(candidate.first_market_cap_usd) if candidate.first_market_cap_usd is not None else None,
            str(candidate.first_liquidity_usd) if candidate.first_liquidity_usd is not None else None,
            candidate.first_holders, str(candidate.first_price_usd) if candidate.first_price_usd is not None else None,
            str(candidate.first_progress_pct) if candidate.first_progress_pct is not None else None,
            candidate.latest_lifecycle, candidate.latest_rank_type,
            str(candidate.latest_progress_pct) if candidate.latest_progress_pct is not None else None,
            candidate.latest_migrate_status, candidate.emit_count, candidate.state,
            str(candidate.market_cap_usd) if candidate.market_cap_usd is not None else None,
            str(candidate.liquidity_usd) if candidate.liquidity_usd is not None else None, candidate.holders,
            str(candidate.current_price_native) if candidate.current_price_native is not None else None,
            str(candidate.ath_price_native) if candidate.ath_price_native is not None else None,
            str(candidate.local_low_native) if candidate.local_low_native is not None else None,
            _iso(candidate.low_started_at), _iso(candidate.stable_since), candidate.pair_address, candidate.pool_type,
            candidate.last_rejection, now.isoformat(),
            str(candidate.first_seen_price_usd) if candidate.first_seen_price_usd is not None else None, candidate.first_seen_price_source,
            str(candidate.current_price_usd) if candidate.current_price_usd is not None else None,
            str(candidate.ath_price_usd) if candidate.ath_price_usd is not None else None, _iso(candidate.ath_at),
            str(candidate.drawdown_pct) if candidate.drawdown_pct is not None else None, candidate.age_seconds,
            candidate.price_status, candidate.price_source, _iso(candidate.price_updated_at), candidate.price_age_ms,
            int(candidate.candidate_eligible), int(candidate.active_candidate), candidate.audit_state, int(candidate.audit_requested), int(candidate.audit_failed),
            candidate.quote_state, int(candidate.quote_requested), int(candidate.ready_to_buy), int(candidate.paper_buy),
            str(candidate.native_token_price_usd) if candidate.native_token_price_usd is not None else None,
            str(candidate.liquidity_current_usd) if candidate.liquidity_current_usd is not None else None,
            str(candidate.liquidity_1m_ago_usd) if candidate.liquidity_1m_ago_usd is not None else None,
            str(candidate.liquidity_2m_ago_usd) if candidate.liquidity_2m_ago_usd is not None else None,
            str(candidate.buy_volume_30s), str(candidate.sell_volume_30s), str(candidate.buy_volume_1m), str(candidate.sell_volume_1m),
            str(candidate.buy_volume_3m), str(candidate.sell_volume_3m), candidate.buy_count_1m, candidate.sell_count_1m,
            str(candidate.buy_sell_volume_ratio_1m) if candidate.buy_sell_volume_ratio_1m is not None else None,
            str(candidate.buy_sell_count_ratio_1m) if candidate.buy_sell_count_ratio_1m is not None else None,
            str(candidate.price_change_1m) if candidate.price_change_1m is not None else None,
            json.dumps(candidate.source_status, sort_keys=True), _iso(candidate.last_price_attempt_at),
            int(candidate.pre_candidate_watch), candidate.pre_candidate_rank,
            str(candidate.candidate_distance_score) if candidate.candidate_distance_score is not None else None,
            _iso(candidate.first_price_at),
            str(candidate.first_price_delay_seconds) if candidate.first_price_delay_seconds is not None else None,
            candidate.price_samples_before_candidate,
            str(candidate.price_coverage_before_candidate_seconds),
            int(candidate.ath_before_candidate),
            str(candidate.ath_before_candidate_price_usd) if candidate.ath_before_candidate_price_usd is not None else None,
            _iso(candidate.ath_before_candidate_at), candidate.price_history_quality,
            _iso(candidate.candidate_at), candidate.price_history_status,
            candidate.history_source, _iso(candidate.history_start_at), _iso(candidate.history_end_at), candidate.history_sample_count,
            candidate.history_interval, str(candidate.max_history_gap_seconds) if candidate.max_history_gap_seconds is not None else None,
            candidate.source_status.get("canonical_strategy_price") or None,
            candidate.source_status.get("canonical_price_source") or None,
        )
        updates = ",".join(f"{column}=excluded.{column}" for column in columns.split(",") if column != "mint")
        self.connection.execute(f"INSERT INTO survivor_candidates({columns}) VALUES({','.join('?' for _ in values)}) ON CONFLICT(mint) DO UPDATE SET {updates}", values)

    def _persist_trade_activity(self, position: SurvivorPosition, observed_at: datetime) -> None:
        self.connection.execute(
            "UPDATE survivor_positions SET last_trade_at=?,updated_at=? WHERE position_id=?",
            (observed_at.isoformat(), observed_at.isoformat(), position.position_id),
        )

    def _persist_flow(self, candidate: SurvivorCandidate, sample: FlowSample) -> None:
        if sample.source == "RPC_RECONCILIATION":
            source = "bsc_flow_reconciliation"
        elif sample.source == "RPC_GAP_FILL":
            source = "bsc_flow_gap_fill"
        elif sample.event_type in {"flap_token_bought", "flap_token_sold"}:
            source = "flap_portal_wss"
        else:
            source = "bsc_pair_wss"
        self.connection.execute("INSERT INTO survivor_flow_samples(mint,observed_at,buy_volume_bnb,sell_volume_bnb,buy_count,sell_count,price_native,liquidity_native,source,price_source,liquidity_usd) VALUES(?,?,?,?,?,?,?,?,?,?,?)", (candidate.mint,sample.observed_at.isoformat(),str(sample.buy_volume_bnb),str(sample.sell_volume_bnb),sample.buy_count,sample.sell_count,str(sample.price_native) if sample.price_native is not None else None,str(sample.liquidity_native) if sample.liquidity_native is not None else None,source,sample.price_source or "BSC_WSS",str(sample.liquidity_usd) if sample.liquidity_usd is not None else None))
        # Flow events are applied in bounded owner-loop batches. Commit once
        # at the end of the tick instead of once per event.
        self._db_dirty = True

    def _runtime_db_size_bytes(self) -> int:
        """Return the owner runtime DB size without opening another SQLite connection."""

        try:
            row = self.connection.execute("PRAGMA database_list").fetchone()
            path = str(row[2]) if row is not None and len(row) > 2 else ""
            return os.path.getsize(path) if path else 0
        except (OSError, TypeError, IndexError):
            return 0

    def _prune_runtime_history(self, now: datetime) -> None:
        """Small owner-loop batches prevent diagnostic history from growing forever.

        No state required to resume an open position, exit intent, active
        candidate, Factory cursor, or verified venue is removed here.  This is
        deliberately a bounded DELETE from the same owner connection; it does
        not create a maintenance worker or a second writer.
        """

        if self._last_retention_at is not None and now - self._last_retention_at < self.RETENTION_INTERVAL:
            return
        cutoffs = {
            "flow": (now - self.FLOW_RETENTION).isoformat(),
            "price": (now - self.PRICE_SNAPSHOT_RETENTION).isoformat(),
            "observability": (now - self.OBSERVABILITY_RETENTION).isoformat(),
            "candidate": (now - self.INACTIVE_CANDIDATE_RETENTION).isoformat(),
            "registry": (now - self.REGISTRY_RETENTION).isoformat(),
            "position": (now - self.CLOSED_POSITION_RETENTION).isoformat(),
        }
        statements = (
            ("survivor_flow_samples", "observed_at", cutoffs["flow"]),
            ("survivor_price_snapshots", "observed_at", cutoffs["price"]),
            ("health_events", "recorded_at", cutoffs["observability"]),
            ("latency_events", "recorded_at", cutoffs["observability"]),
            ("audit_events", "occurred_at", cutoffs["observability"]),
        )
        deleted = 0
        try:
            for table, column, cutoff in statements:
                cursor = self.connection.execute(
                    f"DELETE FROM {table} WHERE rowid IN (SELECT rowid FROM {table} WHERE {column} < ? LIMIT ?)",
                    (cutoff, self.RETENTION_BATCH_SIZE),
                )
                deleted += max(cursor.rowcount, 0)
            # Old non-active candidates are diagnostic history.  Never remove
            # a mint backing an open position, an active candidate, or a
            # pending exit intent.
            cursor = self.connection.execute(
                "DELETE FROM survivor_candidates WHERE rowid IN ("
                "SELECT rowid FROM survivor_candidates "
                "WHERE active_candidate=0 AND last_seen_at < ? "
                "AND mint NOT IN (SELECT mint FROM survivor_positions WHERE status='OPEN') "
                "LIMIT ?)",
                (cutoffs["candidate"], self.RETENTION_BATCH_SIZE),
            )
            deleted += max(cursor.rowcount, 0)
            # Pool/Venue rows are only needed while their token remains in
            # the rolling candidate window or backs an open position.
            for table, timestamp in (("bsc_venue_registry", "updated_at"), ("bsc_pool_registry", "discovered_at")):
                cursor = self.connection.execute(
                    f"DELETE FROM {table} WHERE rowid IN ("
                    f"SELECT rowid FROM {table} WHERE {timestamp} < ? "
                    "AND token_address NOT IN (SELECT mint FROM survivor_candidates) "
                    "AND token_address NOT IN (SELECT mint FROM survivor_positions WHERE status='OPEN') "
                    "LIMIT ?)",
                    (cutoffs["registry"], self.RETENTION_BATCH_SIZE),
                )
                deleted += max(cursor.rowcount, 0)
            cursor = self.connection.execute(
                "DELETE FROM survivor_positions WHERE rowid IN ("
                "SELECT rowid FROM survivor_positions WHERE status='CLOSED' AND closed_at < ? LIMIT ?)",
                (cutoffs["position"], self.RETENTION_BATCH_SIZE),
            )
            deleted += max(cursor.rowcount, 0)
            open_mints = {position.mint for position in self._positions.values() if position.status == "OPEN"}
            for mint, candidate in tuple(self._candidates.items()):
                if (
                    not candidate.active_candidate
                    and candidate.mint not in open_mints
                    and now - candidate.last_seen_at >= self.INACTIVE_CANDIDATE_RETENTION
                ):
                    self._candidates.pop(mint, None)
            self.connection.commit()
            self._retention_deleted_rows += deleted
            self._last_retention_at = now
        except Exception:
            # Retention is strictly best-effort and must never stop realtime
            # discovery, WSS, quote or position evaluation.
            self.connection.rollback()

    def _persist_position(self, position: SurvivorPosition, now: datetime, reason: str | None) -> None:
        pnl = self._position_pnl_pct(position)
        candidate = self._candidates.get(position.mint)
        self.connection.execute(
            "UPDATE survivor_positions SET "
            "status=?,current_price_native=?,position_mark_price_native=?,position_mark_price_usd=?,"
            "position_mark_source=?,position_price_updated_at=?,position_price_freshness=?,"
            "remaining_quantity_token=?,realized_bnb=?,pnl_pct=?,"
            "exit_reason=COALESCE(?,exit_reason),tp1_at=COALESCE(tp1_at,?),tp2_at=COALESCE(tp2_at,?),trailing_active=?,no_trade_profit_partial_done=?,updated_at=?,"
            "high_since_tp1_native=?,"
            "tp1_triggered=?,tp1_triggered_at=?,tp1_trigger_price_native=?,tp1_filled=?,tp1_filled_at=?,"
            "tp2_triggered=?,tp2_triggered_at=?,tp2_trigger_price_native=?,tp2_filled=?,tp2_filled_at=?,"
            "tp3_triggered=?,tp3_triggered_at=?,tp3_trigger_price_native=?,tp3_filled=?,tp3_filled_at=?,"
            "exit_trigger_reason=?,exit_trigger_pnl_pct=?,exit_trigger_price_native=?,exit_triggered_at=?,"
            "closed_at=CASE WHEN ?='CLOSED' THEN ? ELSE closed_at END,"
            "exit_price_native=CASE WHEN ?='CLOSED' THEN ? ELSE exit_price_native END,"
            "exit_price_usd=CASE WHEN ?='CLOSED' THEN ? ELSE exit_price_usd END,"
            "exit_holders=CASE WHEN ?='CLOSED' THEN ? ELSE exit_holders END,"
            "exit_market_cap_usd=CASE WHEN ?='CLOSED' THEN ? ELSE exit_market_cap_usd END,"
            "exit_liquidity_usd=CASE WHEN ?='CLOSED' THEN ? ELSE exit_liquidity_usd END "
            "WHERE position_id=?",
            (
                position.status,
                str(position.current_price_native) if position.current_price_native else None,
                str(position.position_mark_price_native) if position.position_mark_price_native else None,
                str(position.position_mark_price_usd) if position.position_mark_price_usd else None,
                position.position_mark_source,
                _iso(position.position_price_updated_at),
                position.position_price_freshness,
                str(position.remaining_quantity_token), str(position.realized_bnb), str(pnl) if pnl is not None else None,
                reason, _iso(position.tp1_filled_at) if position.tp1_filled else None, _iso(position.tp2_filled_at) if position.tp2_filled else None,
                1 if position.trailing_active else 0, 1 if position.no_trade_profit_partial_done else 0, now.isoformat(),
                str(position.high_since_tp1_native) if position.high_since_tp1_native is not None else None,
                1 if position.tp1_triggered else 0, _iso(position.tp1_triggered_at), str(position.tp1_trigger_price_native) if position.tp1_trigger_price_native is not None else None, 1 if position.tp1_filled else 0, _iso(position.tp1_filled_at),
                1 if position.tp2_triggered else 0, _iso(position.tp2_triggered_at), str(position.tp2_trigger_price_native) if position.tp2_trigger_price_native is not None else None, 1 if position.tp2_filled else 0, _iso(position.tp2_filled_at),
                1 if position.tp3_triggered else 0, _iso(position.tp3_triggered_at), str(position.tp3_trigger_price_native) if position.tp3_trigger_price_native is not None else None, 1 if position.tp3_filled else 0, _iso(position.tp3_filled_at),
                position.exit_trigger_reason,
                str(position.exit_trigger_pnl_pct) if position.exit_trigger_pnl_pct is not None else None,
                str(position.exit_trigger_price_native) if position.exit_trigger_price_native is not None else None,
                _iso(position.exit_triggered_at),
                position.status,
                now.isoformat() if position.status == "CLOSED" else None,
                position.status, str(position.current_price_native) if position.current_price_native else None,
                position.status, str(candidate.current_price_usd) if candidate is not None and candidate.current_price_usd is not None else None,
                position.status, candidate.holders if candidate is not None else None,
                position.status, str(candidate.market_cap_usd) if candidate is not None and candidate.market_cap_usd is not None else None,
                position.status, str(candidate.liquidity_usd) if candidate is not None and candidate.liquidity_usd is not None else None,
                position.position_id,
            ),
        )
        self.connection.commit()
        getattr(self, "_position_mark_dirty", set()).discard(position.position_id)

    @staticmethod
    def _position_pnl_pct(position: SurvivorPosition) -> Decimal | None:
        """Return percentage points using one explicit accounting basis.

        Open positions are marked from the latest same-venue WSS price. Once
        closed, the actual Paper outcome is always executable sell proceeds
        divided by executable buy cost, including any partial exits.
        """

        if position.invested_bnb <= 0:
            return None
        if position.external_exit_unpriced:
            return None
        if position.status == "CLOSED":
            return (position.realized_bnb / position.invested_bnb - Decimal("1")) * Decimal("100")
        if position.current_price_native is None or position.current_price_native <= 0:
            return None
        marked_value = position.realized_bnb + position.remaining_quantity_token * position.current_price_native
        return (marked_value / position.invested_bnb - Decimal("1")) * Decimal("100")

    def _position_payload(self, position: SurvivorPosition) -> dict[str, object]:
        pnl = self._position_pnl_pct(position)
        return {
            "position_id": position.position_id,
            "mint": position.mint,
            "symbol": position.symbol,
            "status": position.status,
            "opened_at": position.opened_at.isoformat(),
            "last_trade_at": _iso(position.last_trade_at),
            "entry_price_native": str(position.entry_price_native),
            "current_price_native": str(position.current_price_native) if position.current_price_native else None,
            "position_mark_price_native": str(position.position_mark_price_native) if position.position_mark_price_native else None,
            "position_mark_price_usd": str(position.position_mark_price_usd) if position.position_mark_price_usd else None,
            "position_mark_source": position.position_mark_source,
            "position_price_updated_at": _iso(position.position_price_updated_at),
            "position_price_freshness": position.position_price_freshness,
            "pnl_pct": str(pnl) if pnl is not None else None,
            "pnl_basis": "EXECUTABLE_QUOTE" if position.status == "CLOSED" else "WSS_MARK",
            "exit_trigger_reason": position.exit_trigger_reason,
            "exit_trigger_pnl_pct": str(position.exit_trigger_pnl_pct) if position.exit_trigger_pnl_pct is not None else None,
            "exit_trigger_price_native": str(position.exit_trigger_price_native) if position.exit_trigger_price_native is not None else None,
            "exit_triggered_at": _iso(position.exit_triggered_at),
            "tp1": position.tp1,
            "tp2": position.tp2,
            "tp_ladder": {
                stage: {
                    "triggered": getattr(position, f"{stage}_triggered"),
                    "triggered_at": _iso(getattr(position, f"{stage}_triggered_at")),
                    "trigger_price_native": str(getattr(position, f"{stage}_trigger_price_native")) if getattr(position, f"{stage}_trigger_price_native") is not None else None,
                    "filled": getattr(position, f"{stage}_filled"),
                    "filled_at": _iso(getattr(position, f"{stage}_filled_at")),
                }
                for stage in ("tp1", "tp2", "tp3")
            },
            "trailing_active": position.trailing_active,
        }

    def _oldest_venue_job_age_ms(self, now: datetime) -> int | None:
        jobs = tuple(getattr(self, "_venue_resolution_jobs", {}).values())
        if not jobs:
            return None
        ages = []
        for job in jobs:
            try:
                ages.append(max(0, int((now - job.requested_at).total_seconds() * 1000)))
            except (AttributeError, TypeError, ValueError):
                continue
        return max(ages) if ages else None

    def _stage_metrics_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {}
        for stage, item in getattr(self, "_stage_metrics", {}).items():
            samples = item.get("samples_ms", ())
            payload[stage] = {
                "calls": int(item.get("calls", 0)),
                "total_ms": round(float(item.get("total_ms", 0.0)), 2),
                **self._distribution(samples, len(samples)),
            }
        return payload

    def _publish(self, now: datetime) -> None:
        payload = self.status()
        payload["updated_at"] = now.isoformat()
        state_key = "survivor_balanced_v1" if self.config.identity == BALANCED_SURVIVOR_REVERSAL_IDENTITY else "survivor_reversal_v1"
        health_name = "survivor_balanced" if self.config.identity == BALANCED_SURVIVOR_REVERSAL_IDENTITY else "survivor_reversal"
        self.store.set_state(state_key, payload)
        self.health.set(health_name, "HEALTHY", details={
            "strategy": self.config.identity.as_dict(), "discovered": payload["discovered"],
            "active": payload["active"], "open_positions": payload["open_positions"],
            "paper_only": True,
            "db_commit_error_count": self._db_commit_error_count,
            "db_locked_error_count": self._db_locked_error_count,
            "db_write_error_count": self._db_write_error_count,
            "last_db_write_at": _iso(self._last_db_write_at),
        })
        flow_states = tuple(self._flow_reconciliation_state.values())
        degraded = sum(1 for item in flow_states if item.get("data_completeness_health") in {"DEGRADED", "FAILED"})
        complete_health = (
            "DEGRADED" if degraded
            else ("HEALTHY" if flow_states and all(item.get("data_completeness_health") == "HEALTHY" for item in flow_states) else "PENDING")
        )
        self.health.set("bsc_flow_reconciliation", complete_health, details={
            "transport_health": self._wss_provider_state,
            "subscription_health": "HEALTHY" if self._wss_provider_state in {"HEALTHY", "READY"} else "DEGRADED",
            "data_completeness_health": complete_health,
            "sources": len(flow_states),
            "silent_miss_count": self._flow_reconciliation_misses_total,
            "recovered_event_count": self._flow_reconciliation_recovered_total,
            "rpc_calls": self._flow_reconciliation_rpc_calls,
            "rpc_429": self._flow_reconciliation_rpc_429,
            "latency_ms": self._distribution(getattr(self, "_flow_reconciliation_latency_ms", ()), len(getattr(self, "_flow_reconciliation_latency_ms", ()))),
            "lag_blocks": self._distribution(getattr(self, "_flow_reconciliation_lag_blocks", ()), len(getattr(self, "_flow_reconciliation_lag_blocks", ()))),
            "states": {key: dict(value) for key, value in self._flow_reconciliation_state.items()},
        })

    def _audit_event(self, event_type: str, candidate: SurvivorCandidate | None, payload: Mapping[str, object]) -> None:
        data = dict(payload)
        if candidate is not None:
            data.update({"mint": candidate.mint, "symbol": candidate.symbol, "state": candidate.state})
        self.audit.append(mode=self.mode, event_type=event_type, occurred_at=self.clock(), payload=data)


class BscBalancedLiveEngine(SurvivorReversalEngine):
    """Balanced strategy owner for isolated asynchronous Live execution.

    Candidate evaluation, Flow, reconciliation and all SQLite mutations remain
    the inherited Balanced logic.  Only settlement is replaced: a real BUY or
    SELL is submitted through the single-flight async bridge and a position is
    created/closed only after ``market-order list`` reports ``FINISHED``.
    """

    # A reservation occupies an entry slot from before the async BUY leaves
    # this process until it becomes an OPEN position or is proved not to have
    # reached the provider.  Keep unknowns fail-closed: a timeout may still
    # become a real order after the CLI returns.
    _ACTIVE_BUY_RESERVATION_STATES = frozenset({
        "BUY_SLOT_RESERVED",
        "SWAP_SUBMITTING",
        "SWAP_SUBMITTED",
        "SWAP_PENDING",
        "SWAP_UNKNOWN",
        "WAITING_CONFIRMATION",
    })
    _DEFINITIVE_PRE_SUBMIT_FAILURES = frozenset({
        "REAL_SWAP_DISABLED",
        "INVALID_REQUEST",
        "GAS_RESERVE_INSUFFICIENT",
        "TOKEN_BALANCE_UNAVAILABLE",
        "AMOUNT_TOO_SMALL",
    })
    _POSITION_MARK_QUOTE_INTERVAL = timedelta(seconds=10)
    _WALLET_RECONCILE_INTERVAL = timedelta(seconds=5)
    _POSITION_MARK_STALE_AFTER = timedelta(seconds=20)
    _WALLET_DUST_RATIO = Decimal("1e-12")

    def __init__(
        self,
        *,
        live_executor: Any,
        live_bridge: AsyncLiveExecutionBridge,
        live_amount_bnb: Decimal,
        live_event_notifier: Callable[[str, Mapping[str, object]], None] | None = None,
        **kwargs: Any,
    ) -> None:
        self.live_executor = live_executor
        self.live_provider = str(getattr(live_executor, "provider", type(live_executor).__name__))
        self.live_bridge = live_bridge
        self.live_amount_bnb = Decimal(str(live_amount_bnb))
        self._live_event_notifier = live_event_notifier
        self._live_buy_context: dict[str, dict[str, Any]] = {}
        self._live_sell_context: dict[str, dict[str, Any]] = {}
        self._live_orders: dict[str, dict[str, Any]] = {}
        self._live_next_poll: dict[str, datetime] = {}
        self._live_swap_unknown = 0
        self._entry_reservations: dict[str, dict[str, Any]] = {}
        self._position_mark_quote_next: dict[str, datetime] = {}
        self._position_mark_quote_context: dict[str, SurvivorPosition] = {}
        self._hard_stop_quote_context: dict[str, dict[str, Any]] = {}
        self._position_mark_generation: dict[str, int] = {}
        self._wallet_reconcile_next: dict[str, datetime] = {}
        self._wallet_reconcile_pending: set[str] = set()
        self._entry_outcome_mark_dirty: set[str] = set()
        # Set by the launcher after executor preflight. A low balance disables
        # only new BUY submissions; existing live positions remain eligible
        # for SELL/exit reconciliation.
        self.live_entry_funds_available = True
        super().__init__(mode="live", **kwargs)
        self._demote_live_reference_prices()
        self._restore_entry_reservations()
        self._restore_executor_orders()
        # A restart must not preserve a local OPEN row after the wallet has
        # already been sold through GMGN, Telegram, or another wallet tool.
        self.reconcile_open_positions_with_wallet(self.clock(), force=True)
        # A durable price crossing is enough to resume the TP queue after a
        # restart; do not wait for the market to cross the level a second time.
        self._resume_latched_tp_exits(self.clock())

    @staticmethod
    def _entry_observation_value(value: object) -> object:
        """Use JSON null for unavailable facts; never manufacture a market value."""
        if isinstance(value, Decimal):
            return str(value)
        return value

    def _entry_flow_window(self, candidate: SurvivorCandidate, now: datetime, seconds: int) -> dict[str, object]:
        samples = [sample for sample in candidate.flows if sample.observed_at >= now - timedelta(seconds=seconds)]
        buy_volume = sum((sample.buy_volume_bnb for sample in samples), Decimal("0"))
        sell_volume = sum((sample.sell_volume_bnb for sample in samples), Decimal("0"))
        buy_count = sum(sample.buy_count for sample in samples)
        sell_count = sum(sample.sell_count for sample in samples)
        label = "3m" if seconds == 180 else f"{seconds}s"
        return {
            f"buy_count_{label}": buy_count,
            f"sell_count_{label}": sell_count,
            f"buy_volume_{label}": self._entry_observation_value(buy_volume),
            f"sell_volume_{label}": self._entry_observation_value(sell_volume),
            f"net_buy_volume_{label}": self._entry_observation_value(buy_volume - sell_volume),
        }

    def _capture_live_entry_snapshot(
        self,
        position: SurvivorPosition,
        candidate: SurvivorCandidate,
        context: Mapping[str, object],
        now: datetime,
    ) -> None:
        """Insert exactly once after a real BUY and balance confirmation."""
        status = candidate.source_status
        buy = context.get("buy_quote")
        sell = context.get("sell_quote")
        buy_price = (buy.input_quantity / buy.output_quantity if isinstance(buy, ExecutableQuote) and buy.output_quantity > 0 else None)
        sell_price = (sell.output_quantity / sell.input_quantity if isinstance(sell, ExecutableQuote) and sell.input_quantity > 0 else None)
        flows: dict[str, object] = {}
        for seconds in (30, 60, 180):
            flows.update(self._entry_flow_window(candidate, now, seconds))
        record = candidate.record
        audit = {
            name: _field(record, name) if record is not None else None
            for name in ("top10_percent", "dev_percent", "insider_percent", "sniper_percent")
        }
        raw_sources = {value for value in str(status.get("discovery_sources") or "").split(",") if value}
        signal_source = "BINANCE+OKX" if raw_sources == {"BINANCE", "OKX"} else ("OKX_ONLY" if raw_sources == {"OKX"} else "BINANCE_ONLY" if raw_sources == {"BINANCE"} else "UNKNOWN")
        token_age = candidate.age_seconds if candidate.age_seconds else self._candidate_age_seconds(candidate, now)
        source_decimal = lambda key: self._entry_observation_value(_decimal(status.get(key)))
        snapshot: dict[str, object] = {
            "token": position.mint, "entry_at": now.isoformat(),
            "actual_entry_price": self._entry_observation_value(position.entry_price_native),
            "actual_entry_price_unit": "BNB_PER_TOKEN",
            "market_cap": self._entry_observation_value(candidate.market_cap_usd),
            "liquidity": self._entry_observation_value(candidate.liquidity_usd),
            "lp_mc_ratio": self._entry_observation_value(candidate.liquidity_usd / candidate.market_cap_usd if candidate.liquidity_usd is not None and candidate.market_cap_usd not in (None, Decimal("0")) else None),
            "holders": candidate.holders, "token_age": token_age,
            "migrate_status": candidate.latest_migrate_status,
            "venue": candidate.descriptor.address if candidate.descriptor is not None else candidate.pair_address,
            "venue_type": candidate.pool_type,
            "drawdown_from_ath": self._entry_observation_value(candidate.drawdown_pct),
            "rebound_from_local_low": self._entry_observation_value(((candidate.current_price_native / candidate.local_low_native - Decimal("1")) * Decimal("100")) if candidate.current_price_native is not None and candidate.local_low_native not in (None, Decimal("0")) else None),
            "unique_buyers_30s": None, "unique_buyers_60s": None, "unique_buyers_3m": None,
            "largest_buyer_share_60s": None,
            "holders_growth_1m": source_decimal("holders_growth_1m"),
            "holders_growth_3m": source_decimal("holders_growth_3m"),
            "gmgn_buy_quote_price": self._entry_observation_value(buy_price),
            "gmgn_sell_quote_price": self._entry_observation_value(sell_price),
            "roundtrip_recovery_pct": self._entry_observation_value((sell.output_quantity / buy.input_quantity * Decimal("100")) if isinstance(buy, ExecutableQuote) and isinstance(sell, ExecutableQuote) and buy.input_quantity > 0 else None),
            "buy_price_impact": self._entry_observation_value(buy.price_impact_pct if isinstance(buy, ExecutableQuote) else None),
            "entry_execution_latency_ms": int((now - context["submitted_at"]).total_seconds() * 1000) if isinstance(context.get("submitted_at"), datetime) else None,
            "top10_pct": self._entry_observation_value(audit["top10_percent"]),
            "dev_pct": self._entry_observation_value(audit["dev_percent"]),
            "insider_pct": self._entry_observation_value(audit["insider_percent"]),
            "sniper_pct": self._entry_observation_value(audit["sniper_percent"]),
            "lp_burn_or_lock_pct": source_decimal("lp_burn_or_lock_pct"),
            "discovered_by_binance": "BINANCE" in raw_sources,
            "okx_signal_present": "OKX" in raw_sources,
            "okx_smart_money_count": source_decimal("okx_smart_money_count"),
            "okx_kol_count": source_decimal("okx_kol_count"),
            "okx_whale_count": source_decimal("okx_whale_count"),
            "okx_signal_amount_usd": source_decimal("latest_okx_amount_usd"),
            "discovery_source_count": len(raw_sources), "signal_source": signal_source,
        }
        snapshot.update(flows)
        buy_60, sell_60 = _decimal(snapshot.get("buy_volume_60s")), _decimal(snapshot.get("sell_volume_60s"))
        buy_count_60, sell_count_60 = int(snapshot["buy_count_60s"]), int(snapshot["sell_count_60s"])
        snapshot["buy_sell_volume_ratio"] = self._entry_observation_value(buy_60 / sell_60 if buy_60 is not None and sell_60 not in (None, Decimal("0")) else None)
        snapshot["buy_sell_count_ratio"] = self._entry_observation_value(Decimal(buy_count_60) / Decimal(sell_count_60) if sell_count_60 else None)
        self.connection.execute(
            "INSERT OR IGNORE INTO live_entry_snapshots(position_id,mint,entry_at,snapshot_json) VALUES(?,?,?,?)",
            (position.position_id, position.mint, now.isoformat(), json.dumps(snapshot, sort_keys=True, default=str)),
        )
        self.connection.execute(
            "INSERT OR IGNORE INTO live_entry_outcomes(position_id,outcome_json,closed_at) VALUES(?,?,NULL)",
            (position.position_id, json.dumps({}, sort_keys=True)),
        )

    def _record_live_outcome(self, position: SurvivorPosition, now: datetime, *, final_reason: str | None = None) -> None:
        row = self.connection.execute("SELECT outcome_json FROM live_entry_outcomes WHERE position_id=?", (position.position_id,)).fetchone()
        if row is None:
            return  # Only entries confirmed after this collector was enabled.
        try:
            outcome = json.loads(row[0]) if row[0] else {}
        except (TypeError, json.JSONDecodeError):
            outcome = {}
        mark = position.position_mark_price_native or position.current_price_native
        if mark is not None and mark > 0 and position.entry_price_native > 0:
            return_pct = (mark / position.entry_price_native - Decimal("1")) * Decimal("100")
            outcome["last_mark_price"] = str(mark)
            outcome["last_mark_at"] = now.isoformat()
            for horizon in (1, 3, 5, 15, 30):
                key = f"return_{horizon}m"
                if key not in outcome and now >= position.opened_at + timedelta(minutes=horizon):
                    outcome[key] = str(return_pct)
            previous_mfe, previous_mae = _decimal(outcome.get("mfe_pct")), _decimal(outcome.get("mae_pct"))
            if previous_mfe is None or return_pct > previous_mfe:
                outcome["mfe_pct"], outcome["mfe_at"] = str(return_pct), now.isoformat()
            if previous_mae is None or return_pct < previous_mae:
                outcome["mae_pct"], outcome["mae_at"] = str(return_pct), now.isoformat()
        for label, hit, when in (("tp1", position.tp1_filled, position.tp1_filled_at), ("tp2", position.tp2_filled, position.tp2_filled_at), ("tp3", position.tp3_filled, position.tp3_filled_at)):
            outcome[f"hit_{label}_{ {'tp1': '50', 'tp2': '100', 'tp3': '200'}[label] }"] = bool(hit)
            if hit and when is not None:
                outcome[f"time_to_{label}"] = max(0.0, (when - position.opened_at).total_seconds())
        closed_at = None
        if position.status == "CLOSED":
            closed_at = now.isoformat()
            exit_price = position.current_price_native if not position.external_exit_unpriced else None
            realized_pct = self._position_pnl_pct(position) if not position.external_exit_unpriced else None
            outcome.update({"actual_exit_price": self._entry_observation_value(exit_price), "realized_pnl_pct": self._entry_observation_value(realized_pct), "realized_pnl_bnb": self._entry_observation_value(position.realized_bnb - position.invested_bnb) if not position.external_exit_unpriced else None, "exit_reason": final_reason, "holding_duration": max(0.0, (now - position.opened_at).total_seconds())})
        self.connection.execute("UPDATE live_entry_outcomes SET outcome_json=?,closed_at=COALESCE(?,closed_at) WHERE position_id=?", (json.dumps(outcome, sort_keys=True, default=str), closed_at, position.position_id))

    def _maybe_write_entry_outcome_report(self) -> None:
        report_state_key = "gmgn_live_entry_outcome_report_v1"
        state = self.connection.execute("SELECT value_json FROM runtime_state WHERE mode='live' AND state_key=?", (report_state_key,)).fetchone()
        if state is not None:
            return
        rows = self.connection.execute("SELECT s.snapshot_json,o.outcome_json FROM live_entry_snapshots s JOIN live_entry_outcomes o ON o.position_id=s.position_id WHERE o.closed_at IS NOT NULL ORDER BY s.entry_at LIMIT 20").fetchall()
        if len(rows) < 20:
            return
        report = build_report((json.loads(row[0]), json.loads(row[1])) for row in rows)
        report["generated_at"] = self.clock().isoformat()
        path = next((str(row[2]) for row in self.connection.execute("PRAGMA database_list") if row[1] == "main"), "")
        if path:
            report_path = os.path.join(os.path.dirname(path), "gmgn_live_entry_outcomes_report.json")
            temporary = report_path + ".tmp"
            with open(temporary, "w", encoding="utf-8") as handle:
                json.dump(report, handle, ensure_ascii=False, indent=2, sort_keys=True)
            os.replace(temporary, report_path)
            report["path"] = report_path
        self.store.set_state(report_state_key, report)

    def _set_position_mark(self, position: SurvivorPosition, **kwargs: Any) -> bool:
        changed = super()._set_position_mark(position, **kwargs)
        if changed:
            self._entry_outcome_mark_dirty.add(position.position_id)
            generations = getattr(self, "_position_mark_generation", None)
            if generations is None:
                generations = self._position_mark_generation = {}
            generation = generations.get(position.position_id, 0) + 1
            generations[position.position_id] = generation
            # A venue/WSS mark is a fast risk signal, not an executable fill.
            # It may request an urgent GMGN sell quote, but only that fresh
            # executable quote may later confirm HARD_STOP.
            source = str(kwargs.get("source") or "")
            pnl = self._current_mark_pnl_pct(position)
            if (
                source != "GMGN_SELL_QUOTE"
                and pnl is not None
                and pnl <= -self.config.hard_stop_pct
            ):
                self._request_hard_stop_confirmation(position, pnl, generation, kwargs.get("observed_at") or self.clock())
        return changed

    def _hard_stop_is_confirmed(self, position: SurvivorPosition, pnl: Decimal) -> bool:
        """Live hard stops require a fresh executable SELL quote confirmation."""

        return (
            position.position_mark_source == "GMGN_SELL_QUOTE"
            and pnl <= -self.config.hard_stop_pct
        )

    def _request_hard_stop_confirmation(
        self,
        position: SurvivorPosition,
        mark_return: Decimal,
        generation: int,
        observed_at: datetime,
    ) -> None:
        """Wake one high-priority, read-only SELL quote for a WSS risk mark."""

        if position.status != "OPEN" or position.exit_intent_reason is not None:
            return
        key = f"hard-stop-quote:{position.position_id}"
        contexts = getattr(self, "_hard_stop_quote_context", None)
        if contexts is None:
            contexts = self._hard_stop_quote_context = {}
        if key in contexts:
            return
        candidate = self._candidates.get(position.mint)
        if candidate is not None:
            candidate.source_status.update({
                "hard_stop_wakeup_at": observed_at.isoformat(),
                "hard_stop_wakeup_mark_price": str(position.current_price_native),
                "hard_stop_wakeup_mark_return": str(mark_return),
                "hard_stop_wakeup_source": position.position_mark_source,
            })
        if self.live_bridge.submit("quote_sell", key, position.mint, position.remaining_quantity_token) is None:
            return
        contexts[key] = {
            "position": position,
            "generation": generation,
            "triggered_at": observed_at,
            "trigger_mark_price": position.current_price_native,
            "trigger_mark_return": mark_return,
        }

    def _candidate_position_mark(
        self,
        candidate: SurvivorCandidate,
        now: datetime | None = None,
    ) -> tuple[Decimal | None, Decimal | None]:
        """Normalize a fresh canonical venue mark to BNB/token for Live risk.

        Flap and some pools publish their raw mark in a non-BNB quote asset.
        Live entry and GMGN execution are BNB-denominated, so raw venue units
        must never be compared directly with the BNB/token entry price.  The
        canonical venue USD price, divided by a fresh BNB/USD mark, produces
        a comparable realtime risk mark without inventing an executable fill.
        """

        observed_at = candidate.price_updated_at
        current = now or self.clock()
        if (
            candidate.price_status != "VALID"
            or candidate.current_price_usd is None
            or candidate.current_price_usd <= 0
            or observed_at is None
            or current - observed_at > self._POSITION_MARK_STALE_AFTER
        ):
            return None, None
        bnb_usd = self._fresh_bnb_usd(current)
        if (
            bnb_usd is None
            and candidate.native_token_price_usd is not None
            and candidate.native_token_price_usd > 0
            and current - candidate.last_seen_at <= timedelta(seconds=90)
        ):
            bnb_usd = candidate.native_token_price_usd
        if bnb_usd is None or bnb_usd <= 0:
            return None, None
        return candidate.current_price_usd / bnb_usd, candidate.current_price_usd

    def _resume_latched_tp_exits(self, now: datetime) -> None:
        for position in tuple(self._positions.values()):
            if position.status != "OPEN" or position.position_id in self._live_sell_context:
                continue
            if self._next_latched_tp(position) is not None:
                self._execute_latched_tp(position, now)

    def _notify_live_event(
        self,
        event_type: str,
        candidate: SurvivorCandidate | None,
        payload: Mapping[str, object],
    ) -> None:
        """Emit a non-blocking-safe notification payload after persisted Live facts.

        The launcher supplies a fire-and-forget transport wrapper.  A Telegram
        failure is intentionally auxiliary: it cannot alter a confirmed order,
        delay the owner loop, or expose any executor credential.
        """

        notifier = getattr(self, "_live_event_notifier", None)
        if notifier is None:
            return
        data = dict(payload)
        data.setdefault("strategy_name", self.config.identity.strategy_name)
        if candidate is not None:
            data.setdefault("mint", candidate.mint)
            data.setdefault("token_name", candidate.symbol)
        try:
            notifier(event_type, data)
        except Exception:
            # Notification transport is deliberately not part of settlement.
            pass

    def _refresh_position_mark_freshness(self, position: SurvivorPosition, now: datetime) -> None:
        updated_at = position.position_price_updated_at
        fresh = (
            updated_at is not None
            and now - updated_at <= self._POSITION_MARK_STALE_AFTER
            and position.position_mark_price_native is not None
        )
        desired = "FRESH" if fresh else ("STALE" if position.position_mark_price_native is not None else "UNAVAILABLE")
        if position.position_price_freshness != desired:
            position.position_price_freshness = desired
            self._position_mark_dirty.add(position.position_id)

    def _schedule_position_monitoring(self, now: datetime) -> None:
        """Schedule low-rate read-only position mark work outside the owner loop."""

        for position in tuple(self._positions.values()):
            if position.status != "OPEN":
                continue
            self._refresh_position_mark_freshness(position, now)
            quote_due = self._position_mark_quote_next.get(position.position_id)
            if quote_due is None or now >= quote_due:
                key = f"position-mark:{position.position_id}"
                if self.live_bridge.submit("quote_sell", key, position.mint, position.remaining_quantity_token) is not None:
                    self._position_mark_quote_context[key] = position
                    self._position_mark_quote_next[position.position_id] = now + self._POSITION_MARK_QUOTE_INTERVAL
    def reconcile_open_positions_with_wallet(
        self,
        now: datetime,
        *,
        force: bool = False,
        completed_position_id: str | None = None,
        completed_balance: Decimal | None = None,
    ) -> None:
        """Make Live OPEN quantity follow direct BSC ERC-20 ``balanceOf``.

        The same function is used for startup, the five-second background
        cadence, and the pre-BUY capacity check.  A failed RPC read is never
        treated as a zero balance.
        """

        if completed_position_id is not None:
            self._wallet_reconcile_pending.discard(completed_position_id)
            positions = (self._positions.get(completed_position_id),)
            balances = {completed_position_id: completed_balance}
        else:
            positions = tuple(self._positions.values())
            balances: dict[str, Decimal | None] = {}
            if force:
                reader = getattr(getattr(self, "resolver", None), "erc20_balance_of", None)
                owner = getattr(getattr(self, "live_executor", None), "wallet", "")
                for position in positions:
                    if position.status == "OPEN":
                        try:
                            balances[position.position_id] = reader(position.mint, owner) if callable(reader) else None
                        except Exception:
                            balances[position.position_id] = None

        for position in positions:
            if position is None or position.status != "OPEN":
                continue
            actual_balance = balances.get(position.position_id)
            if completed_position_id is None and not force:
                due = self._wallet_reconcile_next.get(position.position_id)
                if position.position_id in self._wallet_reconcile_pending or (due is not None and now < due):
                    continue
                key = f"wallet-reconcile:{position.position_id}"
                if self.live_bridge.submit("wallet_reconcile", key, position.mint) is not None:
                    self._wallet_reconcile_pending.add(position.position_id)
                    self._wallet_reconcile_next[position.position_id] = now + self._WALLET_RECONCILE_INTERVAL
                continue
            if actual_balance is None:
                candidate = self._candidates.get(position.mint)
                if candidate is not None:
                    candidate.source_status["wallet_reconciliation"] = "WALLET_BALANCE_UNVERIFIED"
                    self._persist_candidate(candidate, now)
                continue
            expected = position.remaining_quantity_token
            dust_threshold = max(Decimal("1e-18"), expected * self._WALLET_DUST_RATIO)
            if actual_balance >= expected - dust_threshold:
                continue
            if actual_balance <= dust_threshold:
                own_sell_in_flight = position.position_id in getattr(self, "_live_sell_context", {})
                position.status = "CLOSED"
                position.remaining_quantity_token = Decimal("0")
                # A locally submitted sell can reach zero before its receipt/order
                # reconciliation records final price, PnL and reason.  The slot is
                # still released immediately, but that executor path keeps ownership
                # of those final accounting fields.
                position.external_exit_unpriced = not own_sell_in_flight
                self.connection.execute(
                    "UPDATE survivor_positions SET status='CLOSED',remaining_quantity_token='0',"
                    "exit_reason=CASE WHEN ? THEN exit_reason ELSE 'EXTERNAL_EXIT' END,closed_at=?,"
                    "exit_price_native=CASE WHEN ? THEN exit_price_native ELSE NULL END,"
                    "exit_price_usd=CASE WHEN ? THEN exit_price_usd ELSE NULL END,"
                    "actual_exit_proceeds_bnb=CASE WHEN ? THEN actual_exit_proceeds_bnb ELSE NULL END,"
                    "pnl_pct=CASE WHEN ? THEN pnl_pct ELSE NULL END,"
                    "external_exit_unpriced=CASE WHEN ? THEN external_exit_unpriced ELSE 1 END,updated_at=? "
                    "WHERE position_id=?",
                    (
                        int(own_sell_in_flight),
                        now.isoformat(),
                        int(own_sell_in_flight),
                        int(own_sell_in_flight),
                        int(own_sell_in_flight),
                        int(own_sell_in_flight),
                        int(own_sell_in_flight),
                        now.isoformat(),
                        position.position_id,
                    ),
                )
                self.connection.commit()
                self._record_live_outcome(position, now, final_reason="EXTERNAL_EXIT" if not own_sell_in_flight else None)
                self.connection.commit()
                self._maybe_write_entry_outcome_report()
                self._positions.pop(position.position_id, None)
                candidate = self._candidates.get(position.mint)
                if candidate is not None:
                    candidate.state = "POSITION_CLOSED"
                continue
            position.remaining_quantity_token = actual_balance
            position.external_exit_unpriced = True
            self.connection.execute(
                "UPDATE survivor_positions SET remaining_quantity_token=?,pnl_pct=NULL,external_exit_unpriced=1,updated_at=? "
                "WHERE position_id=?",
                (str(actual_balance), now.isoformat(), position.position_id),
            )
            self.connection.commit()

    def _demote_live_reference_prices(self) -> None:
        """Do not let a persisted Binance reference reopen a Live candidate."""
        reference_sources = {"MEME_RUSH", "TOKEN_INFO", "BINANCE_DIRECT_USD_FALLBACK"}
        changed = False
        now = self.clock()
        for candidate in self._candidates.values():
            if candidate.price_source not in reference_sources:
                continue
            if candidate.current_price_usd is not None:
                candidate.source_status.setdefault("discovery_reference_price", str(candidate.current_price_usd))
                candidate.source_status.setdefault("discovery_reference_price_at", _iso(candidate.price_updated_at) or now.isoformat())
            candidate.current_price_usd = None
            candidate.price_status = "CANONICAL_PRICE_PENDING"
            candidate.price_source = "REFERENCE_ONLY"
            candidate.price_updated_at = None
            candidate.candidate_eligible = False
            candidate.active_candidate = False
            candidate.ready_to_buy = False
            candidate.source_status.update({
                "canonical_strategy_price": "",
                "canonical_price_source": "PENDING_VENUE_PRICE",
                "price": "REFERENCE_ONLY",
            })
            self._persist_candidate(candidate, now)
            changed = True
        if changed:
            self.connection.commit()

    def _entry_reservation_key(self, mint: str) -> str:
        return f"entry_reservation:{self._mint_key(mint)}"

    def _entry_capacity(self) -> dict[str, int]:
        open_positions = sum(position.status == "OPEN" for position in self._positions.values())
        reserved = sum(
            1 for item in self._entry_reservations.values()
            if str(item.get("status") or "") in self._ACTIVE_BUY_RESERVATION_STATES
        )
        maximum = max(1, int(getattr(self.config, "max_open_positions", 1)))
        used = open_positions + reserved
        return {
            "max_open_positions": maximum,
            "open_positions": open_positions,
            "buy_reserved": reserved,
            "used_slots": used,
            "available_slots": max(0, maximum - used),
        }

    def _write_entry_reservation(self, item: Mapping[str, Any]) -> bool:
        """Persist before the external BUY is submitted; called by owner loop only."""
        mint = str(item["mint"])
        key = self._entry_reservation_key(mint)
        try:
            self.connection.execute(
                "INSERT INTO runtime_state(mode,state_key,value_json,updated_at) VALUES(?,?,?,?) "
                "ON CONFLICT(mode,state_key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at",
                (self.mode, key, json.dumps(dict(item), ensure_ascii=False, sort_keys=True, default=str), self.clock().isoformat()),
            )
            self.connection.commit()
            self._last_db_write_at = self.clock()
            return True
        except Exception:
            self._db_write_error_count += 1
            try:
                self.connection.rollback()
            except Exception:
                pass
            return False

    def _restore_entry_reservations(self) -> None:
        try:
            rows = self.connection.execute(
                "SELECT value_json FROM runtime_state WHERE mode=? AND state_key LIKE 'entry_reservation:%'",
                (self.mode,),
            ).fetchall()
        except Exception:
            rows = ()
        for row in rows:
            try:
                item = json.loads(str(row[0]))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            mint = str(item.get("mint") or "")
            status = str(item.get("status") or "")
            if mint and status in self._ACTIVE_BUY_RESERVATION_STATES:
                self._entry_reservations[self._mint_key(mint)] = item

    def _reserve_entry_slot(self, candidate: SurvivorCandidate, now: datetime) -> dict[str, Any] | None:
        mint_key = self._mint_key(candidate.mint)
        if mint_key in self._entry_reservations:
            candidate.last_rejection = "ENTRY_CAPACITY_RESERVED"
            return None
        capacity = self._entry_capacity()
        if capacity["available_slots"] <= 0:
            candidate.last_rejection = "ENTRY_CAPACITY_FULL"
            candidate.source_status.update({"live_entry": "PAUSED_BY_CAPACITY", "entry_capacity": capacity})
            return None
        item = {
            "reservation_id": uuid4().hex,
            "mint": candidate.mint,
            "candidate_id": candidate.mint,
            "reserved_at": now.isoformat(),
            "status": "BUY_SLOT_RESERVED",
        }
        # The main loop is the sole writer, so the check and durable insert are
        # ordered with every other candidate evaluation in this tick.
        if not self._write_entry_reservation(item):
            candidate.last_rejection = "ENTRY_CAPACITY_PERSIST_FAILED"
            return None
        self._entry_reservations[mint_key] = item
        return item

    def _set_entry_reservation_status(self, mint: str, status: str, now: datetime, **fields: Any) -> bool:
        item = self._entry_reservations.get(self._mint_key(mint))
        if item is None:
            return False
        item.update(fields)
        item.update({"status": status, "updated_at": now.isoformat()})
        return self._write_entry_reservation(item)

    def _release_entry_reservation(self, mint: str, now: datetime, reason: str) -> None:
        item = self._entry_reservations.get(self._mint_key(mint))
        if item is None:
            return
        item.update({"status": "RELEASED", "released_at": now.isoformat(), "release_reason": reason})
        if self._write_entry_reservation(item):
            self._entry_reservations.pop(self._mint_key(mint), None)

    def _restore_executor_orders(self) -> None:
        """Reconnect persisted provider orders to owner-loop reconciliation."""
        loader = getattr(self.live_executor, "pending_orders", None)
        if not callable(loader):
            return
        try:
            pending = tuple(loader())
        except Exception:
            return
        for item in pending:
            order_id = str(item.get("order_id") or "")
            mint = str(item.get("token") or "")
            side = str(item.get("side") or "").lower()
            quantity = _decimal(item.get("quantity"))
            if not order_id or not mint or quantity is None or quantity <= 0:
                continue
            if side == "buy":
                # A legacy pending order created before reservations existed
                # is converted to one before it can be reconciled.  This is
                # intentionally conservative across a runtime restart.
                mint_key = self._mint_key(mint)
                if mint_key not in self._entry_reservations:
                    restored = {
                        "reservation_id": uuid4().hex,
                        "mint": mint,
                        "candidate_id": mint,
                        "reserved_at": self.clock().isoformat(),
                        "restored_at": self.clock().isoformat(),
                        "status": "SWAP_UNKNOWN" if str(item.get("state") or "").upper() == "UNKNOWN" else "SWAP_PENDING",
                        "order_id": order_id,
                    }
                    if self._write_entry_reservation(restored):
                        self._entry_reservations[mint_key] = restored
                candidate = self._candidates.get(self._mint_key(mint)) or self._candidates.get(mint)
                if candidate is None:
                    continue
                key = f"buy:{candidate.mint}"
                context = {
                    "key": key,
                    "mint": candidate.mint,
                    "candidate": candidate,
                    "buy_quote": SimpleNamespace(input_quantity=quantity),
                    "sell_quote": None,
                    "order_id": order_id,
                    "status": str(item.get("state") or "PENDING"),
                }
                self._live_buy_context[candidate.mint] = context
                self._live_orders[order_id] = {"side": "buy", **context}
            elif side == "sell":
                position = next(
                    (
                        value for value in self._positions.values()
                        if value.status == "OPEN" and self._mint_key(value.mint) == self._mint_key(mint)
                    ),
                    None,
                )
                if position is None:
                    continue
                key = f"sell:{position.position_id}"
                reason = position.exit_intent_reason or "RESTORED_EXIT_INTENT"
                context = {
                    "key": key,
                    "position": position,
                    "mint": position.mint,
                    "quantity": quantity,
                    "reason": reason,
                    "quote": None,
                    "partial": quantity < position.remaining_quantity_token,
                    "order_id": order_id,
                    "status": str(item.get("state") or "PENDING"),
                }
                self._live_sell_context[position.position_id] = context
                self._live_orders[order_id] = {"side": "sell", **context}

    @staticmethod
    def _balance_quantity(payload: Any, token: str) -> Decimal | None:
        values = payload.get("data") if isinstance(payload, dict) else payload
        if isinstance(values, dict):
            values = values.get("balances") or values.get("list") or [values]
        if not isinstance(values, (list, tuple)):
            return None
        wanted = token.lower()
        for item in values:
            if not isinstance(item, Mapping):
                continue
            address = str(item.get("tokenAddress") or item.get("address") or "").lower()
            if address and address != wanted:
                continue
            raw = item.get("tokenAmount") or item.get("balance") or item.get("available") or item.get("free")
            parsed = _decimal(raw)
            if parsed is not None and parsed > 0:
                return parsed
        return None

    def _live_state(self, key: str, payload: Mapping[str, object]) -> None:
        self.store.set_state(key, dict(payload))

    def _schedule_order_poll(self, order_id: str, now: datetime) -> None:
        due = self._live_next_poll.get(order_id)
        if due is not None and now < due:
            return
        if self.live_bridge.submit("order_status", order_id) is not None:
            self._live_next_poll[order_id] = now + timedelta(seconds=2)

    def _drain_live_events(self, now: datetime) -> None:
        for event in self.live_bridge.poll(64):
            result = event.result
            if event.action == "quote_sell":
                position = self._position_mark_quote_context.pop(event.key, None)
                hard_stop_context = self._hard_stop_quote_context.pop(event.key, None)
                if hard_stop_context is not None:
                    position = hard_stop_context.get("position")
                if position is None or position.status != "OPEN":
                    continue
                quote = error = None
                if isinstance(result, tuple) and len(result) >= 2:
                    quote, error = result[0], result[1]
                elif isinstance(result, ExecutableQuote):
                    quote = result
                if quote is not None and getattr(quote, "output_quantity", Decimal("0")) > 0 and position.remaining_quantity_token > 0:
                    native = Decimal(str(quote.output_quantity)) / position.remaining_quantity_token
                    candidate = self._candidates.get(position.mint)
                    self._set_position_mark(
                        position,
                        native_price=native,
                        usd_price=self._position_mark_usd(native, candidate),
                        source="GMGN_SELL_QUOTE",
                        observed_at=event.completed_at,
                    )
                    if candidate is not None:
                        candidate.source_status.update({
                            "position_mark_sell_quote_price": str(native),
                            "position_mark_sell_quote_at": event.completed_at.isoformat(),
                            "position_mark_sell_quote_source": getattr(quote, "provider", self.live_provider),
                        })
                    if hard_stop_context is not None:
                        executable_return = self._current_mark_pnl_pct(position)
                        current_generation = self._position_mark_generation.get(position.position_id, 0)
                        if candidate is not None:
                            candidate.source_status.update({
                                "hard_stop_confirmation_quote_at": event.completed_at.isoformat(),
                                "hard_stop_confirmation_quote_price": str(native),
                                "hard_stop_confirmation_executable_return": str(executable_return) if executable_return is not None else None,
                                "hard_stop_confirmation_generation": current_generation,
                            })
                        # Do not let an old asynchronous response overwrite a
                        # newer confirmed exit.  A newer WSS mark may exist,
                        # but this quote remains a fresh executable check for
                        # the current wallet quantity and can still confirm
                        # the stop only while no other exit owns the position.
                        if (
                            position.exit_intent_reason is None
                            and executable_return is not None
                            and executable_return <= -self.config.hard_stop_pct
                        ):
                            self._record_exit_trigger(position, "HARD_STOP_PNL", event.completed_at, pnl=executable_return)
                            self._submit_live_sell(
                                position,
                                "HARD_STOP_PNL",
                                position.remaining_quantity_token,
                                event.completed_at,
                                confirmed_quote=quote,
                            )
                continue
            if event.action == "wallet_reconcile" and event.key.startswith("wallet-reconcile:"):
                position_id = event.key.removeprefix("wallet-reconcile:")
                actual_balance = result if isinstance(result, Decimal) else None
                self.reconcile_open_positions_with_wallet(
                    event.completed_at,
                    completed_position_id=position_id,
                    completed_balance=actual_balance,
                )
                continue
            if event.action == "buy":
                context = self._live_buy_context.get(event.key)
                if context is None and event.token is not None:
                    context = self._live_buy_context.get(event.token)
                if context is None:
                    continue
                if not isinstance(result, LiveSwapResult):
                    result = LiveSwapResult(stage="SWAP_UNKNOWN", error_code="INVALID_WORKER_RESULT", needs_reconciliation=True)
                if result.stage == "SWAP_PENDING" and not result.order_id and result.error_code == "APP_CONFIRMATION_REQUIRED":
                    self._set_entry_reservation_status(context["mint"], "WAITING_CONFIRMATION", now, error_code=result.error_code)
                    context["candidate"].source_status.update({"live_swap_status": "WAITING_BINANCE_CONFIRMATION", "live_swap_error": result.error_code})
                    context["candidate"].last_rejection = "WAITING_BINANCE_CONFIRMATION"
                    self._persist_candidate(context["candidate"], now)
                elif result.stage in {"SWAP_SUBMITTED", "SWAP_PENDING"} and result.order_id:
                    order_id = result.order_id
                    context.update({"order_id": order_id, "submitted_at": event.completed_at, "status": result.stage})
                    self._live_orders[order_id] = {"side": "buy", **context}
                    self._set_entry_reservation_status(context["mint"], "SWAP_PENDING", now, order_id=order_id, submitted_at=event.completed_at.isoformat())
                    self._live_state(f"live_order:{order_id}", {"side": "buy", "mint": context["mint"], "status": result.stage, "order_id": order_id, "submitted_at": event.completed_at.isoformat()})
                    self._schedule_order_poll(order_id, now)
                else:
                    context["candidate"].last_rejection = result.error_code or result.stage
                    context["candidate"].source_status.update({"live_swap_status": result.stage, "live_swap_error": result.error_code or "UNKNOWN"})
                    if result.stage == "SWAP_FAILED" and result.error_code in self._DEFINITIVE_PRE_SUBMIT_FAILURES:
                        # These executor failures occur before the swap CLI is
                        # invoked, so no provider order can exist.
                        self._release_entry_reservation(context["mint"], now, result.error_code)
                        self._live_buy_context.pop(event.key, None)
                    else:
                        # A provider timeout, malformed result, or failed
                        # response without an order id is not evidence that
                        # nothing was submitted. Keep this slot occupied.
                        self._set_entry_reservation_status(context["mint"], "SWAP_UNKNOWN", now, error_code=result.error_code or result.stage)
                        self._live_swap_unknown += 1
                    self._persist_candidate(context["candidate"], now)
                continue
            if event.action == "sell":
                context = self._live_sell_context.get(event.key)
                if context is None:
                    context = next((item for item in self._live_sell_context.values() if item.get("key") == event.key), None)
                if context is None:
                    continue
                if not isinstance(result, LiveSwapResult):
                    result = LiveSwapResult(stage="SWAP_UNKNOWN", error_code="INVALID_WORKER_RESULT", needs_reconciliation=True)
                if result.stage == "SWAP_PENDING" and not result.order_id and result.error_code == "APP_CONFIRMATION_REQUIRED":
                    self._set_exit_intent(context["position"], context["reason"], context["quantity"], now, result.error_code)
                    self._persist_live_exit_status(context["position"].position_id, "WAITING_CONFIRMATION", now)
                elif result.stage in {"SWAP_SUBMITTED", "SWAP_PENDING"} and result.order_id:
                    order_id = result.order_id
                    context.update({"order_id": order_id, "submitted_at": event.completed_at, "status": result.stage})
                    self._live_orders[order_id] = {"side": "sell", **context}
                    self._persist_live_exit_status(context["position"].position_id, result.stage, now, order_id=order_id, tx_hash=result.tx_hash)
                    self._live_state(f"live_order:{order_id}", {"side": "sell", "position_id": context["position"].position_id, "status": result.stage, "order_id": order_id, "submitted_at": event.completed_at.isoformat()})
                    self._schedule_order_poll(order_id, now)
                else:
                    self._set_exit_intent(context["position"], context["reason"], context["quantity"], now, result.error_code or result.stage)
                    self._persist_live_exit_status(context["position"].position_id, "SWAP_UNKNOWN" if result.stage == "SWAP_UNKNOWN" else "SWAP_FAILED", now)
                    if result.stage == "SWAP_UNKNOWN":
                        self._live_swap_unknown += 1
                    self._live_sell_context.pop(context["position"].position_id, None)
                continue
            if event.action == "balance":
                context = self._live_orders.get(event.key)
                if context is None:
                    continue
                if context.get("side") != "buy":
                    continue
                quantity = self._balance_quantity(result, context["mint"])
                if quantity is None:
                    context["balance_pending"] = True
                    self._schedule_order_poll(context["order_id"], now)
                    continue
                candidate = context["candidate"]
                buy_quote = context["buy_quote"]
                position = SurvivorPosition(
                    position_id=f"live:{candidate.mint}:{uuid4().hex[:12]}", mint=candidate.mint, symbol=candidate.symbol,
                    opened_at=now, entry_price_native=buy_quote.input_quantity / quantity,
                    # The receipt/balance-derived entry price is always a
                    # valid initial BNB mark, even if Candidate canonical USD
                    # conversion is pending or this token is migrating venue.
                    current_price_native=buy_quote.input_quantity / quantity, quantity_token=quantity,
                    remaining_quantity_token=quantity, invested_bnb=buy_quote.input_quantity,
                    position_mark_price_native=buy_quote.input_quantity / quantity,
                    position_mark_price_usd=self._position_mark_usd(buy_quote.input_quantity / quantity, candidate),
                    position_mark_source="GMGN_ENTRY_RECEIPT",
                    position_price_updated_at=now,
                    position_price_freshness="FRESH",
                )
                self._positions[position.position_id] = position
                self.connection.execute(
                    "INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,status,entry_price_native,entry_price_usd,current_price_native,position_mark_price_native,position_mark_price_usd,position_mark_source,position_price_updated_at,position_price_freshness,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,last_trade_at,quote_source,updated_at,entry_holders,entry_market_cap_usd,entry_liquidity_usd,entry_order_id,entry_swap_status,entry_submitted_at,entry_confirmed_at,actual_entry_quantity_token,actual_entry_spend_bnb,actual_entry_price_native) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (position.position_id, position.mint, position.symbol, now.isoformat(), "OPEN", str(position.entry_price_native), str(candidate.current_price_usd) if candidate.current_price_usd is not None else None, str(position.current_price_native), str(position.position_mark_price_native), str(position.position_mark_price_usd) if position.position_mark_price_usd is not None else None, position.position_mark_source, now.isoformat(), position.position_price_freshness, str(quantity), str(quantity), str(position.invested_bnb), "0", now.isoformat(), self.live_provider, now.isoformat(), candidate.holders, str(candidate.market_cap_usd) if candidate.market_cap_usd is not None else None, str(candidate.liquidity_usd) if candidate.liquidity_usd is not None else None, context["order_id"], "SWAP_CONFIRMED", context.get("submitted_at").isoformat() if context.get("submitted_at") else None, now.isoformat(), str(quantity), str(position.invested_bnb), str(position.entry_price_native)),
                )
                self._capture_live_entry_snapshot(position, candidate, context, now)
                self.connection.commit()
                candidate.state = "POSITION_OPEN"
                candidate.paper_buy = False
                candidate.source_status.update({"live_swap_status": "SWAP_CONFIRMED", "live_order_id": context["order_id"]})
                candidate.source_status.update({
                    "actual_entry_price": str(position.entry_price_native),
                    "actual_entry_price_source": "GMGN_RECEIPT_AND_BALANCE",
                    "actual_entry_price_at": now.isoformat(),
                })
                self._live_buy_context.pop(context["key"], None)
                self._live_orders.pop(event.key, None)
                self._live_orders.pop(context["order_id"], None)
                self._release_entry_reservation(context["mint"], now, "SWAP_CONFIRMED_OPEN")
                self._live_state(f"live_order:{context['order_id']}", {"status": "SWAP_CONFIRMED", "position_id": position.position_id, "tx_hash": context.get("tx_hash")})
                self._audit_event("SURVIVOR_LIVE_ENTRY_CONFIRMED", candidate, {"position_id": position.position_id, "order_id": context["order_id"], "quantity": str(quantity)})
                self._notify_live_event("LIVE_ENTRY_CONFIRMED", candidate, {
                    "position_id": position.position_id,
                    "order_id": context["order_id"],
                    "tx_hash": context.get("tx_hash"),
                    "occurred_at": now.isoformat(),
                    "entry_price_native": str(position.entry_price_native),
                    "entry_holders": candidate.holders,
                    "input_quantity": str(position.invested_bnb),
                    "entry_liquidity_usd": str(candidate.liquidity_usd) if candidate.liquidity_usd is not None else None,
                    "actual_received": str(quantity),
                    "settlement_verified": True,
                })
                continue
            if event.action == "order_status":
                context = self._live_orders.get(event.key)
                if context is None or not isinstance(result, LiveSwapResult):
                    continue
                if result.stage in {"SWAP_PENDING", "SWAP_UNKNOWN"}:
                    if result.stage == "SWAP_UNKNOWN":
                        self._live_swap_unknown += 1
                    if context.get("side") == "buy":
                        self._set_entry_reservation_status(context["mint"], result.stage, now, order_id=event.key, error_code=result.error_code)
                    self._schedule_order_poll(event.key, now)
                    if context.get("side") == "sell":
                        self._persist_live_exit_status(context["position"].position_id, result.stage, now)
                    continue
                if result.stage == "SWAP_FAILED":
                    if context.get("side") == "sell":
                        self._set_exit_intent(context["position"], context["reason"], context["quantity"], now, result.error_code or "SWAP_FAILED")
                        self._persist_live_exit_status(context["position"].position_id, "SWAP_FAILED", now)
                    else:
                        context["candidate"].last_rejection = result.error_code or "SWAP_FAILED"
                        self._live_buy_context.pop(context["key"], None)
                        # An order status explicitly reported FAILED, so its
                        # provider order cannot later fill. It is now safe to
                        # release the capacity reservation.
                        self._release_entry_reservation(context["mint"], now, result.error_code or "SWAP_FAILED")
                    self._live_orders.pop(event.key, None)
                    continue
                if result.stage == "SWAP_CONFIRMED":
                    context["tx_hash"] = result.tx_hash
                    context["result"] = result
                    if context.get("side") == "buy":
                        self._live_state(f"live_order:{event.key}", {"status": "SWAP_CONFIRMED", "order_id": event.key, "tx_hash": result.tx_hash, "confirmed_at": now.isoformat()})
                        context["key"] = context.get("key", f"buy:{context['mint']}")
                        if self.live_bridge.submit("balance", f"balance:{event.key}", context["mint"]) is not None:
                            self._live_orders[f"balance:{event.key}"] = context
                    else:
                        position = context["position"]
                        proceeds = result.output_quantity or Decimal("0")
                        position.realized_bnb += proceeds
                        position.remaining_quantity_token = max(Decimal("0"), position.remaining_quantity_token - context["quantity"])
                        position.current_price_native = proceeds / context["quantity"] if context["quantity"] > 0 and proceeds > 0 else position.current_price_native
                        candidate = self._candidates.get(position.mint)
                        if candidate is not None and context["quantity"] > 0 and proceeds > 0:
                            candidate.source_status.update({
                                "actual_exit_price": str(proceeds / context["quantity"]),
                                "actual_exit_price_source": "GMGN_RECEIPT",
                                "actual_exit_price_at": now.isoformat(),
                            })
                            self._persist_candidate(candidate, now)
                        if context.get("partial"):
                            self._mark_tp_filled(position, str(context.get("reason") or ""), now)
                            if str(context.get("reason") or "").startswith("NO_TRADE_") and str(context.get("reason") or "").endswith("_PNL_GT_10_PARTIAL"):
                                position.no_trade_profit_partial_done = True
                        if position.remaining_quantity_token <= 0:
                            position.status = "CLOSED"
                        self._persist_position_live(position, now, context["reason"], event.key, result)
                        self._record_live_outcome(position, now, final_reason=context["reason"] if position.status == "CLOSED" else None)
                        self.connection.commit()
                        if position.status == "CLOSED":
                            self._maybe_write_entry_outcome_report()
                        self._clear_exit_intent(position)
                        self._live_sell_context.pop(position.position_id, None)
                        self._live_orders.pop(event.key, None)
                        self._audit_event("SURVIVOR_LIVE_EXIT_CONFIRMED", self._candidates.get(position.mint), {"position_id": position.position_id, "order_id": event.key, "tx_hash": result.tx_hash, "proceeds_bnb": str(proceeds)})
                        exit_price = proceeds / context["quantity"] if context["quantity"] > 0 and proceeds > 0 else None
                        entry_price = position.entry_price_native
                        return_pct = (
                            (exit_price / entry_price - Decimal("1")) * Decimal("100")
                            if exit_price is not None and entry_price > 0
                            else None
                        )
                        candidate = self._candidates.get(position.mint)
                        self._notify_live_event("LIVE_EXIT_CONFIRMED", candidate, {
                            "position_id": position.position_id,
                            "mint": position.mint,
                            "token_name": position.symbol,
                            "order_id": event.key,
                            "tx_hash": result.tx_hash,
                            "occurred_at": now.isoformat(),
                            "reason": context["reason"],
                            "exit_price_native": str(exit_price) if exit_price is not None else None,
                            "exit_holders": candidate.holders if candidate is not None else None,
                            "exit_liquidity_usd": str(candidate.liquidity_usd) if candidate is not None and candidate.liquidity_usd is not None else None,
                            "actual_received": str(proceeds),
                            "return_pct": str(return_pct) if return_pct is not None else None,
                            "settlement_verified": True,
                        })
                        # TP2/TP3 may have been crossed while this prior TP
                        # was pending.  Continue the durable queue immediately
                        # without requiring a fresh high mark.
                        if position.status == "OPEN":
                            self._execute_latched_tp(position, now)

    def _persist_position_live(self, position: SurvivorPosition, now: datetime, reason: str, order_id: str, result: LiveSwapResult) -> None:
        self._persist_position(position, now, reason if position.status == "CLOSED" else None)
        self.connection.execute("UPDATE survivor_positions SET exit_order_id=?,exit_tx_hash=?,exit_swap_status=?,exit_submitted_at=?,exit_confirmed_at=?,actual_exit_quantity_token=?,actual_exit_proceeds_bnb=? WHERE position_id=?", (order_id, result.tx_hash, "SWAP_CONFIRMED", None, now.isoformat(), str(result.input_quantity or ""), str(result.output_quantity or ""), position.position_id))
        self.connection.commit()

    def _persist_live_exit_status(
        self,
        position_id: str,
        status: str,
        now: datetime,
        *,
        order_id: str | None = None,
        tx_hash: str | None = None,
    ) -> None:
        try:
            self.connection.execute(
                "UPDATE survivor_positions SET exit_swap_status=?,exit_order_id=COALESCE(?,exit_order_id),"
                "exit_tx_hash=COALESCE(?,exit_tx_hash),updated_at=? WHERE position_id=?",
                (status, order_id, tx_hash, now.isoformat(), position_id),
            )
            self.connection.commit()
        except Exception:
            try:
                self.connection.rollback()
            except Exception:
                pass

    def _try_buy(self, candidate: SurvivorCandidate, now: datetime) -> None:
        if self.controls.paused("live"):
            candidate.last_rejection = "ENTRY_PAUSED"
            return
        if not self.live_entry_funds_available:
            candidate.last_rejection = "LIVE_EXECUTOR_BNB_BALANCE_INSUFFICIENT"
            candidate.source_status.update({"live_entry": "BLOCKED_LOW_BALANCE"})
            return
        # Capacity is based on the wallet, not only the last five-second
        # reconciliation result. A manual external sale can therefore free a
        # slot immediately before this Candidate attempts a real BUY.
        self.reconcile_open_positions_with_wallet(now, force=True)
        if candidate.mint in self._live_buy_context or any(item.get("mint") == candidate.mint for item in self._live_orders.values()):
            return
        if self._mint_key(candidate.mint) in self._entry_reservations:
            candidate.last_rejection = "ENTRY_CAPACITY_RESERVED"
            return
        if any(position.status == "OPEN" and self._mint_key(position.mint) == self._mint_key(candidate.mint) for position in self._positions.values()):
            candidate.last_rejection = "SAME_TOKEN_ALREADY_OPEN"
            return
        if self._same_token_cooldown_active(candidate, now):
            return
        provider = getattr(self, "entry_quote_provider", None)
        if provider is None:
            candidate.last_rejection = "QUOTE_GATE_FAILED"
            return
        buy, sell, error = provider.quote_candidate(candidate.mint, self.live_amount_bnb, {})
        if error == "QUOTE_PENDING":
            candidate.last_rejection = "QUOTE_PENDING"
            candidate.quote_state = "PENDING"
            return
        if buy is None or sell is None or buy.output_quantity <= 0 or sell.output_quantity <= 0 or buy.provider != self.live_provider or sell.provider != self.live_provider:
            candidate.last_rejection = error or "ROUNDTRIP_GATE_FAILED"
            candidate.source_status.update({"quote_gate": "ROUNDTRIP_GATE_FAILED", "live_buy": "PAPER_BUY_SKIPPED"})
            return
        # Metadata/route failures are transient.  A later successful
        # roundtrip must clear the old rejection rather than leave a Candidate
        # permanently labelled TOKEN_DECIMALS_UNAVAILABLE.
        candidate.last_rejection = None
        candidate.quote_requested = True
        candidate.quote_state = "VALID"
        candidate.source_status.pop("quote_error", None)
        candidate.source_status.update({
            "quote_gate": "ROUNDTRIP_GATE_PASSED",
            "live_buy": "LIVE_SWAP_PENDING_SUBMISSION",
            "executable_buy_quote_price": str(buy.input_quantity / buy.output_quantity),
            "executable_buy_quote_price_source": buy.provider,
            "executable_buy_quote_at": _iso(getattr(buy, "quoted_at", None)) or now.isoformat(),
            "executable_sell_quote_price": str(sell.output_quantity / buy.output_quantity),
            "executable_sell_quote_price_source": sell.provider,
            "executable_sell_quote_at": _iso(getattr(sell, "quoted_at", None)) or now.isoformat(),
        })
        reservation = self._reserve_entry_slot(candidate, now)
        if reservation is None:
            self._persist_candidate(candidate, now)
            return
        key = f"buy:{candidate.mint}"
        if not self._set_entry_reservation_status(candidate.mint, "SWAP_SUBMITTING", now):
            self._release_entry_reservation(candidate.mint, now, "RESERVATION_UPDATE_FAILED")
            candidate.last_rejection = "ENTRY_CAPACITY_PERSIST_FAILED"
            self._persist_candidate(candidate, now)
            return
        request_id = self.live_bridge.submit("buy", key, candidate.mint, self.live_amount_bnb)
        if request_id is None:
            # The bridge rejected before it could run an executor call.
            self._release_entry_reservation(candidate.mint, now, "BRIDGE_NOT_ACCEPTED")
            candidate.last_rejection = "LIVE_SWAP_PENDING"
            self._persist_candidate(candidate, now)
            return
        self._live_buy_context[candidate.mint] = {"key": key, "mint": candidate.mint, "candidate": candidate, "buy_quote": buy, "sell_quote": sell, "submitted_at": now, "reservation_id": reservation["reservation_id"]}
        candidate.source_status.update({"live_swap_status": "SWAP_SUBMITTING", "live_executor": self.live_provider})
        self._persist_candidate(candidate, now)

    def _submit_live_sell(
        self,
        position: SurvivorPosition,
        reason: str,
        quantity: Decimal,
        now: datetime,
        *,
        partial: bool = False,
        confirmed_quote: ExecutableQuote | None = None,
    ) -> None:
        # A manual dashboard sell may already own the position-level order.
        # Check the persisted state before the automatic path submits another
        # live order. This closes the restart/process boundary as well as the
        # in-memory single-flight guard.
        try:
            persisted = self.connection.execute(
                "SELECT exit_swap_status FROM survivor_positions WHERE position_id=?",
                (position.position_id,),
            ).fetchone()
            persisted_status = str(persisted[0] or "") if persisted is not None else ""
            if persisted is not None and persisted_status in {
                "MANUAL_SUBMITTING",
                "SWAP_SUBMITTING",
                "SWAP_SUBMITTED",
                "SWAP_PENDING",
                "WAITING_CONFIRMATION",
                "SWAP_UNKNOWN",
                # A confirmed provider failure must be retried explicitly
                # (Dashboard/manual retry or a fresh operator decision).  An
                # automatic evaluation tick must never submit the same sell
                # again after the failed response has been persisted.
                "BALANCE_SYNC_PENDING",
            }:
                return
            # A confirmed TP provider failure must not erase a price crossing.
            # Let the existing executor retry this durable TP intent; other
            # exit reasons retain their existing explicit-retry behavior.
            if persisted_status == "SWAP_FAILED" and not reason.startswith("TP"):
                return
        except Exception:
            # The normal owner-loop write path will retry; do not turn a
            # dashboard visibility read into a strategy failure.
            pass
        self._record_exit_trigger(position, reason, now)
        self._set_exit_intent(position, reason, quantity, now, None)
        if position.position_id in self._live_sell_context:
            return
        provider = getattr(self, "exit_quote_provider", None)
        quote = confirmed_quote
        error = None
        if quote is None and provider is not None and hasattr(provider, "sell_quote"):
            quote, error = provider.sell_quote(position.mint, quantity)
        if quote is None or quote.output_quantity <= 0 or quote.provider != self.live_provider:
            return
        candidate = self._candidates.get(position.mint)
        if candidate is not None and quantity > 0:
            candidate.source_status.update({
                "executable_sell_quote_price": str(quote.output_quantity / quantity),
                "executable_sell_quote_price_source": quote.provider,
                "executable_sell_quote_at": _iso(getattr(quote, "quoted_at", None)) or now.isoformat(),
            })
            self._persist_candidate(candidate, now)
        key = f"sell:{position.position_id}"
        # Claim the position in SQLite before submitting the external request
        # so a concurrent Dashboard click cannot race the automatic exit.
        self._persist_live_exit_status(position.position_id, "SWAP_SUBMITTING", now)
        if self.live_bridge.submit("sell", key, position.mint, quantity) is None:
            self._persist_live_exit_status(position.position_id, "SWAP_FAILED", now)
            return
        self._live_sell_context[position.position_id] = {"key": key, "position": position, "mint": position.mint, "quantity": quantity, "reason": reason, "quote": quote, "partial": partial}

    def _close_position(self, position: SurvivorPosition, reason: str, now: datetime) -> None:
        self._submit_live_sell(position, reason, position.remaining_quantity_token, now)

    def _partial_exit(self, position: SurvivorPosition, fraction: Decimal, reason: str, now: datetime) -> bool:
        self._submit_live_sell(position, reason, position.remaining_quantity_token * fraction, now, partial=True)
        return False

    def evaluate(self, now: datetime | None = None) -> None:
        current = now or self.clock()
        self._drain_live_events(current)
        self.reconcile_open_positions_with_wallet(current)
        self._schedule_position_monitoring(current)
        for order_id, context in tuple(self._live_orders.items()):
            if order_id.startswith("balance:"):
                continue
            self._schedule_order_poll(order_id, current)
        super().evaluate(current)
        for position_id in tuple(self._entry_outcome_mark_dirty):
            position = self._positions.get(position_id)
            if position is not None and position.status == "OPEN":
                self._record_live_outcome(position, current)
        self._entry_outcome_mark_dirty.clear()
        self.connection.commit()

    def status(self) -> dict[str, object]:
        payload = super().status()
        capacity = self._entry_capacity()
        payload.update(capacity)
        payload["entry_capacity"] = capacity
        return payload

    def _publish(self, now: datetime) -> None:
        super()._publish(now)
        payload = self.status()
        payload["paper_only"] = False
        payload["live_executor"] = self.live_provider
        payload["live_swap_unknown"] = self._live_swap_unknown
        payload["live_bridge"] = self.live_bridge.status()
        self.store.set_state("survivor_balanced_v1", payload)
        self.health.set("survivor_balanced", "HEALTHY", details={"strategy": self.config.identity.as_dict(), "paper_only": False, "live_executor": self.live_provider, "live_swap_unknown": self._live_swap_unknown, "entry_capacity": self._entry_capacity(), "bridge": self.live_bridge.status(), "db_commit_error_count": self._db_commit_error_count, "db_locked_error_count": self._db_locked_error_count, "db_write_error_count": self._db_write_error_count, "last_db_write_at": _iso(self._last_db_write_at)})
