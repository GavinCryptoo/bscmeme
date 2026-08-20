"""Solana chain profile for the shared Survivor Reversal state machine.

The module reuses the existing lifecycle, price-history, ATH, pullback,
position and persistence core.  It only supplies Solana-specific config,
protocol/WSS binding, verified balance-delta flow and read-only quote routing.
There is intentionally no transaction, wallet, signing or broadcast path.
"""

from __future__ import annotations

import os
import time
from collections import Counter, deque
from concurrent.futures import Future, ThreadPoolExecutor
from threading import RLock
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Mapping, Sequence
from uuid import uuid4

from meme_system.adapters.protocols import ExecutableQuote
from meme_system.adapters.solana_price import SolanaObservedPrice, SolanaPriceBinding, SolanaPriceMonitor
from meme_system.adapters.solana_swap_flow import (
    ParsedSolanaSwap,
    SOL_MINT,
    USDC_MINT,
    USDT_MINT,
    parse_pumpswap_transaction,
)
from meme_system.domain.models import SOL_SURVIVOR_REVERSAL_IDENTITY
from meme_system.strategies.survivor_reversal import (
    COMPLETE_PRICE_HISTORY_STATUSES,
    FlowSample,
    PRICE_HISTORY_QUALITY_GOOD,
    PRICE_HISTORY_QUALITY_INSUFFICIENT,
    SurvivorCandidate,
    SurvivorPosition,
    SurvivorReversalConfig,
    SurvivorReversalEngine,
    _decimal,
    _field,
    _iso,
    _utc_now,
)


@dataclass(frozen=True)
class SolSurvivorReversalConfig(SurvivorReversalConfig):
    enabled: bool = True
    execution_provider: str = "paper"
    position_size_bnb: Decimal = Decimal("0.001")
    active_max: int = 150
    min_age_sec: int = 180
    max_candidate_age_sec: int = 3600
    min_active_mc_usd: Decimal = Decimal("15000")
    min_active_liquidity_usd: Decimal = Decimal("5000")
    min_active_holders: int = 30
    universe_min_mc_usd: Decimal = Decimal("15000")
    universe_max_mc_usd: Decimal = Decimal("3000000")
    universe_min_liquidity_usd: Decimal = Decimal("5000")
    universe_min_holders: int = 30
    drawdown_min_pct: Decimal = Decimal("20")
    drawdown_max_pct: Decimal = Decimal("50")
    min_lp_mc_pct: Decimal = Decimal("0")
    no_trade_exit_sec: int = 10**9
    pre_candidate_watch_max: int = 50
    max_buy_price_impact: Decimal = Decimal("0.05")
    max_sell_price_impact: Decimal = Decimal("0.08")
    max_quote_age_ms: int = 3000
    max_price_source_switch_gap: Decimal = Decimal("0.15")
    max_candidate_price_usd: Decimal = Decimal("0.0001")
    max_open_positions: int = 20
    hard_stop_pct: Decimal = Decimal("80")
    tp1_gain_pct: Decimal = Decimal("50")
    tp1_sell_pct: Decimal = Decimal("30")
    tp1_stop_pct: Decimal = Decimal("50")
    tp2_gain_pct: Decimal = Decimal("100")
    tp2_sell_pct: Decimal = Decimal("30")
    tp3_gain_pct: Decimal = Decimal("200")
    tp3_sell_pct: Decimal = Decimal("40")
    holder_drop_window_sec: int = 120
    holder_drop_pct: Decimal = Decimal("20")
    async_network_io: bool = False
    data_quality_start_at: datetime = field(default_factory=_utc_now)

    @property
    def strategy_name(self) -> str:
        return SOL_SURVIVOR_REVERSAL_IDENTITY.strategy_name

    def effective_values(self) -> dict[str, object]:
        return {
            "SOL_SURVIVOR_ENABLED": self.enabled,
            "SOL_MIN_AGE_SECONDS": self.min_age_sec,
            "SOL_MAX_CANDIDATE_AGE_SECONDS": self.max_candidate_age_sec,
            "SOL_PRE_CANDIDATE_MAX": self.pre_candidate_watch_max,
            "SOL_CANDIDATE_MIN_MC": str(self.min_active_mc_usd),
            "SOL_CANDIDATE_MIN_LIQUIDITY": str(self.min_active_liquidity_usd),
            "SOL_CANDIDATE_MIN_HOLDERS": self.min_active_holders,
            "SOL_ENTRY_MIN_MC": str(self.universe_min_mc_usd),
            "SOL_ENTRY_MAX_MC": str(self.universe_max_mc_usd),
            "SOL_ENTRY_MIN_LIQUIDITY": str(self.universe_min_liquidity_usd),
            "SOL_ENTRY_MIN_HOLDERS": self.universe_min_holders,
            "SOL_PULLBACK_MIN": str(self.drawdown_min_pct / Decimal("100")),
            "SOL_PULLBACK_MAX": str(self.drawdown_max_pct / Decimal("100")),
            "SOL_STOP_CONFIRM_SECONDS": self.low_stable_sec,
            "SOL_MIN_REBOUND": str(self.rebound_min_pct / Decimal("100")),
            "SOL_MAX_REBOUND": str(self.rebound_max_pct / Decimal("100")),
            "SOL_MIN_BUY_SELL_VOLUME_RATIO_1M": str(self.buy_sell_volume_ratio),
            "SOL_MIN_BUY_SELL_COUNT_RATIO_1M": str(self.buy_sell_count_ratio),
            "SOL_MIN_SWAP_COUNT_1M": self.min_swap_count,
            "SOL_MAX_BUY_PRICE_IMPACT": str(self.max_buy_price_impact),
            "SOL_MAX_SELL_PRICE_IMPACT": str(self.max_sell_price_impact),
            "SOL_MAX_QUOTE_AGE_MS": self.max_quote_age_ms,
            "SOL_MAX_PRICE_SOURCE_SWITCH_GAP": str(self.max_price_source_switch_gap),
            "SOL_MAX_CANDIDATE_PRICE_USD": str(self.max_candidate_price_usd),
            "SOL_MAX_OPEN_POSITIONS": self.max_open_positions,
            "SOL_HARD_STOP_PCT": str(self.hard_stop_pct / Decimal("100")),
            "SOL_TP1_GAIN_PCT": str(self.tp1_gain_pct / Decimal("100")),
            "SOL_TP1_SELL_PCT": str(self.tp1_sell_pct / Decimal("100")),
            "SOL_TP1_STOP_PCT": str(self.tp1_stop_pct / Decimal("100")),
            "SOL_TP2_GAIN_PCT": str(self.tp2_gain_pct / Decimal("100")),
            "SOL_TP2_SELL_PCT": str(self.tp2_sell_pct / Decimal("100")),
            "SOL_TP3_GAIN_PCT": str(self.tp3_gain_pct / Decimal("100")),
            "SOL_TP3_SELL_PCT": str(self.tp3_sell_pct / Decimal("100")),
            "SOL_HOLDER_DROP_WINDOW_SECONDS": self.holder_drop_window_sec,
            "SOL_HOLDER_DROP_PCT": str(self.holder_drop_pct / Decimal("100")),
            "ALLOW_LIVE_TRADING": False,
            "EXECUTION_PROVIDER": "paper",
            "SOL_SIGNING_ENABLED": False,
            "SOL_BROADCAST_ENABLED": False,
        }

    def diagnostic_values(self) -> dict[str, object]:
        return {
            "SOL_PRE_CANDIDATE_MAX": self.pre_candidate_watch_max,
            "SOL_SURVIVOR_DATA_QUALITY_START_AT": self.data_quality_start_at.isoformat(),
            "DATA_QUALITY_COHORT": "SOL_FRESH_V1",
        }

    def self_check(self) -> dict[str, object]:
        expected = self.effective_values()
        drift: dict[str, dict[str, object]] = {}
        for name, value in expected.items():
            if name not in os.environ:
                continue
            raw = os.environ[name].strip()
            expected_text = str(value).lower() if isinstance(value, bool) else str(value)
            actual_text = raw.lower() if isinstance(value, bool) else raw
            try:
                if not isinstance(value, (bool, int)):
                    actual_text = str(Decimal(raw))
            except Exception:
                pass
            if actual_text != expected_text:
                drift[name] = {"expected": value, "actual": raw}
        return {
            "strategy": self.strategy_name,
            "effective": expected,
            "config_drift": drift,
            "status": "CONFIG_DRIFT" if drift else "OK",
        }

    @classmethod
    def from_env(cls) -> "SolSurvivorReversalConfig":
        raw_start = os.environ.get("SOL_SURVIVOR_DATA_QUALITY_START_AT", "").strip()
        try:
            started = datetime.fromisoformat(raw_start) if raw_start else _utc_now()
            if started.tzinfo is None:
                started = started.replace(tzinfo=timezone.utc)
        except ValueError:
            started = _utc_now()
        return cls(
            enabled=os.environ.get("SOL_SURVIVOR_ENABLED", "true").strip().lower() == "true",
            async_network_io=os.environ.get("SOL_ASYNC_NETWORK_IO", "true").strip().lower() == "true",
            execution_provider=os.environ.get("EXECUTION_PROVIDER", "paper").strip().lower(),
            data_quality_start_at=started,
            max_candidate_age_sec=int(os.environ.get("SOL_MAX_CANDIDATE_AGE_SECONDS", "3600").strip()),
            min_active_mc_usd=Decimal(os.environ.get("SOL_CANDIDATE_MIN_MC", "15000").strip()),
            min_active_liquidity_usd=Decimal(os.environ.get("SOL_CANDIDATE_MIN_LIQUIDITY", "5000").strip()),
            min_active_holders=int(os.environ.get("SOL_CANDIDATE_MIN_HOLDERS", "30").strip()),
            universe_min_mc_usd=Decimal(os.environ.get("SOL_ENTRY_MIN_MC", "15000").strip()),
            universe_min_liquidity_usd=Decimal(os.environ.get("SOL_ENTRY_MIN_LIQUIDITY", "5000").strip()),
            universe_min_holders=int(os.environ.get("SOL_ENTRY_MIN_HOLDERS", "30").strip()),
            drawdown_min_pct=Decimal(os.environ.get("SOL_PULLBACK_MIN", "0.2").strip()) * Decimal("100"),
            drawdown_max_pct=Decimal(os.environ.get("SOL_PULLBACK_MAX", "0.5").strip()) * Decimal("100"),
            max_candidate_price_usd=Decimal(os.environ.get("SOL_MAX_CANDIDATE_PRICE_USD", "0.0001").strip()),
            max_open_positions=int(os.environ.get("SOL_MAX_OPEN_POSITIONS", "20").strip()),
            hard_stop_pct=Decimal(os.environ.get("SOL_HARD_STOP_PCT", "0.8").strip()) * Decimal("100"),
            tp1_gain_pct=Decimal(os.environ.get("SOL_TP1_GAIN_PCT", "0.5").strip()) * Decimal("100"),
            tp1_sell_pct=Decimal(os.environ.get("SOL_TP1_SELL_PCT", "0.3").strip()) * Decimal("100"),
            tp1_stop_pct=Decimal(os.environ.get("SOL_TP1_STOP_PCT", "0.5").strip()) * Decimal("100"),
            tp2_gain_pct=Decimal(os.environ.get("SOL_TP2_GAIN_PCT", "1").strip()) * Decimal("100"),
            tp2_sell_pct=Decimal(os.environ.get("SOL_TP2_SELL_PCT", "0.3").strip()) * Decimal("100"),
            tp3_gain_pct=Decimal(os.environ.get("SOL_TP3_GAIN_PCT", "2").strip()) * Decimal("100"),
            tp3_sell_pct=Decimal(os.environ.get("SOL_TP3_SELL_PCT", "0.4").strip()) * Decimal("100"),
            holder_drop_window_sec=int(os.environ.get("SOL_HOLDER_DROP_WINDOW_SECONDS", "120").strip()),
            holder_drop_pct=Decimal(os.environ.get("SOL_HOLDER_DROP_PCT", "0.2").strip()) * Decimal("100"),
        )

    def validate(self) -> None:
        super().validate()
        if self.max_open_positions < 1:
            raise ValueError("SOL_MAX_OPEN_POSITIONS must be at least 1")
        if self.max_candidate_age_sec < self.min_age_sec:
            raise ValueError("SOL_MAX_CANDIDATE_AGE_SECONDS must be at least SOL_MIN_AGE_SECONDS")
        if not (Decimal("0") < self.hard_stop_pct < Decimal("100")):
            raise ValueError("SOL_HARD_STOP_PCT must be between 0 and 1")
        if self.tp1_sell_pct + self.tp2_sell_pct + self.tp3_sell_pct != Decimal("100"):
            raise ValueError("SOL staged take-profit sell percentages must total 100")
        if self.holder_drop_window_sec < 1 or not (Decimal("0") < self.holder_drop_pct < Decimal("100")):
            raise ValueError("SOL holder-drop settings are invalid")
        if os.environ.get("ALLOW_LIVE_TRADING", "false").strip().lower() != "false":
            raise ValueError("MEME_SURVIVOR_REVERSAL_SOL_V1 requires ALLOW_LIVE_TRADING=false")
        if os.environ.get("SOL_SIGNING_ENABLED", "false").strip().lower() != "false":
            raise ValueError("MEME_SURVIVOR_REVERSAL_SOL_V1 requires SOL_SIGNING_ENABLED=false")
        if os.environ.get("SOL_BROADCAST_ENABLED", "false").strip().lower() != "false":
            raise ValueError("MEME_SURVIVOR_REVERSAL_SOL_V1 requires SOL_BROADCAST_ENABLED=false")


class SolSurvivorQuoteRouter:
    """Choose Pump before migration and Jupiter after migration, read-only."""

    def __init__(self, pump: Any, jupiter: Any, config: SolSurvivorReversalConfig) -> None:
        self.pump = pump
        self.jupiter = jupiter
        self.config = config
        self.last_provider = "NOT_REQUESTED"
        self.last_error: str | None = None
        # Four independent slots ensure one permanently stuck TLS handshake
        # cannot monopolize the only exit path for every open position.
        self._exit_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="sol-exit-quote")
        self._entry_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="sol-entry-quote")
        self._lock = RLock()
        self._inflight: dict[tuple[str, str, Decimal], Future[ExecutableQuote | None]] = {}
        self._results: dict[tuple[str, str, Decimal], ExecutableQuote | None] = {}
        self._failures = {"exit": 0, "entry": 0}
        self._circuit_until = {"exit": 0.0, "entry": 0.0}

    def _provider(self, mint: str) -> Any:
        if self.pump is not None:
            try:
                state = self.pump.state_adapter.inspect(mint)
                if state.state == "PUMP_BONDING_CURVE":
                    self.last_provider = "pump_bonding_curve_quote"
                    return self.pump
            except Exception:
                pass
        self.last_provider = "jupiter_quote"
        return self.jupiter

    def quote(self, mint: str, side: str, amount: Decimal) -> ExecutableQuote | None:
        """Return only a completed read-only quote; HTTPS runs on a worker."""
        if not self.config.async_network_io:
            return self._quote_sync(mint, side, amount, "exit" if side == "sell" else "entry")
        key = (mint, side, amount)
        lane = "exit" if side == "sell" else "entry"
        with self._lock:
            future = self._inflight.get(key)
            if future is not None and future.done():
                self._inflight.pop(key, None)
                try:
                    self._results[key] = future.result()
                except Exception:
                    self._results[key] = None
            result = self._results.pop(key, None)
            if result is not None:
                self._failures[lane] = 0
                return result
            if time.monotonic() < self._circuit_until[lane]:
                self.last_error = "SOL_EXIT_ROUTE_UNAVAILABLE" if lane == "exit" else "SOL_QUOTE_CIRCUIT_OPEN"
                return None
            if key not in self._inflight:
                executor = self._exit_executor if lane == "exit" else self._entry_executor
                self._inflight[key] = executor.submit(self._quote_sync, mint, side, amount, lane)
            self.last_error = "SOL_EXIT_ROUTE_PENDING" if lane == "exit" else "SOL_QUOTE_PENDING"
            return None

    def _quote_sync(self, mint: str, side: str, amount: Decimal, lane: str) -> ExecutableQuote | None:
        """Worker-only network path; it never accesses engine state or SQLite."""
        provider = self._provider(mint)
        if provider is None:
            self.last_error = "SOL_QUOTE_PROVIDER_UNAVAILABLE"
            return None
        try:
            quote = provider.quote(mint, side, amount)
        except (ArithmeticError, ValueError):
            # A venue can reject a fractional token amount that cannot be
            # encoded in its token base units.  That is a quote failure for
            # this one position, never a reason to interrupt the WSS/position
            # monitoring loop for every other SOL holding.
            self.last_error = "SOL_QUOTE_INVALID_TOKEN_QUANTITY"
            return None
        except Exception:
            with self._lock:
                self._failures[lane] += 1
                if self._failures[lane] >= 3:
                    self._circuit_until[lane] = time.monotonic() + 5.0
            self.last_error = "SOL_EXIT_ROUTE_UNAVAILABLE" if lane == "exit" else "SOL_QUOTE_UNAVAILABLE"
            return None
        if quote is None:
            with self._lock:
                self._failures[lane] += 1
                if self._failures[lane] >= 3:
                    self._circuit_until[lane] = time.monotonic() + 5.0
            self.last_error = "SOL_EXIT_ROUTE_UNAVAILABLE" if lane == "exit" else "SOL_QUOTE_UNAVAILABLE"
        return quote

    def shutdown(self) -> None:
        self._exit_executor.shutdown(wait=False, cancel_futures=True)
        self._entry_executor.shutdown(wait=False, cancel_futures=True)

    def quote_candidate(self, mint: str, input_sol: Decimal, _: Mapping[str, object]) -> tuple[ExecutableQuote | None, ExecutableQuote | None, str | None]:
        now = _utc_now()
        buy = self.quote(mint, "buy", input_sol)
        if buy is None or buy.output_quantity <= 0:
            return buy, None, getattr(buy, "error_class", None) or self.last_error or "BUY_QUOTE_UNAVAILABLE"
        sell = self.quote(mint, "sell", buy.output_quantity)
        for quote, maximum, label in (
            (buy, self.config.max_buy_price_impact, "BUY"),
            (sell, self.config.max_sell_price_impact, "SELL"),
        ):
            if quote is None or quote.output_quantity <= 0 or quote.unusable_reason(now):
                return buy, sell, (getattr(quote, "error_class", None) if quote else None) or f"{label}_QUOTE_UNAVAILABLE"
            age_ms = max(quote.age_ms, int((now - quote.quoted_at).total_seconds() * 1000))
            if age_ms > self.config.max_quote_age_ms:
                return buy, sell, f"{label}_QUOTE_EXPIRED"
            if quote.price_impact_pct is None:
                return buy, sell, f"{label}_PRICE_IMPACT_UNAVAILABLE"
            impact_ratio = abs(quote.price_impact_pct) / Decimal("100")
            if impact_ratio > maximum:
                return buy, sell, f"{label}_PRICE_IMPACT_TOO_HIGH"
        return buy, sell, None

    def status(self) -> dict[str, object]:
        return {"state": "NOT_REQUESTED" if self.last_provider == "NOT_REQUESTED" else "HEALTHY", "provider": self.last_provider, "last_error": self.last_error}


class SolSurvivorReversalEngine(SurvivorReversalEngine):
    def __init__(self, *, price_monitor: SolanaPriceMonitor, config: SolSurvivorReversalConfig, smart_money: Any = None, swap_rpc: Any = None, **kwargs: Any) -> None:
        self.price_monitor = price_monitor
        self.swap_rpc = swap_rpc or getattr(getattr(price_monitor, "pump_adapter", None), "rpc", None)
        self.smart_money = smart_money
        self._smart_money_state = "NOT_REQUESTED"
        self._smart_money_count = 0
        self._smart_money_last_fetch_at: datetime | None = None
        self._bindings: dict[str, SolanaPriceBinding] = {}
        self._wss_state = "IDLE"
        self._wss_stale = False
        self._source_switches = 0
        self._fresh_tolerance = timedelta(seconds=60)
        self._swap_trigger_queue: deque[tuple[datetime, str, str, int | None]] = deque(maxlen=128)
        self._queued_trigger_accounts: set[str] = set()
        self._signature_queue: deque[tuple[datetime, str, str, int | None]] = deque(maxlen=256)
        self._queued_signatures: set[tuple[str, str]] = set()
        self._seen_signatures: dict[tuple[str, str], datetime] = {}
        self._parsed_swap_queue: deque[ParsedSolanaSwap] = deque(maxlen=256)
        self._swap_events_detected = 0
        self._swap_events_parsed = 0
        self._swap_direction_buy = 0
        self._swap_direction_sell = 0
        self._swap_direction_unknown = 0
        self._get_transaction_calls = 0
        self._get_transaction_failures = 0
        self._get_transaction_429 = 0
        self._get_transaction_latency_ms: deque[int] = deque(maxlen=1000)
        self._last_wss_snapshot: dict[str, tuple[datetime, Decimal]] = {}
        self._holder_samples: dict[str, deque[tuple[datetime, int]]] = {}
        super().__init__(config=config, resolver=None, **kwargs)
        self._recover_swap_metrics()
        for candidate in list(self._candidates.values()):
            # Historical non-candidates are not strategy state.  Rewriting
            # every retained discovery row makes restart time proportional to
            # the multi-GB database without affecting current decisions.
            if not candidate.candidate_eligible and not candidate.active_candidate:
                continue
            candidate.data_quality_cohort = self._classify_cohort(candidate)
            if self._exclude_if_first_discovery_price_above_cap(candidate, self.clock()):
                continue
            # Recovery must remain bounded even when the retained snapshot
            # table is large.  The stored first-discovery price is a real
            # local observation and is sufficient under the current rule, so
            # do not scan the full historical snapshot table per candidate at
            # startup.
            if candidate.candidate_at is not None and candidate.candidate_eligible:
                self._freeze_candidate_history(candidate, (), candidate.candidate_at)
            self._apply_price_cap(candidate)
            self._persist_candidate(candidate, self.clock())
        self.connection.commit()

    def _recover_swap_metrics(self) -> None:
        try:
            rows = self.connection.execute(
                "SELECT signature,mint,direction,parse_finished_at,rpc_latency_ms,rpc_failed,rpc_http_429 FROM sol_survivor_swap_events ORDER BY parse_finished_at DESC LIMIT 4096"
            ).fetchall()
        except Exception:
            return
        self._swap_events_detected = len(rows)
        self._swap_direction_buy = sum(str(row[2]) == "BUY" for row in rows)
        self._swap_direction_sell = sum(str(row[2]) == "SELL" for row in rows)
        self._swap_direction_unknown = sum(str(row[2]) == "UNKNOWN" for row in rows)
        self._swap_events_parsed = self._swap_direction_buy + self._swap_direction_sell
        self._get_transaction_calls = len(rows)
        self._get_transaction_failures = sum(bool(row[5]) for row in rows)
        self._get_transaction_429 = sum(bool(row[6]) for row in rows)
        self._get_transaction_latency_ms.extend(int(row[4]) for row in rows if row[4] is not None)
        cutoff = _utc_now() - timedelta(minutes=30)
        for row in rows:
            try:
                parsed_at = datetime.fromisoformat(str(row[3]))
            except Exception:
                continue
            if parsed_at >= cutoff:
                self._seen_signatures[(str(row[0]), str(row[1]))] = parsed_at

    @staticmethod
    def _mint_key(mint: str) -> str:
        return mint

    def _data_quality_cohort(self, first_seen_at: datetime) -> str:
        # A discovery timestamp alone never proves native freshness. The
        # immutable createTime and first lifecycle/rank are classified after
        # the first normalized record and first valid price are available.
        return "BOOTSTRAP_EXISTING"

    def _fresh_cohort_name(self) -> str:
        return "NATIVE_FRESH"

    def _classify_cohort(self, candidate: SurvivorCandidate) -> str:
        created = candidate.token_created_at
        native = (
            created is not None
            and created >= self.data_quality_start_at - self._fresh_tolerance
            and candidate.first_lifecycle == "MEME_NEW"
            and candidate.first_rank_type == 10
            and candidate.first_price_at is not None
        )
        return "NATIVE_FRESH" if native else "BOOTSTRAP_EXISTING"

    def _apply_price_cap(self, candidate: SurvivorCandidate) -> bool:
        """Fail closed for the SOL-only strict USD Candidate ceiling."""
        if candidate.current_price_usd is None or candidate.current_price_usd < self.config.max_candidate_price_usd:
            return False
        candidate.candidate_eligible = False
        candidate.active_candidate = False
        candidate.ready_to_buy = False
        candidate.state = "LIGHT_TRACKING"
        candidate.last_rejection = "SOL_PRICE_AT_OR_ABOVE_0_0001"
        return True

    def _exclude_if_first_discovery_price_above_cap(self, candidate: SurvivorCandidate, now: datetime) -> bool:
        """Permanently discard tokens first discovered at or above the SOL cap.

        The ceiling is deliberately stricter than a current-price Candidate
        gate: a token that first appears expensive must not become eligible
        merely because a later snapshot falls below the ceiling.  Keep only
        the compact exclusion audit marker, then remove all tracking rows.
        """
        first_price = candidate.first_seen_price_usd
        if first_price is None or first_price < self.config.max_candidate_price_usd:
            return False
        mint = self._mint_key(candidate.mint)
        self.connection.execute(
            "INSERT OR IGNORE INTO survivor_exclusions(mint,reason,excluded_at) VALUES(?,?,?)",
            (mint, "SOL_FIRST_DISCOVERY_PRICE_AT_OR_ABOVE_0_0001", now.isoformat()),
        )
        self.connection.execute("DELETE FROM survivor_flow_samples WHERE mint=?", (mint,))
        self.connection.execute("DELETE FROM survivor_price_snapshots WHERE mint=?", (mint,))
        self.connection.execute("DELETE FROM survivor_candidates WHERE mint=?", (mint,))
        self._excluded_mints.add(mint)
        self._candidates.pop(candidate.mint, None)
        self._bindings.pop(candidate.mint, None)
        return True

    def _mark_candidate_entry(self, candidate: SurvivorCandidate, now: datetime) -> None:
        candidate.data_quality_cohort = self._classify_cohort(candidate)
        super()._mark_candidate_entry(candidate, now)
        if candidate.first_price_at is None or not candidate.ath_before_candidate:
            candidate.price_history_status = "PRICE_HISTORY_INCOMPLETE"
            candidate.source_status["price_history"] = "PENDING"

    def _freeze_candidate_history(
        self,
        candidate: SurvivorCandidate,
        samples: Sequence[FlowSample],
        candidate_at: datetime,
    ) -> None:
        """Freeze real prices observed locally since this system first saw the token."""
        before = sorted(
            [
                sample for sample in samples
                if (sample.price_usd is not None or sample.price_native is not None)
                and sample.observed_at <= candidate_at
            ],
            key=lambda sample: sample.observed_at,
        )
        # `first_seen_price_usd` is itself a real locally observed price.  A
        # snapshot write may lag that discovery event, so retain the stored
        # discovery observation as the pullback baseline without inventing a
        # candle or querying any price from before first discovery.
        discovery_price = candidate.first_seen_price_usd
        if discovery_price is not None and discovery_price > 0 and candidate.first_seen_at <= candidate_at:
            if not any(sample.observed_at == candidate.first_seen_at for sample in before):
                before.insert(0, FlowSample(
                    observed_at=candidate.first_seen_at,
                    price_usd=discovery_price,
                    price_source=candidate.first_seen_price_source or "MEME_RUSH",
                ))
            if candidate.first_price_at is None or candidate.first_price_at > candidate.first_seen_at:
                candidate.first_price_at = candidate.first_seen_at
        self._set_history_metadata(candidate, before)
        candidate.price_samples_before_candidate = len(before)
        candidate.price_coverage_before_candidate_seconds = (
            Decimal(str((before[-1].observed_at - before[0].observed_at).total_seconds()))
            if len(before) >= 2
            else Decimal("0")
        )
        priced = [
            (sample.price_usd or sample.price_native, sample)
            for sample in before
        ]
        if priced:
            ath_price, ath_sample = max(priced, key=lambda item: item[0])
            candidate.ath_before_candidate_price_usd = ath_price
            candidate.ath_before_candidate_at = ath_sample.observed_at
            candidate.ath_before_candidate = True
            if ath_sample.price_native is not None:
                candidate.ath_price_native = ath_sample.price_native
        else:
            candidate.ath_before_candidate_price_usd = None
            candidate.ath_before_candidate_at = None
            candidate.ath_before_candidate = False
        locally_observed = (
            candidate.first_price_at is not None
            and candidate.first_price_at <= candidate_at
            and candidate.ath_before_candidate
        )
        candidate.price_history_quality = (
            PRICE_HISTORY_QUALITY_GOOD if locally_observed else PRICE_HISTORY_QUALITY_INSUFFICIENT
        )
        candidate.price_history_status = "LIVE_USABLE" if locally_observed else "PRICE_HISTORY_INCOMPLETE"
        candidate.source_status["price_history"] = "VALID" if locally_observed else "PENDING"
        if locally_observed and candidate.last_rejection in {
            "BOOTSTRAP_EXISTING_RESEARCH_ONLY",
            "PRICE_HISTORY_INCOMPLETE",
            "PRICE_REQUIRED_BEFORE_BUY",
        }:
            candidate.last_rejection = None
        elif not locally_observed and candidate.last_rejection == "PRICE_HISTORY_INCOMPLETE":
            candidate.last_rejection = "PRICE_REQUIRED_BEFORE_BUY"

    def _recompute_candidate_history(self, candidate: SurvivorCandidate) -> None:
        """Recover from the stored first-discovery observation in constant time."""
        if candidate.candidate_at is not None:
            self._freeze_candidate_history(candidate, (), candidate.candidate_at)

    def _history_allows_pullback(self, candidate: SurvivorCandidate) -> bool:
        """Use only real prices recorded locally after first discovery."""
        return (
            candidate.price_history_status == "LIVE_USABLE"
            and candidate.price_history_quality == PRICE_HISTORY_QUALITY_GOOD
        )

    def _history_block_reason(self, candidate: SurvivorCandidate) -> str | None:
        if not self.config.require_pullback:
            return None
        if not self._history_allows_pullback(candidate):
            return "PRICE_REQUIRED_BEFORE_BUY"
        return None

    def _attempt_legacy_history_backfill(self, candidate: SurvivorCandidate) -> None:
        if candidate.data_quality_cohort != "BOOTSTRAP_EXISTING" or candidate.history_source is not None:
            return
        candidate.history_source = "NOT_REQUIRED_NEW_SOL_DATABASE"
        candidate.price_history_status = "BACKFILL_INSUFFICIENT"
        candidate.source_status["price_history"] = "SOURCE_UNAVAILABLE"

    def on_records(self, records: Sequence[Any], now: datetime | None = None) -> None:
        super().on_records(records, now)
        observed_at = now or self.clock()
        self._refresh_smart_money(observed_at)
        with self._lock:
            for candidate in list(self._candidates.values()):
                candidate.data_quality_cohort = self._classify_cohort(candidate)
                if self._exclude_if_first_discovery_price_above_cap(candidate, observed_at):
                    continue
                self._record_holder_observation(candidate, observed_at)
                if candidate.current_price_usd and candidate.native_token_price_usd and candidate.native_token_price_usd > 0 and candidate.current_price_native is None:
                    candidate.current_price_native = candidate.current_price_usd / candidate.native_token_price_usd
                # The shared Core persists its Candidate decision before this
                # SOL profile's post-processing. Re-assert the SOL-only cap
                # here so an already-active row cannot survive a fresh Meme
                # Rush price update above the strict USD limit.
                self._apply_price_cap(candidate)
                lifecycle = candidate.latest_lifecycle or "UNKNOWN"
                candidate.source_status["protocol_state"] = "VALID"
                if candidate.source_status.get("swap_direction") != "VALID":
                    candidate.source_status["swap_direction"] = "SOURCE_UNAVAILABLE"
                setattr(candidate, "protocol_state", self._protocol_state(lifecycle))
                self._persist_sol_fields(candidate)
            # Binding inspection reaches the read-only RPC.  The runner owns
            # it through its dedicated I/O refresh worker; never let a fresh
            # discovery record stall position evaluation on this SQLite thread.
            self.connection.commit()
            self._publish(observed_at)

    @staticmethod
    def _smart_field(record: Any, name: str) -> Any:
        field = record.normalized.fields.get(name)
        return field.value if field is not None and field.available else None

    def _refresh_smart_money(self, now: datetime) -> None:
        if self.smart_money is None:
            self._smart_money_state = "NOT_REQUESTED"
            return
        if self._smart_money_last_fetch_at is not None and now - self._smart_money_last_fetch_at < timedelta(seconds=60):
            return
        self._smart_money_last_fetch_at = now
        try:
            records = self.smart_money.fetch(page=1, page_size=50)
            for record in records:
                normalized = record.normalized
                self.connection.execute(
                    "INSERT OR REPLACE INTO survivor_smart_money(signal_id,mint,direction,trigger_price_usd,current_price_usd,smart_money_count,exit_rate,max_gain,signal_at,fetched_at,raw_response_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        normalized.signal.signal_id,
                        normalized.signal.mint,
                        record.direction,
                        str(self._smart_field(record, "alert_price_usd")) if self._smart_field(record, "alert_price_usd") is not None else None,
                        str(self._smart_field(record, "current_price_usd")) if self._smart_field(record, "current_price_usd") is not None else None,
                        record.smart_money_count,
                        record.exit_rate,
                        str(record.max_gain) if record.max_gain is not None else None,
                        _iso(normalized.source_timestamp or normalized.signal.observed_at),
                        normalized.fetched_at.isoformat(),
                        normalized.raw_response_hash,
                    ),
                )
            self.connection.commit()
            self._smart_money_count = len(records)
            self._smart_money_state = "HEALTHY" if records else "IDLE"
        except Exception:
            self._smart_money_state = "DEGRADED"

    @staticmethod
    def _protocol_state(lifecycle: str) -> str:
        if lifecycle == "MEME_NEW":
            return "BONDING_CURVE"
        if lifecycle == "MEME_FINALIZING":
            return "FINALIZING"
        if lifecycle == "MEME_MIGRATED":
            return "MIGRATED"
        return "UNKNOWN"

    def _sync_bindings(self) -> None:
        desired = {
            candidate.mint
            for candidate in self._candidates.values()
            if candidate.candidate_eligible and candidate.state not in {"EXPIRED", "REJECTED", "POSITION_CLOSED"}
        } | {position.mint for position in self._positions.values() if position.status == "OPEN"}
        for mint in sorted(desired):
            try:
                binding = self.price_monitor.register_position(mint)
            except Exception:
                binding = None
            if binding is not None:
                self._bindings[mint] = binding
                candidate = self._candidates.get(mint)
                if candidate is not None:
                    if candidate.flows.maxlen != 2000:
                        candidate.flows = deque(candidate.flows, maxlen=2000)
                    setattr(candidate, "protocol_state", "DEX_READY" if binding.stage == "pumpswap_pool" else "BONDING_CURVE")
                    candidate.source_status["pump_pumpswap_adapter"] = "VALID"
            elif mint in self._candidates:
                self._candidates[mint].source_status["pump_pumpswap_adapter"] = "SOURCE_UNAVAILABLE"
        self.price_monitor.unregister_missing(desired)
        self._bindings = {mint: binding for mint, binding in self._bindings.items() if mint in desired}
        for mint, candidate in self._candidates.items():
            if mint not in desired and candidate.flows.maxlen != 240:
                candidate.flows = deque(candidate.flows, maxlen=240)

    def subscription_details(self) -> tuple[dict[str, object], ...]:
        positions = {position.mint for position in self._positions.values() if position.status == "OPEN"}
        transition = {"PULLBACK_ZONE", "LOW_FORMING", "STOP_CONFIRMED", "REVERSAL_CONFIRMED", "READY_TO_BUY"}
        details: list[dict[str, object]] = []
        active_count = 0
        for mint, binding in sorted(self._bindings.items()):
            candidate = self._candidates.get(mint)
            if candidate is None:
                continue
            if mint in positions:
                reason = "OPEN_POSITION"
            elif candidate.state in transition:
                reason = "MIGRATION_TRANSITION" if candidate.latest_lifecycle == "MEME_FINALIZING" else "TRANSITION_GRACE"
            elif candidate.state == "ACTIVE_CANDIDATE" and active_count < self.config.active_max:
                reason = "ACTIVE_CANDIDATE"
                active_count += 1
            else:
                continue
            details.append({"contract": mint, "symbol": candidate.symbol, "reason": reason, "account_address": binding.primary_account, "stage": binding.stage})
        return tuple(details)

    def on_solana_price(self, observed: SolanaObservedPrice) -> None:
        with self._lock:
            candidate = self._candidates.get(observed.mint)
            if candidate is None:
                return
            usd = observed.price_sol_per_token * candidate.native_token_price_usd if candidate.native_token_price_usd else None
            canonical = usd or observed.price_sol_per_token
            previous_source = candidate.price_source
            previous_price = candidate.current_price_usd or candidate.current_price_native
            safe = True
            gap: Decimal | None = None
            if previous_source and previous_source != observed.source and previous_price and previous_price > 0:
                gap = abs(canonical / previous_price - Decimal("1"))
                safe = gap <= self.config.max_price_source_switch_gap
                self._source_switches += 1
                candidate.source_status["price_source_switch"] = "VALID" if safe else "INVALID"
                if not safe:
                    candidate.last_rejection = "PRICE_SOURCE_SWITCH_UNSAFE"
                    candidate.ready_to_buy = False
            candidate.source_status["solana_wss"] = "VALID"
            setattr(candidate, "source_switch_safe", safe)
            setattr(candidate, "source_switch_gap_pct", gap * 100 if gap is not None else None)
            setattr(candidate, "old_price_source", previous_source)
            setattr(candidate, "new_price_source", observed.source)
            setattr(candidate, "source_switch_at", observed.observed_at if previous_source != observed.source else None)
            last_snapshot = self._last_wss_snapshot.get(candidate.mint)
            persist_snapshot = (
                last_snapshot is None
                or (
                    canonical != last_snapshot[1]
                    and (observed.observed_at - last_snapshot[0]).total_seconds() >= 1
                )
            )
            self._update_price(
                candidate, canonical, observed.observed_at, observed.source,
                native_price=observed.price_sol_per_token,
                persist_snapshot=persist_snapshot,
            )
            if self._exclude_if_first_discovery_price_above_cap(candidate, observed.observed_at):
                self.connection.commit()
                self._sync_bindings()
                return
            price_capped = self._apply_price_cap(candidate)
            candidate.data_quality_cohort = self._classify_cohort(candidate)
            self._update_rollups(candidate, observed.observed_at)
            if persist_snapshot or price_capped:
                self._last_wss_snapshot[candidate.mint] = (observed.observed_at, canonical)
                self._persist_candidate(candidate, observed.observed_at)
                self._persist_sol_fields(candidate)
                self.connection.commit()
            if price_capped:
                self._sync_bindings()

    def on_solana_account_event(self, event: Mapping[str, object]) -> None:
        """Queue only registered PumpSwap vault changes as transaction triggers."""
        if event.get("method") != "accountNotification":
            return
        subscription_params = event.get("_subscription_params")
        if not isinstance(subscription_params, Sequence) or isinstance(subscription_params, (str, bytes)) or not subscription_params:
            return
        address = subscription_params[0]
        if not isinstance(address, str):
            return
        binding = self.price_monitor.binding_for_account(address)
        pool = binding.market_state.pumpswap_pool if binding is not None else None
        if binding is None or binding.stage != "pumpswap_pool" or pool is None:
            return
        if address not in {pool.pool_base_token_account, pool.pool_quote_token_account}:
            return
        params = event.get("params")
        result = params.get("result") if isinstance(params, Mapping) else None
        context = result.get("context") if isinstance(result, Mapping) else None
        slot = context.get("slot") if isinstance(context, Mapping) and isinstance(context.get("slot"), int) else None
        with self._lock:
            if address in self._queued_trigger_accounts:
                return
            if len(self._swap_trigger_queue) == self._swap_trigger_queue.maxlen:
                dropped = self._swap_trigger_queue.popleft()
                self._queued_trigger_accounts.discard(dropped[2])
            self._swap_trigger_queue.append((_utc_now(), binding.mint, address, slot))
            self._queued_trigger_accounts.add(address)

    def poll_swap_rpc_work(self, *, max_accounts: int = 1, max_transactions: int = 4) -> None:
        """Fetch/parse on the dedicated read-only worker; never touch SQLite."""
        rpc = self.swap_rpc
        if rpc is None:
            return
        for _ in range(min(max_accounts, len(self._swap_trigger_queue))):
            detected_at, mint, address, trigger_slot = self._swap_trigger_queue.popleft()
            self._queued_trigger_accounts.discard(address)
            try:
                signatures = rpc.get_signatures_for_address(address, limit=8, commitment="confirmed")
            except Exception:
                continue
            for item in signatures:
                signature = item.get("signature")
                slot = item.get("slot")
                if not isinstance(signature, str) or not signature:
                    continue
                if trigger_slot is not None and isinstance(slot, int) and (slot > trigger_slot + 2 or slot < trigger_slot - 12):
                    continue
                key = (signature, mint)
                if key in self._seen_signatures or key in self._queued_signatures:
                    continue
                if len(self._signature_queue) == self._signature_queue.maxlen:
                    old = self._signature_queue.popleft()
                    self._queued_signatures.discard((old[1], old[2]))
                self._signature_queue.append((detected_at, signature, mint, slot if isinstance(slot, int) else trigger_slot))
                self._queued_signatures.add(key)
                self._swap_events_detected += 1
        for _ in range(min(max_transactions, len(self._signature_queue))):
            detected_at, signature, mint, slot = self._signature_queue.popleft()
            key = (signature, mint)
            self._queued_signatures.discard(key)
            binding = next((item for item in self.price_monitor.bindings() if item.mint == mint), None)
            if binding is None:
                continue
            started = time.monotonic()
            self._get_transaction_calls += 1
            try:
                transaction = rpc.get_transaction(signature, commitment="confirmed")
                elapsed = int((time.monotonic() - started) * 1000)
                self._get_transaction_latency_ms.append(elapsed)
                fetched_at = _utc_now()
                if transaction is None:
                    parsed = self._unknown_swap(signature, binding, detected_at, fetched_at, slot, "TRANSACTION_NOT_AVAILABLE", rpc_latency_ms=elapsed)
                else:
                    parsed = parse_pumpswap_transaction(
                        transaction, signature=signature, binding=binding,
                        detected_at=detected_at, tx_fetched_at=fetched_at,
                        parse_finished_at=_utc_now(),
                    )
                    parsed = replace(parsed, rpc_latency_ms=elapsed)
            except Exception as exc:
                elapsed = int((time.monotonic() - started) * 1000)
                self._get_transaction_latency_ms.append(elapsed)
                self._get_transaction_failures += 1
                is_429 = "429" in str(exc)
                if is_429:
                    self._get_transaction_429 += 1
                parsed = self._unknown_swap(
                    signature, binding, detected_at, _utc_now(), slot,
                    getattr(exc, "error_class", type(exc).__name__),
                    rpc_latency_ms=elapsed, rpc_failed=True, rpc_http_429=is_429,
                )
            with self._lock:
                if len(self._parsed_swap_queue) == self._parsed_swap_queue.maxlen:
                    self._parsed_swap_queue.popleft()
                self._parsed_swap_queue.append(parsed)
            self._seen_signatures[key] = parsed.parse_finished_at
        cutoff = _utc_now() - timedelta(minutes=30)
        self._seen_signatures = {key: seen_at for key, seen_at in self._seen_signatures.items() if seen_at >= cutoff}

    def drain_swap_results(self, *, max_results: int = 64) -> None:
        """Persist verified worker results only on the coordinator thread."""
        accepted = 0
        while accepted < max_results:
            with self._lock:
                if not self._parsed_swap_queue:
                    break
                parsed = self._parsed_swap_queue.popleft()
            self._accept_parsed_swap(parsed)
            accepted += 1
        if accepted:
            self.connection.commit()

    @staticmethod
    def _unknown_swap(signature: str, binding: SolanaPriceBinding, detected_at: datetime, fetched_at: datetime, slot: int | None, error: str, *, rpc_latency_ms: int | None = None, rpc_failed: bool = False, rpc_http_429: bool = False) -> ParsedSolanaSwap:
        pool = binding.market_state.pumpswap_pool
        finished = _utc_now()
        return ParsedSolanaSwap(
            signature, binding.mint, "pumpswap", pool.quote_mint if pool else None,
            pool.pool_address if pool else None, pool.pool_base_token_account if pool else None,
            pool.pool_quote_token_account if pool else None, None, None, "UNKNOWN", "NONE",
            slot, detected_at, fetched_at, finished,
            max(0, int((finished - detected_at).total_seconds() * 1000)), error,
            rpc_latency_ms, rpc_failed, rpc_http_429,
        )

    def _accept_parsed_swap(self, parsed: ParsedSolanaSwap) -> None:
        candidate = self._candidates.get(parsed.mint)
        if candidate is None:
            return
        quote_volume = parsed.quote_volume_native
        volume_usd: Decimal | None = None
        conversion_source: str | None = None
        if quote_volume is not None and parsed.quote_mint == SOL_MINT and candidate.native_token_price_usd is not None:
            volume_usd = quote_volume * candidate.native_token_price_usd
            conversion_source = "binance_nativeTokenPrice_SOL_USD"
        elif quote_volume is not None and parsed.quote_mint in {USDC_MINT, USDT_MINT}:
            volume_usd = quote_volume
            conversion_source = "stablecoin_quote_native"
        self._persist_swap_event(parsed, volume_usd, conversion_source)
        if parsed.direction == "BUY":
            self._swap_events_parsed += 1
            self._swap_direction_buy += 1
        elif parsed.direction == "SELL":
            self._swap_events_parsed += 1
            self._swap_direction_sell += 1
        else:
            self._swap_direction_unknown += 1
            candidate.source_status["swap_direction"] = "SOURCE_UNAVAILABLE"
            return
        normalized_volume = volume_usd if volume_usd is not None else quote_volume
        if normalized_volume is None or normalized_volume <= 0:
            self._swap_direction_unknown += 1
            candidate.source_status["swap_direction"] = "SOURCE_UNAVAILABLE"
            return
        sample = FlowSample(
            parsed.parse_finished_at,
            buy_volume_bnb=normalized_volume if parsed.direction == "BUY" else Decimal("0"),
            sell_volume_bnb=normalized_volume if parsed.direction == "SELL" else Decimal("0"),
            buy_count=1 if parsed.direction == "BUY" else 0,
            sell_count=1 if parsed.direction == "SELL" else 0,
        )
        candidate.source_status["swap_direction"] = "VALID"
        candidate.source_status["flow_volume_unit"] = "USD" if volume_usd is not None else f"QUOTE_NATIVE:{parsed.quote_mint}"
        candidate.flows.append(sample)
        self._persist_sol_flow(candidate, sample, parsed, quote_volume, volume_usd, conversion_source)
        self._update_rollups(candidate, parsed.parse_finished_at)
        self._persist_candidate(candidate, parsed.parse_finished_at)

    def _persist_swap_event(self, parsed: ParsedSolanaSwap, volume_usd: Decimal | None, conversion_source: str | None) -> None:
        self.connection.execute(
            "INSERT OR IGNORE INTO sol_survivor_swap_events(signature,mint,protocol,quote_mint,pool_address,base_vault,quote_vault,candidate_delta,quote_delta,quote_volume_native,quote_volume_usd,usd_conversion_source,direction,confidence,detected_at,slot,tx_fetched_at,parse_finished_at,latency_ms,error_class,rpc_latency_ms,rpc_failed,rpc_http_429) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (parsed.signature, parsed.mint, parsed.protocol, parsed.quote_mint, parsed.pool_address,
             parsed.base_vault, parsed.quote_vault,
             str(parsed.candidate_delta) if parsed.candidate_delta is not None else None,
             str(parsed.quote_delta) if parsed.quote_delta is not None else None,
             str(parsed.quote_volume_native) if parsed.quote_volume_native is not None else None,
             str(volume_usd) if volume_usd is not None else None, conversion_source,
             parsed.direction, parsed.confidence, parsed.detected_at.isoformat(), parsed.slot,
             parsed.tx_fetched_at.isoformat(), parsed.parse_finished_at.isoformat(), parsed.latency_ms,
             parsed.error_class, parsed.rpc_latency_ms, int(parsed.rpc_failed), int(parsed.rpc_http_429)),
        )

    def _persist_sol_flow(self, candidate: SurvivorCandidate, sample: FlowSample, parsed: ParsedSolanaSwap, quote_volume: Decimal, volume_usd: Decimal | None, conversion_source: str | None) -> None:
        self.connection.execute(
            "INSERT INTO survivor_flow_samples(mint,observed_at,buy_volume_bnb,sell_volume_bnb,buy_count,sell_count,source,signature,slot,quote_mint,quote_volume_native,volume_usd,usd_conversion_source,protocol,detected_at,tx_fetched_at,parse_finished_at,latency_ms) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (candidate.mint, sample.observed_at.isoformat(), str(sample.buy_volume_bnb), str(sample.sell_volume_bnb),
             sample.buy_count, sample.sell_count, "solana_verified_swap", parsed.signature, parsed.slot,
             parsed.quote_mint, str(quote_volume), str(volume_usd) if volume_usd is not None else None,
             conversion_source, parsed.protocol, parsed.detected_at.isoformat(), parsed.tx_fetched_at.isoformat(),
             parsed.parse_finished_at.isoformat(), parsed.latency_ms),
        )

    def ingest_verified_swap_deltas(self, mint: str, observed_at: datetime, *, token_delta: Decimal, quote_delta_sol: Decimal) -> str:
        """Ingest only balance-delta verified swaps; ambiguous events stay out."""
        candidate = self._candidates.get(mint)
        if candidate is None:
            return "SWAP_DIRECTION_UNKNOWN"
        if token_delta > 0 and quote_delta_sol < 0:
            sample = FlowSample(observed_at, buy_volume_bnb=abs(quote_delta_sol), buy_count=1)
            direction = "BUY"
        elif token_delta < 0 and quote_delta_sol > 0:
            sample = FlowSample(observed_at, sell_volume_bnb=quote_delta_sol, sell_count=1)
            direction = "SELL"
        else:
            candidate.source_status["swap_direction"] = "SOURCE_UNAVAILABLE"
            return "SWAP_DIRECTION_UNKNOWN"
        candidate.source_status["swap_direction"] = "VALID"
        candidate.flows.append(sample)
        self._persist_flow(candidate, sample)
        self._update_rollups(candidate, observed_at)
        return direction

    def update_wss_health(self, state: str) -> None:
        self._wss_state = state
        self._wss_stale = state not in {"HEALTHY", "IDLE"}

    def _discovery_state(self, candidate: SurvivorCandidate, now: datetime) -> str:
        state = super()._discovery_state(candidate, now)
        if self._age_seconds(candidate, now) > self.config.max_candidate_age_sec:
            candidate.candidate_eligible = False
            candidate.active_candidate = False
            candidate.ready_to_buy = False
            candidate.last_rejection = f"AGE_ABOVE_{self.config.max_candidate_age_sec}S"
            return "LIGHT_TRACKING"
        if self._apply_price_cap(candidate):
            return "LIGHT_TRACKING"
        if state == "LIGHT_TRACKING" and candidate.last_rejection:
            filtered: list[str] = []
            if candidate.market_cap_usd is None:
                candidate.source_status["market_cap"] = "SOURCE_UNAVAILABLE"
            elif candidate.market_cap_usd < self.config.min_active_mc_usd:
                filtered.append(f"SOL_MC_BELOW_{self._usd_gate_label(self.config.min_active_mc_usd)}")
            if candidate.liquidity_usd is None:
                candidate.source_status["liquidity"] = "SOURCE_UNAVAILABLE"
            elif candidate.liquidity_usd < self.config.min_active_liquidity_usd:
                filtered.append(f"SOL_LIQUIDITY_BELOW_{self._usd_gate_label(self.config.min_active_liquidity_usd)}")
            if candidate.holders is None:
                candidate.source_status["holders"] = "SOURCE_UNAVAILABLE"
            elif candidate.holders < self.config.min_active_holders:
                filtered.append(f"SOL_HOLDERS_BELOW_{self.config.min_active_holders}")
            if filtered:
                candidate.last_rejection = "FILTERED:" + "|".join(filtered)
        return state

    @staticmethod
    def _usd_gate_label(value: Decimal) -> str:
        """Render configured USD gate values in compact, unambiguous rejection codes."""
        if value >= Decimal("1000000") and value % Decimal("1000000") == 0:
            return f"{int(value / Decimal('1000000'))}M"
        if value >= Decimal("1000") and value % Decimal("1000") == 0:
            return f"{int(value / Decimal('1000'))}K"
        return str(value).replace(".", "_")

    def _candidate_eligible(self, candidate: SurvivorCandidate, now: datetime) -> bool:
        return (
            super()._candidate_eligible(candidate, now)
            and self._age_seconds(candidate, now) <= self.config.max_candidate_age_sec
            and (
                candidate.current_price_usd is None
                or candidate.current_price_usd < self.config.max_candidate_price_usd
            )
        )

    def _universe_reason(self, candidate: SurvivorCandidate, now: datetime) -> str | None:
        if candidate.current_price_usd is not None and candidate.current_price_usd >= self.config.max_candidate_price_usd:
            return "SOL_PRICE_AT_OR_ABOVE_0_0001"
        if self._age_seconds(candidate, now) < self.config.min_age_sec:
            return "AGE_BELOW_180S"
        if self._age_seconds(candidate, now) > self.config.max_candidate_age_sec:
            return f"AGE_ABOVE_{self.config.max_candidate_age_sec}S"
        if candidate.market_cap_usd is None or not (self.config.universe_min_mc_usd <= candidate.market_cap_usd <= self.config.universe_max_mc_usd):
            return f"SOL_MC_OUTSIDE_{self._usd_gate_label(self.config.universe_min_mc_usd)}_{self._usd_gate_label(self.config.universe_max_mc_usd)}"
        if candidate.liquidity_usd is None or candidate.liquidity_usd < self.config.universe_min_liquidity_usd:
            return f"SOL_LIQUIDITY_BELOW_{self._usd_gate_label(self.config.universe_min_liquidity_usd)}"
        if candidate.holders is None or candidate.holders < self.config.universe_min_holders:
            return f"SOL_HOLDERS_BELOW_{self.config.universe_min_holders}"
        if not getattr(candidate, "source_switch_safe", True):
            return "PRICE_SOURCE_SWITCH_UNSAFE"
        if self._wss_stale:
            return "SOLANA_WSS_STALE"
        return None

    def _audit_result(self, candidate: SurvivorCandidate) -> tuple[str, str]:
        self._audit_checks += 1
        record = candidate.record
        if record is None:
            return "UNKNOWN", "AUDIT_UNAVAILABLE"
        risk = _field(record, "risk_level")
        if risk is not None and str(risk).lower() == "high":
            return "FAIL", "AUDIT_RISK_HIGH"
        for name in ("mint_authority", "freeze_authority", "wash_trading_tags", "bundler_percent", "dev_percent", "insider_percent", "sniper_percent"):
            candidate.source_status[name] = "VALID" if _field(record, name) is not None else "SOURCE_UNAVAILABLE"
        candidate.source_status["creator_sold"] = "VALID" if _field(record, "creator_sold") is not None else "SOURCE_UNAVAILABLE"
        return "PASS", "AUDIT_PASS"

    def _try_buy(self, candidate: SurvivorCandidate, now: datetime) -> None:
        if candidate.current_price_usd is not None and candidate.current_price_usd >= self.config.max_candidate_price_usd:
            candidate.ready_to_buy = False
            candidate.last_rejection = "SOL_PRICE_AT_OR_ABOVE_0_0001"
            return
        open_positions = sum(position.status == "OPEN" for position in self._positions.values())
        if self.controls.paused("paper"):
            candidate.last_rejection = "ENTRY_PAUSED"
            return
        if open_positions >= self.config.max_open_positions:
            candidate.last_rejection = "MAX_OPEN_POSITIONS_REACHED"
            return
        cutoff = (now - timedelta(hours=24)).isoformat()
        row = self.connection.execute("SELECT 1 FROM survivor_positions WHERE mint=? AND opened_at>=? LIMIT 1", (candidate.mint, cutoff)).fetchone()
        if row is not None:
            candidate.last_rejection = "MINT_24H_COOLDOWN"
            return
        if candidate.current_price_native is None or self.quote_provider is None:
            candidate.last_rejection = "BUY_QUOTE_UNAVAILABLE"
            return
        candidate.quote_requested = True
        candidate.quote_state = "PENDING"
        self._quote_requested_total += 1
        buy, sell, error = self.quote_provider.quote_candidate(candidate.mint, self.config.position_size_bnb, {})
        if error or buy is None or sell is None:
            candidate.quote_state = "SOURCE_UNAVAILABLE"
            candidate.last_rejection = error or "BUY_QUOTE_UNAVAILABLE"
            return
        candidate.quote_state = "VALID"
        entry_price = buy.input_quantity / buy.output_quantity
        position = SurvivorPosition(
            position_id=f"sol-survivor:{candidate.mint}:{uuid4().hex[:12]}", mint=candidate.mint,
            symbol=candidate.symbol, opened_at=now, entry_price_native=entry_price,
            current_price_native=candidate.current_price_native, quantity_token=buy.output_quantity,
            remaining_quantity_token=buy.output_quantity, invested_bnb=buy.input_quantity,
        )
        self._positions[position.position_id] = position
        self.connection.execute(
            "INSERT INTO survivor_positions(position_id,mint,symbol,opened_at,status,entry_price_native,entry_price_usd,current_price_native,quantity_token,remaining_quantity_token,invested_bnb,realized_bnb,last_trade_at,quote_source,updated_at,structure_stop_native,entry_holders,entry_market_cap_usd,entry_liquidity_usd) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                position.position_id, position.mint, position.symbol, now.isoformat(), position.status,
                str(position.entry_price_native), str(candidate.current_price_usd) if candidate.current_price_usd is not None else None,
                str(position.current_price_native), str(position.quantity_token), str(position.remaining_quantity_token),
                str(position.invested_bnb), "0", now.isoformat(), buy.quote_source or buy.provider, now.isoformat(), None,
                candidate.holders,
                str(candidate.market_cap_usd) if candidate.market_cap_usd is not None else None,
                str(candidate.liquidity_usd) if candidate.liquidity_usd is not None else None,
            ),
        )
        self.connection.commit()
        candidate.state = "POSITION_OPEN"
        candidate.paper_buy = True
        self._paper_buy_total += 1
        self._audit_event("SOL_SURVIVOR_PAPER_ENTRY", candidate, {"position_id": position.position_id, "buy_quote_id": buy.quote_id, "sell_quote_id": sell.quote_id, "executable_quote": True})

    def _evaluate_candidate(self, candidate: SurvivorCandidate, now: datetime) -> None:
        """Paper-enter immediately once a SOL token reaches the Candidate gate.

        The Candidate universe is the sole strategy-entry threshold.  Pullback,
        reversal, audit and flow checks remain available as observations only;
        they cannot delay a Paper entry.  A current price and read-only quote
        are still required to avoid inventing a fill.
        """
        if self._apply_price_cap(candidate):
            self._persist_candidate(candidate, now)
            return
        if candidate.state == "EXPIRED" and self._should_retain_candidate(candidate, now):
            candidate.state = "ACTIVE_CANDIDATE"
            candidate.active_candidate = True
            candidate.last_rejection = None
        if (now - candidate.last_seen_at).total_seconds() > self.config.idle_ttl_sec and not self._should_retain_candidate(candidate, now):
            candidate.state = "EXPIRED"
            candidate.active_candidate = False
            candidate.ready_to_buy = False
            candidate.last_rejection = "IDLE_TTL_30M"
            self._persist_candidate(candidate, now)
            return
        candidate.age_seconds = self._age_seconds(candidate, now)
        candidate.price_age_ms = int((now - candidate.price_updated_at).total_seconds() * 1000) if candidate.price_updated_at is not None else None
        if not candidate.candidate_eligible:
            return
        if candidate.current_price_usd is None or candidate.current_price_native is None:
            candidate.ready_to_buy = False
            candidate.last_rejection = "PRICE_REQUIRED_BEFORE_BUY"
            self._persist_candidate(candidate, now)
            return
        candidate.active_candidate = True
        candidate.ready_to_buy = True
        candidate.state = "READY_TO_BUY"
        candidate.last_rejection = None
        self._try_buy(candidate, now)
        self._persist_candidate(candidate, now)

    def _evaluate_positions(self, now: datetime) -> None:
        for position in tuple(self._positions.values()):
            if position.status != "OPEN":
                continue
            candidate = self._candidates.get(position.mint)
            # A WSS mark stops being a valuation as soon as its source is
            # stale.  Do not leave an old mark visible as an active position
            # price: request a read-only executable sell quote and close only
            # if that quote is available.  A failed quote deliberately keeps
            # the position open for a later retry rather than fabricating a
            # paper fill.
            if (
                candidate is not None
                and candidate.price_updated_at is not None
                and (now - candidate.price_updated_at).total_seconds() > self.config.idle_ttl_sec
            ):
                self._close_position(position, "PRICE_DATA_EXPIRED_EXIT", now)
                continue
            dirty_marks = getattr(self, "_position_mark_dirty", set())
            mark_changed = position.position_id in dirty_marks
            if candidate is not None and candidate.current_price_native is not None and candidate.current_price_native > 0:
                mark_changed = mark_changed or position.current_price_native != candidate.current_price_native
                position.current_price_native = candidate.current_price_native
            if position.current_price_native is None or position.entry_price_native <= 0:
                if mark_changed:
                    self._persist_position(position, now, reason=None)
                continue

            price = position.current_price_native
            entry = position.entry_price_native
            if self._holder_drop_triggered(position.mint, now):
                self._close_position(position, "HOLDER_DROP_20PCT_2M", now)
                continue
            if position.tp2 and position.trailing_active and price <= entry * (Decimal("1") + self.config.tp1_gain_pct / Decimal("100")):
                self._close_position(position, "TP3_RETRACE_TO_TP1", now)
                continue
            if position.tp2 and price <= entry:
                self._close_position(position, "TP2_RETRACE_TO_ENTRY", now)
                continue
            if position.tp1 and price <= entry * (Decimal("1") - self.config.tp1_stop_pct / Decimal("100")):
                self._close_position(position, "TP1_STOP_50PCT", now)
                continue
            if not position.tp1 and price <= entry * (Decimal("1") - self.config.hard_stop_pct / Decimal("100")):
                self._close_position(position, "HARD_STOP_80PCT", now)
                continue

            persisted_by_exit = False
            if not position.tp1 and price >= entry * (Decimal("1") + self.config.tp1_gain_pct / Decimal("100")):
                persisted_by_exit = self._partial_exit_of_initial(position, self.config.tp1_sell_pct, "TP1_PLUS_50", now)
            # A WSS tick can jump across more than one target.  Once the
            # first partial exit has settled, continue through every target
            # already reached on this same real price event; never leave a
            # large gain exposed merely because there was no intermediate
            # tick at TP2 or TP3.
            if position.status == "OPEN" and position.tp1 and not position.tp2 and price >= entry * (Decimal("1") + self.config.tp2_gain_pct / Decimal("100")):
                persisted_by_exit = self._partial_exit_of_initial(position, self.config.tp2_sell_pct, "TP2_PLUS_100", now) or persisted_by_exit
                # The shared Core uses this flag for its former TP2 trailing
                # rule.  SOL's trailing exit starts only after TP3.
                if persisted_by_exit and position.status == "OPEN":
                    position.trailing_active = False
                    self._persist_position(position, now, reason=None)
            if position.status == "OPEN" and position.tp2 and not position.trailing_active and price >= entry * (Decimal("1") + self.config.tp3_gain_pct / Decimal("100")):
                persisted_by_exit = self._partial_exit_of_initial(position, self.config.tp3_sell_pct, "TP3_PLUS_200", now) or persisted_by_exit
                if persisted_by_exit and position.status == "OPEN":
                    position.trailing_active = True
                    self._persist_position(position, now, reason=None)
            if mark_changed and not persisted_by_exit and position.status == "OPEN":
                self._persist_position(position, now, reason=None)

    def _partial_exit_of_initial(self, position: SurvivorPosition, initial_pct: Decimal, reason: str, now: datetime) -> bool:
        """Sell an explicit percentage of the original position, never a fabricated fill."""
        target_quantity = position.quantity_token * initial_pct / Decimal("100")
        quantity = min(position.remaining_quantity_token, target_quantity)
        if quantity <= 0:
            return False
        # Decimal division used for the second tranche can leave an
        # insignificant repeating-decimal remainder.  Treat a sub-atomic
        # residual as the final tranche rather than leaving a false OPEN
        # position that cannot be sold meaningfully.
        residual = position.remaining_quantity_token - quantity
        tolerance = max(Decimal("1"), position.quantity_token) * Decimal("1e-18")
        if quantity >= position.remaining_quantity_token or residual <= tolerance:
            self._close_position(position, reason, now)
            return position.status == "CLOSED"
        return self._partial_exit(position, quantity / position.remaining_quantity_token, reason, now)

    def _close_position(self, position: SurvivorPosition, reason: str, now: datetime) -> None:
        """Close from one executable read-only quote and freeze its trade facts."""
        quote = self.quote_provider.quote(position.mint, "sell", position.remaining_quantity_token) if self.quote_provider is not None else None
        if quote is None or quote.output_quantity <= 0 or quote.input_quantity <= 0:
            self._audit_event("SOL_SURVIVOR_EXIT_QUOTE_UNAVAILABLE", self._candidates.get(position.mint), {
                "position_id": position.position_id,
                "reason": reason,
            })
            return
        exit_price_native = quote.output_quantity / quote.input_quantity
        position.realized_bnb += quote.output_quantity
        position.remaining_quantity_token = Decimal("0")
        position.status = "CLOSED"
        self._persist_position(position, now, reason=reason)
        candidate = self._candidates.get(position.mint)
        if candidate is not None:
            candidate.state = "POSITION_CLOSED"
        self.connection.execute(
            "UPDATE survivor_positions SET exit_price_native=?,exit_price_usd=?,exit_holders=?,exit_market_cap_usd=?,exit_liquidity_usd=? WHERE position_id=?",
            (
                str(exit_price_native),
                str(candidate.current_price_usd) if candidate is not None and candidate.current_price_usd is not None else None,
                candidate.holders if candidate is not None else None,
                str(candidate.market_cap_usd) if candidate is not None and candidate.market_cap_usd is not None else None,
                str(candidate.liquidity_usd) if candidate is not None and candidate.liquidity_usd is not None else None,
                position.position_id,
            ),
        )
        self.connection.commit()
        self._audit_event("SOL_SURVIVOR_PAPER_EXIT", candidate, {
            "position_id": position.position_id,
            "reason": reason,
            "sell_quote_id": quote.quote_id,
            "sell_input_quantity": str(quote.input_quantity),
            "sell_output_sol": str(quote.output_quantity),
            "quote_source": quote.quote_source or quote.provider,
            "realized_pnl_pct": str(self._position_pnl_pct(position)),
            "executable_quote": True,
        })

    def _record_holder_observation(self, candidate: SurvivorCandidate, observed_at: datetime) -> None:
        if candidate.holders is None or candidate.holders < 0:
            return
        samples = self._holder_samples.setdefault(candidate.mint, deque())
        if samples and samples[-1][0] == observed_at:
            samples[-1] = (observed_at, candidate.holders)
        else:
            samples.append((observed_at, candidate.holders))
        cutoff = observed_at - timedelta(seconds=self.config.holder_drop_window_sec)
        while samples and samples[0][0] < cutoff:
            samples.popleft()

    def _holder_drop_triggered(self, mint: str, now: datetime) -> bool:
        samples = self._holder_samples.get(mint)
        if samples is None or len(samples) < 2:
            return False
        cutoff = now - timedelta(seconds=self.config.holder_drop_window_sec)
        recent = [sample for sample in samples if sample[0] >= cutoff]
        if len(recent) < 2:
            return False
        current = recent[-1][1]
        baseline = max(value for _, value in recent[:-1])
        return baseline > 0 and Decimal(current) <= Decimal(baseline) * (Decimal("1") - self.config.holder_drop_pct / Decimal("100"))

    def _persist_sol_fields(self, candidate: SurvivorCandidate) -> None:
        try:
            self.connection.execute(
                "UPDATE survivor_candidates SET canonical_strategy_price=?,canonical_price_source=?,protocol_state=?,source_switch_at=?,old_price_source=?,new_price_source=?,source_switch_gap_pct=?,source_switch_safe=? WHERE mint=?",
                (
                    str(candidate.current_price_usd or candidate.current_price_native) if (candidate.current_price_usd or candidate.current_price_native) is not None else None,
                    candidate.price_source,
                    getattr(candidate, "protocol_state", None),
                    _iso(getattr(candidate, "source_switch_at", None)),
                    getattr(candidate, "old_price_source", None),
                    getattr(candidate, "new_price_source", None),
                    str(getattr(candidate, "source_switch_gap_pct", "")) if getattr(candidate, "source_switch_gap_pct", None) is not None else None,
                    int(getattr(candidate, "source_switch_safe", True)),
                    candidate.mint,
                ),
            )
        except Exception:
            candidate.source_status["sol_survivor_schema"] = "SOURCE_UNAVAILABLE"

    def status(self) -> dict[str, object]:
        payload = super().status()
        payload["identity"] = SOL_SURVIVOR_REVERSAL_IDENTITY.as_dict()
        payload["chain"] = "solana"
        payload["data_quality"]["legacy_backfill_capability"] = "NOT_USED_FOR_NATIVE_FRESH"
        payload["data_quality"]["legacy_backfill_definition"] = "NATIVE_FRESH requires createTime at/after cohort start (60s tolerance), first lifecycle NEW/rank 10, and a first valid price. Other tokens are BOOTSTRAP_EXISTING."
        payload["provider_health"] = {
            "binance_meme_rush_sol": "HEALTHY" if self._last_discovery_count >= 0 else "DOWN",
            "binance_token_info_sol": "HEALTHY" if self._record_update_total > 0 else "IDLE",
            "binance_token_audit_sol": "NOT_REQUESTED" if self._audit_requested_total == 0 else "HEALTHY",
            "binance_trading_signal_sol": self._smart_money_state,
            "solana_rpc": "HEALTHY" if self.price_monitor is not None and self._wss_state in {"HEALTHY", "IDLE"} else "DEGRADED",
            "solana_wss": self._wss_state,
            "pump_pumpswap_adapter": "HEALTHY" if self.price_monitor is not None else "DOWN",
            "quote": self.quote_provider.status() if self.quote_provider is not None and hasattr(self.quote_provider, "status") else {"state": "NOT_REQUESTED"},
            "paper_execution": "HEALTHY",
        }
        lifecycle_distribution: dict[str, object] = {}
        for lifecycle in ("MEME_NEW", "MEME_FINALIZING", "MEME_MIGRATED"):
            items = [candidate for candidate in self._candidates.values() if candidate.latest_lifecycle == lifecycle]
            lifecycle_distribution[lifecycle] = {
                "count": len(items),
                "market_cap_usd": self._distribution([c.market_cap_usd for c in items if c.market_cap_usd is not None], len(items)),
                "liquidity_usd": self._distribution([c.liquidity_usd for c in items if c.liquidity_usd is not None], len(items)),
                "holders": self._distribution([Decimal(c.holders) for c in items if c.holders is not None], len(items)),
            }
            for name, values in (
                ("market_cap_usd", [c.market_cap_usd for c in items if c.market_cap_usd is not None]),
                ("liquidity_usd", [c.liquidity_usd for c in items if c.liquidity_usd is not None]),
                ("holders", [Decimal(c.holders) for c in items if c.holders is not None]),
            ):
                lifecycle_distribution[lifecycle][name]["p75"] = self._percentile(values, 75)
        payload["lifecycle_distribution"] = lifecycle_distribution
        native = [c for c in self._candidates.values() if c.data_quality_cohort == "NATIVE_FRESH"]
        bootstrap = [c for c in self._candidates.values() if c.data_quality_cohort == "BOOTSTRAP_EXISTING"]
        native_candidates = [c for c in native if c.candidate_eligible]
        payload["fresh_sol"] = len(native)
        payload["native_fresh"] = len(native)
        payload["bootstrap_existing"] = len(bootstrap)
        payload["native_fresh_candidate"] = len(native_candidates)
        payload["native_fresh_live_complete"] = sum(c.price_history_status == "LIVE_COMPLETE" for c in native_candidates)
        payload["native_fresh_price_history_incomplete"] = sum(c.price_history_status == "PRICE_HISTORY_INCOMPLETE" for c in native_candidates)
        payload["source_switch_count"] = self._source_switches
        payload["smart_money_shadow_rows_last_fetch"] = self._smart_money_count
        latencies = sorted(self._get_transaction_latency_ms)
        def percentile(fraction: float) -> int | None:
            if not latencies:
                return None
            return latencies[min(len(latencies) - 1, max(0, int((len(latencies) - 1) * fraction)))]
        flow_items: list[dict[str, object]] = []
        for candidate in self._candidates.values():
            swap_count = candidate.buy_count_1m + candidate.sell_count_1m
            if swap_count <= 0:
                continue
            flow_items.append({
                "mint": candidate.mint,
                "symbol": candidate.symbol,
                "buy_volume_1m": str(candidate.buy_volume_1m),
                "sell_volume_1m": str(candidate.sell_volume_1m),
                "volume_ratio_1m": str(candidate.buy_sell_volume_ratio_1m) if candidate.buy_sell_volume_ratio_1m is not None else None,
                "buy_count_1m": candidate.buy_count_1m,
                "sell_count_1m": candidate.sell_count_1m,
                "count_ratio_1m": str(candidate.buy_sell_count_ratio_1m) if candidate.buy_sell_count_ratio_1m is not None else None,
                "swap_count_1m": swap_count,
                "volume_unit": candidate.source_status.get("flow_volume_unit"),
            })
        payload["swap_flow"] = {
            "swap_events_detected": self._swap_events_detected,
            "swap_events_parsed": self._swap_events_parsed,
            "swap_direction_buy": self._swap_direction_buy,
            "swap_direction_sell": self._swap_direction_sell,
            "swap_direction_unknown": self._swap_direction_unknown,
            "swap_parse_success_rate": str(Decimal(self._swap_events_parsed) / Decimal(self._swap_events_parsed + self._swap_direction_unknown)) if (self._swap_events_parsed + self._swap_direction_unknown) else None,
            "flow_active_candidates": len(flow_items),
            "flow_candidates": flow_items,
            "get_transaction_calls": self._get_transaction_calls,
            "get_transaction_failures": self._get_transaction_failures,
            "get_transaction_429": self._get_transaction_429,
            "get_transaction_latency_p50_ms": percentile(0.50),
            "get_transaction_latency_p95_ms": percentile(0.95),
            "trigger_queue_size": len(self._swap_trigger_queue),
            "transaction_queue_size": len(self._signature_queue),
            "parsed_result_queue_size": len(self._parsed_swap_queue),
            "seen_signature_cache_size": len(self._seen_signatures),
        }
        return payload

    def _publish(self, now: datetime) -> None:
        payload = self.status()
        payload["updated_at"] = now.isoformat()
        self.store.set_state("survivor_reversal_sol_v1", payload)
        self.health.set("survivor_reversal_sol", "HEALTHY", details={
            "strategy": SOL_SURVIVOR_REVERSAL_IDENTITY.as_dict(),
            "active": payload["active_candidate_now"], "paper_only": True,
            "db_commit_error_count": self._db_commit_error_count,
            "db_locked_error_count": self._db_locked_error_count,
            "db_write_error_count": self._db_write_error_count,
            "last_db_write_at": _iso(self._last_db_write_at),
        })
