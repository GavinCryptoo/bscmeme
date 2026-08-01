"""Gate A realtime coordinator for independent Paper and Shadow lifecycles.

The coordinator consumes normalized Binance Web3 read-only records, builds
only fields that are explicitly available, and delegates all entry/exit
decisions to the frozen deterministic engines. Missing hard fields never
become inferred numbers.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from threading import Event
from typing import Callable, Mapping, Sequence

from meme_system.adapters.binance_web3.errors import BinanceWeb3Error
from meme_system.adapters.binance_web3.market_data import BinanceWeb3MarketDataAdapter
from meme_system.adapters.binance_web3.models import BinanceMarketSnapshot, BinanceNormalizedSignal
from meme_system.adapters.binance_web3.signal_source import BinanceWeb3SignalSource
from meme_system.adapters.protocols import ExecutableQuote, QuoteProvider
from meme_system.domain.models import EntryFeatures, ShadowExitFeatures, Signal, VirtualPosition
from meme_system.engines.simulation import DeterministicSimulation, EntryResult, ExitResult
from meme_system.runtime_ops import HealthRegistry, JsonlAuditWriter, LatencyRecorder, RuntimeControl, utc_now
from meme_system.storage.runtime_store import RuntimeStore
from meme_system.telegram_control import TelegramControl


def _field_value(fields: Mapping[str, object], name: str) -> object | None:
    field = fields.get(name)
    if field is None:
        return None
    available = getattr(field, "available", False)
    return getattr(field, "value", None) if available else None


class BinanceRealtimeFeatureProvider:
    """Build conservative entry/exit features from confirmed adapter fields."""

    def __init__(
        self,
        *,
        market_data: BinanceWeb3MarketDataAdapter | None = None,
        quote_provider: QuoteProvider | None = None,
        position_size_sol: Decimal = Decimal("0.001"),
    ) -> None:
        self.market_data = market_data
        self.quote_provider = quote_provider
        self.position_size_sol = position_size_sol

    def entry_features(
        self,
        normalized: BinanceNormalizedSignal,
        evaluated_at: datetime | None = None,
    ) -> EntryFeatures:
        evaluated_at = evaluated_at or utc_now()
        fields = dict(normalized.fields)
        dynamic: BinanceMarketSnapshot | None = None
        dynamic_error: str | None = None
        if self.market_data is not None:
            try:
                dynamic = self.market_data.snapshot(normalized.signal.mint)
                fields.update(dynamic.fields)
            except BinanceWeb3Error as exc:
                dynamic_error = exc.context.error_class
            except Exception:
                dynamic_error = "binance_dynamic_unavailable"

        token_name = _first_string(_field_value(fields, "name"), _field_value(fields, "symbol"))
        soft = {
            "source": normalized.endpoint_type,
            "raw_response_hash": normalized.raw_response_hash,
            "historical_bootstrap": normalized.historical_bootstrap,
        }
        for name in (
            "holders",
            "market_cap_usd",
            "liquidity_usd",
            "price_usd",
            "progress_pct",
            "migrate_status",
            "dev_position",
            "dev_sold_percent",
        ):
            value = _field_value(fields, name)
            if value is not None:
                soft[name] = value
        if dynamic_error is not None:
            soft["dynamic_error_class"] = dynamic_error

        buy_quote: ExecutableQuote | None = None
        sell_quote: ExecutableQuote | None = None
        quote_errors: list[str] = []
        if self.quote_provider is not None:
            buy_quote = self._quote(normalized.signal.mint, "buy", self.position_size_sol, quote_errors)
            if buy_quote is not None and buy_quote.output_quantity > 0:
                sell_quote = self._quote(normalized.signal.mint, "sell", buy_quote.output_quantity, quote_errors)
        if quote_errors:
            soft["quote_error_classes"] = tuple(sorted(set(quote_errors)))

        # The currently confirmed Binance fields do not expose the frozen
        # 5..120s age, 15s buyer ratio/net-buy windows, or a confident creator
        # sell boolean. They remain unavailable instead of being synthesized
        # from 5m/24h fields or timestamp units that are not frozen.
        return EntryFeatures(
            token_age_sec=None,
            unique_buyers_15s=None,
            buy_sell_count_ratio_15s=None,
            net_buy_15s=None,
            flow_windows_non_negative=(None, None),
            creator_confirmed_sold=None,
            buy_quote=buy_quote,
            sell_quote=sell_quote,
            evaluated_at=evaluated_at,
            token_name=token_name,
            soft_features=soft,
        )

    def quote_for_position(self, position: VirtualPosition) -> ExecutableQuote | None:
        if self.quote_provider is None or position.active_quantity_token <= 0:
            return None
        try:
            return self.quote_provider.quote(
                position.mint,
                "sell",
                position.active_quantity_token,
            )
        except Exception:
            return None

    def shadow_exit_features(
        self,
        position: VirtualPosition,
        sell_quote: ExecutableQuote | None,
        now: datetime,
    ) -> ShadowExitFeatures:
        age = max(0, int((now - position.opened_at).total_seconds()))
        if sell_quote is not None and sell_quote.unusable_reason(now) is None:
            return_pct = (sell_quote.output_quantity - position.quantity_sol) / position.quantity_sol
        else:
            return_pct = position.last_return_pct or Decimal("0")
        # No confirmed realtime field currently supplies these four boolean
        # predicates. False means "not confirmed", not "market is healthy".
        return ShadowExitFeatures(
            position_age_sec=age,
            return_pct=return_pct,
            mfe_pct=position.mfe_pct,
            recent_net_flow_negative=False,
            independent_buyer_growth_stopped=False,
            creator_sell_confident=False,
            buyer_growth_and_flow_slowed=False,
        )

    def _quote(
        self,
        mint: str,
        side: str,
        quantity: Decimal,
        errors: list[str],
    ) -> ExecutableQuote | None:
        try:
            quote = self.quote_provider.quote(mint, side, quantity)  # type: ignore[union-attr]
            if quote is None:
                errors.append(f"{side}_quote_unavailable")
            elif quote.error_class:
                errors.append(quote.error_class)
            return quote
        except Exception as exc:
            errors.append(getattr(exc, "error_class", f"{side}_quote_error"))
            return None


@dataclass(frozen=True)
class CycleResult:
    started_at: datetime
    finished_at: datetime
    fetched: int
    bootstrap_skipped: int
    duplicate_skipped: int
    candidates: int
    accepted: Mapping[str, int]
    exits: Mapping[str, int]
    source_error_class: str | None = None


class RealtimeCoordinator:
    """One bounded cycle or a stoppable loop over both isolated engines."""

    def __init__(
        self,
        *,
        source: BinanceWeb3SignalSource,
        features: BinanceRealtimeFeatureProvider,
        engines: Mapping[str, DeterministicSimulation],
        controls: RuntimeControl,
        health: Mapping[str, HealthRegistry],
        latency: LatencyRecorder,
        stores: Mapping[str, RuntimeStore],
        audits: Mapping[str, JsonlAuditWriter],
        telegram: TelegramControl | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        if not engines or any(mode not in {"paper", "shadow"} for mode in engines):
            raise ValueError("engines must contain paper and/or shadow")
        self.source = source
        self.features = features
        self.engines = dict(engines)
        self.controls = controls
        self.health = dict(health)
        self.latency = latency
        self.stores = dict(stores)
        self.audits = dict(audits)
        self.telegram = telegram
        self.clock = clock

    def run_cycle(self) -> CycleResult:
        started = self.clock()
        if self.telegram is not None:
            try:
                actions = self.telegram.poll_once()
                if actions:
                    for mode in self.engines:
                        self._audit(mode, "TELEGRAM_CONTROL_APPLIED", {"actions": actions})
            except Exception:
                for mode in self.engines:
                    self._set_health(mode, "telegram", "UNAVAILABLE", error_class="telegram_control_error")
        fetched = bootstrap_skipped = duplicate_skipped = candidates = 0
        accepted = {mode: 0 for mode in self.engines}
        exits = {mode: 0 for mode in self.engines}
        source_error: str | None = None
        source_started = time.monotonic()
        try:
            records = tuple(self.source.fetch_once())
            elapsed = (time.monotonic() - source_started) * 1000
            self.latency.record("binance_signal_poll", elapsed)
            for mode in self.engines:
                self.stores[mode].record_latency("binance_signal_poll", elapsed)
                self._set_health(mode, "binance_web3", "HEALTHY", latency_ms=int(elapsed))
        except BinanceWeb3Error as exc:
            source_error = exc.context.error_class
            for mode in self.engines:
                self._set_health(mode, "binance_web3", "UNAVAILABLE", error_class=source_error)
                self._audit(mode, "SOURCE_ERROR", {"error_class": source_error})
            return CycleResult(started, self.clock(), 0, 0, 0, 0, accepted, exits, source_error)

        fetched = len(records)
        for record in records:
            signal = record.signal
            if record.historical_bootstrap:
                bootstrap_skipped += 1
                for mode in self.engines:
                    self._audit(mode, "SIGNAL_BOOTSTRAP_SKIPPED", {"signal_id": signal.signal_id, "mint": signal.mint})
                continue
            feature_started = time.monotonic()
            entry_features = self.features.entry_features(record, self.clock())
            feature_elapsed = (time.monotonic() - feature_started) * 1000
            self.latency.record("feature_and_quote_build", feature_elapsed)
            for mode in self.engines:
                self.stores[mode].record_latency("feature_and_quote_build", feature_elapsed)
                engine = self.engines[mode]
                candidate_id = f"{mode}:{signal.signal_id}:{engine.strategy.config.identity.ruleset_version}"
                position_id = f"{candidate_id}:position"
                if engine.ledger.candidate_exists(candidate_id):
                    duplicate_skipped += 1
                    self._audit(mode, "DUPLICATE_SIGNAL_SKIPPED", {"signal_id": signal.signal_id, "candidate_id": candidate_id})
                    continue
                candidates += 1
                block_reason = "runtime_paused" if self.controls.paused(mode) else None
                result = engine.process_entry(
                    signal,
                    entry_features,
                    candidate_id=candidate_id,
                    position_id=position_id,
                    block_reason=block_reason,
                )
                if result.position is not None:
                    accepted[mode] += 1
                self._audit_candidate(mode, signal, result, entry_features)

        for mode, engine in self.engines.items():
            for position in tuple(engine.ledger.active_positions):
                result = self._process_exit(mode, engine, position)
                if result is not None and result.closed_position is not None:
                    exits[mode] += 1
        finished = self.clock()
        for mode in self.engines:
            self.stores[mode].set_state("last_cycle", {
                "started_at": started.isoformat(),
                "finished_at": finished.isoformat(),
                "fetched": fetched,
                "candidates": candidates,
                "accepted": accepted[mode],
                "exits": exits[mode],
                "source_error_class": source_error,
            })
            self._set_health(mode, "coordinator", "HEALTHY")
        return CycleResult(started, finished, fetched, bootstrap_skipped, duplicate_skipped, candidates, accepted, exits, source_error)

    def run_forever(self, stop_event: Event, *, poll_sec: float = 10.0) -> None:
        interval = max(0.5, min(3600.0, poll_sec))
        while not stop_event.is_set():
            self.run_cycle()
            stop_event.wait(interval)

    def _process_exit(
        self,
        mode: str,
        engine: DeterministicSimulation,
        position: VirtualPosition,
    ) -> ExitResult | None:
        now = self.clock()
        quote = self.features.quote_for_position(position)
        try:
            if mode == "paper":
                result = engine.process_paper_exit(position.position_id, now, quote)
            else:
                shadow_features = self.features.shadow_exit_features(position, quote, now)
                result = engine.process_shadow_exit(position.position_id, shadow_features, now, quote)
        except (KeyError, ValueError):
            return None
        self._audit(mode, "EXIT_EVALUATED", {
            "position_id": position.position_id,
            "triggered": result.decision.triggered,
            "reason": result.decision.reason,
            "return_pct": result.decision.return_pct,
            "quote_id": quote.quote_id if quote else None,
        })
        return result

    def _audit(self, mode: str, event_type: str, payload: Mapping[str, object]) -> None:
        self.audits[mode].append(mode=mode, event_type=event_type, occurred_at=self.clock(), payload=payload)

    def _audit_candidate(self, mode: str, signal: Signal, result: EntryResult, features: EntryFeatures) -> None:
        self._audit(mode, "CANDIDATE_EVALUATED", {
            "signal_id": signal.signal_id,
            "mint": signal.mint,
            "candidate_id": result.candidate.candidate_id,
            "status": result.candidate.status,
            "filter_reason": result.candidate.filter_reason,
            "failed_reason_codes": result.decision.failed_reason_codes,
            "soft_features": features.soft_features or {},
            "buy_quote_id": features.buy_quote.quote_id if features.buy_quote else None,
            "sell_quote_id": features.sell_quote.quote_id if features.sell_quote else None,
        })

    def _set_health(
        self,
        mode: str,
        component: str,
        state: str,
        *,
        error_class: str | None = None,
        latency_ms: int | None = None,
    ) -> None:
        self.health[mode].set(component, state, error_class=error_class, latency_ms=latency_ms)
        self.stores[mode].record_health(component, state, error_class=error_class, latency_ms=latency_ms)


def _first_string(*values: object | None) -> str | None:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None
