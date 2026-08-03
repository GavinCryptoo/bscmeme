"""Realtime coordinator for independent Paper, Shadow and BSC Live lifecycles.

The coordinator consumes normalized Binance Web3 read-only records, builds
only fields that are explicitly available, and delegates all entry/exit
decisions to the frozen deterministic engines. Missing hard fields never
become inferred numbers.
"""

from __future__ import annotations

import json
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from threading import Event, Lock, RLock
from typing import Callable, Mapping, Sequence

from meme_system.adapters.bsc_wss import (
    BscPairEvent,
    BscPoolDescriptor,
    BscPoolPrice,
    BscPoolResolver,
    normalize_bsc_address,
)
from meme_system.adapters.bsc_quote import BscReadOnlyQuoteProvider
from meme_system.adapters.pump_readonly import PumpMarketState, PumpProtocolReadOnlyQuoteProvider
from meme_system.adapters.binance_web3.errors import BinanceWeb3Error
from meme_system.adapters.binance_web3.market_data import BinanceWeb3MarketDataAdapter
from meme_system.adapters.binance_web3.models import BinanceMarketSnapshot, BinanceNormalizedSignal
from meme_system.adapters.binance_web3.signal_source import BinanceWeb3SignalSource
from meme_system.adapters.protocols import ExecutableQuote, QuoteProvider
from meme_system.adapters.solana_price import SolanaObservedPrice, SolanaPriceMonitor
from meme_system.domain.models import EntryFeatures, PriceSnapshot, ShadowExitFeatures, ShadowOutcome, Signal, VirtualPosition
from meme_system.domain.naming import clean_token_name
from meme_system.engines.simulation import DeterministicSimulation, EntryResult, ExitResult
from meme_system.runtime_ops import HealthRegistry, JsonlAuditWriter, LatencyRecorder, RuntimeControl, utc_now
from meme_system.storage.exit_holders import (
    database_path,
    persist_exit_holders_update,
)
from meme_system.storage.exit_market import persist_exit_market_update
from meme_system.storage.runtime_store import RuntimeStore
from meme_system.telegram_control import TelegramControl


_POSITION_QUOTE_MISSING = object()


@dataclass(frozen=True)
class ExitHoldersSnapshot:
    holders: int
    observed_at: datetime
    source: str = "binance_dynamic"


@dataclass(frozen=True)
class ExitMarketSnapshot:
    market_cap_usd: Decimal | None
    liquidity_usd: Decimal | None
    observed_at: datetime
    source: str = "binance_dynamic"


class ExitHoldersBackfill:
    """Bounded, non-blocking Binance Dynamic backfill after a close."""

    def __init__(
        self,
        features: "BinanceRealtimeFeatureProvider",
        db_paths: Mapping[str, Path | None],
        *,
        max_workers: int = 2,
        retry_count: int = 3,
        retry_delay_sec: float = 2.0,
    ) -> None:
        self.features = features
        self.db_paths = dict(db_paths)
        self.retry_count = max(1, min(3, int(retry_count)))
        self.retry_delay_sec = max(0.0, float(retry_delay_sec))
        self._stop = Event()
        self._lock = Lock()
        self._pending: set[tuple[str, str]] = set()
        self._executor = ThreadPoolExecutor(
            max_workers=max(1, int(max_workers)),
            thread_name_prefix="exit-holders-backfill",
        )

    def submit(self, mode: str, position: VirtualPosition) -> bool:
        path = self.db_paths.get(mode)
        if mode not in {"paper", "shadow"} or path is None:
            return False
        key = (mode, position.position_id)
        with self._lock:
            if key in self._pending:
                return False
            self._pending.add(key)
        self._executor.submit(self._run, mode, position, path, key)
        return True

    def _run(
        self,
        mode: str,
        position: VirtualPosition,
        path: Path,
        key: tuple[str, str],
    ) -> None:
        try:
            for attempt in range(self.retry_count):
                if self._stop.is_set():
                    return
                try:
                    snapshot = self.features.fetch_exit_holders_snapshot(position)
                    holders = (
                        _non_negative_int(_field_value(snapshot.fields, "holders"))
                        if snapshot is not None
                        else None
                    )
                    if snapshot is not None and holders is not None:
                        persist_exit_holders_update(
                            path,
                            mode=mode,
                            position_id=position.position_id,
                            holders=holders,
                            observed_at=snapshot.observed_at,
                            source="binance_dynamic",
                            status="completed",
                        )
                        return
                except Exception:
                    # The next bounded attempt is the only retry path. The
                    # error itself is intentionally not exposed with secrets.
                    pass
                if attempt + 1 < self.retry_count and self._stop.wait(self.retry_delay_sec):
                    return
            persist_exit_holders_update(
                path,
                mode=mode,
                position_id=position.position_id,
                holders=None,
                observed_at=None,
                source=None,
                status="unavailable",
            )
        finally:
            with self._lock:
                self._pending.discard(key)

    def shutdown(self) -> None:
        self._stop.set()
        self._executor.shutdown(wait=False, cancel_futures=True)


class ExitMarketBackfill:
    """Bounded, non-blocking Binance Dynamic market backfill after a close."""

    def __init__(
        self,
        features: "BinanceRealtimeFeatureProvider",
        db_paths: Mapping[str, Path | None],
        *,
        max_workers: int = 2,
        retry_count: int = 3,
        retry_delay_sec: float = 2.0,
    ) -> None:
        self.features = features
        self.db_paths = dict(db_paths)
        self.retry_count = max(1, min(3, int(retry_count)))
        self.retry_delay_sec = max(0.0, float(retry_delay_sec))
        self._stop = Event()
        self._lock = Lock()
        self._pending: set[tuple[str, str]] = set()
        self._executor = ThreadPoolExecutor(
            max_workers=max(1, int(max_workers)),
            thread_name_prefix="exit-market-backfill",
        )

    def submit(self, mode: str, position: VirtualPosition) -> bool:
        path = self.db_paths.get(mode)
        if mode not in {"paper", "shadow"} or path is None:
            return False
        key = (mode, position.position_id)
        with self._lock:
            if key in self._pending:
                return False
            self._pending.add(key)
        self._executor.submit(self._run, mode, position, path, key)
        return True

    def _run(
        self,
        mode: str,
        position: VirtualPosition,
        path: Path,
        key: tuple[str, str],
    ) -> None:
        try:
            for attempt in range(self.retry_count):
                if self._stop.is_set():
                    return
                try:
                    snapshot = self.features.fetch_exit_market_snapshot(position)
                    market_cap = (
                        _decimal_value(_field_value(snapshot.fields, "market_cap_usd"))
                        if snapshot is not None
                        else None
                    )
                    liquidity = (
                        _decimal_value(_field_value(snapshot.fields, "liquidity_usd"))
                        if snapshot is not None
                        else None
                    )
                    if market_cap is not None and (not market_cap.is_finite() or market_cap < 0):
                        market_cap = None
                    if liquidity is not None and (not liquidity.is_finite() or liquidity < 0):
                        liquidity = None
                    if snapshot is not None and (market_cap is not None or liquidity is not None):
                        persist_exit_market_update(
                            path,
                            mode=mode,
                            position_id=position.position_id,
                            market_cap_usd=market_cap,
                            liquidity_usd=liquidity,
                            observed_at=snapshot.observed_at,
                            source="binance_dynamic",
                            status="completed",
                        )
                        return
                except Exception:
                    # Only the bounded attempts below are allowed. Secrets or
                    # provider response bodies never enter the audit output.
                    pass
                if attempt + 1 < self.retry_count and self._stop.wait(self.retry_delay_sec):
                    return
            persist_exit_market_update(
                path,
                mode=mode,
                position_id=position.position_id,
                market_cap_usd=None,
                liquidity_usd=None,
                observed_at=None,
                source=None,
                status="unavailable",
            )
        finally:
            with self._lock:
                self._pending.discard(key)

    def shutdown(self) -> None:
        self._stop.set()
        self._executor.shutdown(wait=False, cancel_futures=True)


def _field_value(fields: Mapping[str, object], name: str) -> object | None:
    field = fields.get(name)
    if field is None:
        return None
    available = getattr(field, "available", False)
    return getattr(field, "value", None) if available else None


def _decimal_value(value: object | None) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _positive_decimal(value: object | None) -> Decimal | None:
    parsed = _decimal_value(value)
    if parsed is None or not parsed.is_finite() or parsed <= 0:
        return None
    return parsed


def _non_negative_int(value: object | None) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _relative_drop_pct(entry_value: object | None, current_value: object | None) -> Decimal | None:
    entry = _decimal_value(entry_value)
    current = _decimal_value(current_value)
    if entry is None or current is None or not entry.is_finite() or not current.is_finite():
        return None
    if entry <= 0 or current < 0:
        return None
    return (entry - current) / entry * Decimal("100")


class BinanceRealtimeFeatureProvider:
    """Build conservative entry/exit features from confirmed adapter fields."""

    def __init__(
        self,
        *,
        market_data: BinanceWeb3MarketDataAdapter | None = None,
        quote_provider: QuoteProvider | None = None,
        position_size_sol: Decimal = Decimal("0.001"),
        chain_id: str = "CT_501",
        live_executor: object | None = None,
        bsc_quote_provider: BscReadOnlyQuoteProvider | None = None,
        bsc_executable_quote_enabled: bool = False,
        bsc_pool_resolver: BscPoolResolver | None = None,
        solana_price_monitor: SolanaPriceMonitor | None = None,
        pump_quote_provider: PumpProtocolReadOnlyQuoteProvider | None = None,
    ) -> None:
        self.market_data = market_data
        self.quote_provider = quote_provider
        self.position_size_sol = position_size_sol
        self.chain_id = chain_id
        self.is_bsc = chain_id == "56"
        self.live_executor = live_executor
        self.bsc_quote_provider = bsc_quote_provider
        self.bsc_executable_quote_enabled = bool(bsc_executable_quote_enabled)
        self.bsc_pool_resolver = bsc_pool_resolver
        self.solana_price_monitor = solana_price_monitor
        self.pump_quote_provider = pump_quote_provider
        self._position_snapshots: dict[str, BinanceMarketSnapshot] = {}
        self._latest_market_snapshots: dict[str, BinanceMarketSnapshot] = {}

    def entry_features(
        self,
        normalized: BinanceNormalizedSignal,
        evaluated_at: datetime | None = None,
        *,
        include_bsc_quote: bool = True,
    ) -> EntryFeatures:
        evaluated_at = evaluated_at or utc_now()
        fields = dict(normalized.fields)
        dynamic: BinanceMarketSnapshot | None = None
        dynamic_error: str | None = None
        dynamic_available = False
        if self.market_data is not None:
            try:
                dynamic = self.market_data.snapshot(normalized.signal.mint)
                if self.is_bsc:
                    # Meme Rush already carries a Binance current price. For
                    # BSC, a transiently unavailable Dynamic field must not
                    # erase that real source value; no synthetic/default value
                    # is introduced. Solana keeps the original merge behavior.
                    for name, field in dynamic.fields.items():
                        if field.available or name not in fields:
                            fields[name] = field
                else:
                    fields.update(dynamic.fields)
                self._latest_market_snapshots[normalized.signal.mint] = dynamic
                dynamic_available = True
            except BinanceWeb3Error as exc:
                dynamic_error = exc.context.error_class
            except Exception:
                dynamic_error = "binance_dynamic_unavailable"

        raw_name = _first_string(_field_value(fields, "name"))
        symbol = _first_string(_field_value(fields, "symbol"))
        display_name = clean_token_name(raw_name) if not self.is_bsc else raw_name
        token_name = (
            _first_string(symbol, raw_name)
            if self.is_bsc
            else _first_string(display_name, symbol)
        )
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
            "native_token_price",
            "progress_pct",
            "migrate_status",
            "dev_position",
            "dev_sold_percent",
            "pair_address",
            "bonding_curve_address",
            "protocol",
            "token_version",
            "token_decimals",
        ):
            value = _field_value(fields, name)
            if value is not None:
                soft[name] = value
        if dynamic_error is not None:
            soft["dynamic_error_class"] = dynamic_error
        if dynamic_available:
            soft["observation_snapshot_available"] = True
        if not self.is_bsc:
            soft.update({
                "raw_name": raw_name,
                "display_name": display_name,
                "symbol": symbol,
            })

        buy_quote: ExecutableQuote | None = None
        sell_quote: ExecutableQuote | None = None
        quote_errors: list[str] = []
        pricing_mode = (
            "bsc_executable_quote"
            if self.is_bsc and self.bsc_executable_quote_enabled
            else "binance_indicative" if self.is_bsc else "executable_quote"
        )
        executable_quote = not self.is_bsc or self.bsc_executable_quote_enabled
        pricing_error: str | None = None
        if self.is_bsc and self.live_executor is not None:
            try:
                buy_quote = self.live_executor.quote(normalized.signal.mint, "buy", self.position_size_sol)
                if buy_quote is not None and buy_quote.output_quantity > 0:
                    sell_quote = self.live_executor.quote(
                        normalized.signal.mint,
                        "sell",
                        buy_quote.output_quantity,
                    )
                if buy_quote is None or sell_quote is None:
                    pricing_error = "pancakeswap_quote_unavailable"
            except Exception as exc:
                pricing_error = type(exc).__name__
            pricing_mode = "pancakeswap_smart_router"
            executable_quote = True
            soft.update(
                {
                    "pricing_mode": pricing_mode,
                    "executable_quote": executable_quote,
                    "net_pnl_is_estimated": True,
                    "live_quote_source": "pancakeswap_smart_router",
                }
            )
            if pricing_error is not None:
                soft["pricing_error"] = pricing_error
        elif self.is_bsc and self.bsc_executable_quote_enabled:
            soft.update(
                {
                    "pricing_mode": pricing_mode,
                    "executable_quote": executable_quote,
                    "net_pnl_is_estimated": False,
                    "pricing_reference_mode": "binance_indicative_reference",
                    "pricing_reference_executable_quote": False,
                }
            )
            if include_bsc_quote:
                buy_quote, sell_quote, pricing_error = self._bsc_entry_quote_pair(
                    normalized.signal.mint,
                )
                soft["quote_requested"] = True
                if pricing_error is not None:
                    soft["pricing_error"] = pricing_error
            else:
                # The candidate must first survive purely local gates. This
                # avoids one RPC/router request for every raw Meme Rush row.
                soft["quote_requested"] = False
        elif self.is_bsc:
            buy_quote, sell_quote, pricing_error = self._indicative_entry_quotes(
                normalized.signal.mint,
                fields,
                dynamic,
                evaluated_at,
            )
            soft.update(
                {
                    "pricing_mode": "binance_indicative",
                    "executable_quote": False,
                    "net_pnl_is_estimated": True,
                }
            )
            if pricing_error is not None:
                soft["pricing_error"] = pricing_error
        else:
            pump_state = self._solana_market_state(normalized.signal.mint)
            if pump_state is not None:
                soft["trading_stage"] = pump_state.state
            if (
                pump_state is not None
                and pump_state.state == "PUMP_BONDING_CURVE"
                and self.pump_quote_provider is not None
            ):
                pricing_mode = "pump_bonding_curve_quote"
                buy_quote = self.pump_quote_provider.quote_state(
                    pump_state, "buy", self.position_size_sol
                )
                if buy_quote.output_quantity > 0:
                    sell_quote = self.pump_quote_provider.quote_state(
                        pump_state, "sell", buy_quote.output_quantity
                    )
                for quote in (buy_quote, sell_quote):
                    if quote is not None and quote.error_class:
                        quote_errors.append(quote.error_class)
            elif self.quote_provider is not None:
                pricing_mode = "jupiter_quote"
                buy_quote = self._quote(normalized.signal.mint, "buy", self.position_size_sol, quote_errors)
                if buy_quote is not None and buy_quote.output_quantity > 0:
                    sell_quote = self._quote(normalized.signal.mint, "sell", buy_quote.output_quantity, quote_errors)
        if quote_errors:
            soft["quote_error_classes"] = tuple(sorted(set(quote_errors)))

        # The currently confirmed Binance fields do not expose the frozen
        # 5..120s age, 15s buyer ratio/net-buy windows, or a confident creator
        # sell boolean. They remain unavailable instead of being synthesized
        # from 5m/24h fields or timestamp units that are not frozen.
        native_usd: Decimal | None = None
        if not self.is_bsc:
            token_usd = _positive_decimal(_field_value(fields, "price_usd"))
            token_native = _positive_decimal(_field_value(fields, "native_token_price"))
            if token_usd is not None and token_native is not None:
                native_usd = token_usd / token_native
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
            holders=_field_value(fields, "holders"),
            market_cap_usd=_field_value(fields, "market_cap_usd"),
            liquidity_usd=_field_value(fields, "liquidity_usd"),
            pricing_mode=pricing_mode,
            executable_quote=executable_quote,
            pricing_error=pricing_error,
            raw_name=raw_name if not self.is_bsc else None,
            display_name=display_name if not self.is_bsc else None,
            symbol=symbol if not self.is_bsc else None,
            native_usd=native_usd,
        )

    def bsc_entry_features_with_quote(
        self,
        normalized: BinanceNormalizedSignal,
        features: EntryFeatures,
    ) -> EntryFeatures:
        """Attach one real BSC entry/instant-exit quote after local gates."""

        if not (self.is_bsc and self.bsc_executable_quote_enabled):
            return features
        buy_quote, sell_quote, pricing_error = self._bsc_entry_quote_pair(normalized.signal.mint)
        soft = dict(features.soft_features or {})
        soft["quote_requested"] = True
        if pricing_error is None:
            soft.pop("pricing_error", None)
        else:
            soft["pricing_error"] = pricing_error
        return replace(
            features,
            buy_quote=buy_quote,
            sell_quote=sell_quote,
            pricing_error=pricing_error,
            soft_features=soft,
        )

    def _bsc_entry_quote_pair(
        self,
        mint: str,
    ) -> tuple[ExecutableQuote | None, ExecutableQuote | None, str | None]:
        if self.bsc_quote_provider is None:
            return None, None, "bsc_executable_quote_provider_unavailable"
        return self.bsc_quote_provider.quote_candidate(mint, self.position_size_sol)

    def quote_for_position(self, position: VirtualPosition) -> ExecutableQuote | None:
        if self.is_bsc:
            if self.live_executor is not None:
                if position.active_quantity_token <= 0:
                    return None
                try:
                    return self.live_executor.quote(
                        position.mint,
                        "sell",
                        position.active_quantity_token,
                    )
                except Exception:
                    return None
            if position.active_quantity_token <= 0:
                return None
            if self.bsc_executable_quote_enabled:
                if self.bsc_quote_provider is None:
                    return None
                try:
                    return self.bsc_quote_provider.quote(
                        position.mint,
                        "sell",
                        position.active_quantity_token,
                    )
                except Exception:
                    return None
            if self.market_data is None:
                return None
            try:
                snapshot = self.market_data.snapshot(position.mint)
            except Exception:
                return None
            self._position_snapshots[position.position_id] = snapshot
            self._latest_market_snapshots[position.mint] = snapshot
            return self._indicative_quote(
                position.mint,
                "sell",
                position.active_quantity_token,
                snapshot.fields,
                snapshot.observed_at,
                snapshot.raw_response_hash,
            )
        if position.active_quantity_token <= 0:
            return None
        pump_state = self._solana_market_state(position.mint)
        if (
            pump_state is not None
            and pump_state.state == "PUMP_BONDING_CURVE"
            and self.pump_quote_provider is not None
        ):
            return self.pump_quote_provider.quote_state(
                pump_state, "sell", position.active_quantity_token
            )
        if self.quote_provider is None:
            return None
        try:
            priority_quote = getattr(self.quote_provider, "quote_position", None)
            if callable(priority_quote):
                return priority_quote(
                    position.mint,
                    position.active_quantity_token,
                )
            return self.quote_provider.quote(
                position.mint,
                "sell",
                position.active_quantity_token,
            )
        except Exception:
            return None

    def local_quote_for_position(
        self,
        position: VirtualPosition,
        observed: SolanaObservedPrice,
    ) -> ExecutableQuote | None:
        """Convert a pool/curve observation into a non-executable sell valuation."""
        if self.is_bsc or observed.mint != position.mint or position.active_quantity_token <= 0:
            return None
        output_quantity = position.active_quantity_token * observed.price_sol_per_token
        if output_quantity <= 0:
            return None
        quote_id = f"pool-wss:{observed.account_address}:{observed.observed_slot or 'na'}:{observed.observed_at.timestamp()}"
        return ExecutableQuote(
            quote_id=quote_id,
            mint=position.mint,
            side="sell",
            input_quantity=position.active_quantity_token,
            output_quantity=output_quantity,
            route_fee=None,
            price_impact_pct=None,
            quoted_at=observed.observed_at,
            age_ms=0,
            provider="pool_wss_indicative",
            route=(observed.stage,),
            quote_context_slot=observed.observed_slot,
            requested_at=observed.observed_at,
            received_at=observed.observed_at,
            executable_style=False,
            confidence="indicative",
        )

    def timeout_fallback_quote(self, position: VirtualPosition) -> ExecutableQuote | None:
        """Build a timeout valuation from the latest observed read-only data."""
        if self.is_bsc or position.active_quantity_token <= 0:
            return None
        if self.solana_price_monitor is not None:
            observed = self.solana_price_monitor.latest(position.mint)
            if observed is not None and (utc_now() - observed.observed_at).total_seconds() <= 5:
                local_quote = self.local_quote_for_position(position, observed)
                if local_quote is not None:
                    return local_quote
        if self.market_data is None:
            return None
        snapshot = self._position_snapshots.get(position.position_id)
        if snapshot is None:
            snapshot = self._latest_market_snapshots.get(position.mint)
        if snapshot is None:
            return None
        if (utc_now() - snapshot.observed_at).total_seconds() > 10:
            return None
        return self._indicative_quote(
            position.mint,
            "sell",
            position.active_quantity_token,
            snapshot.fields,
            snapshot.observed_at,
            snapshot.raw_response_hash,
        )

    def price_snapshot_for_position(
        self,
        position: VirtualPosition,
        quote: ExecutableQuote | None,
        *,
        pricing_mode: str,
        executable_quote: bool,
        now: datetime,
    ) -> PriceSnapshot | None:
        """Build a Solana boundary snapshot without mixing stale USD data."""
        if self.is_bsc:
            return None
        native_usd: Decimal | None = None
        market = self._position_snapshots.get(position.position_id)
        if market is None:
            market = self._latest_market_snapshots.get(position.mint)
        if market is not None and quote is not None and quote.quoted_at is not None:
            try:
                age_sec = abs((quote.quoted_at - market.observed_at).total_seconds())
            except (TypeError, ValueError):
                age_sec = float("inf")
            if age_sec <= 10:
                token_usd = _positive_decimal(_field_value(market.fields, "price_usd"))
                token_native = _positive_decimal(_field_value(market.fields, "native_token_price"))
                if token_usd is not None and token_native is not None:
                    native_usd = token_usd / token_native
        return DeterministicSimulation.build_price_snapshot(
            quote,
            native_symbol="SOL",
            native_usd=native_usd,
            pricing_mode=pricing_mode,
            executable_quote=executable_quote,
            now=now,
        )

    def resolve_bsc_pool(
        self,
        mint: str,
        pair_address: object | None,
        bonding_curve_address: object | None = None,
        *,
        protocol: object | None = None,
        migrate_status: object | None = None,
    ) -> BscPoolDescriptor | None:
        if not self.is_bsc or self.bsc_pool_resolver is None:
            return None
        # A resolved Four context is authoritative over Binance discovery
        # fields.  This accessor is cache-only, so it cannot introduce quote
        # RPC traffic for every raw signal.
        cached_context = None
        cached_context_for = getattr(self.bsc_quote_provider, "cached_context", None)
        if callable(cached_context_for):
            cached_context = cached_context_for(mint)
        if cached_context is not None:
            if cached_context.migrated and cached_context.pancake_pair is not None:
                pair_address = cached_context.pancake_pair
                bonding_curve_address = None
                protocol = None
                migrate_status = None
            elif not cached_context.migrated:
                pair_address = None
                bonding_curve_address = cached_context.launchpad
                protocol = 2002
                migrate_status = 0
        return self.bsc_pool_resolver.resolve(
            mint,
            pair_address,
            bonding_curve_address,
            protocol=protocol,
            migrate_status=migrate_status,
        )

    def bsc_pool_quote_for_event(
        self,
        position: VirtualPosition,
        event: BscPairEvent,
        descriptor: BscPoolDescriptor | None,
    ) -> ExecutableQuote | None:
        """Convert an observed BSC pool price into an indicative valuation.

        This is the existing Paper/Shadow fast-path.  It remains explicitly
        non-executable and the 2-second Binance snapshot poll remains the
        fallback when a pool event cannot be decoded.
        """
        if self.bsc_executable_quote_enabled or self.bsc_pool_resolver is None:
            return None
        if position.active_quantity_token <= 0 or descriptor is None:
            return None
        pool_price = self.bsc_pool_resolver.price_from_event(descriptor, event)
        if pool_price is None or pool_price.mint.lower() != position.mint.lower():
            return None
        output_quantity = position.active_quantity_token * pool_price.native_token_price
        if output_quantity <= 0:
            return None
        return ExecutableQuote(
            quote_id=(
                f"bsc-pool-wss:{pool_price.pool_address}:{pool_price.block_number or 'na'}:"
                f"{pool_price.log_index or 'na'}"
            ),
            mint=position.mint,
            side="sell",
            input_quantity=position.active_quantity_token,
            output_quantity=output_quantity,
            route_fee=None,
            price_impact_pct=None,
            quoted_at=pool_price.observed_at,
            age_ms=0,
            provider="bsc_pool_wss",
            route=(pool_price.pool_type,),
            raw_response_hash=pool_price.raw_response_hash,
            executable_style=False,
            confidence="indicative",
        )

    def position_holders(self, position_id: str) -> int | None:
        """Return holders from the latest BSC position snapshot, if available."""
        if not self.is_bsc:
            return None
        snapshot = self._position_snapshots.get(position_id)
        if snapshot is None:
            return None
        value = _field_value(snapshot.fields, "holders")
        if value is None or isinstance(value, bool):
            return None
        try:
            holders = int(value)
        except (TypeError, ValueError):
            return None
        return holders if holders >= 0 else None

    def fetch_exit_holders(self, position: VirtualPosition) -> int | None:
        """Compatibility wrapper around the read-only Dynamic snapshot fetch."""

        try:
            snapshot = self.fetch_exit_holders_snapshot(position)
        except Exception:
            return None
        if snapshot is None:
            return None
        value = _field_value(snapshot.fields, "holders")
        if value is None or isinstance(value, bool):
            return None
        try:
            holders = int(value)
        except (TypeError, ValueError):
            return None
        return holders if holders >= 0 else None

    def fetch_exit_holders_snapshot(
        self,
        position: VirtualPosition,
    ) -> BinanceMarketSnapshot | None:
        """Fetch one pure Binance Dynamic snapshot for asynchronous backfill."""

        if self.market_data is None:
            return None
        return self.market_data.snapshot(position.mint)

    def fetch_exit_market_snapshot(
        self,
        position: VirtualPosition,
    ) -> BinanceMarketSnapshot | None:
        """Fetch one pure Binance Dynamic market snapshot for async backfill."""

        if self.market_data is None:
            return None
        return self.market_data.snapshot(position.mint)

    def latest_holders_snapshot(
        self,
        position: VirtualPosition,
        observed_at: datetime,
        *,
        max_age_sec: int = 10,
    ) -> ExitHoldersSnapshot | None:
        """Read the newest in-memory Mint snapshot when it is fresh enough."""

        snapshots = [
            snapshot
            for snapshot in (
                self._position_snapshots.get(position.position_id),
                self._latest_market_snapshots.get(position.mint),
            )
            if snapshot is not None
        ]
        if not snapshots:
            return None
        snapshot = max(snapshots, key=lambda item: item.observed_at)
        age_sec = (observed_at - snapshot.observed_at).total_seconds()
        if age_sec < 0 or age_sec > max(0, int(max_age_sec)):
            return None
        holders = _non_negative_int(_field_value(snapshot.fields, "holders"))
        if holders is None:
            return None
        return ExitHoldersSnapshot(
            holders=holders,
            observed_at=snapshot.observed_at,
            source="binance_dynamic",
        )

    def latest_market_snapshot(
        self,
        position: VirtualPosition,
        observed_at: datetime,
        *,
        max_age_sec: int = 10,
    ) -> ExitMarketSnapshot | None:
        """Read the newest in-memory Dynamic snapshot if it is close to exit."""

        snapshots = [
            snapshot
            for snapshot in (
                self._position_snapshots.get(position.position_id),
                self._latest_market_snapshots.get(position.mint),
            )
            if snapshot is not None
        ]
        if not snapshots:
            return None
        snapshot = max(snapshots, key=lambda item: item.observed_at)
        age_sec = (observed_at - snapshot.observed_at).total_seconds()
        if age_sec < 0 or age_sec > max(0, int(max_age_sec)):
            return None
        market_cap = _decimal_value(_field_value(snapshot.fields, "market_cap_usd"))
        liquidity = _decimal_value(_field_value(snapshot.fields, "liquidity_usd"))
        if market_cap is not None and (not market_cap.is_finite() or market_cap < 0):
            market_cap = None
        if liquidity is not None and (not liquidity.is_finite() or liquidity < 0):
            liquidity = None
        if market_cap is None and liquidity is None:
            return None
        return ExitMarketSnapshot(
            market_cap_usd=market_cap,
            liquidity_usd=liquidity,
            observed_at=snapshot.observed_at,
            source="binance_dynamic",
        )

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
        holders: int | None = None
        liquidity_usd: Decimal | None = None
        if self.is_bsc:
            snapshot = self._position_snapshots.get(position.position_id)
            if snapshot is not None:
                holders = _non_negative_int(_field_value(snapshot.fields, "holders"))
                liquidity_value = _decimal_value(_field_value(snapshot.fields, "liquidity_usd"))
                if liquidity_value is not None and liquidity_value.is_finite() and liquidity_value >= 0:
                    liquidity_usd = liquidity_value
        elif self.market_data is not None:
            try:
                snapshot = self.market_data.snapshot(position.mint)
                self._position_snapshots[position.position_id] = snapshot
                self._latest_market_snapshots[position.mint] = snapshot
                holders = _non_negative_int(_field_value(snapshot.fields, "holders"))
                liquidity_value = _decimal_value(_field_value(snapshot.fields, "liquidity_usd"))
                if liquidity_value is not None and liquidity_value.is_finite() and liquidity_value >= 0:
                    liquidity_usd = liquidity_value
            except Exception:
                pass
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
            holders=holders,
            liquidity_usd=liquidity_usd,
        )

    def _indicative_entry_quotes(
        self,
        mint: str,
        fields: Mapping[str, object],
        snapshot: BinanceMarketSnapshot | None,
        evaluated_at: datetime,
    ) -> tuple[ExecutableQuote | None, ExecutableQuote | None, str | None]:
        price = _positive_decimal(_field_value(fields, "price_usd"))
        native_price = _positive_decimal(_field_value(fields, "native_token_price"))
        if price is None:
            return None, None, "bsc_price_unavailable"
        if native_price is None:
            return None, None, "bsc_native_token_price_unavailable"
        quoted_at = snapshot.observed_at if snapshot is not None else evaluated_at
        raw_hash = snapshot.raw_response_hash if snapshot is not None else ""
        buy_quote = self._indicative_quote(
            mint,
            "buy",
            self.position_size_sol,
            fields,
            quoted_at,
            raw_hash,
        )
        if buy_quote is None:
            return None, None, "bsc_price_unavailable"
        sell_quote = self._indicative_quote(
            mint,
            "sell",
            buy_quote.output_quantity,
            fields,
            quoted_at,
            raw_hash,
        )
        if sell_quote is None:
            return None, None, "bsc_price_unavailable"
        return buy_quote, sell_quote, None

    def _indicative_quote(
        self,
        mint: str,
        side: str,
        input_quantity: Decimal,
        fields: Mapping[str, object],
        quoted_at: datetime,
        raw_hash: str,
    ) -> ExecutableQuote | None:
        price = _positive_decimal(_field_value(fields, "price_usd"))
        native_price = _positive_decimal(_field_value(fields, "native_token_price"))
        if (
            price is None
            or native_price is None
            or input_quantity <= 0
        ):
            return None
        if side == "buy":
            output_quantity = input_quantity * native_price / price
        elif side == "sell":
            output_quantity = input_quantity * price / native_price
        else:
            raise ValueError("indicative quote side must be buy or sell")
        if output_quantity <= 0:
            return None
        quote_id = f"binance-indicative:{raw_hash[:24]}:{side}:{quoted_at.isoformat()}"
        return ExecutableQuote(
            quote_id=quote_id,
            mint=mint,
            side=side,
            input_quantity=input_quantity,
            output_quantity=output_quantity,
            route_fee=None,
            price_impact_pct=None,
            quoted_at=quoted_at,
            age_ms=max(0, int((utc_now() - quoted_at).total_seconds() * 1000)),
            provider="binance_web3",
            route=("binance_indicative",),
            raw_response_hash=raw_hash or None,
            executable_style=False,
            confidence="indicative",
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

    def _solana_market_state(self, mint: str) -> PumpMarketState | None:
        if self.is_bsc or self.pump_quote_provider is None:
            return None
        try:
            return self.pump_quote_provider.state_adapter.inspect(mint)
        except Exception:
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


@dataclass(frozen=True)
class PositionCycleResult:
    started_at: datetime
    finished_at: datetime
    trigger: str
    event_count: int
    positions: int
    exits: Mapping[str, int]


@dataclass(frozen=True)
class PendingSignalObservation:
    record: BinanceNormalizedSignal
    first_price_usd: Decimal | None
    discovered_at: datetime
    first_holders: int | None = None
    first_liquidity_usd: Decimal | None = None


@dataclass
class PendingShadowFollowUp:
    engine: DeterministicSimulation
    position: VirtualPosition
    exited_at: datetime
    exit_return_pct: Decimal | None
    returns_after_exit_pct: dict[int, Decimal | None] = field(default_factory=dict)


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
        observation_delay_sec: int = 15,
    ) -> None:
        if not engines or any(mode not in {"paper", "shadow", "live"} for mode in engines):
            raise ValueError("engines must contain paper, shadow and/or live")
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
        if observation_delay_sec < 1:
            raise ValueError("observation_delay_sec must be positive")
        self.observation_delay = timedelta(seconds=observation_delay_sec)
        self.pending_observations: dict[str, PendingSignalObservation] = {}
        self._bsc_pool_by_mint: dict[str, str] = {}
        self._bsc_mints_by_pool: dict[str, set[str]] = {}
        self._bsc_descriptor_by_mint: dict[str, BscPoolDescriptor] = {}
        self._bsc_descriptors_by_pool: dict[str, BscPoolDescriptor] = {}
        self._bsc_event_lock = Lock()
        self._bsc_pair_events: deque[BscPairEvent] = deque()
        self._bsc_refresh_event = Event()
        self._bsc_wss_healthy = False
        self._bsc_last_pool_quotes: dict[str, ExecutableQuote] = {}
        self._bsc_wss_status_key: tuple[object, ...] | None = None
        self._bsc_quote_local_conditions_passed = 0
        self._bsc_quote_final_entries = 0
        self._db_lock = RLock()
        self._position_quote_lock = Lock()
        self._position_quotes: dict[tuple[str, str], ExecutableQuote | None] = {}
        self.solana_price_monitor = getattr(features, "solana_price_monitor", None)
        self._solana_event_lock = Lock()
        self._solana_events: deque[Mapping[str, object]] = deque()
        self._shadow_followups: dict[str, PendingShadowFollowUp] = {}
        self._exit_holders_backfill = ExitHoldersBackfill(
            self.features,
            {
                mode: database_path(store.connection)
                for mode, store in self.stores.items()
                if mode in {"paper", "shadow"}
            },
        )
        self._exit_market_backfill = ExitMarketBackfill(
            self.features,
            {
                mode: database_path(store.connection)
                for mode, store in self.stores.items()
                if mode in {"paper", "shadow"}
            },
        )
        self._live_startup_snapshot_complete = "live" not in self.engines
        self._live_startup_excluded_mints: set[str] = set()
        if "live" in self.engines:
            live_ledger = self.engines["live"].ledger
            self._live_startup_excluded_mints.update(
                position.mint
                for position in (*live_ledger.positions.values(), *live_ledger.closed_positions.values())
            )
            if live_ledger.connection is not None:
                rows = live_ledger.connection.execute(
                    "SELECT DISTINCT mint FROM candidates WHERE mode = 'live'"
                )
                self._live_startup_excluded_mints.update(str(row[0]) for row in rows if row[0])
        if self.features.is_bsc:
            self._hydrate_bsc_active_pools()
        for mode, engine in self.engines.items():
            if mode not in {"paper", "shadow"}:
                continue
            for position in tuple(engine.ledger.closed_positions.values()):
                if position.exit_holders_status == "pending":
                    self._exit_holders_backfill.submit(mode, position)
                if position.exit_market_status == "pending":
                    self._exit_market_backfill.submit(mode, position)

    def shutdown(self) -> None:
        """Stop bounded holder backfill workers without changing lifecycle state."""

        self._exit_holders_backfill.shutdown()
        self._exit_market_backfill.shutdown()

    def refresh_position_quotes(self, *, trigger: str = "poll") -> int:
        """Fetch active-position sell Quotes without touching SQLite state."""

        jobs: list[tuple[str, VirtualPosition]] = []
        with self._db_lock:
            for mode, engine in self.engines.items():
                jobs.extend((mode, position) for position in tuple(engine.ledger.active_positions))
        if not jobs:
            return 0
        fetched: dict[tuple[str, str], ExecutableQuote | None] = {}
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="solana-position-quote") as executor:
            future_map = {
                executor.submit(self.features.quote_for_position, position): (mode, position)
                for mode, position in jobs
            }
            for future, (mode, position) in future_map.items():
                try:
                    quote = future.result()
                except Exception:
                    quote = None
                key = (mode, position.position_id)
                fetched[key] = quote
                self._audit(mode, "POSITION_QUOTE_REQUEST", {
                    "position_id": position.position_id,
                    "mint": position.mint,
                    "trigger": trigger,
                    "quote_id": quote.quote_id if quote else None,
                    "quote_error_class": quote.error_class if quote else "quote_unavailable",
                    "quote_requested_at": quote.requested_at.isoformat() if quote and quote.requested_at else None,
                    "quote_request_times": [value.isoformat() for value in quote.request_times] if quote else [],
                    "quote_request_statuses": list(quote.request_statuses) if quote else [],
                    "quote_received_at": quote.received_at.isoformat() if quote and quote.received_at else None,
                    "quote_latency_ms": quote.latency_ms if quote else None,
                })
        with self._position_quote_lock:
            self._position_quotes.update(fetched)
        return len(jobs)

    def sync_solana_price_bindings(self) -> None:
        """Bind active Solana positions to their Pump curve or PumpSwap accounts."""
        if self.features.is_bsc or self.solana_price_monitor is None:
            return
        mints = {
            position.mint
            for engine in self.engines.values()
            for position in engine.ledger.active_positions
        }
        for mint in sorted(mints):
            try:
                self.solana_price_monitor.register_position(mint)
            except Exception:
                continue
        self.solana_price_monitor.unregister_missing(mints)

    def solana_price_subscriptions(self) -> tuple[tuple[str, list[object]], ...]:
        if self.features.is_bsc or self.solana_price_monitor is None:
            return ()
        return self.solana_price_monitor.subscriptions()

    def notify_solana_wss_event(self, event: object) -> None:
        if not isinstance(event, Mapping):
            return
        if event.get("method") != "accountNotification":
            return
        with self._solana_event_lock:
            self._solana_events.append(dict(event))

    def process_solana_wss_events(self) -> int:
        """Apply local price events and settle only locally-triggered TP/SL."""
        if self.features.is_bsc or self.solana_price_monitor is None:
            return 0
        with self._solana_event_lock:
            events = tuple(self._solana_events)
            self._solana_events.clear()
        processed = 0
        for event in events:
            try:
                observed = self.solana_price_monitor.process_wss_event(event)
            except Exception:
                observed = None
            if observed is None:
                continue
            processed += 1
            for mode, engine in self.engines.items():
                if mode not in {"paper", "shadow"}:
                    continue
                for position in tuple(engine.ledger.active_positions):
                    if position.mint != observed.mint:
                        continue
                    local_quote = self.features.local_quote_for_position(position, observed)
                    if local_quote is None:
                        continue
                    with self._db_lock:
                        observation = engine.ledger.record_local_observation(
                            position.position_id,
                            observed.observed_at,
                            observed.price_sol_per_token,
                            account_address=observed.account_address,
                            observed_slot=observed.observed_slot,
                        )
                        current = engine.ledger.positions.get(position.position_id)
                        if current is None:
                            continue
                        decision = engine.strategy.evaluate_paper_exit(current, observed.observed_at, local_quote)
                    self._audit(mode, "SOLANA_LOCAL_PRICE_OBSERVED", {
                        "position_id": position.position_id,
                        "mint": position.mint,
                        "price_source": observed.source,
                        "price_sol_per_token": observed.price_sol_per_token,
                        "observed_at": observed.observed_at.isoformat(),
                        "account_address": observed.account_address,
                        "observed_slot": observed.observed_slot,
                        "return_pct": observation.return_pct,
                        "mfe_pct": observation.mfe_pct,
                        "mae_pct": observation.mae_pct,
                    })
                    if not decision.triggered or decision.reason not in {"take_profit", "stop_loss"}:
                        continue
                    jupiter_quote = self.features.quote_for_position(current)
                    usable_jupiter = (
                        jupiter_quote is not None
                        and jupiter_quote.mint == current.mint
                        and jupiter_quote.side == "sell"
                        and jupiter_quote.input_quantity == current.active_quantity_token
                        and jupiter_quote.output_quantity > 0
                        and jupiter_quote.unusable_reason(self.clock()) is None
                    )
                    settlement = jupiter_quote if usable_jupiter else local_quote
                    pricing_mode = "jupiter_quote" if usable_jupiter else "pool_wss_indicative"
                    self._audit(mode, "SOLANA_TRIGGERED_EXIT_QUOTE", {
                        "position_id": position.position_id,
                        "mint": position.mint,
                        "reason": decision.reason,
                        "pricing_mode": pricing_mode,
                        "executable_quote": bool(usable_jupiter),
                        "quote_id": settlement.quote_id if settlement is not None else None,
                        "quote_error_class": jupiter_quote.error_class if jupiter_quote and not usable_jupiter else None,
                    })
                    with self._db_lock:
                        engine.process_triggered_indicative_exit(
                            position.position_id,
                            observed.observed_at,
                            decision.reason or "exit",
                            settlement,
                            pricing_mode=pricing_mode,
                            executable_quote=bool(usable_jupiter),
                            local_observation=not usable_jupiter,
                            price_snapshot=self.features.price_snapshot_for_position(
                                current,
                                settlement,
                                pricing_mode=pricing_mode,
                                executable_quote=bool(usable_jupiter),
                                now=observed.observed_at,
                            ),
                        )
        return processed

    def _take_position_quote(self, mode: str, position_id: str) -> ExecutableQuote | None | object:
        key = (mode, position_id)
        with self._position_quote_lock:
            if key not in self._position_quotes:
                return _POSITION_QUOTE_MISSING
            return self._position_quotes.pop(key)

    def process_queued_position_exits(self) -> dict[str, int]:
        """Consume only quotes fetched by the dedicated Solana position scheduler."""
        exits = {mode: 0 for mode in self.engines}
        if self.features.is_bsc:
            return exits
        for mode, engine in self.engines.items():
            with self._db_lock:
                active_positions = tuple(engine.ledger.active_positions)
            for position in active_positions:
                queued_quote = self._take_position_quote(mode, position.position_id)
                if queued_quote is _POSITION_QUOTE_MISSING and (
                    position.status != "EXIT_TRIGGERED"
                    and int((self.clock() - position.opened_at).total_seconds()) < engine.strategy.config.max_hold_sec
                ):
                    continue
                event_start = len(engine.ledger.lifecycle_events)
                with self._db_lock:
                    result = self._process_exit(
                        mode,
                        engine,
                        position,
                        quote=None if queued_quote is _POSITION_QUOTE_MISSING else queued_quote,
                        quote_already_fetched=queued_quote is not _POSITION_QUOTE_MISSING,
                    )
                self._notify_live_events(mode, engine, event_start)
                if result is not None and result.closed_position is not None:
                    exits[mode] += 1
        self._process_shadow_followups(self.clock())
        return exits

    def run_cycle(self, *, process_exits: bool = True) -> CycleResult:
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
                with self._db_lock:
                    self.stores[mode].record_latency("binance_signal_poll", elapsed)
                self._set_health(mode, "binance_web3", "HEALTHY", latency_ms=int(elapsed))
        except BinanceWeb3Error as exc:
            source_error = exc.context.error_class
            for mode in self.engines:
                self._set_health(mode, "binance_web3", "UNAVAILABLE", error_class=source_error)
                self._audit(mode, "SOURCE_ERROR", {"error_class": source_error})
            return CycleResult(started, self.clock(), 0, 0, 0, 0, accepted, exits, source_error)

        fetched = len(records)
        if "live" in self.engines and not self._live_startup_snapshot_complete:
            self._live_startup_excluded_mints.update(record.signal.mint for record in records)
            self._live_startup_snapshot_complete = True
        for record in records:
            self._remember_bsc_pool(record)
            signal = record.signal
            if "live" in self.engines and signal.mint in self._live_startup_excluded_mints:
                bootstrap_skipped += 1
                for mode in self.engines:
                    if mode == "live":
                        self._audit(mode, "LIVE_STARTUP_TOKEN_SKIPPED", {
                            "signal_id": signal.signal_id,
                            "mint": signal.mint,
                        })
                continue
            if record.historical_bootstrap:
                bootstrap_skipped += 1
                for mode in self.engines:
                    self._audit(mode, "SIGNAL_BOOTSTRAP_SKIPPED", {"signal_id": signal.signal_id, "mint": signal.mint})
                continue
            if signal.signal_id in self.pending_observations:
                if self.features.is_bsc:
                    pending = self.pending_observations[signal.signal_id]
                    self.pending_observations[signal.signal_id] = replace(
                        pending,
                        record=record,
                    )
                continue
            matching_pending = next(
                (
                    (pending_id, pending)
                    for pending_id, pending in self.pending_observations.items()
                    if pending.record.signal.mint == signal.mint
                ),
                None,
            )
            if matching_pending is not None:
                if self.features.is_bsc:
                    pending_id, pending = matching_pending
                    self.pending_observations[pending_id] = replace(
                        pending,
                        record=record,
                    )
                continue
            if all(
                engine.ledger.candidate_exists(
                    f"{mode}:{signal.signal_id}:{engine.strategy.config.identity.ruleset_version}"
                )
                for mode, engine in self.engines.items()
            ):
                duplicate_skipped += 1
                for mode, engine in self.engines.items():
                    candidate_id = f"{mode}:{signal.signal_id}:{engine.strategy.config.identity.ruleset_version}"
                    self._audit(mode, "DUPLICATE_SIGNAL_SKIPPED", {"signal_id": signal.signal_id, "candidate_id": candidate_id})
                continue
            discovered_at = self.clock()
            pending = PendingSignalObservation(
                record=record,
                first_price_usd=_decimal_value(_field_value(record.fields, "price_usd")),
                discovered_at=discovered_at,
                first_holders=_non_negative_int(_field_value(record.fields, "holders")),
                first_liquidity_usd=_decimal_value(_field_value(record.fields, "liquidity_usd")),
            )
            self.pending_observations[signal.signal_id] = pending
            for mode in self.engines:
                self._audit(mode, "SIGNAL_OBSERVATION_STARTED", {
                    "signal_id": signal.signal_id,
                    "mint": signal.mint,
                    "observation_sec": int(self.observation_delay.total_seconds()),
                    "first_price_usd": pending.first_price_usd,
                    "first_holders": pending.first_holders,
                    "first_liquidity_usd": pending.first_liquidity_usd,
                })

        now = self.clock()
        ready_observations = [
            pending
            for pending in self.pending_observations.values()
            if now - pending.discovered_at >= self.observation_delay
        ]
        for pending in ready_observations:
            self.pending_observations.pop(pending.record.signal.signal_id, None)
            candidate_count, accepted_count, duplicate_count = self._evaluate_observation(pending, now)
            candidates += candidate_count
            duplicate_skipped += duplicate_count
            for mode in accepted:
                accepted[mode] += accepted_count.get(mode, 0)

        if process_exits:
            if self.features.is_bsc:
                position_result = self.run_position_cycle(trigger="cycle")
                exits = dict(position_result.exits)
            else:
                exits = self.process_queued_position_exits()
        finished = self.clock()
        for mode in self.engines:
            with self._db_lock:
                self.stores[mode].set_state("last_cycle", {
                "started_at": started.isoformat(),
                "finished_at": finished.isoformat(),
                "fetched": fetched,
                "bootstrap_skipped": bootstrap_skipped,
                "pending_observations": len(self.pending_observations),
                "candidates": candidates,
                "accepted": accepted[mode],
                "exits": exits[mode],
                "source_error_class": source_error,
                })
            self._set_health(mode, "coordinator", "HEALTHY")
        return CycleResult(started, finished, fetched, bootstrap_skipped, duplicate_skipped, candidates, accepted, exits, source_error)

    def run_position_cycle(self, *, trigger: str = "poll") -> PositionCycleResult:
        """Refresh holdings and evaluate the existing exit rules.

        For BSC executable Paper/Shadow, WSS is only an immediate refresh
        trigger; every value still comes from a new chain quote.  The
        non-executable compatibility mode remains separately opt-out only.
        """

        started = self.clock()
        events = self._consume_bsc_pair_events()
        if events:
            trigger = "wss"
            for event in events:
                descriptor = self._bsc_descriptors_by_pool.get(event.pair_address)
                if not self.features.bsc_executable_quote_enabled and descriptor is not None:
                    for mode, engine in self.engines.items():
                        for position in tuple(engine.ledger.active_positions):
                            if position.mint not in self._bsc_mints_by_pool.get(descriptor.address, set()):
                                continue
                            quote = self.features.bsc_pool_quote_for_event(position, event, descriptor)
                            if quote is not None:
                                self._bsc_last_pool_quotes[position.position_id] = quote
                for mode in self.engines:
                    self._audit(mode, "BSC_PAIR_EVENT_REFRESH", {
                        "pair_address": event.pair_address,
                        "event_type": event.event_type,
                        "block_number": event.block_number,
                        "transaction_hash": event.transaction_hash,
                        "log_index": event.log_index,
                        "pool_type": event.pool_type,
                        "price_source": "wss_refresh_trigger_only",
                        "event_is_not_fill_price": True,
                    })
        exits = {mode: 0 for mode in self.engines}
        positions = 0
        jobs: list[tuple[str, DeterministicSimulation, VirtualPosition]] = []
        for mode, engine in self.engines.items():
            with self._db_lock:
                active_positions = tuple(engine.ledger.active_positions)
            positions += len(active_positions)
            jobs.extend((mode, engine, position) for position in active_positions)

        if self.features.is_bsc:
            fetched_quotes: dict[tuple[str, str], ExecutableQuote | None] = {}
            for mode, _engine, position in jobs:
                quote: ExecutableQuote | None = None
                if not self.features.bsc_executable_quote_enabled and self._bsc_wss_healthy:
                    cached = self._bsc_last_pool_quotes.get(position.position_id)
                    if (
                        cached is not None
                        and cached.input_quantity == position.active_quantity_token
                        and cached.unusable_reason(self.clock()) is None
                    ):
                        quote = cached
                if quote is None:
                    # Executable mode always obtains a fresh read-only sell
                    # quote.  The compatibility mode retains its bounded
                    # Binance reference fallback.
                    quote = self.features.quote_for_position(position)
                fetched_quotes[(mode, position.position_id)] = quote
                self._audit(mode, "BSC_POSITION_QUOTE_REFRESH", {
                    "position_id": position.position_id,
                    "trigger": trigger,
                    "quote_id": quote.quote_id if quote is not None else None,
                    "quote_source": quote.quote_source or quote.provider if quote is not None else None,
                    "executable_quote": bool(quote.executable_style) if quote is not None else False,
                    "quote_unavailable": quote is None,
                    "wss_is_trigger_only": self.features.bsc_executable_quote_enabled,
                })
        else:
            # Quote GETs are the only work done concurrently. Simulation
            # ledger/database mutations remain on this coordinator thread.
            fetched_quotes: dict[tuple[str, str], ExecutableQuote | None] = {}
            if jobs:
                with ThreadPoolExecutor(max_workers=2, thread_name_prefix="solana-position-quote") as executor:
                    future_map = {
                        executor.submit(self.features.quote_for_position, position): (mode, position)
                        for mode, _engine, position in jobs
                    }
                    for future, (mode, position) in future_map.items():
                        try:
                            fetched_quotes[(mode, position.position_id)] = future.result()
                        except Exception:
                            fetched_quotes[(mode, position.position_id)] = None

        for mode, engine, position in jobs:
            event_start = len(engine.ledger.lifecycle_events)
            with self._db_lock:
                result = self._process_exit(
                    mode,
                    engine,
                    position,
                    quote=fetched_quotes.get((mode, position.position_id)),
                    quote_already_fetched=True,
                )
            self._notify_live_events(mode, engine, event_start)
            if result is not None and result.closed_position is not None:
                exits[mode] += 1
        self._process_shadow_followups(self.clock())
        finished = self.clock()
        for mode in self.engines:
            with self._db_lock:
                self.stores[mode].set_state("last_position_cycle", {
                    "started_at": started.isoformat(),
                    "finished_at": finished.isoformat(),
                    "trigger": trigger,
                    "event_count": len(events),
                    "positions": positions,
                    "exits": exits[mode],
                    "fallback_poll_sec": 2.0,
                })
        return PositionCycleResult(started, finished, trigger, len(events), positions, exits)

    def _process_shadow_followups(self, now: datetime) -> None:
        if "shadow" not in self.engines:
            return
        windows = (5, 15, 30, 60, 120)
        take_profit_pct = self.engines["shadow"].strategy.config.take_profit_pct
        for position_id, followup in tuple(self._shadow_followups.items()):
            elapsed_sec = (now - followup.exited_at).total_seconds()
            due_windows = [
                window
                for window in windows
                if window not in followup.returns_after_exit_pct and elapsed_sec >= window
            ]
            for window in due_windows:
                probe = replace(
                    followup.position,
                    status="OPEN",
                    remaining_quantity_token=followup.position.entry_quantity_token,
                )
                quote = self.features.quote_for_position(probe)
                return_pct: Decimal | None = None
                if (
                    quote is not None
                    and quote.unusable_reason(now) is None
                    and followup.position.quantity_sol > 0
                ):
                    return_pct = (
                        quote.output_quantity / followup.position.quantity_sol
                    ) - Decimal("1")
                followup.returns_after_exit_pct[window] = return_pct
                self._audit("shadow", "SHADOW_FOLLOW_UP", {
                    "position_id": position_id,
                    "window_sec": window,
                    "return_pct": return_pct,
                    "pricing_mode": (
                        "bsc_executable_quote"
                        if self.features.is_bsc and self.features.bsc_executable_quote_enabled
                        else "binance_indicative" if self.features.is_bsc else "jupiter_quote"
                    ),
                    "executable_quote": not self.features.is_bsc or self.features.bsc_executable_quote_enabled,
                    "net_pnl_is_estimated": (
                        False if self.features.bsc_executable_quote_enabled else True
                    ) if self.features.is_bsc else None,
                    "unavailable": return_pct is None,
                })
            if len(followup.returns_after_exit_pct) < len(windows):
                continue
            values = [
                value
                for value in followup.returns_after_exit_pct.values()
                if value is not None
            ]
            minimum_return = min(values) if values else None
            maximum_return = max(values) if values else None
            extreme_loss_after_exit = (
                minimum_return is not None and minimum_return <= Decimal("-0.50")
            )
            avoided_loss_pct = (
                followup.exit_return_pct - minimum_return
                if extreme_loss_after_exit and followup.exit_return_pct is not None
                else None
            )
            missed_profit_pct = (
                maximum_return - followup.exit_return_pct
                if maximum_return is not None
                and maximum_return >= take_profit_pct
                and followup.exit_return_pct is not None
                else None
            )
            outcome = ShadowOutcome(
                position_id=position_id,
                identity=followup.position.identity,
                returns_after_exit_pct=dict(followup.returns_after_exit_pct),
                paper_tp_reached=maximum_return is not None and maximum_return >= take_profit_pct,
                avoided_loss_pct=avoided_loss_pct,
                missed_profit_pct=missed_profit_pct,
                recorded_at=followup.exited_at,
            )
            with self._db_lock:
                followup.engine.ledger.update_shadow_outcome(outcome)
            self._audit("shadow", "SHADOW_EXTREME_LOSS_COMPARISON", {
                "position_id": position_id,
                "rule_triggered": True,
                "extreme_loss_threshold_pct": Decimal("-50"),
                "extreme_loss_after_exit": extreme_loss_after_exit,
                "min_return_after_exit_pct": minimum_return,
                "max_return_after_exit_pct": maximum_return,
                "avoided_loss_pct": avoided_loss_pct,
                "missed_profit_pct": missed_profit_pct,
                "follow_up_complete": True,
            })
            self._shadow_followups.pop(position_id, None)

    def remember_bsc_pool(self, record: BinanceNormalizedSignal) -> None:
        """Expose the read-only pool discovery hook for bounded runners."""

        self._remember_bsc_pool(record)

    def bsc_position_pool_addresses(self) -> tuple[str, ...]:
        if not self.features.is_bsc:
            return ()
        return tuple(sorted({
            descriptor.address
            for descriptor in self.bsc_position_pool_descriptors()
        }))

    def bsc_position_pool_descriptors(self) -> tuple[BscPoolDescriptor, ...]:
        if not self.features.is_bsc:
            return ()
        active_mints = {
            position.mint
            for engine in self.engines.values()
            for position in engine.ledger.active_positions
        }
        return tuple(sorted(
            (
                descriptor
                for mint, descriptor in self._bsc_descriptor_by_mint.items()
                if mint in active_mints
            ),
            key=lambda descriptor: descriptor.address,
        ))

    def notify_bsc_pair_event(self, event: BscPairEvent) -> None:
        if not self.features.is_bsc:
            return
        with self._bsc_event_lock:
            if event.pair_address not in self._bsc_mints_by_pool:
                return
            self._bsc_pair_events.append(event)
            self._bsc_refresh_event.set()

    def position_refresh_requested(self) -> bool:
        return self._bsc_refresh_event.is_set()

    def wait_for_position_refresh(self, stop_event: Event, timeout: float) -> bool:
        deadline = time.monotonic() + max(0.0, timeout)
        while not stop_event.is_set():
            if self._bsc_refresh_event.is_set():
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            stop_event.wait(min(0.1, remaining))
        return False

    def update_bsc_wss_status(self, status: Mapping[str, object]) -> None:
        if not self.features.is_bsc:
            return
        key = (
            status.get("state"),
            status.get("last_error_class"),
            status.get("connection_count"),
            status.get("disconnect_count"),
        )
        if key == self._bsc_wss_status_key:
            return
        self._bsc_wss_status_key = key
        state = str(status.get("state") or "UNAVAILABLE")
        self._bsc_wss_healthy = state == "HEALTHY"
        health_state = "HEALTHY" if state == "HEALTHY" else "UNAVAILABLE" if state == "UNAVAILABLE" else "DEGRADED"
        error_class = status.get("last_error_class")
        error_value = str(error_class) if error_class else None
        for mode in self.engines:
            self._set_health(
                mode,
                "bsc_pair_wss",
                health_state,
                error_class=error_value,
                details={
                    "pool_addresses": status.get("pool_addresses", 0),
                    "pool_types": status.get("pool_types", ()),
                    "topics": status.get("topics", ()),
                    "fallback_poll_sec": status.get("fallback_poll_sec", 2.0),
                },
            )
            self._audit(mode, "BSC_WSS_STATUS", dict(status))

    def _consume_bsc_pair_events(self) -> tuple[BscPairEvent, ...]:
        with self._bsc_event_lock:
            events = tuple(self._bsc_pair_events)
            self._bsc_pair_events.clear()
            self._bsc_refresh_event.clear()
        return events

    def _remember_bsc_pool(self, record: BinanceNormalizedSignal) -> None:
        if not self.features.is_bsc:
            return
        pair_field = record.fields.get("pair_address")
        pair_address = _field_value({"pair_address": pair_field}, "pair_address")
        curve_field = record.fields.get("bonding_curve_address")
        curve_address = _field_value({"bonding_curve_address": curve_field}, "bonding_curve_address")
        descriptor = self.features.resolve_bsc_pool(
            record.signal.mint,
            pair_address,
            curve_address,
            protocol=_field_value(record.fields, "protocol"),
            migrate_status=_field_value(record.fields, "migrate_status"),
        )
        mint = record.signal.mint
        legacy_pair = normalize_bsc_address(pair_address)
        previous_descriptor = self._bsc_descriptor_by_mint.get(mint)
        if previous_descriptor == descriptor and (
            descriptor is not None or self._bsc_pool_by_mint.get(mint) == legacy_pair
        ):
            return
        if previous_descriptor is not None:
            self._bsc_mints_by_pool.get(previous_descriptor.address, set()).discard(mint)
            self._bsc_descriptors_by_pool.pop(previous_descriptor.address, None)
        self._bsc_descriptor_by_mint.pop(mint, None)
        if descriptor is None:
            # Keep the legacy address map only as an event wake-up guard.  It
            # is deliberately not exposed to the WSS subscription unless the
            # resolver confirmed its pool type, so an unknown address cannot
            # produce a fabricated pool price.
            if legacy_pair is not None:
                self._bsc_pool_by_mint[mint] = legacy_pair
                self._bsc_mints_by_pool.setdefault(legacy_pair, set()).add(mint)
            else:
                self._bsc_pool_by_mint.pop(mint, None)
            return
        self._bsc_pool_by_mint[mint] = descriptor.address
        self._bsc_descriptor_by_mint[mint] = descriptor
        self._bsc_descriptors_by_pool[descriptor.address] = descriptor
        self._bsc_mints_by_pool.setdefault(descriptor.address, set()).add(mint)

    def _hydrate_bsc_active_pools(self) -> None:
        """Recover pool descriptors for positions restored from SQLite."""

        for mode, engine in self.engines.items():
            store = self.stores.get(mode)
            if store is None:
                continue
            for position in tuple(engine.ledger.active_positions):
                candidate_id = position.position_id[:-len(":position")] if position.position_id.endswith(":position") else position.position_id
                try:
                    row = store.connection.execute(
                        "SELECT soft_features_json FROM candidates WHERE candidate_id = ?",
                        (candidate_id,),
                    ).fetchone()
                    soft = json.loads(row[0] or "{}") if row is not None else {}
                except (TypeError, ValueError, sqlite3.Error):
                    soft = {}
                if not isinstance(soft, Mapping):
                    continue
                if self.features.bsc_executable_quote_enabled and self.features.bsc_quote_provider is not None:
                    self.features.bsc_quote_provider.remember_candidate(position.mint, soft)
                descriptor = self.features.resolve_bsc_pool(
                    position.mint,
                    soft.get("pair_address"),
                    soft.get("bonding_curve_address"),
                    protocol=soft.get("protocol"),
                    migrate_status=soft.get("migrate_status"),
                )
                if descriptor is None:
                    continue
                self._bsc_descriptor_by_mint[position.mint] = descriptor
                self._bsc_descriptors_by_pool[descriptor.address] = descriptor
                self._bsc_pool_by_mint[position.mint] = descriptor.address
                self._bsc_mints_by_pool.setdefault(descriptor.address, set()).add(position.mint)

    def _evaluate_observation(
        self,
        pending: PendingSignalObservation,
        evaluated_at: datetime,
    ) -> tuple[int, dict[str, int], int]:
        record = pending.record
        signal = record.signal
        feature_started = time.monotonic()
        defer_bsc_quote = self.features.is_bsc and self.features.bsc_executable_quote_enabled
        entry_features = self.features.entry_features(
            record,
            evaluated_at,
            include_bsc_quote=not defer_bsc_quote,
        )
        entry_features = replace(
            entry_features,
            soft_features={
                **(entry_features.soft_features or {}),
                "first_discovered_holders": pending.first_holders,
                "observation_holders": entry_features.holders,
                "first_discovered_liquidity_usd": pending.first_liquidity_usd,
                "observation_liquidity_usd": entry_features.liquidity_usd,
            },
        )
        strategy_config = next(iter(self.engines.values())).strategy.config
        if defer_bsc_quote:
            quote_needed = False
            for mode, engine in self.engines.items():
                candidate_id = f"{mode}:{signal.signal_id}:{engine.strategy.config.identity.ruleset_version}"
                if engine.ledger.candidate_exists(candidate_id):
                    continue
                block_reason = self._observation_gate(
                    pending,
                    entry_features,
                    strategy_config.min_liquidity_usd,
                    price_must_rise=True,
                    holders_must_not_decrease=strategy_config.require_holders_non_decreasing_after_observation,
                    liquidity_must_not_decrease=mode == "shadow",
                )
                if (
                    block_reason is None
                    and not self.controls.paused(mode)
                    and engine.strategy.local_entry_eligible(entry_features)
                ):
                    quote_needed = True
                    break
            if quote_needed:
                self._bsc_quote_local_conditions_passed += 1
                entry_features = self.features.bsc_entry_features_with_quote(record, entry_features)
                # A successful quote may have discovered the real Pancake
                # pair. Refresh only this candidate's WSS binding from the
                # cache; never from a Meme Rush venue guess.
                self._remember_bsc_pool(record)
        feature_elapsed = (time.monotonic() - feature_started) * 1000
        self.latency.record("feature_and_quote_build", feature_elapsed)
        accepted = {mode: 0 for mode in self.engines}
        duplicate_skipped = 0
        candidates = 0
        for mode, engine in self.engines.items():
            with self._db_lock:
                self.stores[mode].record_latency("feature_and_quote_build", feature_elapsed)
            candidate_id = f"{mode}:{signal.signal_id}:{engine.strategy.config.identity.ruleset_version}"
            position_id = f"{candidate_id}:position"
            block_reason = self._observation_gate(
                pending,
                entry_features,
                strategy_config.min_liquidity_usd,
                price_must_rise=self.features.is_bsc,
                holders_must_not_decrease=strategy_config.require_holders_non_decreasing_after_observation,
                liquidity_must_not_decrease=self.features.is_bsc and mode == "shadow",
            )
            if block_reason is None and self.controls.paused(mode):
                block_reason = "runtime_paused"
            with self._db_lock:
                if engine.ledger.candidate_exists(candidate_id):
                    duplicate_skipped += 1
                    self._audit(mode, "DUPLICATE_SIGNAL_SKIPPED", {"signal_id": signal.signal_id, "candidate_id": candidate_id})
                    continue
                candidates += 1
                event_start = len(engine.ledger.lifecycle_events)
                result = engine.process_entry(
                    signal,
                    entry_features,
                    candidate_id=candidate_id,
                    position_id=position_id,
                    block_reason=block_reason,
                )
            self._notify_live_events(mode, engine, event_start)
            if result.position is not None:
                accepted[mode] += 1
                if defer_bsc_quote:
                    self._bsc_quote_final_entries += 1
            self._audit_candidate(mode, signal, result, entry_features)
        if defer_bsc_quote:
            provider = self.features.bsc_quote_provider
            quote_metrics = provider.metrics() if provider is not None else {}
            metrics = {
                "local_conditions_passed": self._bsc_quote_local_conditions_passed,
                **quote_metrics,
                "final_entries": self._bsc_quote_final_entries,
            }
            for mode in self.engines:
                with self._db_lock:
                    self.stores[mode].set_state("bsc_quote_metrics", metrics)
        return candidates, accepted, duplicate_skipped

    @staticmethod
    def _observation_gate(
        pending: PendingSignalObservation,
        features: EntryFeatures,
        min_liquidity_usd: Decimal,
        *,
        price_must_rise: bool = True,
        holders_must_not_decrease: bool = False,
        liquidity_must_not_decrease: bool = False,
    ) -> str | None:
        if pending.first_price_usd is None:
            return "observation_price_unavailable"
        soft_features = features.soft_features or {}
        if soft_features.get("observation_snapshot_available") is not True:
            return "observation_price_unavailable"
        observed_price = _decimal_value(soft_features.get("price_usd"))
        if observed_price is None:
            return "observation_price_unavailable"
        if price_must_rise and observed_price <= pending.first_price_usd:
            return "price_not_up_after_observation"
        if not price_must_rise and observed_price < pending.first_price_usd:
            return "price_below_after_observation"
        if holders_must_not_decrease:
            current_holders = _non_negative_int(features.holders)
            if pending.first_holders is None or current_holders is None:
                return "holders_observation_unavailable"
            if current_holders < pending.first_holders:
                return "holders_below_first_discovery_after_observation"
        if features.liquidity_usd is None:
            return "observation_liquidity_unavailable"
        if features.liquidity_usd < min_liquidity_usd:
            return "observation_liquidity_below_min"
        if liquidity_must_not_decrease:
            if pending.first_liquidity_usd is None:
                return "observation_liquidity_unavailable"
            if features.liquidity_usd < pending.first_liquidity_usd:
                return "liquidity_below_first_discovery_after_observation"
        return None

    def run_forever(self, stop_event: Event, *, poll_sec: float = 10.0) -> None:
        interval = max(0.5, min(3600.0, poll_sec))
        while not stop_event.is_set():
            self.run_cycle()
            stop_event.wait(interval)

    def _finalize_exit_holders(
        self,
        mode: str,
        engine: DeterministicSimulation,
        closed_position: VirtualPosition,
        snapshot: ExitHoldersSnapshot | None,
    ) -> str:
        """Persist the immediate result and schedule only missing-value work."""

        if snapshot is not None:
            engine.ledger.set_exit_holders_snapshot(
                closed_position.position_id,
                snapshot.holders,
                observed_at=snapshot.observed_at,
                source=snapshot.source,
                status="completed",
            )
            return "completed"

        pending = engine.ledger.set_exit_holders_snapshot(
            closed_position.position_id,
            None,
            status="pending",
        )
        if not self._exit_holders_backfill.submit(mode, pending):
            # A file-backed runtime is expected in production.  A memory-only
            # deterministic harness has no safe cross-thread SQLite target;
            # mark it unavailable instead of blocking or retrying inline.
            engine.ledger.set_exit_holders_snapshot(
                closed_position.position_id,
                None,
                status="unavailable",
            )
            return "unavailable"
        return "pending"

    def _finalize_exit_market(
        self,
        mode: str,
        engine: DeterministicSimulation,
        closed_position: VirtualPosition,
        snapshot: ExitMarketSnapshot | None,
    ) -> str:
        """Persist close-time market data and queue only missing data."""

        if snapshot is not None:
            engine.ledger.set_exit_market_snapshot(
                closed_position.position_id,
                snapshot.market_cap_usd,
                snapshot.liquidity_usd,
                observed_at=snapshot.observed_at,
                source=snapshot.source,
                status="completed",
            )
            return "completed"

        pending = engine.ledger.set_exit_market_snapshot(
            closed_position.position_id,
            None,
            None,
            status="pending",
        )
        if not self._exit_market_backfill.submit(mode, pending):
            engine.ledger.set_exit_market_snapshot(
                closed_position.position_id,
                None,
                None,
                status="unavailable",
            )
            return "unavailable"
        return "pending"

    def _process_exit(
        self,
        mode: str,
        engine: DeterministicSimulation,
        position: VirtualPosition,
        *,
        quote: ExecutableQuote | None = None,
        quote_already_fetched: bool = False,
    ) -> ExitResult | None:
        now = self.clock()
        shadow_features: ShadowExitFeatures | None = None
        exit_holders_snapshot: ExitHoldersSnapshot | None = None
        exit_market_snapshot: ExitMarketSnapshot | None = None
        try:
            solana_timeout_path = (
                not self.features.is_bsc
                and mode in {"paper", "shadow"}
                and (
                    position.status == "EXIT_TRIGGERED"
                    or int((now - position.opened_at).total_seconds())
                    >= engine.strategy.config.max_hold_sec
                )
            )
            if solana_timeout_path:
                if not quote_already_fetched:
                    quote = self.features.quote_for_position(position)
                age_sec = max(0, int((now - position.opened_at).total_seconds()))
                fallback_quote = (
                    self.features.timeout_fallback_quote(position)
                    if age_sec >= engine.strategy.config.max_hold_sec + 10
                    else None
                )
                exit_holders_snapshot = self.features.latest_holders_snapshot(position, now)
                jupiter_snapshot = self.features.price_snapshot_for_position(
                    position,
                    quote,
                    pricing_mode="jupiter_quote",
                    executable_quote=True,
                    now=now,
                )
                fallback_snapshot = self.features.price_snapshot_for_position(
                    position,
                    fallback_quote,
                    pricing_mode="indicative_timeout_fallback",
                    executable_quote=False,
                    now=now,
                )
                result = engine.process_timeout_exit(
                    position.position_id,
                    now,
                    quote,
                    fallback_quote,
                    exit_holders=(
                        exit_holders_snapshot.holders
                        if exit_holders_snapshot is not None
                        else None
                    ),
                    price_snapshot=jupiter_snapshot,
                    fallback_price_snapshot=fallback_snapshot,
                )
            else:
                if not quote_already_fetched:
                    quote = self.features.quote_for_position(position)
                if mode == "paper":
                    exit_holders_snapshot = self.features.latest_holders_snapshot(position, now)
                    result = engine.process_paper_exit(
                        position.position_id,
                        now,
                        quote,
                        exit_holders=(
                            exit_holders_snapshot.holders
                            if exit_holders_snapshot is not None
                            else None
                        ),
                        price_snapshot=(
                            self.features.price_snapshot_for_position(
                                position,
                                quote,
                                pricing_mode="jupiter_quote",
                                executable_quote=True,
                                now=now,
                            ) if not self.features.is_bsc else None
                        ),
                    )
                elif mode == "shadow":
                    shadow_features = self.features.shadow_exit_features(position, quote, now)
                    exit_holders_snapshot = self.features.latest_holders_snapshot(position, now)
                    result = engine.process_shadow_exit(
                        position.position_id,
                        shadow_features,
                        now,
                        quote,
                        exit_holders=(
                            exit_holders_snapshot.holders
                            if exit_holders_snapshot is not None
                            else None
                        ),
                        price_snapshot=(
                            self.features.price_snapshot_for_position(
                                position,
                                quote,
                                pricing_mode="jupiter_quote",
                                executable_quote=True,
                                now=now,
                            ) if not self.features.is_bsc else None
                        ),
                    )
                else:
                    result = engine.process_live_exit(
                        position.position_id,
                        now,
                        quote,
                        exit_holders=self.features.position_holders(position.position_id),
                    )
        except (KeyError, ValueError):
            return None
        closed_position = result.closed_position
        exit_holders_status: str | None = None
        if mode in {"paper", "shadow"} and result.closed_position is not None:
            exit_holders_status = self._finalize_exit_holders(
                mode,
                engine,
                result.closed_position,
                exit_holders_snapshot,
            )
            closed_position = engine.ledger.closed_positions.get(
                result.closed_position.position_id,
                result.closed_position,
            )
            # Solana Dynamic provides the market snapshot. Paper/Shadow close
            # is never blocked by this metadata collection; a missing/failing
            # request is handled by the bounded background worker.
            if not self.features.is_bsc:
                exit_market_snapshot = self.features.latest_market_snapshot(position, now)
                exit_market_status = self._finalize_exit_market(
                    mode,
                    engine,
                    closed_position,
                    exit_market_snapshot,
                )
                closed_position = engine.ledger.closed_positions.get(
                    result.closed_position.position_id,
                    closed_position,
                )
            else:
                exit_market_status = None
        else:
            exit_market_status = None
        early_exit_reasons = {
            "shadow_holders_drop_over_10pct",
            "shadow_liquidity_drop_over_15pct",
        }
        if (
            mode == "shadow"
            and closed_position is not None
            and result.decision.reason in early_exit_reasons
        ):
            self._shadow_followups[position.position_id] = PendingShadowFollowUp(
                engine=engine,
                position=closed_position,
                exited_at=now,
                exit_return_pct=result.decision.return_pct,
            )
        audit_payload = {
            "position_id": position.position_id,
            "triggered": result.decision.triggered,
            "reason": result.decision.reason,
            "return_pct": result.decision.return_pct,
            "quote_id": quote.quote_id if quote else None,
            "quote_error_class": quote.error_class if quote else "quote_unavailable",
            "quote_requested_at": quote.requested_at.isoformat() if quote and quote.requested_at else None,
            "quote_request_times": [value.isoformat() for value in quote.request_times] if quote else [],
            "quote_request_statuses": list(quote.request_statuses) if quote else [],
            "quote_received_at": quote.received_at.isoformat() if quote and quote.received_at else None,
            "quote_latency_ms": quote.latency_ms if quote else None,
            "exit_holders": closed_position.exit_holders
            if closed_position is not None
            else None,
            "exit_holders_status": exit_holders_status
            or (closed_position.exit_holders_status if closed_position is not None else None),
            "exit_holders_observed_at": (
                closed_position.exit_holders_observed_at.isoformat()
                if closed_position is not None
                and closed_position.exit_holders_observed_at is not None
                else None
            ),
            "exit_market_cap_usd": (
                closed_position.exit_market_cap_usd
                if closed_position is not None
                else None
            ),
            "exit_liquidity_usd": (
                closed_position.exit_liquidity_usd
                if closed_position is not None
                else None
            ),
            "exit_market_status": exit_market_status
            or (closed_position.exit_market_status if closed_position is not None else None),
            "exit_market_observed_at": (
                closed_position.exit_market_observed_at.isoformat()
                if closed_position is not None
                and closed_position.exit_market_observed_at is not None
                else None
            ),
            "exit_market_source": (
                closed_position.exit_market_source
                if closed_position is not None
                else None
            ),
        }
        if (
            not self.features.is_bsc
            and mode in {"paper", "shadow"}
            and result.decision.reason == "max_hold_timeout"
        ):
            audit_payload["timeout_exit_path"] = True
            audit_payload["exit_status"] = (
                "closed" if closed_position is not None else "pending_quote"
            )
            audit_payload["pnl_status"] = (
                "unknown" if result.decision.cost is None and closed_position is not None else
                "estimated" if result.decision.cost is not None else "pending"
            )
        if mode == "shadow" and shadow_features is not None:
            holders_drop_pct = _relative_drop_pct(position.entry_holders, shadow_features.holders)
            liquidity_drop_pct = _relative_drop_pct(
                position.entry_liquidity_usd,
                shadow_features.liquidity_usd,
            )
            metric_rule_triggered = (
                (holders_drop_pct is not None and holders_drop_pct > Decimal("10"))
                or (liquidity_drop_pct is not None and liquidity_drop_pct > Decimal("15"))
            )
            audit_payload.update({
                "shadow_metric_rule": True,
                "entry_holders": position.entry_holders,
                "current_holders": shadow_features.holders,
                "holders_drop_pct": holders_drop_pct,
                "entry_liquidity_usd": position.entry_liquidity_usd,
                "current_liquidity_usd": shadow_features.liquidity_usd,
                "liquidity_drop_pct": liquidity_drop_pct,
                "holders_drop_threshold_pct": Decimal("10"),
                "liquidity_drop_threshold_pct": Decimal("15"),
                "early_exit_rule_triggered": metric_rule_triggered,
                "early_exit_recorded": (
                    metric_rule_triggered and result.closed_position is not None
                ),
                "extreme_loss_comparison": (
                    "pending_follow_up"
                    if metric_rule_triggered
                    else "not_triggered"
                ),
            })
        self._audit(mode, "EXIT_EVALUATED", audit_payload)
        return result

    def _audit(self, mode: str, event_type: str, payload: Mapping[str, object]) -> None:
        self.audits[mode].append(mode=mode, event_type=event_type, occurred_at=self.clock(), payload=payload)

    def _notify_live_events(self, mode: str, engine: DeterministicSimulation, event_start: int) -> None:
        if self.telegram is None or mode != "live":
            return
        notify_types = {
            "LIVE_ENTRY_CONFIRMED",
            "LIVE_ENTRY_FAILED",
            "LIVE_ENTRY_LIMIT_REACHED",
            "LIVE_EXIT_CONFIRMED",
            "LIVE_EXIT_FAILED",
        }
        for event in engine.ledger.lifecycle_events[event_start:]:
            if event.event_type not in notify_types:
                continue
            position = engine.ledger.positions.get(event.position_id) or engine.ledger.closed_positions.get(event.position_id)
            payload = dict(event.payload)
            payload.setdefault("position_id", event.position_id)
            payload.setdefault("occurred_at", event.occurred_at.isoformat())
            if position is not None:
                payload.setdefault("mint", position.mint)
                payload.setdefault("token_name", position.token_name)
                payload.setdefault("strategy_name", position.identity.strategy_name)
            try:
                self.telegram.notify_event(event.event_type, payload, chain="BSC", mode="live")
            except Exception:
                self._set_health(mode, "telegram", "UNAVAILABLE", error_class="telegram_notification_error")

    def _audit_candidate(self, mode: str, signal: Signal, result: EntryResult, features: EntryFeatures) -> None:
        self._audit(mode, "CANDIDATE_EVALUATED", {
            "signal_id": signal.signal_id,
            "mint": signal.mint,
            "candidate_id": result.candidate.candidate_id,
            "status": result.candidate.status,
            "filter_reason": result.candidate.filter_reason,
            "failed_reason_codes": result.decision.failed_reason_codes,
            "unavailable_reason_codes": result.decision.unavailable_reason_codes,
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
        details: Mapping[str, object] | None = None,
    ) -> None:
        self.health[mode].set(
            component,
            state,
            error_class=error_class,
            latency_ms=latency_ms,
            details=details,
        )
        self.stores[mode].record_health(
            component,
            state,
            error_class=error_class,
            latency_ms=latency_ms,
            details=details,
        )


def _first_string(*values: object | None) -> str | None:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None
