#!/usr/bin/env python3
"""Run the realtime Paper/Shadow coordinator or isolated BSC Live coordinator.

This command is intentionally explicit and stoppable. Paper/Shadow remain
virtual; BSC Live is an isolated, explicitly configured execution path.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from decimal import Decimal
from pathlib import Path
from threading import Event, Thread
from typing import Mapping

from dataclasses import replace

from meme_system.adapters.bsc_live import BscLiveConfig, BscLiveError, BscLiveExecutor
from meme_system.adapters.bsc_quote import BscReadOnlyQuoteProvider
from meme_system.adapters.bsc_wss import BscPairWssMonitor, BscPoolResolver
from meme_system.adapters.binance_agentic_wallet import (
    AsyncLiveExecutionBridge,
    AsyncRoundTripQuoteProvider,
)
from meme_system.adapters.bitget_wallet import (
    BITGET_WALLET_PROVIDER,
    BitgetWalletApiClient,
    BitgetWalletLiveExecutor,
    BitgetWalletRouteProvider,
)
from meme_system.adapters.gmgn_openapi import (
    GMGN_CLI_PROVIDER,
    GmgnCliLiveExecutor,
    GmgnCliRouteProvider,
)
from meme_system.adapters.binance_web3.client import BinanceWeb3Client
from meme_system.adapters.binance_web3.market_data import BinanceWeb3MarketDataAdapter
from meme_system.adapters.binance_web3.signal_source import (
    BscPaperCandidateMirrorSource,
    BinanceWeb3SignalSource,
)
from meme_system.adapters.okx_signal import BscDiscoverySource, OkxSignalError, OkxBscSignalSource
from meme_system.adapters.binance_web3.smart_money import BinanceWeb3SmartMoneyAdapter
from meme_system.adapters.jupiter import JupiterReadOnlyQuoteProvider, TokenDecimalsCache
from meme_system.adapters.pump_readonly import PumpProtocolReadOnlyQuoteProvider, PumpReadOnlyAdapter
from meme_system.adapters.solana_readonly import SolanaRpcClient, SolanaWssMonitor
from meme_system.adapters.solana_price import SolanaPriceMonitor
from meme_system.config.data_source import DataSourceConfig
from meme_system.config.runtime import RuntimePaths
from meme_system.config.safety import SafetyConfig
from meme_system.domain.models import BSC_BASELINE_IDENTITY
from meme_system.engines.ledger import SimulationLedger
from meme_system.engines.simulation import DeterministicSimulation
from meme_system.realtime import BinanceRealtimeFeatureProvider, RealtimeCoordinator
from meme_system.runtime_ops import HealthRegistry, JsonlAuditWriter, LatencyRecorder, RuntimeControl, SingleInstanceLock


class _AsyncReadOnlySource:
    """Turn a potentially stuck HTTPS discovery call into a nonblocking poll.

    Worker threads return immutable records only.  The coordinator thread is
    still the only caller that normalizes records or writes SQLite.
    """
    def __init__(self, source: object) -> None:
        self._source = source
        self._lock = __import__("threading").Lock()
        self._result: tuple[object, ...] = ()
        self._inflight = False
        self._last_error: str | None = None

    def fetch_once(self) -> tuple[object, ...]:
        with self._lock:
            # Deliver each completed worker batch exactly once.  Replaying a
            # stale batch on every 4-second owner tick can otherwise inflate
            # source provenance counters (notably OKX signal counts).
            ready = self._result
            self._result = ()
            if not self._inflight:
                self._inflight = True
                Thread(target=self._fetch, name="sol-discovery-io", daemon=True).start()
            return ready

    def _fetch(self) -> None:
        try:
            values = tuple(self._source.fetch_once())  # type: ignore[attr-defined]
            with self._lock:
                self._result = values
                self._last_error = None
        except Exception as exc:
            with self._lock:
                self._last_error = type(exc).__name__
        finally:
            with self._lock:
                self._inflight = False

    def stats(self) -> Mapping[str, object]:
        base = getattr(self._source, "stats", None)
        payload = dict(base()) if callable(base) else {}
        with self._lock:
            payload.update({"async_io": True, "inflight": self._inflight, "last_error_class": self._last_error})
        return payload
from meme_system.storage.database import initialize_database
from meme_system.storage.runtime_store import RuntimeStore
from meme_system.strategies.baseline import (
    BaselineStrategy,
    SOLANA_SHADOW_MIN_LIQUIDITY_USD,
    bsc_baseline_config,
    solana_baseline_config,
)
from meme_system.strategies.survivor_reversal import BalancedSurvivorConfig, BscBalancedLiveEngine, SurvivorReversalConfig, SurvivorReversalEngine
from meme_system.strategies.sol_survivor_reversal import (
    SolSurvivorQuoteRouter,
    SolSurvivorReversalConfig,
    SolSurvivorReversalEngine,
)
from meme_system.telegram_control import TelegramConfig, TelegramControl


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Solana/BSC Paper/Shadow or isolated BSC Live realtime runner")
    parser.add_argument("--chain", choices=("solana", "bsc"), default="solana")
    parser.add_argument("--mode", choices=("paper", "shadow", "both", "live"), default="both")
    parser.add_argument("--once", action="store_true", help="run one polling cycle and exit")
    parser.add_argument("--duration", type=float, default=0.0, help="bounded run duration in seconds; 0 means until stopped")
    parser.add_argument("--poll-sec", type=float, default=None)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--strategy-env-file", type=Path, default=None)
    parser.add_argument("--strategy-profile", choices=("high", "balanced"), default="high")
    return parser


def _load_env(path: Path) -> None:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (FileNotFoundError, OSError):
        return
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        name, value = stripped.split("=", 1)
        name = name.strip()
        value = value.strip().strip("\"'")
        if name and name.isidentifier():
            os.environ.setdefault(name, value)


def _modes(value: str) -> tuple[str, ...]:
    return ("paper", "shadow") if value == "both" else (value,)


def _configure_balanced_bsc_isolation(profile: str) -> None:
    """Force the balanced profile onto its own Paper-only state tree."""
    if profile != "balanced":
        return
    root = "data/bsc-balanced"
    os.environ["BSC_DATA_DIR"] = root
    os.environ["BSC_PAPER_DB_PATH"] = f"{root}/paper/runtime.db"
    os.environ["BSC_PAPER_AUDIT_LOG_PATH"] = f"{root}/paper/events.jsonl"
    os.environ["BSC_PAPER_HEALTH_PATH"] = f"{root}/paper/health.json"
    os.environ["BSC_PAPER_ARCHIVE_DIR"] = f"{root}/paper/archive"
    os.environ["BSC_RUNTIME_CONTROL_PATH"] = f"{root}/runtime_control.json"
    os.environ["BSC_RUNTIME_LOCK_DIR"] = f"{root}/locks"


def _token_decimals() -> dict[str, int]:
    raw = os.environ.get("TOKEN_DECIMALS_JSON", "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("TOKEN_DECIMALS_JSON must be valid JSON") from exc
    if not isinstance(parsed, dict):
        raise ValueError("TOKEN_DECIMALS_JSON must be an object")
    return {str(key): int(value) for key, value in parsed.items() if isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= 18}


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.duration < 0 or args.duration > 172800:
        _parser().error("--duration must be between 0 and 172800 seconds")
    _load_env(args.env_file)
    if args.strategy_env_file is not None:
        _load_env(args.strategy_env_file)
    if args.strategy_profile == "balanced" and (args.chain != "bsc" or args.mode not in {"paper", "live"}):
        _parser().error("--strategy-profile balanced requires --chain bsc --mode paper or live")
    _configure_balanced_bsc_isolation(args.strategy_profile)
    safety = SafetyConfig.from_env()
    try:
        safety.validate_for_mode(chain=args.chain, mode=args.mode)
    except (ValueError, RuntimeError) as exc:
        print(json.dumps({"status": "blocked", "error_class": type(exc).__name__, "message": str(exc)[:300]}, ensure_ascii=False))
        return 2
    data_source = DataSourceConfig.from_env()
    if data_source.data_source != "binance_web3":
        print(json.dumps({"status": "blocked", "error_class": "realtime_requires_binance_web3", "data_source": data_source.data_source}, ensure_ascii=False))
        return 2
    paths = RuntimePaths.from_env(args.chain)
    paths.validate_isolation()
    bsc_executable_quote_enabled = (
        args.chain == "bsc"
        and (
            args.mode == "live"
            or os.environ.get("BSC_EXECUTABLE_QUOTE_ENABLED", "true").strip().lower() == "true"
        )
    )
    modes = _modes(args.mode)
    chain_id = "56" if args.chain == "bsc" else "CT_501"
    survivor_config = (
        (BalancedSurvivorConfig.from_env() if args.strategy_profile == "balanced" else SurvivorReversalConfig.from_env())
        if args.chain == "bsc" and args.mode in {"paper", "live"} and args.strategy_profile == "balanced"
        else SurvivorReversalConfig.from_env()
        if args.chain == "bsc" and args.mode == "paper"
        else SolSurvivorReversalConfig.from_env()
        if args.chain == "solana" and args.mode == "paper"
        else None
    )
    # The strategy configuration is also the runtime self-check payload.  Do
    # not leave it at its Paper default after a live executor has been chosen;
    # that previously produced a misleading "paper / false" report while the
    # GMGN live engine was actually running.
    if args.chain == "bsc" and args.mode == "live" and args.strategy_profile == "balanced" and survivor_config is not None:
        configured_route = os.environ.get("BSC_BALANCED_ROUTE_PROVIDER", "").strip().upper()
        runtime_provider = "gmgn" if configured_route == GMGN_CLI_PROVIDER else configured_route.lower() or "unconfigured"
        # Reflect the effective live limits in the startup self-check as well
        # as in the later executor construction.  These are validated again
        # after preflight before any live bridge is created.
        configured_amount = Decimal(os.environ.get("BSC_LIVE_TRADE_AMOUNT_BNB", "0.001"))
        configured_max_positions = int(os.environ.get("BSC_BALANCED_LIVE_MAX_OPEN_POSITIONS", str(survivor_config.max_open_positions)))
        survivor_config = replace(
            survivor_config,
            execution_provider=runtime_provider,
            live_execution_enabled=True,
            position_size_bnb=configured_amount,
            max_open_positions=configured_max_positions,
        )
    survivor_only = survivor_config is not None and survivor_config.enabled
    if args.chain == "bsc" and args.mode == "paper" and not survivor_only:
        print(json.dumps({"status": "blocked", "error_class": "survivor_v1_required", "message": "BSC Paper requires MEME_SURVIVOR_REVERSAL_V1"}, ensure_ascii=False))
        return 2
    if args.chain == "solana" and args.mode == "paper" and not survivor_only:
        print(json.dumps({"status": "blocked", "error_class": "sol_survivor_v1_required", "message": "Solana Paper requires MEME_SURVIVOR_REVERSAL_SOL_V1"}, ensure_ascii=False))
        return 2
    if survivor_config is not None and survivor_config.enabled:
        survivor_config.validate()
        self_check = survivor_config.self_check()
        print(json.dumps({"event": "SURVIVOR_CONFIG_SELF_CHECK", **self_check}, ensure_ascii=False, sort_keys=True), flush=True)
    connections = {}
    engines = {}
    stores = {}
    health = {}
    audits = {}
    strategy_identity = None if survivor_only else BSC_BASELINE_IDENTITY if args.chain == "bsc" else None
    strategy_config = (
        bsc_baseline_config()
        if args.chain == "bsc"
        else solana_baseline_config()
    )
    live_config: BscLiveConfig | None = None
    live_executor: object | None = None
    live_route_executor: object | None = None
    live_route_bridge: AsyncLiveExecutionBridge | None = None
    live_route_preflight: dict[str, object] | None = None
    live_entry_funds_available = True
    wss_monitor: SolanaWssMonitor | None = None
    wss_thread: Thread | None = None
    position_thread: Thread | None = None
    control_thread: Thread | None = None
    wss_loop: asyncio.AbstractEventLoop | None = None
    wss_async_stop: asyncio.Event | None = None
    bsc_wss_monitor: BscPairWssMonitor | None = None
    telegram: TelegramControl | None = None
    coordinator: RealtimeCoordinator | None = None
    survivor_engine: SurvivorReversalEngine | None = None
    bsc_quote_provider: BscReadOnlyQuoteProvider | None = None
    bsc_entry_quote_provider: AsyncRoundTripQuoteProvider | None = None
    quote_probe_thread: Thread | None = None
    quote_probe_interval_sec = max(15.0, float(os.environ.get("BSC_QUOTE_HEALTH_PROBE_SEC", "60")))
    live_telegram_started = False
    try:
        if args.chain == "bsc" and bsc_executable_quote_enabled:
            bsc_quote_provider = BscReadOnlyQuoteProvider.from_env(project_root=Path.cwd())
            route_provider = os.environ.get("BSC_BALANCED_ROUTE_PROVIDER", "").strip().upper()
            if args.strategy_profile == "balanced" and route_provider == BITGET_WALLET_PROVIDER:
                # Bitget network calls run in worker bridges.  The owner loop
                # only consumes immutable results and remains the sole DB writer.
                if args.mode == "live":
                    live_route_executor = BitgetWalletLiveExecutor.from_env(
                        swaps_enabled=True,
                        env_file=args.env_file,
                    )
                    bitget_route_provider = live_route_executor
                else:
                    bitget_route_provider = BitgetWalletRouteProvider(
                        BitgetWalletApiClient.from_env(),
                        os.environ.get("PAPER_TAKER_ADDRESS", ""),
                    )
                bsc_entry_quote_provider = AsyncRoundTripQuoteProvider(bitget_route_provider)
            elif args.strategy_profile == "balanced" and route_provider == GMGN_CLI_PROVIDER:
                # GMGN CLI handles authenticated route lookup and, in Live
                # mode only, the explicitly enabled swap boundary.  The
                # asynchronous bridge keeps all CLI I/O off the owner loop.
                if args.mode == "live":
                    live_route_executor = GmgnCliLiveExecutor.from_env(
                        swaps_enabled=True,
                        env_file=args.env_file,
                    )
                    gmgn_route_provider = live_route_executor
                else:
                    gmgn_route_provider = GmgnCliRouteProvider(
                        os.environ.get("GMGN_BSC_WALLET_ADDRESS", ""),
                    )
                bsc_entry_quote_provider = AsyncRoundTripQuoteProvider(gmgn_route_provider)
        if args.mode == "live":
            if args.strategy_profile == "balanced":
                if survivor_config is None or bsc_entry_quote_provider is None:
                    raise BscLiveError("balanced_live_quote_provider_unavailable")
                assert live_route_executor is not None
                preflight = live_route_executor.preflight()
                live_route_preflight = preflight
                if not preflight.get("ok"):
                    raise BscLiveError(str(preflight.get("error_class") or "LIVE_EXECUTOR_PREFLIGHT_FAILED"))
                if not preflight.get("bsc_supported"):
                    raise BscLiveError("LIVE_EXECUTOR_BSC_CHAIN_UNAVAILABLE")
                live_amount = Decimal(os.environ.get("BSC_LIVE_TRADE_AMOUNT_BNB", "0.001"))
                if live_amount <= 0:
                    raise BscLiveError("BSC_LIVE_TRADE_AMOUNT_BNB_INVALID")
                bnb_balance = Decimal(str(preflight.get("bnb_balance") or "0"))
                # A depleted entry balance must not prevent the runtime from
                # starting and managing already-confirmed live positions.  It
                # only disables new BUY submissions until funds are restored;
                # SELL/exit reconciliation remains available.
                minimum_live_balance = live_amount + live_route_executor.gas_reserve_bnb
                live_entry_funds_available = bnb_balance >= minimum_live_balance
                live_max_positions = int(os.environ.get("BSC_BALANCED_LIVE_MAX_OPEN_POSITIONS", str(survivor_config.max_open_positions)))
                if live_max_positions < 1:
                    raise BscLiveError("BSC_BALANCED_LIVE_MAX_OPEN_POSITIONS_INVALID")
                survivor_config = replace(
                    survivor_config,
                    position_size_bnb=live_amount,
                    max_open_positions=live_max_positions,
                )
                live_route_bridge = AsyncLiveExecutionBridge(live_route_executor)
            else:
                live_config = BscLiveConfig.from_env(args.env_file)
                if bsc_quote_provider is None:
                    raise BscLiveError("bsc_venue_quote_provider_unavailable")
                live_executor = BscLiveExecutor(live_config, quote_provider=bsc_quote_provider)
                strategy_config = replace(
                    bsc_baseline_config(),
                    position_size_sol=live_config.trade_amount_bnb,
                    max_open_positions=live_config.max_positions,
                )
        for mode in modes:
            if mode == "paper":
                db_path = paths.paper_db
                health_path = paths.paper_health_file
                audit_path = paths.paper_audit_log
            elif mode == "shadow":
                db_path = paths.shadow_db
                health_path = paths.shadow_health_file
                audit_path = paths.shadow_audit_log
            else:
                db_path = paths.live_db
                health_path = paths.live_health_file
                audit_path = paths.live_audit_log
            connection = initialize_database(db_path)
            connections[mode] = connection
            stores[mode] = RuntimeStore(connection, mode)
            health[mode] = HealthRegistry(health_path)
            audits[mode] = JsonlAuditWriter(audit_path)
            if survivor_only:
                continue
            ledger = SimulationLedger.recover(mode, connection, identity=strategy_identity)
            mode_strategy_config = (
                replace(
                    strategy_config,
                    min_liquidity_usd=SOLANA_SHADOW_MIN_LIQUIDITY_USD,
                )
                if args.chain == "solana" and mode == "shadow"
                else strategy_config
            )
            engines[mode] = DeterministicSimulation(
                mode,
                strategy=BaselineStrategy(mode_strategy_config),
                ledger=ledger,
                pricing_mode=(
                    "bsc_venue_aware_executable"
                    if mode == "live"
                    else "bsc_executable_quote"
                    if bsc_executable_quote_enabled
                    else "binance_indicative" if args.chain == "bsc" else "jupiter_quote"
                ),
                executable_quote=(
                    mode == "live" or args.chain != "bsc" or bsc_executable_quote_enabled
                ),
                live_executor=live_executor if mode == "live" else None,
                live_max_entries=live_config.max_entries if mode == "live" and live_config else 0,
            )

        if live_route_executor is not None and args.mode == "live":
            for mode in modes:
                health[mode].set(
                    "live_route_executor",
                    "HEALTHY" if live_entry_funds_available else "DEGRADED",
                    error_class=None if live_entry_funds_available else "LIVE_EXECUTOR_BNB_BALANCE_INSUFFICIENT",
                    details={
                        "authenticated": True,
                        "chain_id": 56,
                        "swap_enabled": True,
                        "entry_funds_available": live_entry_funds_available,
                        "minimum_live_balance_bnb": str(live_amount + live_route_executor.gas_reserve_bnb),
                        "preflight": live_route_preflight or {},
                    },
                )
                health[mode].set(
                    "quote_provider",
                    "HEALTHY",
                    details={"primary": str(getattr(live_route_executor, "provider", "UNKNOWN")), "roundtrip_gate": True, "direct_fallback": False},
                )

        client = BinanceWeb3Client.from_env()
        if bsc_quote_provider is not None and not (args.mode == "live" and args.strategy_profile == "balanced"):
            for mode in modes:
                health[mode].set(
                    "quote_provider",
                    "DEGRADED",
                    error_class="startup_probe_pending",
                    details={
                        "persistent_client": True,
                        "startup_health_check": False,
                        "quote_gate_required": True,
                        "automatic_reprobe_sec": quote_probe_interval_sec,
                    },
                )
        solana_rpc = SolanaRpcClient.from_env() if args.chain == "solana" else None
        decimals_cache = (
            TokenDecimalsCache.from_env(rpc=solana_rpc, overrides=_token_decimals())
            if args.chain == "solana"
            else None
        )
        solana_price_monitor = (
            SolanaPriceMonitor(PumpReadOnlyAdapter(solana_rpc), decimals_cache.resolve)
            if solana_rpc is not None and decimals_cache is not None
            else None
        )
        pump_quote_provider = (
            PumpProtocolReadOnlyQuoteProvider(
                solana_price_monitor.pump_adapter,
                decimals_resolver=decimals_cache.resolve,
            )
            if solana_price_monitor is not None and decimals_cache is not None
            else None
        )
        source_limit = int(os.environ.get("BINANCE_MEME_LIMIT", "40"))
        if survivor_config is not None and survivor_config.enabled:
            source_limit = max(source_limit, 200)
        binance_source = (
            BscPaperCandidateMirrorSource(paths.paper_db)
            if args.chain == "bsc" and args.mode == "live" and not survivor_only
            else BinanceWeb3SignalSource(
                client,
                chain_id=chain_id,
                rank_types=(10, 20, 30) if survivor_only else (int(os.environ.get("BINANCE_MEME_RANK_TYPE", "10")),),
                limit=min(200, source_limit),
            )
        )
        # OKX is discovery metadata only.  Keep Binance as the primary feed;
        # missing credentials or a premium API outage must never interrupt the
        # live strategy, its existing positions, or GMGN execution.
        okx_signal_source = None
        if args.chain == "bsc" and args.mode == "live" and args.strategy_profile == "balanced":
            try:
                okx_signal_source = OkxBscSignalSource.from_env()
            except OkxSignalError:
                okx_signal_source = None
        source = BscDiscoverySource(binance_source, okx_signal_source) if args.chain == "bsc" and args.mode == "live" and args.strategy_profile == "balanced" else binance_source
        # A Binance discovery TLS stall must never stop the BSC position
        # scheduler: an already-triggered exit intent still needs to drain an
        # asynchronous SELL quote.  The bridge only returns immutable source
        # records; all strategy evaluation and SQLite writes remain on the
        # owner loop.
        if survivor_only:
            source = _AsyncReadOnlySource(source)
        quote_provider = (
            JupiterReadOnlyQuoteProvider.from_env(decimals_resolver=decimals_cache.resolve)
            if decimals_cache is not None
            else None
        )
        features = BinanceRealtimeFeatureProvider(
            market_data=BinanceWeb3MarketDataAdapter(client, chain_id=chain_id),
            quote_provider=quote_provider,
            position_size_sol=survivor_config.position_size_bnb if survivor_only and survivor_config is not None else strategy_config.position_size_sol,
            chain_id=chain_id,
            live_executor=live_executor,
            bsc_quote_provider=bsc_quote_provider,
            bsc_executable_quote_enabled=bsc_executable_quote_enabled,
            bsc_pool_resolver=(BscPoolResolver.from_env() if args.chain == "bsc" else None),
            solana_price_monitor=solana_price_monitor,
            pump_quote_provider=pump_quote_provider,
        )
        if (
            live_route_bridge is not None
            and live_route_executor is not None
            and features.bsc_pool_resolver is not None
        ):
            # Wallet reconciliation is a read-only BSC RPC balanceOf call,
            # not a GMGN portfolio response.  It shares the existing bridge
            # so no SQLite-owning loop performs network I/O.
            live_route_bridge.wallet_balance_reader = (
                lambda token: features.bsc_pool_resolver.erc20_balance_of(
                    token, getattr(live_route_executor, "wallet", "")
                )
            )
        control = RuntimeControl(paths.live_control_file if args.mode == "live" else paths.control_file)
        telegram_config = TelegramConfig.from_env()
        if telegram_config.enabled != safety.telegram_enabled:
            raise ValueError("TELEGRAM_ENABLED and Telegram configuration disagree")

        def telegram_status() -> Mapping[str, object]:
            mode_status: dict[str, object] = {}
            for current_mode, engine in engines.items():
                mode_status[current_mode] = {
                    "new_entries_paused": control.paused(current_mode),
                    "open_positions": len(engine.ledger.active_positions),
                    "strategy": engine.strategy.config.identity.as_dict(),
                }
            return {
                "chain": "BSC mainnet" if args.chain == "bsc" else "Solana mainnet",
                "chain_id": chain_id,
                "modes": mode_status,
                "live_trading": safety.live_trading,
                "telegram_enabled": safety.telegram_enabled,
                "telegram_controls": ["status", "pause/resume new entries"],
                "live_max_positions": live_config.max_positions if live_config else None,
                "live_max_entries": live_config.max_entries if live_config else None,
            }

        telegram = TelegramControl(
            telegram_config,
            control,
            allowed_modes=modes,
            status_provider=telegram_status,
        )
        if survivor_config is not None and survivor_config.enabled:
            survivor_mode = "live" if args.mode == "live" else "paper"
            common_survivor = {
                "connection": connections[survivor_mode],
                "store": stores[survivor_mode],
                "health": health[survivor_mode],
                "audit": audits[survivor_mode],
                "controls": control,
                "config": survivor_config,
                "market_data": features.market_data,
            }
            if args.chain == "solana":
                assert solana_price_monitor is not None
                survivor_engine = SolSurvivorReversalEngine(
                    **common_survivor,
                    quote_provider=SolSurvivorQuoteRouter(pump_quote_provider, quote_provider, survivor_config),
                    price_monitor=solana_price_monitor,
                    smart_money=BinanceWeb3SmartMoneyAdapter(client, chain_id=chain_id),
                    swap_rpc=SolanaRpcClient.from_env(),
                )
            else:
                if args.mode == "live":
                    assert live_route_executor is not None and live_route_bridge is not None and survivor_config is not None

                    def notify_live_execution_event(event_type: str, payload: Mapping[str, object]) -> None:
                        # Telegram is auxiliary to settlement.  Keep its HTTPS
                        # call off the SQLite-owning Live main loop.
                        Thread(
                            target=telegram.notify_event,
                            args=(event_type, payload),
                            kwargs={"chain": "BSC", "mode": "live"},
                            name=f"bsc-live-telegram-{event_type.lower()}",
                            daemon=True,
                        ).start()

                    survivor_engine = BscBalancedLiveEngine(
                        **common_survivor,
                        quote_provider=bsc_quote_provider,
                        entry_quote_provider=bsc_entry_quote_provider,
                        exit_quote_provider=bsc_entry_quote_provider,
                        resolver=features.bsc_pool_resolver,
                        live_executor=live_route_executor,
                        live_bridge=live_route_bridge,
                        live_amount_bnb=survivor_config.position_size_bnb,
                        live_event_notifier=notify_live_execution_event,
                    )
                    survivor_engine.live_entry_funds_available = live_entry_funds_available
                else:
                    survivor_engine = SurvivorReversalEngine(
                        **common_survivor,
                        quote_provider=bsc_quote_provider,
                        entry_quote_provider=bsc_entry_quote_provider,
                        exit_quote_provider=bsc_entry_quote_provider,
                        resolver=features.bsc_pool_resolver,
                        config=survivor_config,
                    )
        coordinator = RealtimeCoordinator(
            source=source,
            features=features,
            engines=engines,
            controls=control,
            health=health,
            latency=LatencyRecorder(),
            stores=stores,
            audits=audits,
            telegram=telegram,
            observation_delay_sec=4 if survivor_only else strategy_config.observation_delay_sec,
            survivor_engine=survivor_engine,
        )
        if args.mode == "live":
            # Notification transport is auxiliary. A slow Telegram TLS
            # handshake must never delay Live factory/WSS startup.
            Thread(
                target=telegram.notify_event,
                args=("BSC_LIVE_STARTED", {}),
                kwargs={"chain": "BSC", "mode": "live"},
                name="bsc-live-start-notify",
                daemon=True,
            ).start()
            live_telegram_started = True
        if args.chain == "bsc" and not args.once:
            bsc_wss_monitor = BscPairWssMonitor.from_env()
            bsc_wss_monitor.callback = coordinator.notify_bsc_pair_event
            if survivor_engine is not None and hasattr(survivor_engine, "set_flow_reconciliation_resubscribe_callback"):
                survivor_engine.set_flow_reconciliation_resubscribe_callback(bsc_wss_monitor.request_resubscribe)
            # Factory events are discovery-only.  They are queued back to the
            # owner loop; the WSS thread never writes a runtime SQLite DB.
            bsc_wss_monitor.enable_factory_registry(True)
            bsc_wss_monitor.set_pool_descriptors(coordinator.bsc_subscription_descriptors())
            bsc_wss_monitor.start()
            # The factory subscriptions form the realtime discovery safety
            # boundary.  Do not start Binance processing until their ACKs
            # have either completed or produced an explicit timeout/error.
            factory_deadline = time.monotonic() + 15.0
            factory_status = bsc_wss_monitor.safe_status()
            while factory_status.get("state") != "HEALTHY" and time.monotonic() < factory_deadline:
                time.sleep(0.1)
                factory_status = bsc_wss_monitor.safe_status()
            coordinator.update_bsc_wss_status(factory_status)
            factory_healthy = factory_status.get("state") == "HEALTHY"
            factory_error = None if factory_healthy else str(factory_status.get("last_error_class") or "FACTORY_ACK_TIMEOUT")
            for mode in modes:
                health[mode].set(
                    "factory_v2_wss",
                    "HEALTHY" if factory_healthy else "DEGRADED",
                    error_class=factory_error,
                    details={"ack": factory_healthy, "subscription": "PairCreated"},
                )
                health[mode].set(
                    "factory_v3_wss",
                    "HEALTHY" if factory_healthy else "DEGRADED",
                    error_class=factory_error,
                    details={"ack": factory_healthy, "subscription": "PoolCreated"},
                )
            if factory_healthy and survivor_engine is not None:
                gap_complete = survivor_engine.complete_factory_startup_gap_fill(timeout_sec=30.0)
                for mode in modes:
                    health[mode].set(
                        "factory_gap_fill",
                        "HEALTHY" if gap_complete else "DEGRADED",
                        error_class=None if gap_complete else "FACTORY_GAP_FILL_INCOMPLETE",
                        details={"logs_rpc": "BSC_LOGS_RPC", "startup": True},
                    )
        stop_event = Event()
        wss_trigger = Event()
        swap_worker_thread: Thread | None = None

        def _publish_quote_health(healthy: bool, error_class: str | None) -> None:
            state = "HEALTHY" if healthy else "DEGRADED"
            details = {
                "persistent_client": True,
                "startup_health_check": False,
                "quote_gate_required": True,
                "automatic_reprobe_sec": quote_probe_interval_sec,
            }
            for mode in modes:
                health[mode].set("quote_provider", state, error_class=error_class, details=details)
                # Preserve the existing dashboard health key during the
                # transition to the explicit provider capability status.
                health[mode].set("pancakeswap_router", state, error_class=error_class, details=details)

        def run_quote_health_probe() -> None:
            """Continuously probe quote capability without touching SQLite.

            This deliberately owns no strategy state, so an unrelated slow
            discovery cycle cannot prevent Quote health from recovering.
            """

            while not stop_event.is_set() and bsc_quote_provider is not None:
                try:
                    healthy = bsc_quote_provider.router_health_check(timeout_sec=1.0)
                    # HealthRegistry is internally locked and writes only the
                    # JSON health artifact.  This worker never touches the
                    # runtime SQLite connection or strategy state.
                    _publish_quote_health(healthy, None if healthy else "pancakeswap_quote_unavailable")
                except Exception as exc:  # Provider failures remain a buy-only gate.
                    _publish_quote_health(False, type(exc).__name__)
                stop_event.wait(quote_probe_interval_sec)

        if args.chain == "bsc" and bsc_quote_provider is not None and not (args.mode == "live" and args.strategy_profile == "balanced"):
            quote_probe_thread = Thread(target=run_quote_health_probe, name="bsc-quote-health-probe", daemon=True)
            quote_probe_thread.start()

        def on_wss_event(event: object) -> None:
            if isinstance(event, dict) and event.get("method") in {"accountNotification", "programNotification"}:
                if survivor_engine is not None and hasattr(survivor_engine, "on_solana_account_event"):
                    survivor_engine.on_solana_account_event(event)
                if coordinator is not None:
                    coordinator.notify_solana_wss_event(event)
                wss_trigger.set()

        def run_wss_monitor() -> None:
            nonlocal wss_loop, wss_async_stop
            loop = asyncio.new_event_loop()
            wss_loop = loop
            asyncio.set_event_loop(loop)
            async_stop = asyncio.Event()
            wss_async_stop = async_stop
            try:
                loop.run_until_complete(wss_monitor.run(on_wss_event, async_stop))  # type: ignore[union-attr]
            finally:
                loop.close()

        def refresh_wss_subscriptions() -> None:
            if wss_monitor is None:
                return
            assert coordinator is not None
            # Pool/curve binding inspection performs HTTP RPC reads.  Keep it
            # outside the owner loop; WSS stays independently alive while an
            # endpoint is slow or its TLS handshake is stuck.
            if not getattr(refresh_wss_subscriptions, "binding_inflight", False):
                setattr(refresh_wss_subscriptions, "binding_inflight", True)
                def refresh_bindings() -> None:
                    try:
                        coordinator.sync_solana_price_bindings()
                    finally:
                        setattr(refresh_wss_subscriptions, "binding_inflight", False)
                Thread(target=refresh_bindings, name="sol-wss-binding-io", daemon=True).start()
            wss_monitor.replace_subscriptions(coordinator.solana_price_subscriptions())
            wss_health = wss_monitor.health()
            health_state = {
                "HEALTHY": "HEALTHY",
                "STALE": "DEGRADED",
                "ERROR": "DEGRADED",
                "CONNECTING": "DEGRADED",
                "DISCONNECTED": "UNAVAILABLE",
                "UNAVAILABLE": "UNAVAILABLE",
                "STOPPED": "UNAVAILABLE",
            }.get(wss_health.state, "DEGRADED")
            for mode in modes:
                health[mode].set(
                    "solana_wss",
                    health_state,
                    error_class=wss_health.last_error_class,
                    details={
                        "subscriptions": wss_health.subscriptions,
                        "disconnect_count": wss_health.disconnect_count,
                        "reconnect_count": wss_health.reconnect_count,
                        "failover_count": wss_health.failover_count,
                    },
                )
            if survivor_engine is not None and hasattr(survivor_engine, "update_wss_health"):
                survivor_engine.update_wss_health("IDLE" if wss_health.subscriptions == 0 and wss_health.state not in {"ERROR", "STALE"} else health_state)

        if args.chain == "solana" and not args.once:
            wss_monitor = SolanaWssMonitor()
            refresh_wss_subscriptions()
            wss_thread = Thread(target=run_wss_monitor, name="solana-position-wss", daemon=True)
            wss_thread.start()
            if survivor_only and survivor_engine is not None:
                def run_swap_worker() -> None:
                    while not stop_event.is_set():
                        survivor_engine.poll_swap_rpc_work()
                        stop_event.wait(0.05)
                swap_worker_thread = Thread(target=run_swap_worker, name="solana-survivor-swap-rpc", daemon=True)
                swap_worker_thread.start()

        configured_poll_sec = args.poll_sec if args.poll_sec is not None else float(os.environ.get("POLL_INTERVAL_SEC", "10"))
        poll_sec = 4.0 if survivor_only else configured_poll_sec
        signal.signal(signal.SIGINT, lambda *_: stop_event.set())
        signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
        started = time.monotonic()
        if args.once:
            result = coordinator.run_cycle()
            print(json.dumps({"status": "ok", "once": True, "chain": args.chain, **result.__dict__}, ensure_ascii=False, default=str))
        elif args.chain == "bsc":
            # Signal discovery keeps its configured cadence. Existing BSC
            # holdings use a separate 2-second Binance indicative-price
            # fallback and can wake earlier from a read-only Pair log.
            position_poll_sec = 2.0
            next_signal_at = time.monotonic()
            next_position_at = time.monotonic()
            while not stop_event.is_set() and (args.duration <= 0 or time.monotonic() - started < args.duration):
                now = time.monotonic()
                if now >= next_signal_at:
                    coordinator.run_cycle(process_exits=False)
                    coordinator.sync_bsc_holder_baselines()
                    if bsc_wss_monitor is not None:
                        bsc_wss_monitor.set_pool_descriptors(coordinator.bsc_subscription_descriptors())
                        bsc_wss_monitor.set_holder_token_addresses(coordinator.bsc_holder_token_addresses())
                    next_signal_at = time.monotonic() + poll_sec
                if now >= next_position_at or coordinator.position_refresh_requested():
                    trigger = "wss" if coordinator.position_refresh_requested() else "poll"
                    coordinator.run_position_cycle(trigger=trigger)
                    if bsc_wss_monitor is not None:
                        bsc_wss_monitor.set_pool_descriptors(coordinator.bsc_subscription_descriptors())
                        bsc_wss_monitor.set_holder_token_addresses(coordinator.bsc_holder_token_addresses())
                    next_position_at = time.monotonic() + position_poll_sec
                if bsc_wss_monitor is not None:
                    coordinator.update_bsc_wss_status(bsc_wss_monitor.safe_status())
                remaining = args.duration - (time.monotonic() - started) if args.duration > 0 else None
                wait_for = min(0.2, max(0.0, next_signal_at - time.monotonic()), max(0.0, next_position_at - time.monotonic()))
                if remaining is not None:
                    wait_for = min(wait_for, max(0.0, remaining))
                coordinator.wait_for_position_refresh(stop_event, wait_for)
            if bsc_wss_monitor is not None:
                coordinator.update_bsc_wss_status(bsc_wss_monitor.safe_status())
            print(json.dumps({"status": "stopped", "bounded": args.duration > 0, "chain": args.chain, "modes": modes}, ensure_ascii=False))
        else:
            # Solana holdings retain the established 2-second Jupiter quote
            # refresh, with account WSS able to trigger earlier processing.
            position_poll_sec = 2.0

            if survivor_only:
                # The Survivor engine and its WSS callbacks share one SQLite
                # connection. Keep every mutation on the main thread so a
                # price notification cannot commit while discovery is still
                # persisting its cohort. This also avoids creating empty WSS
                # flow samples: queued events are applied only by the engine's
                # verified price handler.
                while not stop_event.is_set() and (args.duration <= 0 or time.monotonic() - started < args.duration):
                    coordinator.process_solana_wss_events()
                    coordinator.run_cycle()
                    refresh_wss_subscriptions()
                    coordinator.publish_runtime_control_state()
                    wait_deadline = time.monotonic() + max(0.1, poll_sec)
                    while not stop_event.is_set() and time.monotonic() < wait_deadline:
                        coordinator.process_solana_wss_events()
                        stop_event.wait(min(0.2, max(0.0, wait_deadline - time.monotonic())))
                print(json.dumps({"status": "stopped", "bounded": args.duration > 0, "chain": args.chain, "modes": modes}, ensure_ascii=False))
                return 0

            def run_solana_control_heartbeat() -> None:
                # Control changes must remain observable even if a bounded
                # source/Quote request is still completing on the main loop.
                while not stop_event.is_set():
                    coordinator.publish_runtime_control_state()
                    stop_event.wait(1.0)

            def run_solana_position_scheduler() -> None:
                next_position_at = time.monotonic()
                while not stop_event.is_set() and (args.duration <= 0 or time.monotonic() - started < args.duration):
                    wait_for = max(0.0, next_position_at - time.monotonic())
                    triggered_by_wss = wss_trigger.wait(min(wait_for, 0.2)) if wait_for > 0 else wss_trigger.is_set()
                    if triggered_by_wss:
                        wss_trigger.clear()
                        coordinator.process_solana_wss_events()
                        refresh_wss_subscriptions()
                    if stop_event.is_set():
                        break
                    if time.monotonic() >= next_position_at:
                        coordinator.refresh_position_quotes(trigger="interval_10s")
                        coordinator.process_queued_position_exits()
                        next_position_at = time.monotonic() + position_poll_sec
                        refresh_wss_subscriptions()

            position_thread = Thread(target=run_solana_position_scheduler, name="solana-position-scheduler", daemon=True)
            position_thread.start()
            control_thread = Thread(target=run_solana_control_heartbeat, name="solana-control-heartbeat", daemon=True)
            control_thread.start()
            while not stop_event.is_set() and (args.duration <= 0 or time.monotonic() - started < args.duration):
                coordinator.run_cycle()
                refresh_wss_subscriptions()
                stop_event.wait(max(0.1, poll_sec))
            print(json.dumps({"status": "stopped", "bounded": args.duration > 0, "chain": args.chain, "modes": modes}, ensure_ascii=False))
        return 0
    except (ValueError, OSError, BscLiveError) as exc:
        print(json.dumps({"status": "blocked", "error_class": type(exc).__name__, "message": str(exc)[:300]}, ensure_ascii=False))
        return 2
    finally:
        if live_telegram_started and telegram is not None:
            telegram.notify_event("BSC_LIVE_STOPPED", {}, chain="BSC", mode="live")
        if bsc_wss_monitor is not None:
            bsc_wss_monitor.stop()
        if wss_loop is not None and wss_async_stop is not None:
            wss_loop.call_soon_threadsafe(wss_async_stop.set)
        if coordinator is not None:
            coordinator.shutdown()
        if live_executor is not None:
            live_executor.close()
        if bsc_quote_provider is not None:
            bsc_quote_provider.close()
        if bsc_entry_quote_provider is not None:
            bsc_entry_quote_provider.close()
        if live_route_bridge is not None:
            live_route_bridge.close()
        if live_route_executor is not None:
            live_route_executor.close()
        if quote_probe_thread is not None:
            quote_probe_thread.join(timeout=2.0)
        if wss_thread is not None:
            wss_thread.join(timeout=5.0)
        if position_thread is not None:
            position_thread.join(timeout=5.0)
        if control_thread is not None:
            control_thread.join(timeout=5.0)
        for connection in connections.values():
            connection.close()


def run_with_lock(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    _load_env(args.env_file)
    if args.strategy_env_file is not None:
        _load_env(args.strategy_env_file)
    _configure_balanced_bsc_isolation(args.strategy_profile)
    paths = RuntimePaths.from_env(args.chain)
    lock_name = f"realtime-{args.chain}-{args.mode}-{args.strategy_profile}"
    lock_dir = paths.live_lock_dir if args.chain == "bsc" and args.mode == "live" else paths.lock_dir
    lock = SingleInstanceLock(lock_dir / f"{lock_name}.lock", name=lock_name)
    try:
        with lock:
            return main(argv)
    except Exception as exc:
        print(json.dumps({"status": "blocked", "error_class": type(exc).__name__, "message": str(exc)[:300]}, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(run_with_lock())
