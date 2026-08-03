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
from meme_system.adapters.binance_web3.client import BinanceWeb3Client
from meme_system.adapters.binance_web3.market_data import BinanceWeb3MarketDataAdapter
from meme_system.adapters.binance_web3.signal_source import BinanceWeb3SignalSource
from meme_system.adapters.jupiter import JupiterReadOnlyQuoteProvider, TokenDecimalsCache
from meme_system.adapters.pump_readonly import PumpReadOnlyAdapter
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
from meme_system.storage.database import initialize_database
from meme_system.storage.runtime_store import RuntimeStore
from meme_system.strategies.baseline import (
    BaselineConfig,
    BaselineStrategy,
    SOLANA_SHADOW_MIN_LIQUIDITY_USD,
    bsc_baseline_config,
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
    modes = _modes(args.mode)
    chain_id = "56" if args.chain == "bsc" else "CT_501"
    connections = {}
    engines = {}
    stores = {}
    health = {}
    audits = {}
    strategy_identity = BSC_BASELINE_IDENTITY if args.chain == "bsc" else None
    strategy_config = (
        bsc_baseline_config()
        if strategy_identity is not None
        else BaselineConfig(
            min_holders=5,
            min_holders_inclusive=True,
            pause_new_entries_after_large_losses=1000,
        )
    )
    live_config: BscLiveConfig | None = None
    live_executor: BscLiveExecutor | None = None
    wss_monitor: SolanaWssMonitor | None = None
    wss_thread: Thread | None = None
    position_thread: Thread | None = None
    wss_loop: asyncio.AbstractEventLoop | None = None
    wss_async_stop: asyncio.Event | None = None
    bsc_wss_monitor: BscPairWssMonitor | None = None
    telegram: TelegramControl | None = None
    coordinator: RealtimeCoordinator | None = None
    live_telegram_started = False
    try:
        if args.mode == "live":
            live_config = BscLiveConfig.from_env(args.env_file)
            live_executor = BscLiveExecutor(live_config)
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
                assert live_config is not None
                db_path = paths.live_db
                health_path = paths.live_health_file
                audit_path = paths.live_audit_log
            connection = initialize_database(db_path)
            connections[mode] = connection
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
                    "pancakeswap_smart_router"
                    if mode == "live"
                    else "bsc_executable_quote" if args.chain == "bsc" else "executable_quote"
                ),
                executable_quote=True if args.chain == "bsc" else mode == "live" or args.chain != "bsc",
                live_executor=live_executor if mode == "live" else None,
                live_max_entries=live_config.max_entries if mode == "live" and live_config else 0,
            )
            stores[mode] = RuntimeStore(connection, mode)
            health[mode] = HealthRegistry(health_path)
            audits[mode] = JsonlAuditWriter(audit_path)

        client = BinanceWeb3Client.from_env()
        bsc_quote_provider = (
            BscReadOnlyQuoteProvider.from_env(project_root=Path.cwd())
            if args.chain == "bsc" and args.mode != "live"
            else None
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
        source = BinanceWeb3SignalSource(
            client,
            chain_id=chain_id,
            rank_type=int(os.environ.get("BINANCE_MEME_RANK_TYPE", "10")),
            limit=int(os.environ.get("BINANCE_MEME_LIMIT", "40")),
        )
        quote_provider = (
            JupiterReadOnlyQuoteProvider.from_env(decimals_resolver=decimals_cache.resolve)
            if decimals_cache is not None
            else None
        )
        features = BinanceRealtimeFeatureProvider(
            market_data=BinanceWeb3MarketDataAdapter(client, chain_id=chain_id),
            quote_provider=quote_provider,
            position_size_sol=strategy_config.position_size_sol,
            chain_id=chain_id,
            live_executor=live_executor,
            bsc_quote_provider=bsc_quote_provider,
            bsc_pool_resolver=(BscPoolResolver.from_env() if args.chain == "bsc" else None),
            solana_price_monitor=solana_price_monitor,
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
            observation_delay_sec=strategy_config.observation_delay_sec,
        )
        if args.mode == "live":
            telegram.notify_event("BSC_LIVE_STARTED", {}, chain="BSC", mode="live")
            live_telegram_started = True
        if args.chain == "bsc" and not args.once:
            bsc_wss_monitor = BscPairWssMonitor.from_env()
            bsc_wss_monitor.callback = coordinator.notify_bsc_pair_event
            bsc_wss_monitor.set_pool_descriptors(coordinator.bsc_position_pool_descriptors())
            bsc_wss_monitor.start()
            coordinator.update_bsc_wss_status(bsc_wss_monitor.safe_status())
        stop_event = Event()
        wss_trigger = Event()

        def on_wss_event(event: object) -> None:
            if isinstance(event, dict) and event.get("method") == "accountNotification":
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
            coordinator.sync_solana_price_bindings()
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

        if args.chain == "solana" and not args.once:
            wss_monitor = SolanaWssMonitor()
            refresh_wss_subscriptions()
            wss_thread = Thread(target=run_wss_monitor, name="solana-position-wss", daemon=True)
            wss_thread.start()

        configured_poll_sec = args.poll_sec if args.poll_sec is not None else float(os.environ.get("POLL_INTERVAL_SEC", "10"))
        poll_sec = configured_poll_sec
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
                    if bsc_wss_monitor is not None:
                        bsc_wss_monitor.set_pool_descriptors(coordinator.bsc_position_pool_descriptors())
                    next_signal_at = time.monotonic() + poll_sec
                if now >= next_position_at or coordinator.position_refresh_requested():
                    trigger = "wss" if coordinator.position_refresh_requested() else "poll"
                    coordinator.run_position_cycle(trigger=trigger)
                    if bsc_wss_monitor is not None:
                        bsc_wss_monitor.set_pool_descriptors(coordinator.bsc_position_pool_descriptors())
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
            # Solana holdings use account WSS for local prices and a bounded
            # 10-second executable Jupiter check; no fixed 2-second Quote GET.
            position_poll_sec = 10.0

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
        if wss_thread is not None:
            wss_thread.join(timeout=5.0)
        if position_thread is not None:
            position_thread.join(timeout=5.0)
        for connection in connections.values():
            connection.close()


def run_with_lock(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    _load_env(args.env_file)
    paths = RuntimePaths.from_env(args.chain)
    lock_name = f"realtime-{args.chain}-{args.mode}"
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
