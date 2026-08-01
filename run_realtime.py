#!/usr/bin/env python3
"""Run the Gate A realtime Paper/Shadow coordinator.

This command is intentionally explicit and stoppable. It only builds read-only
Binance/Jupiter/Solana-adjacent adapters and virtual simulation engines.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from decimal import Decimal
from pathlib import Path
from threading import Event

from meme_system.adapters.binance_web3.client import BinanceWeb3Client
from meme_system.adapters.binance_web3.market_data import BinanceWeb3MarketDataAdapter
from meme_system.adapters.binance_web3.signal_source import BinanceWeb3SignalSource
from meme_system.adapters.jupiter import JupiterReadOnlyQuoteProvider
from meme_system.config.data_source import DataSourceConfig
from meme_system.config.runtime import RuntimePaths
from meme_system.config.safety import SafetyConfig
from meme_system.engines.ledger import SimulationLedger
from meme_system.engines.simulation import DeterministicSimulation
from meme_system.realtime import BinanceRealtimeFeatureProvider, RealtimeCoordinator
from meme_system.runtime_ops import HealthRegistry, JsonlAuditWriter, LatencyRecorder, RuntimeControl, SingleInstanceLock
from meme_system.storage.database import initialize_database
from meme_system.storage.runtime_store import RuntimeStore
from meme_system.telegram_control import TelegramConfig, TelegramControl


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Gate A Solana Paper/Shadow realtime runner")
    parser.add_argument("--mode", choices=("paper", "shadow", "both"), default="both")
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
    data_source = DataSourceConfig.from_env()
    if data_source.data_source != "binance_web3":
        print(json.dumps({"status": "blocked", "error_class": "realtime_requires_binance_web3", "data_source": data_source.data_source}, ensure_ascii=False))
        return 2
    paths = RuntimePaths.from_env()
    paths.validate_isolation()
    modes = _modes(args.mode)
    connections = {}
    engines = {}
    stores = {}
    health = {}
    audits = {}
    try:
        for mode in modes:
            db_path = paths.paper_db if mode == "paper" else paths.shadow_db
            connection = initialize_database(db_path)
            connections[mode] = connection
            ledger = SimulationLedger.recover(mode, connection)
            engines[mode] = DeterministicSimulation(mode, ledger=ledger)
            stores[mode] = RuntimeStore(connection, mode)
            health_path = paths.paper_health_file if mode == "paper" else paths.shadow_health_file
            health[mode] = HealthRegistry(health_path)
            audit_path = paths.paper_audit_log if mode == "paper" else paths.shadow_audit_log
            audits[mode] = JsonlAuditWriter(audit_path)

        client = BinanceWeb3Client.from_env()
        source = BinanceWeb3SignalSource(
            client,
            rank_type=int(os.environ.get("BINANCE_MEME_RANK_TYPE", "10")),
            limit=int(os.environ.get("BINANCE_MEME_LIMIT", "40")),
        )
        quote_provider = JupiterReadOnlyQuoteProvider.from_env(token_decimals=_token_decimals())
        features = BinanceRealtimeFeatureProvider(
            market_data=BinanceWeb3MarketDataAdapter(client),
            quote_provider=quote_provider,
            position_size_sol=engines[modes[0]].strategy.config.position_size_sol,
        )
        control = RuntimeControl(paths.control_file)
        telegram_config = TelegramConfig.from_env()
        if telegram_config.enabled != safety.telegram_enabled:
            raise ValueError("TELEGRAM_ENABLED and Telegram configuration disagree")
        telegram = TelegramControl(telegram_config, control)
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
        )
        poll_sec = args.poll_sec if args.poll_sec is not None else float(os.environ.get("POLL_INTERVAL_SEC", "10"))
        stop_event = Event()
        signal.signal(signal.SIGINT, lambda *_: stop_event.set())
        signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
        started = time.monotonic()
        if args.once:
            result = coordinator.run_cycle()
            print(json.dumps({"status": "ok", "once": True, **result.__dict__}, ensure_ascii=False, default=str))
        else:
            while not stop_event.is_set() and (args.duration <= 0 or time.monotonic() - started < args.duration):
                coordinator.run_cycle()
                remaining = args.duration - (time.monotonic() - started) if args.duration > 0 else poll_sec
                stop_event.wait(max(0.5, min(poll_sec, max(0.0, remaining))))
            print(json.dumps({"status": "stopped", "bounded": args.duration > 0, "modes": modes}, ensure_ascii=False))
        return 0
    except (ValueError, OSError) as exc:
        print(json.dumps({"status": "blocked", "error_class": type(exc).__name__, "message": str(exc)[:300]}, ensure_ascii=False))
        return 2
    finally:
        for connection in connections.values():
            connection.close()


def run_with_lock(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    _load_env(args.env_file)
    paths = RuntimePaths.from_env()
    lock_name = "realtime-" + args.mode
    lock = SingleInstanceLock(paths.lock_dir / f"{lock_name}.lock", name=lock_name)
    try:
        with lock:
            return main(argv)
    except Exception as exc:
        print(json.dumps({"status": "blocked", "error_class": type(exc).__name__, "message": str(exc)[:300]}, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(run_with_lock())

