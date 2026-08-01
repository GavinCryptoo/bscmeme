#!/usr/bin/env python3
"""Bounded, read-only Binance Web3 probe.

This command never starts a runner, creates a database, or enters Paper/Shadow
simulation. It prints only aggregate/safe metadata and exits after one request
or the explicitly bounded duration.
"""

from __future__ import annotations

import argparse
import json
import time

from meme_system.adapters.binance_web3 import (
    BinanceWeb3Client,
    BinanceWeb3KlineAdapter,
    BinanceWeb3MarketDataAdapter,
    BinanceWeb3SignalSource,
    BinanceWeb3SmartMoneyAdapter,
)
from meme_system.adapters.binance_web3.errors import BinanceWeb3Error


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one bounded Binance Web3 read-only probe")
    parser.add_argument("--source", choices=("binance_web3",), default="binance_web3")
    parser.add_argument("--endpoint", choices=("meme", "smart-money", "dynamic", "kline"), default="meme")
    parser.add_argument("--mint", help="Solana Mint for dynamic or kline probes")
    parser.add_argument("--interval", default="1min")
    parser.add_argument("--rank-type", type=int, default=10)
    parser.add_argument("--limit", type=int, default=3)
    parser.add_argument("--duration", type=float, default=0.0)
    parser.add_argument("--poll-sec", type=float, default=10.0)
    parser.add_argument("--once", action="store_true", help="Make exactly one request")
    return parser


def _base_summary(client: BinanceWeb3Client, endpoint: str) -> dict[str, object]:
    return {
        "source": "binance_web3",
        "endpoint": endpoint,
        "chain_id": "CT_501",
        "auth": client.auth.safe_status(),
        "requests": client.rate_limit_state.requests,
        "retries": client.rate_limit_state.retries,
        "rate_limited": client.rate_limit_state.rate_limited,
    }


def _probe_once(args: argparse.Namespace, client: BinanceWeb3Client) -> dict[str, object]:
    endpoint = args.endpoint
    if endpoint == "meme":
        source = BinanceWeb3SignalSource(client, rank_type=args.rank_type, limit=args.limit)
        rows = source.fetch_once()
        return {
            **_base_summary(client, endpoint),
            "records": len(rows),
            "bootstrap_records": sum(row.historical_bootstrap for row in rows),
            "new_records": sum(not row.historical_bootstrap for row in rows),
            "mint_fields_available": sum(row.fields["mint"].available for row in rows),
        }
    if endpoint == "smart-money":
        rows = BinanceWeb3SmartMoneyAdapter(client).fetch(page=1, page_size=args.limit)
        return {
            **_base_summary(client, endpoint),
            "records": len(rows),
            "trigger_entry": False,
            "shadow_feature_only": True,
        }
    if not args.mint:
        raise ValueError("--mint is required for dynamic and kline probes")
    if endpoint == "dynamic":
        snapshot = BinanceWeb3MarketDataAdapter(client).snapshot(args.mint)
        return {
            **_base_summary(client, endpoint),
            "mint": args.mint,
            "available_fields": sorted(name for name, field in snapshot.fields.items() if field.available),
            "unavailable_fields": sorted(name for name, field in snapshot.fields.items() if not field.available),
            "raw_response_hash": snapshot.raw_response_hash,
        }
    result = BinanceWeb3KlineAdapter(client).candles(args.mint, interval=args.interval, limit=args.limit)
    return {
        **_base_summary(client, endpoint),
        "mint": args.mint,
        "interval": args.interval,
        "candles": len(result.candles),
        "first_open_time_ms": result.candles[0].open_time_ms if result.candles else None,
        "last_open_time_ms": result.candles[-1].open_time_ms if result.candles else None,
        "raw_response_hash": result.raw_response_hash,
        "api_latency_ms": result.api_latency_ms,
    }


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.duration < 0 or args.duration > 300:
        _parser().error("--duration must be between 0 and 300 seconds")
    if args.poll_sec <= 0:
        _parser().error("--poll-sec must be positive")
    client = BinanceWeb3Client.from_env()
    started = time.monotonic()
    summaries: list[dict[str, object]] = []
    try:
        while True:
            summaries.append(_probe_once(args, client))
            if args.once or args.duration <= 0 or time.monotonic() - started >= args.duration:
                break
            remaining = args.duration - (time.monotonic() - started)
            time.sleep(min(args.poll_sec, max(0.0, remaining)))
    except BinanceWeb3Error as exc:
        output = {
            "source": "binance_web3",
            "endpoint": args.endpoint,
            "status": "blocked",
            "error_class": exc.context.error_class,
            "request_id": exc.context.request_id,
            "retryable": exc.context.retryable,
            "retry_count": exc.context.retry_count,
            "http_status": exc.context.http_status,
            "redacted_message": exc.redacted_message,
            "no_wallet_or_execution": True,
        }
        print(json.dumps(output, ensure_ascii=False, sort_keys=True))
        return 2
    except ValueError as exc:
        print(json.dumps({"status": "invalid_arguments", "message": str(exc)}, ensure_ascii=False))
        return 2
    print(json.dumps({"status": "ok", "probes": summaries, "bounded": True}, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
