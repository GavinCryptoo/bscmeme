#!/usr/bin/env python3
"""Bounded 50-token SOL quote-only Binance Wallet roundtrip benchmark."""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from meme_system.adapters.solana_agentic_wallet import SolanaAgenticWalletQuoteProvider


def percentile(values: list[int], fraction: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def rate(value: int, total: int) -> float:
    return round(value * 100 / total, 2) if total else 0.0


def sample_tokens(db: Path, limit: int) -> list[dict[str, Any]]:
    connection = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            "SELECT c.mint,c.symbol,c.latest_migrate_status,c.latest_lifecycle,c.active_candidate,c.updated_at,"
            "EXISTS(SELECT 1 FROM survivor_positions p WHERE p.mint=c.mint) AS position_token "
            "FROM survivor_candidates c ORDER BY position_token DESC,c.active_candidate DESC,c.updated_at DESC"
        ).fetchall()
    finally:
        connection.close()
    chosen: list[sqlite3.Row] = []
    seen: set[str] = set()
    selectors = (
        lambda row: bool(row["position_token"]),
        lambda row: bool(row["active_candidate"]),
        lambda row: int(row["latest_migrate_status"] or 0) == 0,
        lambda row: int(row["latest_migrate_status"] or 0) == 1,
    )
    for selector in selectors:
        for row in rows:
            if len(chosen) >= limit:
                break
            if row["mint"] not in seen and selector(row):
                chosen.append(row); seen.add(row["mint"])
    for row in rows:
        if len(chosen) >= limit:
            break
        if row["mint"] not in seen:
            chosen.append(row); seen.add(row["mint"])
    output = []
    for row in chosen[:limit]:
        migrated = int(row["latest_migrate_status"] or 0) == 1
        categories = ["Post-migration", "PumpSwap"] if migrated else ["Pre-migration", "Pump"]
        if row["active_candidate"]: categories.append("Recent Candidate")
        if row["position_token"]: categories.append("Recent Position")
        output.append({"mint": row["mint"], "symbol": row["symbol"], "categories": categories})
    return output


def run_one(provider: SolanaAgenticWalletQuoteProvider, sample: dict[str, Any], amount: Decimal) -> dict[str, Any]:
    buy, buy_error = provider.quote_result(sample["mint"], "buy", amount)
    result = {**sample, "buy": None, "sell": None, "outcome": "BUY_FAILED"}
    if buy is None:
        result["buy"] = {"success": False, "reason": buy_error.reason if buy_error else "OTHER", "latency_ms": buy_error.latency_ms if buy_error else None}
        return result
    result["buy"] = {"success": True, "latency_ms": buy.latency_ms, "amount_out": str(buy.output_quantity)}
    sell, sell_error = provider.quote_result(sample["mint"], "sell", buy.output_quantity)
    if sell is None:
        result["sell"] = {"success": False, "reason": sell_error.reason if sell_error else "OTHER", "latency_ms": sell_error.latency_ms if sell_error else None}
        result["outcome"] = "BUY_ONLY"
        return result
    result["sell"] = {"success": True, "latency_ms": sell.latency_ms, "amount_out": str(sell.output_quantity)}
    result["outcome"] = "ROUNDTRIP_EXECUTABLE"
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=Path("data/solana/survivor_v2_shadow/runtime.db"))
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--amount-sol", default="0.001")
    parser.add_argument("--timeout-sec", type=float, default=8.0)
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    samples = sample_tokens(args.db, max(1, min(50, args.limit)))
    provider = SolanaAgenticWalletQuoteProvider(timeout_sec=args.timeout_sec)
    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, min(10, args.workers))) as pool:
        futures = {pool.submit(run_one, provider, sample, Decimal(args.amount_sol)): sample for sample in samples}
        for future in as_completed(futures):
            results.append(future.result())
    results.sort(key=lambda row: next((i for i, sample in enumerate(samples) if sample["mint"] == row["mint"]), 10**9))
    buy_ok = sum(bool(row["buy"] and row["buy"]["success"]) for row in results)
    sell_ok = sum(bool(row["sell"] and row["sell"]["success"]) for row in results)
    roundtrip_ok = sum(row["outcome"] == "ROUNDTRIP_EXECUTABLE" for row in results)
    buy_latency = [row["buy"]["latency_ms"] for row in results if row["buy"] and row["buy"]["success"] and row["buy"]["latency_ms"] is not None]
    sell_latency = [row["sell"]["latency_ms"] for row in results if row["sell"] and row["sell"]["success"] and row["sell"]["latency_ms"] is not None]
    failures = Counter(attempt["reason"] for row in results for attempt in (row["buy"], row["sell"]) if attempt and not attempt["success"])
    grouped: dict[str, Any] = {}
    for category in sorted({category for row in results for category in row["categories"]}):
        members = [row for row in results if category in row["categories"]]
        success = sum(row["outcome"] == "ROUNDTRIP_EXECUTABLE" for row in members)
        grouped[category] = {"samples": len(members), "success": success, "coverage_pct": rate(success, len(members))}
    coverage = rate(roundtrip_ok, len(results))
    policy = "BINANCE_PRIMARY" if coverage >= 90 else "HYBRID" if coverage >= 70 else "JUPITER_DIRECT_PRIMARY"
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(), "chain": "CT_501", "mode": "QUOTE_ONLY",
        "sample_count": len(results), "amount_sol": args.amount_sol,
        "buy": {"success": buy_ok, "coverage_pct": rate(buy_ok, len(results)), "latency_ms": {"p50": percentile(buy_latency,.5), "p95": percentile(buy_latency,.95), "p99": percentile(buy_latency,.99)}},
        "sell": {"success": sell_ok, "coverage_pct": rate(sell_ok, len(results)), "latency_ms": {"p50": percentile(sell_latency,.5), "p95": percentile(sell_latency,.95), "p99": percentile(sell_latency,.99)}},
        "roundtrip": {"success": roundtrip_ok, "coverage_pct": coverage}, "category_roundtrip": grouped,
        "failure_reasons": dict(sorted(failures.items())), "selected_route_policy": policy, "results": results,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: report[key] for key in ("sample_count","buy","sell","roundtrip","category_roundtrip","failure_reasons","selected_route_policy")}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
