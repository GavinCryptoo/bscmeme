#!/usr/bin/env python3
"""Run a bounded, quote-only BSC Agentic Wallet roundtrip benchmark.

The script reads candidate data and emits an auditable JSON report.  It does
not write strategy state or invoke any order, approval, signing, or broadcast
command.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from meme_system.adapters.binance_agentic_wallet import BinanceAgenticWalletRouteProvider


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _source(value: str | None) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}


def _categories(row: sqlite3.Row) -> set[str]:
    source = _source(row["source_status_json"])
    text = " ".join(str(value) for value in source.values()).upper()
    categories = {f"migrateStatus={int(row['latest_migrate_status'] or 0)}"}
    protocol = str(source.get("protocol") or source.get("venue") or "").upper()
    family = str(source.get("protocol_family") or "").upper()
    pool_status = str(source.get("pool_status") or "").upper()
    if "FOUR" in protocol or "FOUR" in family:
        categories.add("FourMeme")
    if "FLAP" in protocol or "FLAP" in family:
        categories.add("Flap")
    if "VENUE_UNKNOWN" in text or family.startswith("UNKNOWN_"):
        categories.add("VENUE_UNKNOWN")
    if "UNSUPPORTED_POOL_TYPE" in text or pool_status == "UNSUPPORTED_POOL_TYPE":
        categories.add("UNSUPPORTED_POOL_TYPE")
    if "UNSUPPORTED_QUOTE_ASSET" in text:
        categories.add("UNSUPPORTED_QUOTE_ASSET")
    if "POOL_FOUND_BUT_UNSUPPORTED" in text:
        categories.add("POOL_FOUND_BUT_UNSUPPORTED")
    if str(row["pool_type"] or "").lower() == "v2":
        categories.add("Pancake_V2")
    if str(row["pool_type"] or "").lower() == "v3":
        categories.add("Pancake_V3")
    return categories


def _load_samples(db_path: Path, limit: int) -> list[dict[str, Any]]:
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            "SELECT mint,symbol,latest_migrate_status,pool_type,source_status_json,updated_at "
            "FROM survivor_candidates WHERE mint LIKE '0x%' "
            "ORDER BY updated_at DESC"
        ).fetchall()
    finally:
        connection.close()
    chosen: list[sqlite3.Row] = []
    seen: set[str] = set()
    wanted = (
        "migrateStatus=0", "migrateStatus=1", "Pancake_V2", "Pancake_V3",
        "FourMeme", "Flap", "VENUE_UNKNOWN", "UNSUPPORTED_POOL_TYPE",
        "UNSUPPORTED_QUOTE_ASSET", "POOL_FOUND_BUT_UNSUPPORTED",
    )
    for category in wanted:
        for row in rows:
            if category in _categories(row) and row["mint"] not in seen:
                chosen.append(row)
                seen.add(row["mint"])
                break
    for row in rows:
        if len(chosen) >= limit:
            break
        if row["mint"] not in seen:
            chosen.append(row)
            seen.add(row["mint"])
    return [
        {
            "mint": row["mint"],
            "symbol": row["symbol"],
            "updated_at": row["updated_at"],
            "categories": sorted(_categories(row)),
        }
        for row in chosen[:limit]
    ]


def _percentile(values: list[int], percentile: int) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * percentile / 100) - 1)
    return ordered[index]


def _rate(successes: int, total: int) -> float | None:
    return round(successes * 100 / total, 2) if total else None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=Path("data/bsc-balanced/paper/runtime.db"))
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--amount-bnb", default="0.01")
    parser.add_argument("--timeout-sec", type=float, default=15.0)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    amount = Decimal(args.amount_bnb)
    if amount <= 0:
        raise SystemExit("--amount-bnb must be positive")
    samples = _load_samples(args.db, max(1, args.limit))
    provider = BinanceAgenticWalletRouteProvider(timeout_sec=args.timeout_sec)
    results: list[dict[str, Any]] = []
    for index, sample in enumerate(samples, start=1):
        print(f"[{index}/{len(samples)}] {sample['symbol'] or sample['mint']}", flush=True)
        buy, buy_failure = provider.quote_result(sample["mint"], "buy", amount)
        result: dict[str, Any] = {**sample, "buy": None, "sell": None, "outcome": "BUY_FAILED"}
        if buy is None:
            result["buy"] = {"success": False, "reason": buy_failure.reason if buy_failure else "OTHER_ERROR", "latency_ms": buy_failure.latency_ms if buy_failure else None}
        else:
            result["buy"] = {"success": True, "amount_in": str(buy.input_quantity), "amount_out": str(buy.output_quantity), "latency_ms": buy.latency_ms, "quoted_at": buy.quoted_at.isoformat()}
            sell, sell_failure = provider.quote_result(sample["mint"], "sell", buy.output_quantity)
            if sell is None:
                result["sell"] = {"success": False, "reason": sell_failure.reason if sell_failure else "OTHER_ERROR", "latency_ms": sell_failure.latency_ms if sell_failure else None}
                result["outcome"] = "BUY_ONLY"
            else:
                result["sell"] = {"success": True, "amount_in": str(sell.input_quantity), "amount_out": str(sell.output_quantity), "latency_ms": sell.latency_ms, "quoted_at": sell.quoted_at.isoformat()}
                result["outcome"] = "ROUNDTRIP_EXECUTABLE"
        results.append(result)
    buy_ok = sum(item["buy"]["success"] for item in results)
    sell_ok = sum(bool(item["sell"] and item["sell"]["success"]) for item in results)
    roundtrip_ok = sum(item["outcome"] == "ROUNDTRIP_EXECUTABLE" for item in results)
    buy_latency = [item["buy"]["latency_ms"] for item in results if item["buy"]["success"] and item["buy"]["latency_ms"] is not None]
    sell_latency = [item["sell"]["latency_ms"] for item in results if item["sell"] and item["sell"]["success"] and item["sell"]["latency_ms"] is not None]
    failures = Counter()
    for item in results:
        for side in ("buy", "sell"):
            attempt = item[side]
            if attempt and not attempt["success"]:
                failures[attempt["reason"]] += 1
    grouped: dict[str, dict[str, int | float | None]] = {}
    all_categories = sorted({category for item in results for category in item["categories"]})
    for category in all_categories:
        members = [item for item in results if category in item["categories"]]
        executable = sum(item["outcome"] == "ROUNDTRIP_EXECUTABLE" for item in members)
        grouped[category] = {"samples": len(members), "roundtrip_success": executable, "roundtrip_coverage_pct": _rate(executable, len(members))}
    report = {
        "generated_at": _now(),
        "provider": "BINANCE_AGENTIC_WALLET",
        "chain": "BSC",
        "binance_chain_id": 56,
        "mode": "PAPER_QUOTE_ONLY",
        "amount_bnb": str(amount),
        "sample_count": len(results),
        "buy": {"success": buy_ok, "coverage_pct": _rate(buy_ok, len(results)), "latency_ms": {"p50": _percentile(buy_latency, 50), "p95": _percentile(buy_latency, 95), "p99": _percentile(buy_latency, 99)}},
        "sell": {"success": sell_ok, "coverage_pct": _rate(sell_ok, len(results)), "latency_ms": {"p50": _percentile(sell_latency, 50), "p95": _percentile(sell_latency, 95), "p99": _percentile(sell_latency, 99)}},
        "roundtrip": {"success": roundtrip_ok, "coverage_pct": _rate(roundtrip_ok, len(results))},
        "category_roundtrip": grouped,
        "failure_reasons": dict(sorted(failures.items())),
        "results": results,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: report[key] for key in ("sample_count", "buy", "sell", "roundtrip", "category_roundtrip", "failure_reasons")}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
