#!/usr/bin/env python3
"""Quote-only Bitget Wallet Order Mode benchmark for recent BSC Meme Rush tokens."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import statistics
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from web3 import Web3

from meme_system.adapters.bitget_wallet import BitgetWalletApiClient, BitgetWalletRouteProvider


def load_env(path: Path) -> None:
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def percentile(values: list[int], fraction: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, round((len(ordered) - 1) * fraction)))]


def categories(row: sqlite3.Row) -> list[str]:
    try:
        source = json.loads(row["source_status_json"] or "{}")
    except json.JSONDecodeError:
        source = {}
    text = (" ".join(str(value) for value in (row["pool_type"], row["latest_lifecycle"])) + " " + json.dumps(source, sort_keys=True)).upper()
    values = [f"migrateStatus={int(row['latest_migrate_status'] or 0)}"]
    if "FLAP" in text:
        values.append("Flap")
    if "FOUR" in text:
        values.append("FourMeme")
    if "PANCAKE" in text or str(row["pool_type"] or "").upper() in {"V2", "V3"}:
        values.append("Pancake")
    if "UNKNOWN" in text or "UNSUPPORTED" in text:
        values.append("VENUE_UNKNOWN_OR_UNSUPPORTED")
    return values


def select_rows(db: Path, limit: int) -> list[sqlite3.Row]:
    connection = sqlite3.connect(f"file:{db.resolve()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        "SELECT mint,symbol,latest_migrate_status,latest_lifecycle,pool_type,source_status_json,last_seen_at "
        "FROM survivor_candidates WHERE lower(mint) LIKE '0x%' ORDER BY last_seen_at DESC"
    ).fetchall()
    connection.close()
    selected: list[sqlite3.Row] = []
    wanted = {"Flap", "FourMeme", "Pancake", "migrateStatus=0", "migrateStatus=1", "VENUE_UNKNOWN_OR_UNSUPPORTED"}
    for target in tuple(wanted):
        row = next((item for item in rows if target in categories(item) and item["mint"] not in {entry["mint"] for entry in selected}), None)
        if row is not None:
            selected.append(row)
    for row in rows:
        if len(selected) >= limit:
            break
        if row["mint"] not in {entry["mint"] for entry in selected}:
            selected.append(row)
    return selected[:limit]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--db", type=Path, default=Path("data/bsc-balanced/live/runtime.db"))
    parser.add_argument("--samples", type=int, default=50)
    parser.add_argument("--amount", type=Decimal, default=Decimal("0.001"))
    parser.add_argument("--output", type=Path, default=Path("reports/bitget_wallet_benchmark.json"))
    parser.add_argument("--delay-sec", type=float, default=1.1)
    args = parser.parse_args()
    load_env(args.env_file)
    wallet = Web3().eth.account.from_key(os.environ["BSC_PRIVATE_KEY"]).address
    provider = BitgetWalletRouteProvider(BitgetWalletApiClient.from_env(), wallet)
    results: list[dict[str, Any]] = []
    for row in select_rows(args.db, max(1, args.samples)):
        buy = buy_error = None
        for attempt in range(4):
            buy, buy_error = provider.quote_result(row["mint"], "buy", args.amount)
            if getattr(buy_error, "reason", None) != "RATE_LIMIT":
                break
            time.sleep(2 ** attempt)
        time.sleep(max(0.0, args.delay_sec))
        sell = sell_error = None
        if buy is not None:
            for attempt in range(4):
                sell, sell_error = provider.quote_result(row["mint"], "sell", buy.output_quantity)
                if getattr(sell_error, "reason", None) != "RATE_LIMIT":
                    break
                time.sleep(2 ** attempt)
            time.sleep(max(0.0, args.delay_sec))
        results.append(
            {
                "mint": row["mint"],
                "symbol": row["symbol"],
                "categories": categories(row),
                "buy_success": buy is not None,
                "sell_success": sell is not None,
                "roundtrip": buy is not None and sell is not None,
                "buy_latency_ms": buy.latency_ms if buy else getattr(buy_error, "latency_ms", None),
                "sell_latency_ms": sell.latency_ms if sell else getattr(sell_error, "latency_ms", None),
                "buy_failure": getattr(buy_error, "reason", None),
                "sell_failure": getattr(sell_error, "reason", None),
                "buy_route": list(buy.route) if buy else [],
                "sell_route": list(sell.route) if sell else [],
                "buy_fee_usd": str(buy.route_fee) if buy and buy.route_fee is not None else None,
                "sell_fee_usd": str(sell.route_fee) if sell and sell.route_fee is not None else None,
                "buy_price_impact": str(buy.price_impact_pct) if buy and buy.price_impact_pct is not None else None,
                "sell_price_impact": str(sell.price_impact_pct) if sell and sell.price_impact_pct is not None else None,
                "buy_gas_fee_usd": provider._quote_context.get(buy.quote_id, {}).get("gas_fee_usd") if buy else None,
                "sell_gas_fee_usd": provider._quote_context.get(sell.quote_id, {}).get("gas_fee_usd") if sell else None,
            }
        )
    def coverage(key: str, category: str | None = None) -> dict[str, Any]:
        subset = [item for item in results if category is None or category in item["categories"]]
        passed = sum(bool(item[key]) for item in subset)
        return {"samples": len(subset), "success": passed, "pct": round(passed * 100 / len(subset), 2) if subset else None}
    buy_lat = [int(item["buy_latency_ms"]) for item in results if item["buy_latency_ms"] is not None]
    sell_lat = [int(item["sell_latency_ms"]) for item in results if item["sell_latency_ms"] is not None]
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sample_count": len(results),
        "amount_bnb": str(args.amount),
        "coverage": {"buy": coverage("buy_success"), "sell": coverage("sell_success"), "roundtrip": coverage("roundtrip")},
        "category_roundtrip": {name: coverage("roundtrip", name) for name in ("Flap", "FourMeme", "Pancake", "migrateStatus=0", "migrateStatus=1", "VENUE_UNKNOWN_OR_UNSUPPORTED")},
        "latency_ms": {
            "buy": {"p50": percentile(buy_lat, .50), "p95": percentile(buy_lat, .95), "p99": percentile(buy_lat, .99)},
            "sell": {"p50": percentile(sell_lat, .50), "p95": percentile(sell_lat, .95), "p99": percentile(sell_lat, .99)},
        },
        "failure_counts": {reason: sum(item["buy_failure"] == reason or item["sell_failure"] == reason for item in results) for reason in sorted({str(item["buy_failure"] or item["sell_failure"]) for item in results if item["buy_failure"] or item["sell_failure"]})},
        "mean_fee_usd": str(statistics.mean([float(value) for item in results for value in (item["buy_fee_usd"], item["sell_fee_usd"]) if value is not None])) if any(item["buy_fee_usd"] or item["sell_fee_usd"] for item in results) else None,
        "mean_gas_fee_usd": str(statistics.mean([float(value) for item in results for value in (item["buy_gas_fee_usd"], item["sell_gas_fee_usd"]) if value not in (None, "")])) if any(item["buy_gas_fee_usd"] or item["sell_gas_fee_usd"] for item in results) else None,
        "results": results,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({key: payload[key] for key in ("sample_count", "coverage", "category_roundtrip", "latency_ms", "failure_counts", "mean_fee_usd", "mean_gas_fee_usd")}, ensure_ascii=False))
    provider.client.session.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
