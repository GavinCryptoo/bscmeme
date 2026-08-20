#!/usr/bin/env python3
"""Benchmark the exact 50-token Bitget cohort without signing/broadcasting."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
from typing import Any

from web3 import Web3

from meme_system.adapters.universal_execution_router import (
    BitgetAggregateRouteProvider,
    LaunchpadDirectRouteProvider,
    RouteResult,
    RoundtripResult,
    providers_from_env,
)


def load_env(path: Path) -> None:
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" not in line or line.lstrip().startswith("#"):
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def percentile(values: list[int], fraction: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round((len(ordered) - 1) * fraction))]


def summarize(results: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(results); buy = sum(item["buy_success"] for item in results); sell = sum(item["sell_success"] for item in results); rt = sum(item["roundtrip"] for item in results)
    latencies = [value for item in results for value in (item.get("buy_latency_ms"), item.get("sell_latency_ms")) if value is not None]
    failures = Counter(item.get("buy_failure") or item.get("sell_failure") for item in results if item.get("buy_failure") or item.get("sell_failure"))
    return {
        "samples": total,
        "buy_success": buy, "buy_pct": round(100 * buy / total, 2) if total else None,
        "sell_success": sell, "sell_pct": round(100 * sell / total, 2) if total else None,
        "roundtrip_success": rt, "roundtrip_pct": round(100 * rt / total, 2) if total else None,
        "latency_ms": {"p50": percentile(latencies, .50), "p95": percentile(latencies, .95), "p99": percentile(latencies, .99)},
        "no_route": failures.get("NO_ROUTE", 0), "failure_counts": dict(failures),
    }


def by_category(results: list[dict[str, Any]], category: str) -> dict[str, Any]:
    return summarize([item for item in results if category in item["categories"]])


def route_json(result: RouteResult | None) -> dict[str, Any] | None:
    if result is None:
        return None
    return {
        "provider": result.provider, "success": result.ok, "output_quantity": str(result.output_quantity) if result.output_quantity is not None else None,
        "latency_ms": result.latency_ms, "failure": result.failure_reason,
        "gas_hint": result.gas_hint, "gas_fee_usd": str(result.gas_fee_usd) if result.gas_fee_usd is not None else None,
        "provider_fee_usd": str(result.provider_fee_usd) if result.provider_fee_usd is not None else None,
        "lp_fee_usd": str(result.lp_fee_usd) if result.lp_fee_usd is not None else None,
        "fee_detail": result.fee_detail,
        "price_impact_pct": str(result.price_impact_pct) if result.price_impact_pct is not None else None,
        "route": list(result.route), "transaction_built": result.transaction is not None,
        "approval_built": result.approval is not None,
        "build_failure": result.build_failure_reason,
        "transaction": asdict(result.transaction) if result.transaction else None,
        "approval": asdict(result.approval) if result.approval else None,
    }


def retry_roundtrip(provider, token: str, amount: Decimal, *, build: bool = True) -> RoundtripResult:
    last = None
    for attempt in range(3):
        last = provider.roundtrip(token, amount, build=build)
        reasons = {last.buy.failure_reason, last.sell.failure_reason if last.sell else None}
        if not reasons.intersection({"RATE_LIMIT", "REQUEST_TIMEOUT", "PROVIDER_UNAVAILABLE"}):
            return last
        time.sleep(1.5 * (attempt + 1))
    assert last is not None
    return last


def retry_quote_build(provider, token: str, side: str, quantity: Decimal) -> RouteResult:
    last = None
    for attempt in range(4):
        last = provider.quote_build(token, side, quantity, build=True)
        if last.failure_reason not in {"RATE_LIMIT", "REQUEST_TIMEOUT", "PROVIDER_UNAVAILABLE"} and last.build_failure_reason != "RATE_LIMIT":
            return last
        time.sleep(2 * (attempt + 1))
    assert last is not None
    return last


def estimate_transaction(web3: Web3, wallet: str, item: dict[str, Any] | None) -> dict[str, Any]:
    if not item:
        return {"ok": False, "reason": "NO_TRANSACTION"}
    try:
        to = web3.to_checksum_address(item["to"])
        if int(item.get("chain_id") or 56) != 56 or web3.eth.get_code(to) in {b"", b"\x00"}:
            return {"ok": False, "reason": "SAFETY_CHECK_FAILED"}
        request = {"from": wallet, "to": to, "data": item["data"], "value": int(item.get("value") or 0)}
        gas = int(web3.eth.estimate_gas(request)); gas_price = int(web3.eth.gas_price)
        return {"ok": True, "gas": gas, "gas_price": gas_price, "gas_bnb": str(Decimal(gas * gas_price) / Decimal(10**18))}
    except Exception as exc:
        return {"ok": False, "reason": type(exc).__name__}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--cohort", type=Path, default=Path("reports/bitget_wallet_benchmark.json"))
    parser.add_argument("--output", type=Path, default=Path("reports/universal_execution_router_benchmark.json"))
    parser.add_argument("--amount", type=Decimal, default=Decimal("0.001"))
    args = parser.parse_args()
    load_env(args.env_file)
    cohort = json.loads(args.cohort.read_text(encoding="utf-8"))["results"]
    if len(cohort) != 50 or len({item["mint"].lower() for item in cohort}) != 50:
        raise RuntimeError("EXACT_50_TOKEN_COHORT_REQUIRED")
    web3 = Web3(Web3.HTTPProvider(os.environ["BSC_RPC_URL"], request_kwargs={"timeout": 20}))
    if int(web3.eth.chain_id) != 56:
        raise RuntimeError("BSC_CHAIN_REQUIRED")
    wallet = Web3().eth.account.from_key(os.environ["BSC_PRIVATE_KEY"]).address
    providers, direct = providers_from_env(wallet, web3)
    provider_results: dict[str, list[dict[str, Any]]] = {provider.provider: [] for provider in providers}
    direct_results: list[dict[str, Any]] = []
    for index, sample in enumerate(cohort, 1):
        token = sample["mint"]; categories = list(sample["categories"])
        with ThreadPoolExecutor(max_workers=len(providers)) as executor:
            # Coverage is measured on two-sided real quotes.  Transaction build
            # is sampled separately because a sell builder may correctly reject
            # the benchmark EOA before it owns the post-buy tokens.
            jobs = {executor.submit(retry_roundtrip, provider, token, args.amount, build=False): provider.provider for provider in providers}
            roundtrips = {jobs[future]: future.result() for future in as_completed(jobs)}
        for name, result in roundtrips.items():
            provider_results[name].append({
                "mint": token, "symbol": sample.get("symbol"), "categories": categories,
                "buy_success": result.buy.ok, "sell_success": bool(result.sell and result.sell.ok), "roundtrip": result.ok,
                "buy_latency_ms": result.buy.latency_ms, "sell_latency_ms": result.sell.latency_ms if result.sell else None,
                "buy_failure": result.buy.failure_reason, "sell_failure": result.sell.failure_reason if result.sell else None,
                "buy": route_json(result.buy), "sell": route_json(result.sell),
            })
        if "migrateStatus=0" in categories:
            result = retry_roundtrip(direct, token, args.amount, build=True)
            direct_family = result.buy.provider
            failure_text = " ".join(filter(None, [result.buy.failure_reason, result.sell.failure_reason if result.sell else None])).upper()
            if "Flap" in categories or "FLAP" in failure_text:
                direct_family = "FLAP_DIRECT"
            elif "FourMeme" in categories or "FOURMEME" in failure_text:
                direct_family = "FOURMEME_DIRECT"
            direct_results.append({
                "mint": token, "symbol": sample.get("symbol"), "categories": categories,
                "direct_family": direct_family,
                "buy_success": result.buy.ok, "sell_success": bool(result.sell and result.sell.ok), "roundtrip": result.ok,
                "buy_latency_ms": result.buy.latency_ms, "sell_latency_ms": result.sell.latency_ms if result.sell else None,
                "buy_failure": result.buy.failure_reason, "sell_failure": result.sell.failure_reason if result.sell else None,
                "buy": route_json(result.buy), "sell": route_json(result.sell),
            })
        print(json.dumps({"progress": index, "total": len(cohort), "token": token, "roundtrip": {name: value.ok for name, value in roundtrips.items()}}, ensure_ascii=False), flush=True)
    summaries = {}
    for name, results in provider_results.items():
        summaries[name] = {
            "all": summarize(results),
            "pre_migration": by_category(results, "migrateStatus=0"),
            "migrated": by_category(results, "migrateStatus=1"),
            "flap": by_category(results, "Flap"), "fourmeme": by_category(results, "FourMeme"),
            "pancake": by_category(results, "Pancake"), "venue_unknown": by_category(results, "VENUE_UNKNOWN_OR_UNSUPPORTED"),
        }
    direct_flap = [item for item in direct_results if item["direct_family"] == "FLAP_DIRECT"]
    direct_four = [item for item in direct_results if item["direct_family"] == "FOURMEME_DIRECT"]
    # Combination: launchpad-native route for recognized pre-migration tokens,
    # otherwise any successful aggregator.  This is capability routing, not a
    # fixed primary.
    combined = []
    direct_by_token = {item["mint"].lower(): item for item in direct_results}
    for sample in cohort:
        key = sample["mint"].lower(); direct_item = direct_by_token.get(key)
        aggregate_items = [next(item for item in values if item["mint"].lower() == key) for values in provider_results.values()]
        success = bool(direct_item and direct_item["roundtrip"]) or any(item["roundtrip"] for item in aggregate_items)
        combined.append({"mint": sample["mint"], "categories": sample["categories"], "roundtrip": success, "buy_success": success, "sell_success": success})
    # Bitget real RPC gas comparison: use the first ten roundtrips with built
    # unsigned transactions.  No signing or send call exists in this script.
    gas_samples = []
    bitget_provider = next(provider for provider in providers if provider.provider == "BITGET")
    for item in provider_results.get("BITGET", []):
        if len(gas_samples) >= 10:
            break
        if not item["roundtrip"]:
            continue
        buy_result = retry_quote_build(bitget_provider, item["mint"], "buy", args.amount)
        time.sleep(1.2)
        sell_result = retry_quote_build(bitget_provider, item["mint"], "sell", buy_result.output_quantity) if buy_result.ok and buy_result.output_quantity else None
        time.sleep(1.2)
        buy = route_json(buy_result) or {}; sell = route_json(sell_result) or {}
        buy_est = estimate_transaction(web3, wallet, buy.get("transaction"))
        sell_approval_est = estimate_transaction(web3, wallet, sell.get("approval")) if sell.get("approval") else None
        sell_est = estimate_transaction(web3, wallet, sell.get("transaction"))
        gas_samples.append({
            "mint": item["mint"], "buy_reported_usd": buy.get("gas_fee_usd"), "sell_reported_usd": sell.get("gas_fee_usd"),
            "buy_rpc": buy_est, "sell_approval_rpc": sell_approval_est, "sell_rpc": sell_est,
        })
    payload = {
        "sample_source": str(args.cohort), "sample_count": 50, "amount_bnb": str(args.amount),
        "safety": {"signed": False, "broadcast": False, "submitted": False, "live_runtime_stopped": True},
        "provider_summaries": summaries,
        "direct": {"flap": summarize(direct_flap), "fourmeme": summarize(direct_four), "results": direct_results},
        "universal": {"all": summarize(combined), "pre_migration": by_category(combined, "migrateStatus=0"), "migrated": by_category(combined, "migrateStatus=1")},
        "gas_reconciliation": gas_samples,
        "provider_results": provider_results,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"provider_summaries": summaries, "direct": payload["direct"], "universal": payload["universal"], "gas_samples": len(gas_samples)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
