#!/usr/bin/env python3
"""Build unsigned routes and reconcile provider gas hints with BSC RPC.

This script never signs, sends, or submits a transaction.  SELL estimation is
expected to fail when the benchmark EOA does not own the synthetic BUY output;
that condition is recorded instead of being bypassed with fabricated state.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
from pathlib import Path

from web3 import Web3

from benchmark_universal_execution_router import estimate_transaction, retry_quote_build, route_json
from meme_system.adapters.universal_execution_router import providers_from_env


def mean(values):
    values = [float(value) for value in values if value not in (None, "")]
    return statistics.mean(values) if values else None


def run_provider(provider, source, web3, wallet, limit):
    samples = []
    for row in source[provider.provider]:
        if len(samples) >= limit:
            break
        if not row.get("roundtrip"):
            continue
        buy = retry_quote_build(provider, row["mint"], "buy", Decimal("0.001"))
        if provider.leg_delay_sec:
            time.sleep(provider.leg_delay_sec)
        sell = retry_quote_build(provider, row["mint"], "sell", buy.output_quantity) if buy.ok and buy.output_quantity else None
        buy_json = route_json(buy) or {}; sell_json = route_json(sell) or {}
        samples.append({
            "mint": row["mint"],
            "buy_reported_gas_usd": buy_json.get("gas_fee_usd"),
            "buy_rpc": estimate_transaction(web3, wallet, buy_json.get("transaction")),
            "sell_reported_gas_usd": sell_json.get("gas_fee_usd"),
            "sell_approval_rpc": estimate_transaction(web3, wallet, sell_json.get("approval")) if sell_json.get("approval") else None,
            "sell_rpc": estimate_transaction(web3, wallet, sell_json.get("transaction")),
        })
    buy_rpc_bnb = [item["buy_rpc"].get("gas_bnb") for item in samples if item["buy_rpc"].get("ok")]
    sell_rpc_bnb = [item["sell_rpc"].get("gas_bnb") for item in samples if item["sell_rpc"].get("ok")]
    approval_rpc_bnb = [item["sell_approval_rpc"].get("gas_bnb") for item in samples if item.get("sell_approval_rpc") and item["sell_approval_rpc"].get("ok")]
    return provider.provider, {
        "samples": samples,
        "buy_builds": sum(item["buy_rpc"].get("reason") != "NO_TRANSACTION" for item in samples),
        "buy_rpc_estimates": len(buy_rpc_bnb),
        "mean_buy_rpc_gas_bnb": mean(buy_rpc_bnb),
        "sell_builds": sum(item["sell_rpc"].get("reason") != "NO_TRANSACTION" for item in samples),
        "sell_rpc_estimates": len(sell_rpc_bnb),
        "mean_sell_rpc_gas_bnb": mean(sell_rpc_bnb),
        "sell_approval_rpc_estimates": len(approval_rpc_bnb),
        "mean_sell_approval_rpc_gas_bnb": mean(approval_rpc_bnb),
        "mean_reported_buy_gas_usd": mean(item["buy_reported_gas_usd"] for item in samples),
        "mean_reported_sell_gas_usd": mean(item["sell_reported_gas_usd"] for item in samples),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark", type=Path, default=Path("reports/universal_execution_router_benchmark.json"))
    parser.add_argument("--output", type=Path, default=Path("reports/universal_execution_router_gas.json"))
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()
    rpc_url = os.environ.get("BSC_RPC_URL", "").strip()
    if not rpc_url or not os.environ.get("BSC_PRIVATE_KEY", "").strip():
        raise RuntimeError("BSC_RPC_URL_AND_LOCAL_EOA_REQUIRED")
    web3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 15}))
    if int(web3.eth.chain_id) != 56:
        raise RuntimeError("BSC_CHAIN_REQUIRED")
    wallet = web3.eth.account.from_key(os.environ["BSC_PRIVATE_KEY"]).address
    providers, _ = providers_from_env(wallet, web3)
    source = json.loads(args.benchmark.read_text(encoding="utf-8"))["provider_results"]
    results = {}
    with ThreadPoolExecutor(max_workers=len(providers)) as executor:
        jobs = [executor.submit(run_provider, provider, source, web3, wallet, args.limit) for provider in providers]
        for future in as_completed(jobs):
            name, result = future.result(); results[name] = result
            print(json.dumps({"provider": name, "samples": len(result["samples"]), "buy_rpc": result["buy_rpc_estimates"], "sell_rpc": result["sell_rpc_estimates"]}), flush=True)
    payload = {"chain_id": 56, "signed": False, "broadcast": False, "submitted": False, "providers": results}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
