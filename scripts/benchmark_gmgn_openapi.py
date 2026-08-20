#!/usr/bin/env python3
"""Quote-only GMGN benchmark on the immutable Universal Router 50-token cohort."""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import time
from collections import Counter
from pathlib import Path

from web3 import Web3

from meme_system.adapters.gmgn_openapi import GmgnCliQuoteProvider


def percentile(values, q):
    values = sorted(values)
    return values[math.ceil(len(values) * q) - 1] if values else None


def summarize(rows):
    total = len(rows); buy = sum(x["buy_success"] for x in rows); sell = sum(x["sell_success"] for x in rows); rt = sum(x["roundtrip"] for x in rows)
    b = [x["buy_latency_ms"] for x in rows]; s = [x["sell_latency_ms"] for x in rows if x["sell_latency_ms"] is not None]
    failures = Counter(x["buy_failure"] or x["sell_failure"] for x in rows if x["buy_failure"] or x["sell_failure"])
    gas = [x["estimated_gas_bnb"] for x in rows if x["estimated_gas_bnb"] is not None]
    return {
        "samples": total, "buy_success": buy, "buy_pct": round(100 * buy / total, 2) if total else None,
        "sell_success": sell, "sell_pct": round(100 * sell / total, 2) if total else None,
        "roundtrip_success": rt, "roundtrip_pct": round(100 * rt / total, 2) if total else None,
        "buy_latency_ms": {"p50": percentile(b,.5), "p95": percentile(b,.95), "p99": percentile(b,.99)},
        "sell_latency_ms": {"p50": percentile(s,.5), "p95": percentile(s,.95), "p99": percentile(s,.99)},
        "failures": dict(failures), "rate_limit": failures.get("RATE_LIMIT", 0),
        "mean_estimated_gas_bnb": statistics.mean(gas) if gas else None,
    }


def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--universal",type=Path,default=Path("reports/universal_execution_router_benchmark.json")); ap.add_argument("--output",type=Path,default=Path("reports/gmgn_openapi_benchmark.json")); args=ap.parse_args()
    u=json.loads(args.universal.read_text()); bitget=json.loads(Path(u["sample_source"]).read_text()); cohort=bitget["results"]
    if len(cohort)!=50 or len({x["mint"].lower() for x in cohort})!=50: raise RuntimeError("EXACT_50_COHORT_REQUIRED")
    providers=u["provider_results"]; direct={x["mint"].lower():x for x in u["direct"]["results"]}
    blind=set()
    for sample in cohort:
        key=sample["mint"].lower()
        if "migrateStatus=0" not in sample["categories"]: continue
        agg=any(next(x for x in rows if x["mint"].lower()==key)["roundtrip"] for rows in providers.values())
        native=bool(direct.get(key) and direct[key]["roundtrip"])
        if not agg and not native: blind.add(key)
    values=dict(os.environ); values.pop("BSC_PRIVATE_KEY",None)
    for line in Path('.env').read_text().splitlines():
        if line.startswith('BSC_PRIVATE_KEY='): values['BSC_PRIVATE_KEY']=line.split('=',1)[1].strip().strip("\"'"); break
    wallet=Web3().eth.account.from_key(values['BSC_PRIVATE_KEY']).address
    provider=GmgnCliQuoteProvider(wallet)
    gas_price=120_000_000
    rows=[]
    for i,sample in enumerate(cohort,1):
        buy,sell=provider.roundtrip(sample['mint'],10**15)
        gas_units=(buy.gas_limit or 0)+((sell.gas_limit or 0) if sell else 0)
        row={
            "mint":sample['mint'],"symbol":sample.get('symbol'),"categories":sample['categories'],"universal_blind_spot":sample['mint'].lower() in blind,
            "buy_success":buy.success,"sell_success":bool(sell and sell.success),"roundtrip":bool(buy.success and sell and sell.success),
            "buy_latency_ms":buy.latency_ms,"sell_latency_ms":sell.latency_ms if sell else None,"buy_failure":buy.failure_reason,"sell_failure":sell.failure_reason if sell else None,
            "buy_error_code":buy.error_code,"sell_error_code":sell.error_code if sell else None,
            "buy_output":str(buy.output_amount) if buy.output_amount else None,"buy_min_output":str(buy.min_output_amount) if buy.min_output_amount else None,
            "sell_output":str(sell.output_amount) if sell and sell.output_amount else None,"sell_min_output":str(sell.min_output_amount) if sell and sell.min_output_amount else None,
            "slippage":str(buy.slippage) if buy.slippage is not None else None,"buy_route_type":buy.route_type,"sell_route_type":sell.route_type if sell else None,
            "launch_exchange":buy.launch_exchange,"estimated_gas_bnb":gas_units*gas_price/10**18 if gas_units else None,
        }
        rows.append(row); print(json.dumps({"progress":i,"roundtrip":row['roundtrip'],"blind":row['universal_blind_spot'],"failure":row['buy_failure'] or row['sell_failure']}),flush=True)
        time.sleep(.05)
    def cat(name): return summarize([x for x in rows if name in x['categories']])
    gmgn_flap=[x for x in rows if x.get('launch_exchange')=='flap']
    gmgn_four=[x for x in rows if str(x.get('launch_exchange') or '').startswith(('fourmeme','openfour'))]
    payload={
        "sample_source":u["sample_source"],"sample_count":50,"amount_bnb":"0.001","mode":"QUOTE_ONLY","signed":False,"broadcast":False,"submitted":False,
        "summary":{"all":summarize(rows),"pre_migration":cat('migrateStatus=0'),"migrated":cat('migrateStatus=1'),"flap":summarize(gmgn_flap),"fourmeme":summarize(gmgn_four),"pancake":cat('Pancake'),"venue_unknown":cat('VENUE_UNKNOWN_OR_UNSUPPORTED'),"universal_blind_spot":summarize([x for x in rows if x['universal_blind_spot']])},
        "blind_spot_count":len(blind),"results":rows,
    }
    args.output.write_text(json.dumps(payload,ensure_ascii=False,indent=2)); print(json.dumps(payload['summary'],ensure_ascii=False)); return 0

if __name__=='__main__': raise SystemExit(main())
