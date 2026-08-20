"""Small, dependency-free exploratory summaries for confirmed GMGN Live fills."""

from __future__ import annotations

from collections import defaultdict
from statistics import median
from typing import Any, Iterable, Mapping


def _number(value: object) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _rank(values: list[float]) -> list[float]:
    ordered = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    index = 0
    while index < len(ordered):
        end = index + 1
        while end < len(ordered) and ordered[end][1] == ordered[index][1]:
            end += 1
        rank = (index + 1 + end) / 2.0
        for original, _ in ordered[index:end]:
            ranks[original] = rank
        index = end
    return ranks


def spearman(pairs: Iterable[tuple[object, object]]) -> float | None:
    clean = [(float(x), float(y)) for x, y in pairs if _number(x) is not None and _number(y) is not None]
    if len(clean) < 5:
        return None
    left, right = zip(*clean)
    left_rank, right_rank = _rank(list(left)), _rank(list(right))
    left_mean = sum(left_rank) / len(left_rank)
    right_mean = sum(right_rank) / len(right_rank)
    numerator = sum((x - left_mean) * (y - right_mean) for x, y in zip(left_rank, right_rank))
    denominator = (sum((x - left_mean) ** 2 for x in left_rank) * sum((y - right_mean) ** 2 for y in right_rank)) ** 0.5
    return numerator / denominator if denominator else None


def build_report(rows: Iterable[tuple[Mapping[str, Any], Mapping[str, Any]]]) -> dict[str, Any]:
    """Return descriptive-only correlations; this deliberately does not tune rules."""
    data = [(dict(snapshot), dict(outcome)) for snapshot, outcome in rows]
    factors = (
        "market_cap", "liquidity", "lp_mc_ratio", "holders", "token_age",
        "drawdown_from_ath", "rebound_from_local_low", "buy_volume_60s",
        "sell_volume_60s", "net_buy_volume_60s", "buy_sell_volume_ratio",
        "buy_sell_count_ratio", "holders_growth_1m", "holders_growth_3m",
        "roundtrip_recovery_pct", "buy_price_impact", "top10_pct", "dev_pct",
        "insider_pct", "sniper_pct", "okx_smart_money_count", "okx_kol_count",
        "okx_whale_count", "okx_signal_amount_usd",
    )
    targets = ("mfe_pct", "mae_pct", "realized_pnl_pct")
    correlations: list[dict[str, Any]] = []
    for factor in factors:
        for target in targets:
            coefficient = spearman((snapshot.get(factor), outcome.get(target)) for snapshot, outcome in data)
            if coefficient is not None:
                correlations.append({"factor": factor, "target": target, "spearman": round(coefficient, 4)})
    correlations.sort(key=lambda item: abs(float(item["spearman"])), reverse=True)

    def split_summary(factor: str, target: str) -> dict[str, Any] | None:
        values = [(float(snapshot[factor]), _number(outcome.get(target))) for snapshot, outcome in data if _number(snapshot.get(factor)) is not None and _number(outcome.get(target)) is not None]
        if len(values) < 6:
            return None
        pivot = median(value for value, _ in values)
        high = [result for value, result in values if value >= pivot]
        low = [result for value, result in values if value < pivot]
        if not high or not low:
            return None
        return {"factor": factor, "target": target, "median_split": pivot, "high_median": median(high), "low_median": median(low), "difference": median(high) - median(low), "n_high": len(high), "n_low": len(low)}

    tp_comparisons = []
    for factor in factors:
        for target in ("hit_tp1_50", "hit_tp2_100"):
            yes = [_number(snapshot.get(factor)) for snapshot, outcome in data if outcome.get(target) is True and _number(snapshot.get(factor)) is not None]
            no = [_number(snapshot.get(factor)) for snapshot, outcome in data if outcome.get(target) is False and _number(snapshot.get(factor)) is not None]
            if len(yes) >= 3 and len(no) >= 3:
                tp_comparisons.append({"factor": factor, "target": target, "hit_median": median(yes), "miss_median": median(no), "difference": median(yes) - median(no), "hit_n": len(yes), "miss_n": len(no)})
    tp_comparisons.sort(key=lambda item: abs(float(item["difference"])), reverse=True)

    source_groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for snapshot, outcome in data:
        source_groups[str(snapshot.get("signal_source") or "UNKNOWN")].append(outcome)
    source_summary = {
        source: {"n": len(items), "median_mfe_pct": median(values) if (values := [_number(item.get("mfe_pct")) for item in items if _number(item.get("mfe_pct")) is not None]) else None,
                 "median_realized_pnl_pct": median(values) if (values := [_number(item.get("realized_pnl_pct")) for item in items if _number(item.get("realized_pnl_pct")) is not None]) else None,
                 "tp1_hit_rate": sum(item.get("hit_tp1_50") is True for item in items) / len(items) if items else None,
                 "tp2_hit_rate": sum(item.get("hit_tp2_100") is True for item in items) / len(items) if items else None}
        for source, items in source_groups.items()
    }
    comparisons = [item for factor in factors if (item := split_summary(factor, "realized_pnl_pct")) is not None]
    comparisons.sort(key=lambda item: abs(float(item["difference"])), reverse=True)
    combination_comparisons = []
    for left, right in (("unique_buyers_60s", "holders_growth_1m"), ("unique_buyers_60s", "okx_smart_money_count"), ("roundtrip_recovery_pct", "liquidity")):
        usable = [(float(snapshot[left]), float(snapshot[right]), _number(outcome.get("realized_pnl_pct"))) for snapshot, outcome in data if _number(snapshot.get(left)) is not None and _number(snapshot.get(right)) is not None and _number(outcome.get("realized_pnl_pct")) is not None]
        if len(usable) < 6:
            continue
        left_mid, right_mid = median(item[0] for item in usable), median(item[1] for item in usable)
        both_high = [item[2] for item in usable if item[0] >= left_mid and item[1] >= right_mid]
        other = [item[2] for item in usable if not (item[0] >= left_mid and item[1] >= right_mid)]
        if both_high and other:
            combination_comparisons.append({"factors": [left, right], "both_high_median_realized_pnl_pct": median(both_high), "other_median_realized_pnl_pct": median(other), "difference": median(both_high) - median(other), "both_high_n": len(both_high), "other_n": len(other)})
    return {"sample_size": len(data), "correlations": correlations, "top_positive": [item for item in correlations if item["spearman"] > 0][:5], "top_negative": [item for item in correlations if item["spearman"] < 0][:5], "tp_group_comparisons": tp_comparisons[:10], "high_low_realized_pnl": comparisons[:10], "signal_source_comparison": source_summary,
            "combination_comparisons": combination_comparisons,
            "note": "Exploratory descriptive analysis only; no parameter or decision changes."}
