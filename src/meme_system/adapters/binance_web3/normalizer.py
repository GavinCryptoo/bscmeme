"""Strict field normalization; unavailable never becomes numeric zero."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from meme_system.adapters.binance_web3.errors import BinanceWeb3Error, ErrorContext
from meme_system.adapters.binance_web3.models import (
    BinanceNormalizedSignal,
    BinanceMarketSnapshot,
    ObservedField,
)
from meme_system.adapters.binance_web3.redaction import schema_hash
from meme_system.domain.models import Signal


ADAPTER_VERSION = "binance_web3_v1"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _hash_row(row: Mapping[str, Any]) -> str:
    encoded = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def parse_decimal(value: Any, *, endpoint_type: str, field_name: str) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise BinanceWeb3Error(
            f"invalid decimal for {field_name}",
            context=ErrorContext("binance_invalid_decimal", endpoint_type),
        )
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise BinanceWeb3Error(
            f"invalid decimal for {field_name}",
            context=ErrorContext("binance_invalid_decimal", endpoint_type),
        ) from exc


def parse_integer(value: Any, *, endpoint_type: str, field_name: str) -> int:
    if isinstance(value, bool) or value is None:
        raise BinanceWeb3Error(
            f"invalid integer for {field_name}",
            context=ErrorContext("binance_schema_changed", endpoint_type),
        )
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise BinanceWeb3Error(
            f"invalid integer for {field_name}",
            context=ErrorContext("binance_schema_changed", endpoint_type),
        ) from exc
    return parsed


def parse_millisecond_timestamp(value: Any, *, endpoint_type: str, field_name: str) -> datetime:
    parsed = parse_integer(value, endpoint_type=endpoint_type, field_name=field_name)
    if parsed < 100_000_000_000:
        raise BinanceWeb3Error(
            f"timestamp unit is not documented as milliseconds for {field_name}",
            context=ErrorContext("binance_invalid_timestamp", endpoint_type),
        )
    try:
        return datetime.fromtimestamp(parsed / 1000, tz=timezone.utc)
    except (OverflowError, OSError, ValueError) as exc:
        raise BinanceWeb3Error(
            f"invalid millisecond timestamp for {field_name}",
            context=ErrorContext("binance_invalid_timestamp", endpoint_type),
        ) from exc


def _field(
    row: Mapping[str, Any],
    *,
    normalized_name: str,
    source_field: str,
    observed_at: datetime,
    source_timestamp: datetime | None,
    endpoint_type: str,
    decimal_value: bool = False,
    integer_value: bool = False,
    unavailable_error: str | None = None,
) -> ObservedField:
    if source_field not in row:
        return ObservedField(
            value=None,
            source="binance_web3",
            source_field=source_field,
            observed_at=observed_at,
            source_timestamp=source_timestamp,
            age_ms=None,
            available=False,
            parse_error=unavailable_error or "missing_field",
            adapter_version=ADAPTER_VERSION,
        )
    if unavailable_error is not None:
        return ObservedField(
            value=None,
            source="binance_web3",
            source_field=source_field,
            observed_at=observed_at,
            source_timestamp=source_timestamp,
            age_ms=None,
            available=False,
            parse_error=unavailable_error,
            adapter_version=ADAPTER_VERSION,
        )
    raw = row[source_field]
    try:
        value = (
            parse_decimal(raw, endpoint_type=endpoint_type, field_name=source_field)
            if decimal_value
            else parse_integer(raw, endpoint_type=endpoint_type, field_name=source_field)
            if integer_value
            else raw
        )
    except BinanceWeb3Error as exc:
        return ObservedField(
            value=None,
            source="binance_web3",
            source_field=source_field,
            observed_at=observed_at,
            source_timestamp=source_timestamp,
            age_ms=None,
            available=False,
            parse_error=exc.context.error_class,
            adapter_version=ADAPTER_VERSION,
        )
    age_ms = (
        max(0, int((observed_at - source_timestamp).total_seconds() * 1000))
        if source_timestamp is not None
        else None
    )
    return ObservedField(
        value=value,
        source="binance_web3",
        source_field=source_field,
        observed_at=observed_at,
        source_timestamp=source_timestamp,
        age_ms=age_ms,
        available=True,
        adapter_version=ADAPTER_VERSION,
    )


def _timestamp_field(
    row: Mapping[str, Any],
    *,
    source_field: str,
    observed_at: datetime,
    endpoint_type: str,
) -> ObservedField:
    """Normalize documented millisecond timestamps without guessing units."""

    if source_field not in row or row[source_field] in (None, ""):
        return ObservedField(
            value=None,
            source="binance_web3",
            source_field=source_field,
            observed_at=observed_at,
            source_timestamp=None,
            age_ms=None,
            available=False,
            parse_error="missing_field",
            adapter_version=ADAPTER_VERSION,
        )
    try:
        parsed = parse_millisecond_timestamp(row[source_field], endpoint_type=endpoint_type, field_name=source_field)
    except BinanceWeb3Error as exc:
        return ObservedField(
            value=None,
            source="binance_web3",
            source_field=source_field,
            observed_at=observed_at,
            source_timestamp=None,
            age_ms=None,
            available=False,
            parse_error=exc.context.error_class,
            adapter_version=ADAPTER_VERSION,
        )
    return ObservedField(
        value=parsed,
        source="binance_web3",
        source_field=source_field,
        observed_at=observed_at,
        source_timestamp=parsed,
        age_ms=max(0, int((observed_at - parsed).total_seconds() * 1000)),
        available=True,
        adapter_version=ADAPTER_VERSION,
    )


def _derived_field(value: Any, *, source_field: str, observed_at: datetime) -> ObservedField:
    """Record request-context metadata without presenting it as an upstream field."""

    return ObservedField(
        value=value,
        source="binance_web3:meme_rush",
        source_field=source_field,
        observed_at=observed_at,
        source_timestamp=None,
        age_ms=0,
        available=value is not None,
        parse_error=None if value is not None else "not_provided",
        adapter_version=ADAPTER_VERSION,
    )


def normalize_meme_row(
    row: Mapping[str, Any],
    *,
    fetched_at: datetime,
    historical_bootstrap: bool,
    chain_id: str = "CT_501",
    rank_type: int | None = None,
    lifecycle: str | None = None,
) -> BinanceNormalizedSignal:
    mint = row.get("contractAddress")
    if not isinstance(mint, str) or not mint:
        raise BinanceWeb3Error(
            "Meme Rush row is missing contractAddress",
            context=ErrorContext("binance_missing_required_field", "meme_rush"),
        )
    source_id = row.get("signalId") or row.get("id")
    source_signal_id = str(source_id) if source_id is not None else None
    # A changed snapshot must produce a new event even when upstream reuses an
    # id. Include rankType so the same contract can move through all three
    # lifecycle feeds without one feed suppressing another.
    stable_id = f"meme-rush:{rank_type or 'unknown'}:{_hash_row(row)[:24]}"
    fields = {
        "mint": _field(row, normalized_name="mint", source_field="contractAddress", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush"),
        # pairAnchorAddress is the launchpad's quote/anchor asset (the live
        # BSC values are stablecoin/WBNB-style token addresses), not an AMM
        # Pair contract. Keep it explicit and never feed it to WSS as a pool.
        "pair_anchor_address": _field(row, normalized_name="pair_anchor_address", source_field="pairAnchorAddress", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush"),
        "pair_address": _field(row, normalized_name="pair_address", source_field="pairAddress", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush"),
        # poolAddress is a separate upstream hint.  Like pairAddress it must
        # be chain-validated before it can ever become a WSS subscription.
        "pool_address": _field(row, normalized_name="pool_address", source_field="poolAddress", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush"),
        # A bonding-curve contract is only usable when Binance explicitly
        # identifies it.  Do not derive it from the token or marker address.
        "bonding_curve_address": _field(row, normalized_name="bonding_curve_address", source_field="bondingCurveAddress", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush"),
        # Protocol/version are discovery metadata used only to select a
        # read-only venue adapter. They never create a quote by themselves.
        "protocol": _field(row, normalized_name="protocol", source_field="protocol", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush", integer_value=True),
        "token_version": _field(row, normalized_name="token_version", source_field="tokenVersion", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush", integer_value=True),
        "token_decimals": _field(row, normalized_name="token_decimals", source_field="decimals", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush", integer_value=True),
        "symbol": _field(row, normalized_name="symbol", source_field="symbol", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush"),
        "name": _field(row, normalized_name="name", source_field="name", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush"),
        "price_usd": _field(row, normalized_name="price_usd", source_field="price", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush", decimal_value=True),
        "market_cap_usd": _field(row, normalized_name="market_cap_usd", source_field="marketCap", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush", decimal_value=True),
        "liquidity_usd": _field(row, normalized_name="liquidity_usd", source_field="liquidity", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush", decimal_value=True),
        "volume_24h_usd": _field(row, normalized_name="volume_24h_usd", source_field="volume", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush", decimal_value=True),
        "holders": _field(row, normalized_name="holders", source_field="holders", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush", integer_value=True),
        "count_24h": _field(row, normalized_name="count_24h", source_field="count", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush", integer_value=True),
        "count_buy_24h": _field(row, normalized_name="count_buy_24h", source_field="countBuy", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush", integer_value=True),
        "count_sell_24h": _field(row, normalized_name="count_sell_24h", source_field="countSell", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush", integer_value=True),
        "progress_pct": _field(row, normalized_name="progress_pct", source_field="progress", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush", decimal_value=True),
        "token_created_at": _timestamp_field(row, source_field="createTime", observed_at=fetched_at, endpoint_type="meme_rush"),
        "migrate_time": _timestamp_field(row, source_field="migrateTime", observed_at=fetched_at, endpoint_type="meme_rush"),
        "dev_sold_percent": _field(row, normalized_name="dev_sold_percent", source_field="devSellPercent", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush", decimal_value=True),
        "dev_position": _field(row, normalized_name="dev_position", source_field="devPosition", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush", integer_value=True),
        "migrate_status": _field(row, normalized_name="migrate_status", source_field="migrateStatus", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush", integer_value=True),
        # These are raw audit/risk observations when Meme Rush supplies them.
        # Missing values stay unavailable; the strategy never infers them from
        # volume, price, or holder counts.
        "audit_info_json": _field(row, normalized_name="audit_info_json", source_field="auditInfoJson", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush"),
        "tax_rate_buy": _field(row, normalized_name="tax_rate_buy", source_field="taxRateBuy", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush", decimal_value=True),
        "tax_rate_sell": _field(row, normalized_name="tax_rate_sell", source_field="taxRateSell", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush", decimal_value=True),
        "risk_level": _field(row, normalized_name="risk_level", source_field="riskLevel", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush"),
        "honeypot": _field(row, normalized_name="honeypot", source_field="honeypot", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush"),
        "dev_percent": _field(row, normalized_name="dev_percent", source_field="devPercent", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush", decimal_value=True),
        "insider_percent": _field(row, normalized_name="insider_percent", source_field="insiderPercent", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush", decimal_value=True),
        "sniper_percent": _field(row, normalized_name="sniper_percent", source_field="sniperPercent", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush", decimal_value=True),
        "top10_percent": _field(row, normalized_name="top10_percent", source_field="top10Percent", observed_at=fetched_at, source_timestamp=None, endpoint_type="meme_rush", decimal_value=True),
    }
    if rank_type is not None:
        fields["rank_type"] = _derived_field(rank_type, source_field="request.rankType", observed_at=fetched_at)
        fields["lifecycle"] = _derived_field(lifecycle, source_field="request.lifecycle", observed_at=fetched_at)
    dev_position = fields["dev_position"].value if fields["dev_position"].available else None
    creator_sold = None if dev_position is None else int(dev_position) == 2
    fields["creator_sold"] = _derived_field(creator_sold, source_field="derived.devPosition", observed_at=fetched_at)
    chain = "bsc" if chain_id == "56" else "solana" if chain_id == "CT_501" else chain_id
    signal = Signal(
        signal_id=stable_id,
        mint=mint,
        observed_at=fetched_at,
        source="binance_web3:meme_rush",
        chain=chain,
    )
    return BinanceNormalizedSignal(
        signal=signal,
        source_signal_id=source_signal_id,
        source_timestamp=None,
        fetched_at=fetched_at,
        historical_bootstrap=historical_bootstrap,
        raw_response_hash=schema_hash(row),
        fields=fields,
        chain_id=chain_id,
    )


def normalize_smart_money_row(
    row: Mapping[str, Any],
    *,
    fetched_at: datetime,
    historical_bootstrap: bool,
) -> BinanceNormalizedSignal:
    mint = row.get("contractAddress")
    if not isinstance(mint, str) or not mint:
        raise BinanceWeb3Error(
            "Smart Money row is missing contractAddress",
            context=ErrorContext("binance_missing_required_field", "smart_money"),
        )
    source_id = row.get("signalId") or row.get("id")
    source_signal_id = str(source_id) if source_id is not None else None
    stable_id = source_signal_id or f"smart-money:{_hash_row(row)[:24]}"
    source_timestamp = None
    if row.get("signalTriggerTime") is not None:
        source_timestamp = parse_millisecond_timestamp(
            row["signalTriggerTime"], endpoint_type="smart_money", field_name="signalTriggerTime"
        )
    fields = {
        "mint": _field(row, normalized_name="mint", source_field="contractAddress", observed_at=fetched_at, source_timestamp=source_timestamp, endpoint_type="smart_money"),
        "ticker": _field(row, normalized_name="ticker", source_field="ticker", observed_at=fetched_at, source_timestamp=source_timestamp, endpoint_type="smart_money"),
        "direction": _field(row, normalized_name="direction", source_field="direction", observed_at=fetched_at, source_timestamp=source_timestamp, endpoint_type="smart_money"),
        "signal_timestamp": _field(row, normalized_name="signal_timestamp", source_field="signalTriggerTime", observed_at=fetched_at, source_timestamp=source_timestamp, endpoint_type="smart_money", integer_value=True),
        "alert_price_usd": _field(row, normalized_name="alert_price_usd", source_field="alertPrice", observed_at=fetched_at, source_timestamp=source_timestamp, endpoint_type="smart_money", decimal_value=True),
        "current_price_usd": _field(row, normalized_name="current_price_usd", source_field="currentPrice", observed_at=fetched_at, source_timestamp=source_timestamp, endpoint_type="smart_money", decimal_value=True),
        "current_market_cap_usd": _field(row, normalized_name="current_market_cap_usd", source_field="currentMarketCap", observed_at=fetched_at, source_timestamp=source_timestamp, endpoint_type="smart_money", decimal_value=True),
        "max_gain": _field(row, normalized_name="max_gain", source_field="maxGain", observed_at=fetched_at, source_timestamp=source_timestamp, endpoint_type="smart_money", decimal_value=True),
        "exit_rate": _field(row, normalized_name="exit_rate", source_field="exitRate", observed_at=fetched_at, source_timestamp=source_timestamp, endpoint_type="smart_money", integer_value=True),
        "smart_money_count": _field(row, normalized_name="smart_money_count", source_field="smartMoneyCount", observed_at=fetched_at, source_timestamp=source_timestamp, endpoint_type="smart_money", integer_value=True),
        "status": _field(row, normalized_name="status", source_field="status", observed_at=fetched_at, source_timestamp=source_timestamp, endpoint_type="smart_money"),
    }
    signal = Signal(signal_id=stable_id, mint=mint, observed_at=source_timestamp or fetched_at, source="binance_web3:smart_money")
    return BinanceNormalizedSignal(
        signal=signal,
        source_signal_id=source_signal_id,
        source_timestamp=source_timestamp,
        fetched_at=fetched_at,
        historical_bootstrap=historical_bootstrap,
        raw_response_hash=schema_hash(row),
        fields=fields,
        endpoint_type="smart_money",
    )


def normalize_dynamic(
    row: Mapping[str, Any],
    *,
    mint: str,
    chain_id: str,
    fetched_at: datetime,
) -> BinanceMarketSnapshot:
    decimal_fields = {
        "price_usd": "price",
        "native_token_price": "nativeTokenPrice",
        # Token Dynamic live responses include marketCap. Preserve it only
        # when the field is present and parseable; missing stays unavailable.
        "market_cap_usd": "marketCap",
        "liquidity_usd": "liquidity",
    }
    for window in ("5m", "1h", "4h", "24h"):
        decimal_fields.update(
            {
                f"volume_usd_{window}": f"volume{window}",
                f"buy_amount_usd_{window}": f"volume{window}Buy",
                f"sell_amount_usd_{window}": f"volume{window}Sell",
                f"net_buy_usd_{window}": f"volume{window}NetBuy",
                f"binance_volume_usd_{window}": f"volume{window}Binance",
                f"binance_net_buy_usd_{window}": f"volume{window}NetBinance",
            }
        )
    fields: dict[str, ObservedField] = {}
    for normalized_name, source_field in decimal_fields.items():
        fields[normalized_name] = _field(
            row,
            normalized_name=normalized_name,
            source_field=source_field,
            observed_at=fetched_at,
            source_timestamp=None,
            endpoint_type="token_dynamic",
            decimal_value=True,
        )
    fields["holders"] = _field(
        row,
        normalized_name="holders",
        source_field="holders",
        observed_at=fetched_at,
        source_timestamp=None,
        endpoint_type="token_dynamic",
        integer_value=True,
    )
    return BinanceMarketSnapshot(
        mint=mint,
        chain_id=chain_id,
        observed_at=fetched_at,
        source_timestamp=None,
        raw_response_hash=schema_hash(row),
        fields=fields,
    )
