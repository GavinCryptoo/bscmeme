"""Strict Kline adapter for the documented Solana mapping."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from meme_system.adapters.binance_web3.client import BinanceWeb3Client
from meme_system.adapters.binance_web3.errors import BinanceWeb3Error, ErrorContext
from meme_system.adapters.binance_web3.models import BinanceKline, BinanceKlineResult
from meme_system.adapters.binance_web3.normalizer import parse_decimal, parse_integer
from meme_system.adapters.binance_web3.redaction import schema_hash


SUPPORTED_INTERVALS = frozenset(
    {"1s", "1min", "3min", "5min", "15min", "30min", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1m"}
)


class BinanceWeb3KlineAdapter:
    def __init__(self, client: BinanceWeb3Client, *, chain_id: str = "CT_501") -> None:
        if chain_id != "CT_501":
            raise BinanceWeb3Error(
                "only Solana CT_501 is enabled in this phase",
                context=ErrorContext("binance_unsupported_chain", "kline"),
            )
        self.client = client
        self.chain_id = chain_id

    def candles(
        self,
        mint: str,
        *,
        interval: str = "1min",
        limit: int = 100,
        from_ms: int | None = None,
        to_ms: int | None = None,
        price_mode: str = "p",
    ) -> BinanceKlineResult:
        if interval not in SUPPORTED_INTERVALS:
            raise ValueError(f"unsupported interval: {interval}")
        if not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500")
        if price_mode not in {"p", "m"}:
            raise ValueError("price_mode must be p or m")
        params = {
            "platform": "solana",
            "address": mint,
            "interval": interval,
            "limit": limit,
            "from": from_ms,
            "to": to_ms,
            "pm": price_mode,
        }
        response = self.client.request_json("kline", params=params)
        data = response.payload.get("data")
        if not isinstance(data, list):
            raise BinanceWeb3Error(
                "Binance Kline data is not a list",
                context=ErrorContext("binance_schema_changed", "kline", response.request_id),
            )
        status = response.payload.get("status")
        if isinstance(status, Mapping) and status.get("error_code") not in {None, 0, "0", "000000"}:
            raise BinanceWeb3Error(
                "Binance Kline business response was not successful",
                context=ErrorContext("binance_business_error", "kline", response.request_id),
            )
        candles: list[BinanceKline] = []
        timestamps: set[int] = set()
        for row in data:
            if not isinstance(row, list) or len(row) != 7:
                raise BinanceWeb3Error(
                    "Kline row must contain exactly seven values",
                    context=ErrorContext("binance_schema_changed", "kline", response.request_id),
                )
            try:
                open_time_ms = parse_integer(row[5], endpoint_type="kline", field_name="timestamp_ms")
                if open_time_ms < 100_000_000_000:
                    raise ValueError("timestamp is not milliseconds")
                candle = BinanceKline(
                    open=parse_decimal(row[0], endpoint_type="kline", field_name="open"),
                    high=parse_decimal(row[1], endpoint_type="kline", field_name="high"),
                    low=parse_decimal(row[2], endpoint_type="kline", field_name="low"),
                    close=parse_decimal(row[3], endpoint_type="kline", field_name="close"),
                    volume=parse_decimal(row[4], endpoint_type="kline", field_name="volume"),
                    open_time_ms=open_time_ms,
                    trade_count=parse_integer(row[6], endpoint_type="kline", field_name="trade_count"),
                )
            except (BinanceWeb3Error, ValueError, InvalidOperation) as exc:
                if isinstance(exc, BinanceWeb3Error):
                    raise exc
                raise BinanceWeb3Error(
                    "invalid Kline row",
                    context=ErrorContext("binance_schema_changed", "kline", response.request_id),
                ) from exc
            if candle.open_time_ms in timestamps:
                raise BinanceWeb3Error(
                    "duplicate Kline timestamp",
                    context=ErrorContext("binance_kline_duplicate", "kline", response.request_id),
                )
            timestamps.add(candle.open_time_ms)
            candles.append(candle)
        candles.sort(key=lambda item: item.open_time_ms)
        return BinanceKlineResult(
            mint=mint,
            chain_id=self.chain_id,
            interval=interval,
            candles=tuple(candles),
            fetched_at=datetime.now(timezone.utc),
            raw_response_hash=schema_hash(response.payload),
            api_latency_ms=response.api_latency_ms,
        )
