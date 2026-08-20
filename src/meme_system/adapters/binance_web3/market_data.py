"""Token Dynamic read-only adapter."""

from __future__ import annotations

import time
from threading import Event, Lock
from typing import Any, Mapping

from meme_system.adapters.binance_web3.client import BinanceWeb3Client
from meme_system.adapters.binance_web3.errors import BinanceWeb3Error, ErrorContext
from meme_system.adapters.binance_web3.models import BinanceMarketSnapshot
from meme_system.adapters.binance_web3.normalizer import normalize_dynamic


class BinanceWeb3MarketDataAdapter:
    def __init__(self, client: BinanceWeb3Client, *, chain_id: str = "CT_501") -> None:
        if chain_id not in {"CT_501", "56"}:
            raise BinanceWeb3Error(
                "only Solana CT_501 and BSC 56 are enabled in this phase",
                context=ErrorContext("binance_unsupported_chain", "token_dynamic"),
            )
        self.client = client
        self.chain_id = chain_id
        self._snapshot_lock = Lock()
        self._snapshot_cache: dict[str, tuple[BinanceMarketSnapshot, float]] = {}
        self._snapshot_errors: dict[str, tuple[Exception, float]] = {}
        self._snapshot_inflight: dict[str, Event] = {}
        self._snapshot_cache_ttl_sec = 4.0
        self._snapshot_error_ttl_sec = 1.0

    def snapshot(self, mint: str) -> BinanceMarketSnapshot:
        if not mint:
            raise ValueError("mint must not be empty")
        key = mint.lower()
        now = time.monotonic()
        with self._snapshot_lock:
            cached = self._snapshot_cache.get(key)
            if cached is not None and now - cached[1] < self._snapshot_cache_ttl_sec:
                return cached[0]
            error = self._snapshot_errors.get(key)
            if error is not None and now - error[1] < self._snapshot_error_ttl_sec:
                raise error[0]
            waiter = self._snapshot_inflight.get(key)
            leader = waiter is None
            if leader:
                waiter = Event()
                self._snapshot_inflight[key] = waiter
        if not leader:
            if not waiter.wait(timeout=12.0):
                raise BinanceWeb3Error(
                    "Binance token dynamic request remained in flight",
                    context=ErrorContext("binance_inflight_timeout", "token_dynamic"),
                )
            with self._snapshot_lock:
                cached = self._snapshot_cache.get(key)
                if cached is not None and time.monotonic() - cached[1] < self._snapshot_cache_ttl_sec:
                    return cached[0]
                error = self._snapshot_errors.get(key)
                if error is not None:
                    raise error[0]
            raise BinanceWeb3Error(
                "Binance token dynamic request returned no snapshot",
                context=ErrorContext("binance_inflight_empty", "token_dynamic"),
            )
        try:
            response = self.client.request_json(
                "token_dynamic", params={"chainId": self.chain_id, "contractAddress": mint}
            )
            if response.payload.get("code") != "000000":
                raise BinanceWeb3Error(
                    "Binance Web3 dynamic response was not successful",
                    context=ErrorContext("binance_business_error", "token_dynamic", response.request_id),
                )
            data = response.payload.get("data")
            if not isinstance(data, Mapping):
                raise BinanceWeb3Error(
                    "Binance Web3 dynamic data is not an object",
                    context=ErrorContext("binance_schema_changed", "token_dynamic", response.request_id),
                )
            from datetime import datetime, timezone

            snapshot = normalize_dynamic(
                data,
                mint=mint,
                chain_id=self.chain_id,
                fetched_at=datetime.now(timezone.utc),
            )
        except Exception as exc:
            with self._snapshot_lock:
                self._snapshot_errors[key] = (exc, time.monotonic())
            raise
        finally:
            with self._snapshot_lock:
                event = self._snapshot_inflight.pop(key, None)
                if event is not None:
                    event.set()
        with self._snapshot_lock:
            self._snapshot_cache[key] = (snapshot, time.monotonic())
            self._snapshot_errors.pop(key, None)
        return snapshot
