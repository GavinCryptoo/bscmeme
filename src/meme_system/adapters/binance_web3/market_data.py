"""Token Dynamic read-only adapter."""

from __future__ import annotations

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

    def snapshot(self, mint: str) -> BinanceMarketSnapshot:
        if not mint:
            raise ValueError("mint must not be empty")
        response = self.client.request_json(
            "token_dynamic",
            params={"chainId": self.chain_id, "contractAddress": mint},
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

        return normalize_dynamic(
            data,
            mint=mint,
            chain_id=self.chain_id,
            fetched_at=datetime.now(timezone.utc),
        )
