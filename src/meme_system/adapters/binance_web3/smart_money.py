"""Optional Smart Money read-only adapter; never an entry trigger."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping

from meme_system.adapters.binance_web3.client import BinanceWeb3Client
from meme_system.adapters.binance_web3.errors import BinanceWeb3Error, ErrorContext
from meme_system.adapters.binance_web3.models import BinanceSmartMoneyRecord
from meme_system.adapters.binance_web3.normalizer import normalize_smart_money_row
from meme_system.adapters.binance_web3.signal_source import _rows


class BinanceWeb3SmartMoneyAdapter:
    trigger_entry = False
    shadow_feature_only = True

    def __init__(self, client: BinanceWeb3Client, *, chain_id: str = "CT_501") -> None:
        if chain_id != "CT_501":
            raise BinanceWeb3Error(
                "only Solana CT_501 is enabled in this phase",
                context=ErrorContext("binance_unsupported_chain", "smart_money"),
            )
        self.client = client
        self.chain_id = chain_id

    def fetch(self, *, page: int = 1, page_size: int = 50) -> tuple[BinanceSmartMoneyRecord, ...]:
        if page < 1 or page_size < 1 or page_size > 100:
            raise ValueError("page must be positive and page_size must be between 1 and 100")
        fetched_at = datetime.now(timezone.utc)
        response = self.client.request_json(
            "smart_money",
            body={"chainId": self.chain_id, "page": page, "pageSize": page_size},
        )
        rows = _rows(response.payload, "smart_money")
        result: list[BinanceSmartMoneyRecord] = []
        for row in rows:
            normalized = normalize_smart_money_row(row, fetched_at=fetched_at, historical_bootstrap=True)
            result.append(
                BinanceSmartMoneyRecord(
                    normalized=normalized,
                    direction=row.get("direction") if isinstance(row.get("direction"), str) else None,
                    smart_money_count=int(row["smartMoneyCount"]) if row.get("smartMoneyCount") is not None else None,
                    exit_rate=int(row["exitRate"]) if row.get("exitRate") is not None else None,
                    max_gain=Decimal(str(row["maxGain"])) if row.get("maxGain") is not None else None,
                )
            )
        return tuple(result)
