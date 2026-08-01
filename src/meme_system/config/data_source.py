"""Explicit data-source selection; fixture remains the safe default."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


SUPPORTED_DATA_SOURCES = frozenset({"fixture", "replay", "binance_web3"})


@dataclass(frozen=True)
class DataSourceConfig:
    data_source: str = "fixture"

    @classmethod
    def from_mapping(cls, values: Mapping[str, str]) -> "DataSourceConfig":
        source = values.get("DATA_SOURCE", "fixture").strip().lower()
        if source not in SUPPORTED_DATA_SOURCES:
            raise ValueError(f"DATA_SOURCE must be one of {sorted(SUPPORTED_DATA_SOURCES)}")
        return cls(data_source=source)

    @classmethod
    def from_env(cls) -> "DataSourceConfig":
        return cls.from_mapping(os.environ)
