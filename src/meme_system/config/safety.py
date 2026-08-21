"""Fail-closed capability switches for Paper/Shadow and BSC Live."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


class SafetyViolation(RuntimeError):
    """Raised when a dangerous capability is enabled or malformed."""


@dataclass(frozen=True)
class SafetyConfig:
    paper_only: bool = True
    live_trading: bool = False
    wallet_enabled: bool = False
    signing_enabled: bool = False
    broadcast_enabled: bool = False
    telegram_enabled: bool = False
    bsc_live_enabled: bool = False

    @classmethod
    def from_mapping(cls, values: Mapping[str, str]) -> "SafetyConfig":
        def read_bool(name: str, default: str) -> bool:
            raw = values.get(name, default).strip().lower()
            if raw not in {"true", "false"}:
                raise SafetyViolation(f"{name} must be true or false")
            return raw == "true"

        config = cls(
            paper_only=read_bool("PAPER_ONLY", "true"),
            live_trading=read_bool("LIVE_TRADING", "false"),
            wallet_enabled=read_bool("WALLET_ENABLED", "false"),
            signing_enabled=read_bool("SIGNING_ENABLED", "false"),
            broadcast_enabled=read_bool("BROADCAST_ENABLED", "false"),
            telegram_enabled=read_bool("TELEGRAM_ENABLED", "false"),
            bsc_live_enabled=read_bool("BSC_LIVE_ENABLED", "false"),
        )
        config.validate()
        return config

    @classmethod
    def from_env(cls) -> "SafetyConfig":
        return cls.from_mapping(os.environ)

    def validate(self) -> None:
        if self.paper_only and self.live_trading:
            raise SafetyViolation(
                "PAPER_ONLY=true conflicts with LIVE_TRADING=true; "
                "set PAPER_ONLY=false for BSC Live"
            )
        if self.paper_only and self.bsc_live_enabled:
            raise SafetyViolation(
                "PAPER_ONLY=true conflicts with BSC_LIVE_ENABLED=true; "
                "set PAPER_ONLY=false for BSC Live"
            )

        invalid: list[str] = []
        if self.bsc_live_enabled != self.live_trading:
            invalid.extend(("LIVE_TRADING", "BSC_LIVE_ENABLED"))
        if not self.bsc_live_enabled:
            if not self.paper_only:
                invalid.append("PAPER_ONLY")
            for name, enabled in {
                "WALLET_ENABLED": self.wallet_enabled,
                "SIGNING_ENABLED": self.signing_enabled,
                "BROADCAST_ENABLED": self.broadcast_enabled,
            }.items():
                if enabled:
                    invalid.append(name)
        if invalid:
            raise SafetyViolation(
                "invalid execution capability combination: " + ", ".join(dict.fromkeys(invalid))
            )

    def validate_for_mode(self, *, chain: str, mode: str) -> None:
        """Reject capability/mode mismatches before any network or wallet work."""

        if mode == "live":
            if chain != "bsc":
                raise SafetyViolation("live mode is available only for BSC")
            if self.paper_only:
                raise SafetyViolation(
                    "BSC Live requires PAPER_ONLY=false; "
                    "PAPER_ONLY=true cannot run with LIVE_TRADING=true"
                )
            if not self.live_trading or not self.bsc_live_enabled:
                raise SafetyViolation("LIVE_TRADING=true and BSC_LIVE_ENABLED=true are required for BSC Live")
            return
        if mode not in {"paper", "shadow", "both"}:
            raise SafetyViolation(f"unsupported runtime mode: {mode}")
        if self.live_trading or self.bsc_live_enabled:
            raise SafetyViolation("BSC Live switches require --chain bsc --mode live")
        if not self.paper_only or self.wallet_enabled or self.signing_enabled or self.broadcast_enabled:
            raise SafetyViolation("Paper/Shadow require PAPER_ONLY=true and execution capabilities disabled")
        if chain == "solana":
            sol_signing = os.environ.get("SOL_SIGNING_ENABLED", "false").strip().lower()
            sol_broadcast = os.environ.get("SOL_BROADCAST_ENABLED", "false").strip().lower()
            allow_live = os.environ.get("ALLOW_LIVE_TRADING", "false").strip().lower()
            if sol_signing != "false" or sol_broadcast != "false" or allow_live != "false":
                raise SafetyViolation(
                    "Solana Paper requires ALLOW_LIVE_TRADING=false, "
                    "SOL_SIGNING_ENABLED=false and SOL_BROADCAST_ENABLED=false"
                )
