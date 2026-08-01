"""Fail-closed safety configuration for the non-live MVP."""

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
        )
        config.validate()
        return config

    @classmethod
    def from_env(cls) -> "SafetyConfig":
        return cls.from_mapping(os.environ)

    def validate(self) -> None:
        required_safe_values = {
            "PAPER_ONLY": self.paper_only,
            "LIVE_TRADING": not self.live_trading,
            "WALLET_ENABLED": not self.wallet_enabled,
            "SIGNING_ENABLED": not self.signing_enabled,
            "BROADCAST_ENABLED": not self.broadcast_enabled,
        }
        invalid = [name for name, safe in required_safe_values.items() if not safe]
        if invalid:
            raise SafetyViolation(
                "Gate A permits only Paper/Shadow with all execution capabilities disabled: "
                + ", ".join(invalid)
            )
