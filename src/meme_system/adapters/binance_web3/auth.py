"""Authentication boundary for public Binance Web3 endpoints.

The official read-only skill implementation uses a documented User-Agent and
does not add an API-key header. This module intentionally does not consume
Jupiter, wallet, browser, Cookie, or Session credentials.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from meme_system.adapters.binance_web3.errors import UnsupportedAuthMethod


@dataclass(frozen=True)
class BinanceWeb3Auth:
    auth_mode: str = "none"
    credentials_configured: bool = False
    user_agent: str = "binance-web3/2.0 (Skill)"

    @classmethod
    def from_env(cls) -> "BinanceWeb3Auth":
        requested_mode = os.environ.get("BINANCE_WEB3_AUTH_MODE", "none").strip().lower()
        if requested_mode not in {"", "none"}:
            raise UnsupportedAuthMethod(requested_mode)
        return cls(auth_mode="none", credentials_configured=False)

    def headers(self) -> dict[str, str]:
        return {
            "Accept-Encoding": "identity",
            "User-Agent": self.user_agent,
        }

    def safe_status(self) -> dict[str, object]:
        return {
            "credentials_configured": self.credentials_configured,
            "auth_mode": self.auth_mode,
        }
