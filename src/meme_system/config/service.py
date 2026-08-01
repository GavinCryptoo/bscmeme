"""Versioned, whitelisted Paper/Shadow configuration service.

The service intentionally cannot write safety flags, wallet settings, live
settings, network endpoints, or execution settings. A configuration change is
an explicit JSON snapshot with an audit-friendly version and reason.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping


EDITABLE_KEYS = frozenset({
    "poll_interval_sec",
    "jupiter_quote_ttl_ms",
    "jupiter_slippage_bps",
    "dashboard_refresh_sec",
    "archive_after_days",
})
FORBIDDEN_KEYS = frozenset({
    "PAPER_ONLY",
    "LIVE_TRADING",
    "WALLET_ENABLED",
    "SIGNING_ENABLED",
    "BROADCAST_ENABLED",
    "TELEGRAM_ENABLED",
    "SOLANA_RPC_URL",
    "SOLANA_WS_URL",
    "JUPITER_API_KEY",
    "JUPITER_QUOTE_URL",
    "position_size_sol",
    "take_profit_pct",
    "stop_loss_trigger_pct",
})


@dataclass(frozen=True)
class ConfigVersion:
    version: str
    mode: str
    values: Mapping[str, object]
    changed_at: str
    reason: str


class ConfigService:
    def __init__(self, path: Path, *, mode: str) -> None:
        if mode not in {"paper", "shadow"}:
            raise ValueError("mode must be paper or shadow")
        self.path = path
        self.mode = mode

    def current(self) -> ConfigVersion:
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(payload, Mapping) and isinstance(payload.get("values"), Mapping):
                return ConfigVersion(
                    version=str(payload.get("version", "0.1.0")),
                    mode=self.mode,
                    values=dict(payload["values"]),
                    changed_at=str(payload.get("changed_at", "")),
                    reason=str(payload.get("reason", "initial")),
                )
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            pass
        return ConfigVersion("0.1.0", self.mode, {}, "", "initial")

    def update(self, values: Mapping[str, object], *, reason: str) -> ConfigVersion:
        unknown = set(values) - EDITABLE_KEYS
        forbidden = set(values) & FORBIDDEN_KEYS
        if unknown or forbidden:
            raise ValueError("only whitelisted non-safety monitoring fields may be edited")
        current = self.current()
        canonical = json.dumps(dict(values), ensure_ascii=False, sort_keys=True, default=str)
        version = "cfg-" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]
        changed = datetime.now(timezone.utc).isoformat()
        result = ConfigVersion(version, self.mode, dict(values), changed, reason[:200])
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary.write_text(json.dumps(result.__dict__, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
        temporary.replace(self.path)
        return result

