"""Frozen runtime identity and isolated storage paths."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RuntimePaths:
    paper_db: Path = Path("data/solana/paper/runtime.db")
    shadow_db: Path = Path("data/solana/shadow/runtime.db")
    paper_audit_log: Path = Path("data/solana/paper/events.jsonl")
    shadow_audit_log: Path = Path("data/solana/shadow/events.jsonl")

    def validate_isolation(self) -> None:
        if self.paper_db.resolve() == self.shadow_db.resolve():
            raise ValueError("Paper and Shadow databases must be different")
        if self.paper_audit_log.resolve() == self.shadow_audit_log.resolve():
            raise ValueError("Paper and Shadow audit logs must be different")

