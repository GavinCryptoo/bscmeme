"""Frozen runtime identity and isolated storage paths."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


@dataclass(frozen=True)
class RuntimePaths:
    paper_db: Path = Path("data/solana/paper/runtime.db")
    shadow_db: Path = Path("data/solana/shadow/runtime.db")
    paper_audit_log: Path = Path("data/solana/paper/events.jsonl")
    shadow_audit_log: Path = Path("data/solana/shadow/events.jsonl")
    control_file: Path = Path("data/solana/runtime_control.json")
    paper_health_file: Path = Path("data/solana/paper/health.json")
    shadow_health_file: Path = Path("data/solana/shadow/health.json")
    paper_archive_dir: Path = Path("data/solana/paper/archive")
    shadow_archive_dir: Path = Path("data/solana/shadow/archive")
    lock_dir: Path = Path("data/solana/locks")

    @classmethod
    def from_env(cls) -> "RuntimePaths":
        def path(name: str, default: str) -> Path:
            value = os.environ.get(name, default).strip()
            return Path(value or default)

        paper_db = path("PAPER_DB_PATH", str(cls.paper_db))
        shadow_db = path("SHADOW_DB_PATH", str(cls.shadow_db))
        return cls(
            paper_db=paper_db,
            shadow_db=shadow_db,
            paper_audit_log=path("PAPER_AUDIT_LOG_PATH", str(paper_db.parent / "events.jsonl")),
            shadow_audit_log=path("SHADOW_AUDIT_LOG_PATH", str(shadow_db.parent / "events.jsonl")),
            control_file=path("RUNTIME_CONTROL_PATH", "data/solana/runtime_control.json"),
            paper_health_file=path("PAPER_HEALTH_PATH", str(paper_db.parent / "health.json")),
            shadow_health_file=path("SHADOW_HEALTH_PATH", str(shadow_db.parent / "health.json")),
            paper_archive_dir=path("PAPER_ARCHIVE_DIR", str(paper_db.parent / "archive")),
            shadow_archive_dir=path("SHADOW_ARCHIVE_DIR", str(shadow_db.parent / "archive")),
            lock_dir=path("RUNTIME_LOCK_DIR", "data/solana/locks"),
        )

    def validate_isolation(self) -> None:
        pairs = (
            (self.paper_db, self.shadow_db, "databases"),
            (self.paper_audit_log, self.shadow_audit_log, "audit logs"),
            (self.paper_health_file, self.shadow_health_file, "health files"),
            (self.paper_archive_dir, self.shadow_archive_dir, "archive directories"),
        )
        for paper, shadow, label in pairs:
            if paper.resolve() == shadow.resolve():
                raise ValueError(f"Paper and Shadow {label} must be different")
