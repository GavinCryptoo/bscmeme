"""Frozen runtime identity and isolated storage paths."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


@dataclass(frozen=True)
class RuntimePaths:
    """Isolated database, audit, health, archive, and lock paths."""

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
    live_db: Path = Path("data/bsc/live/runtime.db")
    live_audit_log: Path = Path("data/bsc/live/events.jsonl")
    live_control_file: Path = Path("data/bsc/live/runtime_control.json")
    live_health_file: Path = Path("data/bsc/live/health.json")
    live_archive_dir: Path = Path("data/bsc/live/archive")
    live_lock_dir: Path = Path("data/bsc/live/locks")
    chain: str = "solana"

    @classmethod
    def from_env(cls, chain: str = "solana") -> "RuntimePaths":
        """Resolve chain-specific paths from environment overrides."""
        if chain not in {"solana", "bsc"}:
            raise ValueError("chain must be solana or bsc")

        def path(name: str, default: str) -> Path:
            """Resolve one environment path, falling back when it is blank."""
            value = os.environ.get(name, default).strip()
            return Path(value or default)

        if chain == "bsc":
            root = os.environ.get("BSC_DATA_DIR", "data/bsc").strip() or "data/bsc"
            paper_db_default = f"{root}/paper/runtime.db"
            shadow_db_default = f"{root}/shadow/runtime.db"
            paper_db = path("BSC_PAPER_DB_PATH", paper_db_default)
            shadow_db = path("BSC_SHADOW_DB_PATH", shadow_db_default)
            paper_audit_name = "BSC_PAPER_AUDIT_LOG_PATH"
            shadow_audit_name = "BSC_SHADOW_AUDIT_LOG_PATH"
            control_name = "BSC_RUNTIME_CONTROL_PATH"
            control_default = f"{root}/runtime_control.json"
            paper_health_name = "BSC_PAPER_HEALTH_PATH"
            shadow_health_name = "BSC_SHADOW_HEALTH_PATH"
            paper_archive_name = "BSC_PAPER_ARCHIVE_DIR"
            shadow_archive_name = "BSC_SHADOW_ARCHIVE_DIR"
            lock_name = "BSC_RUNTIME_LOCK_DIR"
            lock_default = f"{root}/locks"
            live_db = path("BSC_LIVE_DB_PATH", f"{root}/live/runtime.db")
            live_audit_name = "BSC_LIVE_AUDIT_LOG_PATH"
            live_control_name = "BSC_LIVE_CONTROL_PATH"
            live_health_name = "BSC_LIVE_HEALTH_PATH"
            live_archive_name = "BSC_LIVE_ARCHIVE_DIR"
            live_lock_name = "BSC_LIVE_LOCK_DIR"
        else:
            root = os.environ.get("SOL_SURVIVOR_DATA_DIR", "data/solana").strip() or "data/solana"
            paper_db = path("PAPER_DB_PATH", f"{root}/paper/runtime.db")
            shadow_db = path("SHADOW_DB_PATH", f"{root}/shadow/runtime.db")
            paper_audit_name = "PAPER_AUDIT_LOG_PATH"
            shadow_audit_name = "SHADOW_AUDIT_LOG_PATH"
            control_name = "RUNTIME_CONTROL_PATH"
            control_default = f"{root}/runtime_control.json"
            paper_health_name = "PAPER_HEALTH_PATH"
            shadow_health_name = "SHADOW_HEALTH_PATH"
            paper_archive_name = "PAPER_ARCHIVE_DIR"
            shadow_archive_name = "SHADOW_ARCHIVE_DIR"
            lock_name = "RUNTIME_LOCK_DIR"
            lock_default = f"{root}/locks"
            live_db = cls.live_db
            live_audit_name = "BSC_LIVE_AUDIT_LOG_PATH"
            live_control_name = "BSC_LIVE_CONTROL_PATH"
            live_health_name = "BSC_LIVE_HEALTH_PATH"
            live_archive_name = "BSC_LIVE_ARCHIVE_DIR"
            live_lock_name = "BSC_LIVE_LOCK_DIR"
        return cls(
            paper_db=paper_db,
            shadow_db=shadow_db,
            paper_audit_log=path(paper_audit_name, str(paper_db.parent / "events.jsonl")),
            shadow_audit_log=path(shadow_audit_name, str(shadow_db.parent / "events.jsonl")),
            control_file=path(control_name, control_default),
            paper_health_file=path(paper_health_name, str(paper_db.parent / "health.json")),
            shadow_health_file=path(shadow_health_name, str(shadow_db.parent / "health.json")),
            paper_archive_dir=path(paper_archive_name, str(paper_db.parent / "archive")),
            shadow_archive_dir=path(shadow_archive_name, str(shadow_db.parent / "archive")),
            lock_dir=path(lock_name, lock_default),
            live_db=live_db,
            live_audit_log=path(live_audit_name, str(live_db.parent / "events.jsonl")),
            live_control_file=path(live_control_name, str(live_db.parent / "runtime_control.json")),
            live_health_file=path(live_health_name, str(live_db.parent / "health.json")),
            live_archive_dir=path(live_archive_name, str(live_db.parent / "archive")),
            live_lock_dir=path(live_lock_name, str(live_db.parent / "locks")),
            chain=chain,
        )

    def validate_isolation(self) -> None:
        """Reject paths that would mix Paper, Shadow, or BSC Live state."""
        pairs = (
            (self.paper_db, self.shadow_db, "databases"),
            (self.paper_audit_log, self.shadow_audit_log, "audit logs"),
            (self.paper_health_file, self.shadow_health_file, "health files"),
            (self.paper_archive_dir, self.shadow_archive_dir, "archive directories"),
        )
        for paper, shadow, label in pairs:
            if paper.resolve() == shadow.resolve():
                raise ValueError(f"Paper and Shadow {label} must be different")
        if self.chain == "bsc":
            live_paths = (self.live_db, self.live_audit_log, self.live_health_file, self.live_archive_dir)
            for paper, live, label in zip(
                (self.paper_db, self.paper_audit_log, self.paper_health_file, self.paper_archive_dir),
                live_paths,
                ("database", "audit log", "health file", "archive directory"),
            ):
                if paper.resolve() == live.resolve():
                    raise ValueError(f"BSC Paper and Live {label} must be different")
            for shadow, live, label in zip(
                (self.shadow_db, self.shadow_audit_log, self.shadow_health_file, self.shadow_archive_dir),
                live_paths,
                ("database", "audit log", "health file", "archive directory"),
            ):
                if shadow.resolve() == live.resolve():
                    raise ValueError(f"BSC Shadow and Live {label} must be different")
