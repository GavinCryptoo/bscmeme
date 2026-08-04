"""Runtime controls, health, latency, locks, and append-only audit."""

from __future__ import annotations

import csv
import json
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from meme_system.adapters.binance_web3.redaction import redact_payload


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class RuntimeLockError(RuntimeError):
    pass


class SingleInstanceLock:
    """Exclusive lock file that preserves verified-stale lock evidence."""

    def __init__(self, path: Path, *, name: str) -> None:
        self.path = path
        self.name = name
        self._owned = False

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        metadata = {
            "pid": os.getpid(),
            "started_at": utc_now().isoformat(),
            "working_directory": str(Path.cwd()),
            "name": self.name,
        }
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError as exc:
            existing = "unavailable"
            existing_pid: int | None = None
            try:
                existing = self.path.read_text(encoding="utf-8")[:500]
                parsed = json.loads(existing)
                value = parsed.get("pid") if isinstance(parsed, Mapping) else None
                existing_pid = value if isinstance(value, int) and value > 0 else None
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                pass
            if existing_pid is not None and _pid_is_running(existing_pid):
                raise RuntimeLockError(f"{self.name} already running: {existing}") from exc
            stale_path = self.path.with_name(
                f"{self.path.name}.stale-{utc_now().strftime('%Y%m%dT%H%M%SZ')}"
            )
            try:
                self.path.replace(stale_path)
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except OSError as stale_exc:
                raise RuntimeLockError(f"{self.name} stale lock could not be preserved: {existing}") from stale_exc
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(metadata, handle, ensure_ascii=False, sort_keys=True)
        self._owned = True

    def release(self) -> None:
        if not self._owned:
            return
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass
        self._owned = False

    def __enter__(self) -> "SingleInstanceLock":
        self.acquire()
        return self

    def __exit__(self, *_: object) -> None:
        self.release()


def _pid_is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class JsonlAuditWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()

    def append(self, *, mode: str, event_type: str, occurred_at: datetime, payload: Mapping[str, object]) -> None:
        record = redact_payload(
            {
                "mode": mode,
                "event_type": event_type,
                "occurred_at": occurred_at.isoformat(),
                "payload": dict(payload),
            }
        )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock, self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True, default=str) + "\n")
            handle.flush()
            os.fsync(handle.fileno())


class RuntimeControl:
    """Persist new-entry pause flags without exposing execution controls."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._state = {
            "paper_new_entries_paused": False,
            "shadow_new_entries_paused": False,
            "live_new_entries_paused": False,
            "updated_at": None,
        }
        self._last_mtime_ns: int | None = None
        self._load()

    def _load(self) -> bool:
        """Load a complete control snapshot, retaining the last valid state on error."""
        try:
            loaded = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(loaded, Mapping):
                return False
            for key in (
                "paper_new_entries_paused",
                "shadow_new_entries_paused",
                "live_new_entries_paused",
            ):
                if isinstance(loaded.get(key), bool):
                    self._state[key] = loaded[key]
            self._state["updated_at"] = loaded.get("updated_at")
            self._last_mtime_ns = self.path.stat().st_mtime_ns
            return True
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            return False

    def _reload_if_changed_locked(self) -> None:
        try:
            mtime_ns = self.path.stat().st_mtime_ns
        except OSError:
            return
        if mtime_ns == self._last_mtime_ns:
            return
        # Atomic dashboard writes can briefly expose an incomplete file.  _load
        # intentionally leaves the prior valid snapshot intact in that case.
        self._load()

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            self._reload_if_changed_locked()
            return dict(self._state)

    def set_paused(self, mode: str, paused: bool) -> dict[str, object]:
        if mode not in {"paper", "shadow", "live"}:
            raise ValueError("mode must be paper, shadow or live")
        with self._lock:
            self._state[f"{mode}_new_entries_paused"] = bool(paused)
            self._state["updated_at"] = utc_now().isoformat()
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.path.with_suffix(self.path.suffix + ".tmp")
            temporary.write_text(json.dumps(self._state, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
            temporary.replace(self.path)
            try:
                self._last_mtime_ns = self.path.stat().st_mtime_ns
            except OSError:
                self._last_mtime_ns = None
            return dict(self._state)

    def paused(self, mode: str) -> bool:
        return bool(self.snapshot()[f"{mode}_new_entries_paused"])


@dataclass
class HealthRegistry:
    path: Path | None = None
    _items: dict[str, dict[str, object]] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def set(self, name: str, state: str, *, error_class: str | None = None, latency_ms: int | None = None, details: Mapping[str, object] | None = None) -> None:
        with self._lock:
            self._items[name] = {
                "name": name,
                "state": state,
                "error_class": error_class,
                "latency_ms": latency_ms,
                "updated_at": utc_now().isoformat(),
                "details": dict(details or {}),
            }
            self._write_locked()

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return {"updated_at": utc_now().isoformat(), "items": dict(self._items)}

    def _write_locked(self) -> None:
        if self.path is None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        payload = {"updated_at": utc_now().isoformat(), "items": dict(self._items)}
        temporary.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
        temporary.replace(self.path)


class LatencyRecorder:
    def __init__(self) -> None:
        self._values: dict[str, list[float]] = {}
        self._lock = threading.Lock()

    def record(self, stage: str, milliseconds: float) -> None:
        with self._lock:
            self._values.setdefault(stage, []).append(float(milliseconds))

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return {stage: _summary(values) for stage, values in self._values.items()}


def _summary(values: list[float]) -> dict[str, float | int]:
    if not values:
        return {"count": 0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
    ordered = sorted(values)
    def percentile(percent: float) -> float:
        index = min(len(ordered) - 1, int((len(ordered) - 1) * percent))
        return round(ordered[index], 3)
    return {"count": len(values), "p50": percentile(0.50), "p95": percentile(0.95), "p99": percentile(0.99), "max": round(max(values), 3)}


def export_rows_csv(path: Path, rows: list[Mapping[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_value(row.get(key)) for key in fieldnames})


def _csv_value(value: object) -> object:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return value


def export_rows_parquet(path: Path, rows: list[Mapping[str, object]]) -> None:
    """Use pyarrow when installed; never emit a fake Parquet file."""
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
    except ImportError as exc:
        raise RuntimeError("parquet_dependency_missing") from exc
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist([dict(row) for row in rows])
    pq.write_table(table, path)
