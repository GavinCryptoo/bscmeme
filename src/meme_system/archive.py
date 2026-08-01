"""Explicit, append-only runtime export helpers."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from meme_system.runtime_ops import export_rows_csv, export_rows_parquet
from meme_system.storage.database import initialize_database


def export_mode(db_path: Path, output_dir: Path, *, mode: str, write_parquet: bool = False) -> dict[str, object]:
    if mode not in {"paper", "shadow"}:
        raise ValueError("mode must be paper or shadow")
    output_dir.mkdir(parents=True, exist_ok=True)
    connection = initialize_database(db_path)
    try:
        tables = ("signals", "candidates", "virtual_positions", "executions", "lifecycle_events")
        if mode == "shadow":
            tables = (*tables, "shadow_outcomes")
        files: list[dict[str, object]] = []
        for table in tables:
            where = " WHERE mode = ?" if table not in {"signals", "shadow_outcomes"} else ""
            params = (mode,) if where else ()
            rows = [dict(row) for row in connection.execute(f"SELECT * FROM {table}{where} ORDER BY rowid", params)]
            path = output_dir / f"{table}.csv"
            export_rows_csv(path, rows)
            files.append({"table": table, "path": str(path), "rows": len(rows), "sha256": _sha256(path)})
            if write_parquet:
                parquet_path = output_dir / f"{table}.parquet"
                export_rows_parquet(parquet_path, rows)
                files.append({"table": table, "path": str(parquet_path), "rows": len(rows), "sha256": _sha256(parquet_path)})
        manifest = {
            "mode": mode,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "source_db": str(db_path),
            "files": files,
        }
        manifest_path = output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
        return {**manifest, "manifest_path": str(manifest_path)}
    finally:
        connection.close()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

