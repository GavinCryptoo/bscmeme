#!/usr/bin/env python3
"""Start the loopback-only BSC Balanced Live monitor."""

from __future__ import annotations

import os
from pathlib import Path

from meme_system.live_dashboard import LiveDashboardService, serve


def _load_env(path: Path = Path(".env")) -> None:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (FileNotFoundError, OSError):
        return
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        os.environ.setdefault(name.strip(), value.strip().strip("\"'"))


def main() -> int:
    _load_env()
    root = Path(os.environ.get("BSC_DATA_DIR", "data/bsc-balanced"))
    db_path = Path(os.environ.get("BSC_LIVE_DB_PATH", str(root / "live" / "runtime.db")))
    health_path = Path(os.environ.get("BSC_LIVE_HEALTH_PATH", str(root / "live" / "health.json")))
    service = LiveDashboardService(
        db_path=db_path,
        health_path=health_path,
        host="127.0.0.1",
        port=int(os.environ.get("BSC_LIVE_DASHBOARD_PORT", "8791")),
    )
    print(f"BSC Balanced Live Dashboard: http://127.0.0.1:{service.port}/", flush=True)
    serve(service)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
