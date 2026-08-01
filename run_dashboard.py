#!/usr/bin/env python3
"""Start the local Gate A Dashboard explicitly."""

from __future__ import annotations

import json
import os
from pathlib import Path

from meme_system.config.dashboard import DashboardConfig
from meme_system.config.runtime import RuntimePaths
from meme_system.config.safety import SafetyConfig
from meme_system.dashboard_server import DashboardService, serve


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
    try:
        safety = SafetyConfig.from_env()
        config = DashboardConfig(
            host=os.environ.get("DASHBOARD_HOST", "127.0.0.1"),
            port=int(os.environ.get("DASHBOARD_PORT", "8788")),
        )
        service = DashboardService(paths=RuntimePaths.from_env(), config=config, safety=safety)
        print(json.dumps({"status": "starting", "host": config.host, "port": config.port, "read_only": True}, ensure_ascii=False), flush=True)
        serve(service)
        return 0
    except (ValueError, OSError) as exc:
        print(json.dumps({"status": "blocked", "error_class": type(exc).__name__, "message": str(exc)[:300]}, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

