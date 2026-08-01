#!/usr/bin/env python3
"""Print safe local runtime status without touching execution paths."""

from __future__ import annotations

import json
import os
from pathlib import Path
from urllib.request import urlopen


def main() -> int:
    host = os.environ.get("DASHBOARD_HOST", "127.0.0.1")
    port = os.environ.get("DASHBOARD_PORT", "8788")
    try:
        with urlopen(f"http://{host}:{port}/api/status", timeout=3) as response:
            print(response.read().decode("utf-8"))
        return 0
    except Exception as exc:
        print(json.dumps({"status": "unavailable", "error_class": type(exc).__name__}, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

