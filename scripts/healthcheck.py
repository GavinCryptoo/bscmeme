#!/usr/bin/env python3
"""Check the local Dashboard health endpoint and report safe JSON."""

from __future__ import annotations

import json
import os
from urllib.request import urlopen


def main() -> int:
    url = f"http://{os.environ.get('DASHBOARD_HOST', '127.0.0.1')}:{os.environ.get('DASHBOARD_PORT', '8788')}/api/health"
    try:
        with urlopen(url, timeout=3) as response:
            payload = json.loads(response.read().decode("utf-8"))
        print(json.dumps(payload, ensure_ascii=False, default=str))
        return 0
    except Exception as exc:
        print(json.dumps({"status": "unavailable", "error_class": type(exc).__name__}, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

