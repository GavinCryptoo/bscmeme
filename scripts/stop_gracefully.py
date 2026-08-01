#!/usr/bin/env python3
"""Request only a safe stop through the process lock owner convention.

No force-kill or process-wide termination is performed. The runner should be
stopped with SIGINT/SIGTERM by the supervisor or terminal that owns it.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect Gate A lock metadata")
    parser.add_argument("--lock", type=Path, default=Path("data/solana/locks/realtime-both.lock"))
    parser.add_argument("--confirm", action="store_true", help="send SIGTERM only to the PID recorded in this lock")
    args = parser.parse_args()
    try:
        metadata = json.loads(args.lock.read_text(encoding="utf-8"))
        print(json.dumps({"status": "running", "lock": str(args.lock), "pid": metadata.get("pid"), "name": metadata.get("name")}, ensure_ascii=False))
        if args.confirm:
            pid = metadata.get("pid")
            if not isinstance(pid, int) or pid <= 1:
                print(json.dumps({"status": "blocked", "error_class": "invalid_lock_pid"}, ensure_ascii=False))
                return 2
            os.kill(pid, signal.SIGTERM)
            print(json.dumps({"status": "stop_requested", "pid": pid}, ensure_ascii=False))
        return 0
    except FileNotFoundError:
        print(json.dumps({"status": "not_running", "lock": str(args.lock)}, ensure_ascii=False))
        return 0
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(json.dumps({"status": "unavailable", "error_class": type(exc).__name__}, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
