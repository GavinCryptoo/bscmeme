#!/usr/bin/env python3
"""Explicit launcher for the isolated BSC balanced Paper strategy."""

from __future__ import annotations

import sys

from run_realtime import run_with_lock


if __name__ == "__main__":
    raise SystemExit(run_with_lock(["--chain", "bsc", "--mode", "paper", "--strategy-profile", "balanced", *sys.argv[1:]]))
