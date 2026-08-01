#!/usr/bin/env python3
"""Explicit Gate A Paper runner wrapper."""

from __future__ import annotations

import sys

from run_realtime import run_with_lock


if __name__ == "__main__":
    raise SystemExit(run_with_lock(["--mode", "paper", *sys.argv[1:]]))

