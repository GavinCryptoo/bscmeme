#!/usr/bin/env python3
"""Export one isolated Paper or Shadow ledger to CSV and optional Parquet."""

from __future__ import annotations

import argparse
import json

from meme_system.archive import export_mode
from meme_system.config.runtime import RuntimePaths


def main() -> int:
    parser = argparse.ArgumentParser(description="Export Gate A runtime data")
    parser.add_argument("--mode", choices=("paper", "shadow"), required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--parquet", action="store_true")
    args = parser.parse_args()
    paths = RuntimePaths.from_env()
    db_path = paths.paper_db if args.mode == "paper" else paths.shadow_db
    output = export_mode(db_path, __import__("pathlib").Path(args.output), mode=args.mode, write_parquet=args.parquet)
    print(json.dumps(output, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

