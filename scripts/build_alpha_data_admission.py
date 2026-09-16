#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import tempfile

from bt.institutional.alpha import real_data_admission_receipt


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build an immutable ALPHA-001 real-data admission receipt."
    )
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--panel", required=True, type=Path)
    parser.add_argument("--venue", required=True, choices=("bybit", "binance"))
    parser.add_argument("--instrument", required=True)
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--backup-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    receipt = real_data_admission_receipt(
        data_root=args.data_root,
        panel_path=args.panel,
        venue=args.venue,
        instrument=args.instrument.upper(),
        timeframe=args.timeframe,
        source_commit=args.source_commit,
        backup_root=args.backup_root,
    ).as_dict()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=args.output.parent, delete=False
    ) as handle:
        json.dump(receipt, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    temporary.replace(args.output)
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
