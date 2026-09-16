#!/usr/bin/env python3
"""Build the fast manifest-first DATA-002 discovery catalog."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path

from bt.institutional.lake_manifest import manifest_catalog_receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument(
        "--venues", nargs="+", default=["binance", "bybit"],
        choices=["binance", "bybit"],
    )
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    receipt = manifest_catalog_receipt(
        data_root=args.data_root, source_commit=args.source_commit,
        venues=tuple(args.venues),
    ).as_dict()
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=args.output.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        os.chmod(temporary, 0o600)
        json.dump(receipt, handle, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(args.output)
    print(json.dumps({
        "receipt_digest": receipt["receipt_digest"],
        "assets": len(receipt["result"]["assets"]),
        "availability_records": receipt["result"]["availability_record_count"],
        "execution_authority": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
