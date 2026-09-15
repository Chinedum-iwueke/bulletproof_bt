#!/usr/bin/env python3
"""Measure a frozen research window over locally retained inventory shards."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import time

from bt.institutional.lake_inventory import _open_beneath
from bt.institutional.lake_quality import full_lake_quality_receipt
from bt.institutional.receipt import ProducerReceipt
from inventory_full_lake import write_receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--inventory", required=True, type=Path)
    parser.add_argument("--window-start", required=True)
    parser.add_argument("--window-end", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    inventory = ProducerReceipt(**json.loads(args.inventory.read_text(encoding="utf-8")))
    last_report = 0.0

    def load_shard(descriptor):
        name = f"shard-{descriptor['shard_index']:06d}.json"
        with _open_beneath(args.inventory.parent.resolve(strict=True), Path(name)) as handle:
            return ProducerReceipt(**json.load(handle))

    def progress(current, partition_id):
        nonlocal last_report
        if time.monotonic() - last_report >= 5:
            print(json.dumps({"event": "lake_quality_progress", "panels_completed": current,
                              "partition_id": partition_id}), flush=True)
            last_report = time.monotonic()

    receipt = full_lake_quality_receipt(
        data_root=args.data_root, inventory=inventory,
        window_start=args.window_start, window_end=args.window_end,
        source_commit=args.source_commit, load_shard=load_shard, progress=progress,
    )
    write_receipt(receipt, args.output)
    print(json.dumps({"event": "lake_quality_complete", "receipt_digest": receipt.receipt_digest,
                      "dispositions": receipt.result["dispositions"],
                      "unprocessed_dispositions": receipt.result["unprocessed_dispositions"],
                      "execution_authority": False}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
