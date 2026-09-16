#!/usr/bin/env python3
"""Account for the complete local research lake without granting execution."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
from pathlib import Path

from bt.institutional.lake_inventory import full_lake_inventory_receipt, sharded_lake_inventory_receipt
from bt.institutional.lake_inventory_checkpoint import InventoryCheckpoint


def write_receipt(receipt, output):
    output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=output.parent, delete=False) as handle:
            temporary = Path(handle.name)
            os.chmod(temporary, 0o600)
            json.dump(receipt.as_dict(), handle, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(output)
    finally:
        if temporary and temporary.exists():
            temporary.unlink()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--run-id", help="Enable bounded shards with this immutable operation binding")
    parser.add_argument("--shard-size", type=int, default=10000)
    parser.add_argument("--checkpoint", type=Path,
                        help="Private local metadata cache; not execution-integrity authority")
    args = parser.parse_args()
    last_report = 0.0

    def progress(current, partition_id):
        nonlocal last_report
        if time.monotonic() - last_report >= 5:
            print(json.dumps({"event": "lake_inventory_progress", "objects_completed": current,
                              "partition_id": partition_id}), flush=True)
            last_report = time.monotonic()

    if args.run_id:
        def emit_shard(receipt):
            output = args.output.parent / f"shard-{receipt.result['shard_index']:06d}.json"
            write_receipt(receipt, output)
            print(json.dumps({"event": "lake_inventory_shard_ready", "path": str(output),
                              "receipt_digest": receipt.receipt_digest}), flush=True)
        checkpoint = (InventoryCheckpoint(args.checkpoint, root=args.data_root)
                      if args.checkpoint else None)
        try:
            receipt = sharded_lake_inventory_receipt(
                data_root=args.data_root, source_commit=args.source_commit, run_id=args.run_id,
                emit_shard=emit_shard, shard_size=args.shard_size, progress=progress,
                checkpoint=checkpoint,
            )
        finally:
            if checkpoint:
                checkpoint.close()
    else:
        if args.checkpoint:
            parser.error("checkpoint recovery requires bounded --run-id shards")
        receipt = full_lake_inventory_receipt(
            data_root=args.data_root, source_commit=args.source_commit, progress=progress,
        )
    write_receipt(receipt, args.output)
    print(json.dumps({"event": "lake_inventory_complete", "receipt_digest": receipt.receipt_digest,
                      "object_count": receipt.result["object_count"],
                      "dispositions": receipt.result["dispositions"], "execution_authority": False}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
