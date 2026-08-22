#!/usr/bin/env python3
"""Build or replay one bounded immutable dataset snapshot manifest."""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from bt.research_data.dataset_contract import (
    SnapshotRequest,
    build_snapshot_manifest,
    validate_snapshot_manifest,
    write_manifest,
)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    commands = result.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build")
    build.add_argument("--source-root", type=Path, required=True)
    build.add_argument("--partition", type=Path, action="append", required=True)
    build.add_argument("--membership", type=Path, required=True)
    build.add_argument("--output", type=Path, required=True)
    build.add_argument("--dataset-family", default="ohlcv")
    build.add_argument("--source", required=True)
    build.add_argument("--market", choices=("perp", "spot"), required=True)
    build.add_argument("--exchange", required=True)
    build.add_argument("--timeframe", required=True)
    build.add_argument("--timestamp-semantics", choices=("bar_open", "bar_close"), required=True)
    build.add_argument("--availability-lag-seconds", type=int, default=0)
    build.add_argument("--knowledge-cutoff", required=True)
    replay = commands.add_parser("validate")
    replay.add_argument("--manifest", type=Path, required=True)
    replay.add_argument("--source-root", type=Path)
    return result


def main() -> int:
    args = parser().parse_args()
    if args.command == "validate":
        document = json.loads(args.manifest.read_text(encoding="utf-8"))
        print(json.dumps(validate_snapshot_manifest(document, source_root=args.source_root), indent=2, sort_keys=True))
        return 0
    membership = json.loads(args.membership.read_text(encoding="utf-8"))
    request = SnapshotRequest(
        dataset_family=args.dataset_family,
        source=args.source,
        market=args.market,
        exchange=args.exchange,
        timeframe=args.timeframe,
        timestamp_semantics=args.timestamp_semantics,
        availability_lag_seconds=args.availability_lag_seconds,
        knowledge_cutoff=datetime.fromisoformat(args.knowledge_cutoff.replace("Z", "+00:00")),
    )
    manifest = build_snapshot_manifest(
        args.partition,
        source_root=args.source_root,
        request=request,
        membership=membership,
    )
    write_manifest(manifest, args.output)
    print(json.dumps(validate_snapshot_manifest(manifest, source_root=args.source_root), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
