#!/usr/bin/env python3
"""Admit only a preregistered set of manifest-visible canonical panels."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import tempfile

from bt.institutional.alpha import real_data_admission_receipt


def build_batch(assignment: dict) -> dict:
    data_root = Path(assignment["data_root"])
    source_commit = assignment["source_commit"]
    backup_root = Path(assignment["backup_root"])
    assets = assignment["assets"]
    if not 1 <= len(assets) <= 20:
        raise ValueError("selected-panel admission requires 1-20 assets")
    identities = [
        (item["venue"], item["instrument"], item.get("timeframe", "1m"))
        for item in assets
    ]
    if len(identities) != len(set(identities)):
        raise ValueError("selected-panel admission assets must be unique")
    receipts = []
    for venue, instrument, timeframe in identities:
        panel = (
            data_root
            / "canonical"
            / "perp"
            / venue
            / instrument
            / f"timeframe={timeframe}"
            / "research_panel.parquet"
        )
        receipts.append(
            real_data_admission_receipt(
                data_root=data_root,
                panel_path=panel,
                venue=venue,
                instrument=instrument,
                timeframe=timeframe,
                source_commit=source_commit,
                backup_root=backup_root,
            ).as_dict()
        )
    return {
        "schema_version": "alpha007-selected-panel-admission-batch-v1.0.0",
        "candidate_id": assignment["candidate_id"],
        "candidate_digest": assignment["candidate_digest"],
        "catalog_receipt_digest": assignment["catalog_receipt_digest"],
        "receipts": receipts,
        "capital_or_order_authority": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--assignment", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    assignment = json.loads(args.assignment.read_text(encoding="utf-8"))
    document = build_batch(assignment)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=args.output.parent, delete=False
    ) as stream:
        json.dump(document, stream, indent=2, sort_keys=True)
        stream.write("\n")
        temporary = Path(stream.name)
    temporary.chmod(0o600)
    temporary.replace(args.output)
    print(json.dumps(document, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
