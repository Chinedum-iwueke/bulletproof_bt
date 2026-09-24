#!/usr/bin/env python3
"""Replay an ALPHA-009 certification suite from immutable local inputs."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path

import pandas as pd

from bt.institutional.adaptive_representation_certification import (
    adaptive_representation_certification_receipt,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", required=True, type=Path)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    payload = json.loads(args.suite.read_text(encoding="utf-8"))
    cases = []
    for item in payload["cases"]:
        cases.append(
            {
                "case_key": item["case_key"],
                "plan": item["plan"],
                "panels": {
                    instrument: pd.read_parquet(path)
                    for instrument, path in item["panel_paths"].items()
                },
                "dataset_digests": item["dataset_digests"],
                "strategy_execution_semantics": item[
                    "strategy_execution_semantics"
                ],
                "terminal_evidence": item.get("terminal_evidence"),
            }
        )
    receipt = adaptive_representation_certification_receipt(
        cases=cases, source_commit=args.source_commit
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
    print(
        json.dumps(
            {
                "status": receipt["result"]["status"],
                "receipt_digest": receipt["receipt_digest"],
                "capability_checks": receipt["result"]["capability_checks"],
                "capital_or_order_authority": False,
            },
            sort_keys=True,
        )
    )
    return 0 if receipt["result"]["status"] == "qualified" else 2


if __name__ == "__main__":
    raise SystemExit(main())
