#!/usr/bin/env python3
"""Build a BT-007 contract from JSON and certify a materialized Parquet frame."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from bt.experiments.representation_contract import (
    EvaluationSplit,
    FieldContract,
    RepresentationContract,
    certify_representation_frame,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--frame", type=Path, required=True)
    parser.add_argument("--manifest-output", type=Path, required=True)
    parser.add_argument("--report-output", type=Path, required=True)
    args = parser.parse_args()
    raw = json.loads(args.contract.read_text(encoding="utf-8"))
    contract = RepresentationContract(
        **{
            **raw,
            "fields": tuple(FieldContract(**item) for item in raw["fields"]),
            "split": EvaluationSplit(**raw["split"]),
        }
    )
    report = certify_representation_frame(contract, pd.read_parquet(args.frame))
    for path, document in (
        (args.manifest_output, contract.document()),
        (args.report_output, report),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"representation_digest": contract.digest, "report": report}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
