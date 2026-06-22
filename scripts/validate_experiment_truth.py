#!/usr/bin/env python3
"""Validate completed backtest artifacts before downstream research ingestion."""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from bt.validation.experiment_truth import validate_experiment_root, write_truth_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate experiment output truth invariants.")
    parser.add_argument("--experiment-root", required=True)
    parser.add_argument("--allow-incomplete", action="store_true", default=False)
    parser.add_argument(
        "--notional-tolerance-pct",
        type=float,
        default=0.005,
        help="Allowed floating/rounding tolerance above max_notional before failing.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.experiment_root)
    report = validate_experiment_root(
        root,
        allow_incomplete=args.allow_incomplete,
        notional_tolerance_pct=args.notional_tolerance_pct,
    )
    json_path, md_path = write_truth_report(report, root / "summaries")
    print(f"truth_validation status={report.status} hard_failures={report.hard_failures} warnings={report.warnings}")
    print(f"truth_validation_json={json_path}")
    print(f"truth_validation_md={md_path}")
    return 0 if report.status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
