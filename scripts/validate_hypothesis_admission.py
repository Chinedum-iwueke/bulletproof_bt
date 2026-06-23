#!/usr/bin/env python3
"""Validate a hypothesis/strategy package before expensive research begins."""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from bt.validation.strategy_admission import (  # noqa: E402
    validate_hypothesis_admission,
    write_strategy_admission_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hypothesis", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--allow-legacy-without-truth-contract", action="store_true")
    args = parser.parse_args()
    report = validate_hypothesis_admission(
        args.hypothesis,
        require_truth_contract=not args.allow_legacy_without_truth_contract,
    )
    output = write_strategy_admission_report(report, args.output)
    print(f"strategy_admission status={report.status} report={output}")
    for issue in report.issues:
        print(f"{issue.severity}: {issue.check}: {issue.message}")
    return 0 if report.status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
