#!/usr/bin/env python3
"""Canonical cleanup step for post-experiment run artifacts."""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from bt.experiments.cleanup import CleanupConfig, run_experiment_cleanup


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Rank experiment runs, retain curated references, and prune heavyweight artifacts."
    )
    parser.add_argument("--experiment-root", required=True)
    parser.add_argument("--runs-glob", default="runs/*")
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--retain-top-n", type=int, default=5)
    parser.add_argument("--retain-median", type=int, default=1)
    parser.add_argument("--retain-worst", type=int, default=1)
    parser.add_argument("--ranking-metric", default="net_pnl")
    parser.add_argument("--delete-logs", action="store_true", default=False)
    parser.add_argument("--delete-nonretained-runs", action="store_true", default=False)
    parser.add_argument("--keep-equity-for-retained", action="store_true", default=False)
    parser.add_argument("--skip-existing-extraction", action="store_true", default=False)
    parser.add_argument("--overwrite-extraction", action="store_true", default=False)
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--verbose", action="store_true", default=False)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = CleanupConfig(
        experiment_root=Path(args.experiment_root),
        runs_glob=args.runs_glob,
        out_dir=Path(args.out_dir) if args.out_dir else None,
        retain_top_n=args.retain_top_n,
        retain_median=args.retain_median,
        retain_worst=args.retain_worst,
        ranking_metric=args.ranking_metric,
        delete_logs=args.delete_logs,
        delete_nonretained_runs=args.delete_nonretained_runs,
        keep_equity_for_retained=args.keep_equity_for_retained,
        skip_existing_extraction=args.skip_existing_extraction,
        overwrite_extraction=args.overwrite_extraction,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )
    print(run_experiment_cleanup(config))


if __name__ == "__main__":
    main()
