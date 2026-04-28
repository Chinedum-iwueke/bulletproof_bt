#!/usr/bin/env python3
"""Unified post-run summary + diagnostics CLI."""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from bt.analytics.postmortem import run_postmortem_for_experiment
from bt.analytics.run_summary import summarize_experiment_runs
import pandas as pd
from bt.analysis.ev_by_bucket import run_structural_bucket_analysis


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate post-run run_summary + diagnostics artifacts.")
    parser.add_argument("--experiment-root", required=True)
    parser.add_argument("--runs-glob", default="runs/*")
    parser.add_argument("--completed-only", action="store_true", default=False)
    parser.add_argument("--include-diagnostics", action="store_true", default=False)
    parser.add_argument("--skip-existing", action="store_true", default=False)
    parser.add_argument("--enable-structural-buckets", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--bucket-min-trades", type=int, default=10)
    parser.add_argument("--bucket-output-prefix", default="summaries")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    root = Path(args.experiment_root)
    summary_path = root / "summaries" / "run_summary.csv"

    if args.skip_existing and summary_path.exists():
        print(f"Skipping summary generation because --skip-existing was set and file exists: {summary_path}")
    else:
        summary_df, warnings_df = summarize_experiment_runs(
            root,
            runs_glob=args.runs_glob,
            completed_only=args.completed_only,
        )
        print(f"Wrote run summary rows={len(summary_df)} warnings={len(warnings_df)} -> {summary_path}")

    if args.include_diagnostics:
        outputs = run_postmortem_for_experiment(root)
        print(f"Diagnostics outputs generated={len(outputs)}")
    if args.enable_structural_buckets:
        trades_path = root / "research_data" / "trades_dataset.parquet"
        if trades_path.exists():
            trades_df = pd.read_parquet(trades_path)
        else:
            rows = []
            for p in root.glob("runs/*/trades.csv"):
                try:
                    rows.append(pd.read_csv(p))
                except Exception:
                    continue
            trades_df = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
        if trades_df.empty:
            print("WARNING: Structural bucket analysis skipped because no trade-level files were found.")
        else:
            outputs = run_structural_bucket_analysis(
                trades_df=trades_df,
                output_dir=root / args.bucket_output_prefix,
                min_trades=args.bucket_min_trades,
            )
            if "ev_by_bucket_missing_fields" in outputs:
                print(
                    "WARNING: Structural bucket analysis partially skipped due to missing entry_state_* columns. "
                    f"See {outputs['ev_by_bucket_missing_fields']}"
                )


if __name__ == "__main__":
    main()
