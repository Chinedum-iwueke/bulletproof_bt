#!/usr/bin/env python3
"""Extract canonical research-ready datasets from a completed experiment root."""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))



def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convert completed experiment outputs into compact research_data parquet datasets."
    )
    parser.add_argument("--experiment-root", required=True)
    parser.add_argument("--runs-glob", default="runs/*")
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--skip-existing", action="store_true", default=False)
    parser.add_argument("--overwrite", action="store_true", default=False)
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument("--min-trades-per-run", type=int, default=1)
    parser.add_argument("--top-run-count-for-labels", type=int, default=5)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    from bt.experiments.dataset_builder import extract_experiment_dataset

    result = extract_experiment_dataset(
        experiment_root=Path(args.experiment_root),
        runs_glob=args.runs_glob,
        out_dir=Path(args.out_dir) if args.out_dir else None,
        skip_existing=args.skip_existing,
        overwrite=args.overwrite,
        verbose=args.verbose,
        min_trades_per_run=args.min_trades_per_run,
        top_run_count_for_labels=args.top_run_count_for_labels,
    )
    print(result)


if __name__ == "__main__":
    main()
