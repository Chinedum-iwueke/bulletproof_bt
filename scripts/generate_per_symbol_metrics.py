"""Generate per-symbol metric artifacts from a run's trades.csv."""
from __future__ import annotations

import argparse

from bt.metrics.per_symbol import write_per_symbol_metrics


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate per-symbol metrics from trades.csv")
    parser.add_argument("--run-dir", required=True, help="Run directory containing trades.csv")
    parser.add_argument("--out-dir-name", default="per_symbol", help="Output subdirectory name")
    args = parser.parse_args()

    out_path = write_per_symbol_metrics(args.run_dir, out_dir_name=args.out_dir_name)
    print(f"Wrote per-symbol metrics to: {out_path}")


if __name__ == "__main__":
    main()
