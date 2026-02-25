"""CLI entrypoint for deterministic experiment grids."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from bt.api import run_grid
from bt.config import load_yaml
from bt.logging.cli_footer import print_grid_footer
from bt.logging.run_contract import validate_run_artifacts
from bt.logging.artifacts_manifest import write_artifacts_manifest
from bt.logging.run_manifest import write_run_manifest
from bt.logging.summary import write_summary_txt
from bt.metrics.per_symbol import write_per_symbol_metrics


def main() -> None:
    parser = argparse.ArgumentParser(description="Run deterministic backtest experiment grid")
    parser.add_argument("--config", required=True)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--data", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--override", action="append", default=[])
    parser.add_argument("--local-config")
    args = parser.parse_args()

    override_paths = list(args.override)
    if args.local_config:
        override_paths.append(args.local_config)

    experiment_dir = run_grid(
        config_path=args.config,
        experiment_path=args.experiment,
        data_path=args.data,
        out_dir=args.out,
        override_paths=override_paths or None,
        experiment_name=None,
    )

    runs_dir = Path(experiment_dir) / "runs"
    summary_path = Path(experiment_dir) / "summary.json"
    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    runs = summary_payload.get("runs")
    if not isinstance(runs, list):
        raise ValueError(f"Invalid summary.json format at {summary_path}; expected list at 'runs'.")

    run_dirs: list[Path] = []

    for row in runs:
        if not isinstance(row, dict):
            raise ValueError(f"Invalid run row in {summary_path}; expected object entries in 'runs'.")
        run_name = row.get("run_name")
        if not isinstance(run_name, str) or not run_name:
            raise ValueError(f"Invalid run_name in {summary_path}; expected non-empty string.")

        run_dir = runs_dir / run_name
        config: dict | None = None
        try:
            config_path = run_dir / "config_used.yaml"
            try:
                loaded_config = load_yaml(config_path)
            except Exception as exc:  # pragma: no cover - defensive user-facing guard
                raise ValueError(f"Unable to read config_used.yaml from run_dir={run_dir}: {exc}") from exc
            if not isinstance(loaded_config, dict):
                raise ValueError(f"Invalid config_used.yaml format in run_dir={run_dir}; expected mapping.")

            config = loaded_config
            if row.get("status") == "PASS":
                validate_run_artifacts(run_dir)
                write_per_symbol_metrics(run_dir)
                write_summary_txt(run_dir)
                write_run_manifest(run_dir, config=config, data_path=args.data)
                run_dirs.append(run_dir)
        finally:
            if config is not None:
                write_artifacts_manifest(run_dir, config=config)

    print_grid_footer(run_dirs, out_dir=Path(args.out))


if __name__ == "__main__":
    main()
