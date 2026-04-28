from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class DatasetBundle:
    summary_rows: list[dict[str, Any]]
    summary_path: Path | None
    strategy_summary_paths: list[Path]
    runs_dataset_path: Path | None
    trades_dataset_path: Path | None
    runs_dataset_rows: list[dict[str, Any]]
    trades_dataset_rows: list[dict[str, Any]]


@dataclass
class ExperimentContext:
    name: str
    hypothesis_path: Path
    hypothesis_text: str
    stable_root: Path
    vol_root: Path
    stable: DatasetBundle
    volatile: DatasetBundle
    parquet_supported: bool


def _read_csv_rows(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        return [dict(row) for row in reader]


def _try_read_parquet(path: Path | None) -> tuple[list[dict[str, Any]], bool]:
    if path is None or not path.exists():
        return [], False
    try:
        import pandas as pd  # type: ignore
    except Exception:
        return [], False

    try:
        frame = pd.read_parquet(path)
    except Exception:
        return [], True

    return frame.to_dict(orient="records"), True


def _collect_bundle(experiment_root: Path) -> tuple[DatasetBundle, bool]:
    summaries_dir = experiment_root / "summaries"
    summary_path = summaries_dir / "run_summary.csv"
    strategy_paths = sorted(
        p
        for p in summaries_dir.glob("*.csv")
        if p.is_file() and p.name != "run_summary.csv"
    ) if summaries_dir.exists() else []

    research_data = experiment_root / "research_data"
    runs_dataset_path = research_data / "runs_dataset.parquet"
    trades_dataset_path = research_data / "trades_dataset.parquet"

    runs_rows, parquet_available_1 = _try_read_parquet(runs_dataset_path)
    trades_rows, parquet_available_2 = _try_read_parquet(trades_dataset_path)

    bundle = DatasetBundle(
        summary_rows=_read_csv_rows(summary_path),
        summary_path=summary_path if summary_path.exists() else None,
        strategy_summary_paths=strategy_paths,
        runs_dataset_path=runs_dataset_path if runs_dataset_path.exists() else None,
        trades_dataset_path=trades_dataset_path if trades_dataset_path.exists() else None,
        runs_dataset_rows=runs_rows,
        trades_dataset_rows=trades_rows,
    )
    return bundle, (parquet_available_1 or parquet_available_2)


def load_experiment_context(
    *,
    name: str,
    hypothesis_path: Path,
    stable_root: Path,
    vol_root: Path,
) -> ExperimentContext:
    if not hypothesis_path.exists():
        raise FileNotFoundError(f"Hypothesis YAML not found: {hypothesis_path}")
    hypothesis_text = hypothesis_path.read_text(encoding="utf-8")

    stable_bundle, stable_parquet_ok = _collect_bundle(stable_root)
    vol_bundle, vol_parquet_ok = _collect_bundle(vol_root)

    return ExperimentContext(
        name=name,
        hypothesis_path=hypothesis_path,
        hypothesis_text=hypothesis_text,
        stable_root=stable_root,
        vol_root=vol_root,
        stable=stable_bundle,
        volatile=vol_bundle,
        parquet_supported=(stable_parquet_ok or vol_parquet_ok),
    )
