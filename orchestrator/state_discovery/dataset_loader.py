from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

import pandas as pd

from orchestrator.db import ResearchDB


@dataclass
class DiscoveryDataset:
    experiment_root: Path
    experiment_id: str | None
    hypothesis_id: str | None
    hypothesis_name: str | None
    dataset_type: str | None
    trades: pd.DataFrame
    manifest: dict[str, Any]
    schema_coverage: dict[str, Any] | None


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _load_trades(experiment_root: Path) -> tuple[pd.DataFrame, str | None]:
    parquet_path = experiment_root / "research_data" / "trades_dataset.parquet"
    if parquet_path.exists():
        try:
            return pd.read_parquet(parquet_path), str(parquet_path)
        except Exception:
            pass

    rows: list[pd.DataFrame] = []
    for trades_csv in sorted(experiment_root.glob("runs/*/trades.csv")):
        try:
            rows.append(pd.read_csv(trades_csv))
        except Exception:
            continue
    if rows:
        return pd.concat(rows, ignore_index=True), "runs/*/trades.csv"
    return pd.DataFrame(), None


def _discover_experiment_roots(experiments_root: Path) -> list[Path]:
    if not experiments_root.exists():
        return []
    return sorted([p for p in experiments_root.iterdir() if p.is_dir() and (p / "runs").exists()])


def _db_experiment_map(db: ResearchDB) -> dict[str, dict[str, Any]]:
    conn = db.connect()
    rows = conn.execute("SELECT id, hypothesis_id, name, dataset_type, experiment_root FROM experiments").fetchall()
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        root = str(row["experiment_root"])
        out[root] = {
            "experiment_id": str(row["id"]),
            "hypothesis_id": str(row["hypothesis_id"]) if row["hypothesis_id"] else None,
            "experiment_name": str(row["name"]) if row["name"] else None,
            "dataset_type": str(row["dataset_type"]) if row["dataset_type"] else None,
        }
    return out


def load_discovery_datasets(
    *,
    db_path: Path,
    experiments_root: Path | None = None,
    experiment_root: Path | None = None,
    dataset_type: str | None = None,
    hypothesis_id: str | None = None,
    name: str | None = None,
) -> list[DiscoveryDataset]:
    db = ResearchDB(db_path)
    db.init_schema()
    exp_map = _db_experiment_map(db)

    roots = [experiment_root] if experiment_root is not None else _discover_experiment_roots(experiments_root or Path("outputs"))
    datasets: list[DiscoveryDataset] = []
    for root in roots:
        trades, trades_source = _load_trades(root)
        if trades.empty:
            continue

        key = str(root)
        rel_key = db.normalize_path(root)
        meta = exp_map.get(key) or exp_map.get(rel_key or "") or {}
        if dataset_type and meta.get("dataset_type") and meta.get("dataset_type") != dataset_type:
            continue
        if hypothesis_id and meta.get("hypothesis_id") and meta.get("hypothesis_id") != hypothesis_id:
            continue
        if name and meta.get("experiment_name") and meta.get("experiment_name") != name:
            continue

        schema_coverage = _read_json(root / "summaries" / "trade_schema_coverage.json")
        manifest = {
            "experiment_root": str(root),
            "trades_source": trades_source,
            "has_schema_coverage": schema_coverage is not None,
        }
        datasets.append(
            DiscoveryDataset(
                experiment_root=root,
                experiment_id=meta.get("experiment_id"),
                hypothesis_id=meta.get("hypothesis_id"),
                hypothesis_name=meta.get("experiment_name"),
                dataset_type=meta.get("dataset_type"),
                trades=trades,
                manifest=manifest,
                schema_coverage=schema_coverage,
            )
        )

    db.close()
    return datasets
