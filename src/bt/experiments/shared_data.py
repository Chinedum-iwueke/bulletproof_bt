"""Shared-read dataset planning for parallel hypothesis execution."""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SharedDatasetPlan:
    dataset_path: str
    dataset_id: str
    dataset_mode: str
    source: str
    metadata: dict[str, Any]


_PARQUET_SUFFIXES = {".parquet"}


def _dataset_fingerprint(path: Path) -> str:
    stat = path.stat()
    token = f"{path.resolve()}::{stat.st_mtime_ns}::{stat.st_size}"
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:20]


def build_shared_dataset_plan(*, dataset_path: Path) -> SharedDatasetPlan:
    dataset_mode = "dataset_dir" if dataset_path.is_dir() else dataset_path.suffix.lower().lstrip(".")
    dataset_id = _dataset_fingerprint(dataset_path)

    if dataset_path.is_file() and dataset_path.suffix.lower() in _PARQUET_SUFFIXES:
        source = "opened_memory_mapped"
    elif dataset_path.is_dir():
        # Dataset dirs are consumed in streaming mode and loaded lazily per symbol.
        source = "attached_from_cache"
    else:
        source = "fallback_loaded_normally"

    metadata: dict[str, Any] = {
        "path": str(dataset_path),
        "is_dir": dataset_path.is_dir(),
        "suffix": dataset_path.suffix.lower(),
    }
    return SharedDatasetPlan(
        dataset_path=str(dataset_path),
        dataset_id=dataset_id,
        dataset_mode=dataset_mode,
        source=source,
        metadata=metadata,
    )


def write_shared_cache_manifest(experiment_root: Path, plan: SharedDatasetPlan) -> Path:
    out_path = experiment_root / "summaries" / "shared_cache_manifest.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "dataset": asdict(plan),
    }
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return out_path
