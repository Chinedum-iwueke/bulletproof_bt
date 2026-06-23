"""Stable SaaS contract for exposing only certified engine experiments."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class TruthCertificationError(ValueError):
    """Raised when engine-generated results are not certified for exposure."""


def require_truth_certification(experiment_root: str | Path) -> dict[str, Any]:
    root = Path(experiment_root)
    report_path = root / "summaries" / "truth_validation_report.json"
    if not report_path.exists():
        raise TruthCertificationError(f"Missing truth certification: {report_path}")
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    if payload.get("status") != "PASS" or int(payload.get("hard_failures", 1)) != 0:
        raise TruthCertificationError(
            f"Experiment is not truth-certified: status={payload.get('status')} "
            f"hard_failures={payload.get('hard_failures')}"
        )
    for name in ("runs_dataset.parquet", "trades_dataset.parquet", "dataset_manifest.json"):
        path = root / "research_data" / name
        if not path.exists():
            raise TruthCertificationError(f"Certified experiment missing canonical artifact: {path}")
    return payload
