from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from bt.saas.truth_certification import TruthCertificationError, require_truth_certification


def _write_certified(root: Path, *, status: str = "PASS", hard_failures: int = 0) -> None:
    summaries = root / "summaries"
    research = root / "research_data"
    summaries.mkdir(parents=True)
    research.mkdir(parents=True)
    (summaries / "truth_validation_report.json").write_text(
        json.dumps({"status": status, "hard_failures": hard_failures}), encoding="utf-8"
    )
    pd.DataFrame([{"run_id": "r1"}]).to_parquet(research / "runs_dataset.parquet", index=False)
    pd.DataFrame([{"trade_id": "t1"}]).to_parquet(research / "trades_dataset.parquet", index=False)
    (research / "dataset_manifest.json").write_text("{}", encoding="utf-8")


def test_saas_accepts_truth_certified_experiment(tmp_path: Path) -> None:
    _write_certified(tmp_path)

    report = require_truth_certification(tmp_path)

    assert report["status"] == "PASS"


def test_saas_rejects_failed_or_missing_certification(tmp_path: Path) -> None:
    with pytest.raises(TruthCertificationError, match="Missing truth certification"):
        require_truth_certification(tmp_path)

    _write_certified(tmp_path, status="FAIL", hard_failures=1)
    with pytest.raises(TruthCertificationError, match="not truth-certified"):
        require_truth_certification(tmp_path)
