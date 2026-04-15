from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from bt.experiments.cleanup import CleanupConfig, run_experiment_cleanup, validate_extraction_outputs
from bt.experiments.dataset_builder import REQUIRED_OUTPUT_FILES


def _make_experiment_root(tmp_path: Path) -> Path:
    exp = tmp_path / "exp"
    for name in ("contract_snapshot", "manifests", "runs", "summaries"):
        (exp / name).mkdir(parents=True, exist_ok=True)
    (exp / "contract_snapshot" / "snapshot.yaml").write_text("hypothesis_id: L1-H5A\n", encoding="utf-8")
    (exp / "manifests" / "manifest.csv").write_text("row_id\nrow_00001\n", encoding="utf-8")
    (exp / "summaries" / "run_summary.csv").write_text("run_id,net_pnl\n", encoding="utf-8")
    return exp


def _write_research_outputs(exp: Path, runs_df: pd.DataFrame) -> Path:
    out_dir = exp / "research_data"
    out_dir.mkdir(parents=True, exist_ok=True)
    runs_df.to_parquet(out_dir / REQUIRED_OUTPUT_FILES["runs_dataset"], index=False)
    pd.DataFrame([{"run_id": "stub", "trade_id": 1, "pnl_r": 0.1}]).to_parquet(
        out_dir / REQUIRED_OUTPUT_FILES["trades_dataset"],
        index=False,
    )
    (out_dir / REQUIRED_OUTPUT_FILES["dataset_manifest"]).write_text("{}", encoding="utf-8")
    (out_dir / REQUIRED_OUTPUT_FILES["feature_dictionary"]).write_text("{}", encoding="utf-8")
    (out_dir / REQUIRED_OUTPUT_FILES["experiment_summary"]).write_text("{}", encoding="utf-8")
    (out_dir / REQUIRED_OUTPUT_FILES["extraction_log"]).write_text("{}", encoding="utf-8")
    pd.DataFrame(columns=["run_id", "reason", "missing_artifact", "notes"]).to_csv(
        out_dir / REQUIRED_OUTPUT_FILES["dropped_runs"],
        index=False,
    )
    return out_dir


def _write_run_artifacts(run_dir: Path) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    for name in (
        "decisions.jsonl",
        "fills.jsonl",
        "equity.csv",
        "trades.csv",
        "performance.json",
        "config_used.yaml",
    ):
        (run_dir / name).write_text("x", encoding="utf-8")


def test_cleanup_dry_run_writes_retention_artifacts(tmp_path: Path) -> None:
    exp = _make_experiment_root(tmp_path)
    rows = []
    for idx, pnl in enumerate([100.0, 60.0, 10.0, -5.0, -30.0], start=1):
        run_id = f"run_{idx:02d}"
        _write_run_artifacts(exp / "runs" / run_id)
        rows.append(
            {
                "run_id": run_id,
                "net_pnl": pnl,
                "sharpe": float(idx),
                "max_drawdown": float(10 + idx),
                "trade_count": 5 + idx,
            }
        )

    out_dir = _write_research_outputs(exp, pd.DataFrame(rows))
    cfg = CleanupConfig(
        experiment_root=exp,
        delete_logs=True,
        dry_run=True,
        retain_top_n=2,
        retain_median=1,
        retain_worst=1,
    )
    result = run_experiment_cleanup(cfg)

    assert result["status"] == "ok"
    assert (out_dir / "retention_plan.json").exists()
    assert (out_dir / "retained_runs.csv").exists()
    assert (out_dir / "deleted_runs.csv").exists()
    assert (out_dir / "cleanup_log.json").exists()

    plan = json.loads((out_dir / "retention_plan.json").read_text(encoding="utf-8"))
    retained = set(plan["retained_run_ids"])
    assert {"run_01", "run_02", "run_03", "run_05"}.issubset(retained)

    assert (exp / "runs" / "run_01" / "decisions.jsonl").exists()


def test_validate_extraction_outputs_fails_for_empty_runs_dataset(tmp_path: Path) -> None:
    exp = _make_experiment_root(tmp_path)
    out_dir = _write_research_outputs(exp, pd.DataFrame(columns=["run_id", "net_pnl"]))

    valid, errors = validate_extraction_outputs(out_dir)
    assert not valid
    assert any("runs dataset is empty" in err for err in errors)


def test_cleanup_deletes_nonretained_run_folders_and_logs(tmp_path: Path) -> None:
    exp = _make_experiment_root(tmp_path)
    rows = []
    for idx, pnl in enumerate([50.0, 20.0, 0.0, -20.0], start=1):
        run_id = f"run_{idx:02d}"
        _write_run_artifacts(exp / "runs" / run_id)
        rows.append(
            {
                "run_id": run_id,
                "net_pnl": pnl,
                "sharpe": float(10 - idx),
                "max_drawdown": float(idx),
                "trade_count": 2,
            }
        )

    out_dir = _write_research_outputs(exp, pd.DataFrame(rows))
    cfg = CleanupConfig(
        experiment_root=exp,
        delete_logs=True,
        delete_nonretained_runs=True,
        dry_run=False,
        retain_top_n=1,
        retain_median=0,
        retain_worst=0,
    )
    run_experiment_cleanup(cfg)

    assert (exp / "runs" / "run_01").exists()
    assert not (exp / "runs" / "run_01" / "decisions.jsonl").exists()
    assert not (exp / "runs" / "run_02").exists()

    deleted_df = pd.read_csv(out_dir / "deleted_runs.csv")
    assert not deleted_df.empty
    assert (deleted_df["deletion_action"] == "delete_folder:run_dir").any()
