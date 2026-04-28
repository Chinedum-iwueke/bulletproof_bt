from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import subprocess
import sys

import pandas as pd


def _write_experiment(root: Path, trades: pd.DataFrame) -> None:
    run_dir = root / "runs" / "run_1"
    run_dir.mkdir(parents=True, exist_ok=True)
    trades.to_csv(run_dir / "trades.csv", index=False)


def _run_agent(tmp_path: Path, exp_root: Path, extra: list[str] | None = None) -> Path:
    db_path = tmp_path / "research.sqlite"
    cmd = [
        sys.executable,
        "orchestrator/state_discovery.py",
        "--db",
        str(db_path),
        "--experiment-root",
        str(exp_root),
        "--name",
        "demo",
        "--output-dir",
        str(tmp_path / "findings"),
        "--min-trades",
        "1",
        "--min-bucket-trades",
        "1",
        "--top-n",
        "10",
        "--include-negative-findings",
    ]
    if extra:
        cmd.extend(extra)
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr
    return tmp_path / "findings"


def test_positive_state_findings_generated(tmp_path: Path) -> None:
    exp = tmp_path / "exp"
    trades = pd.DataFrame(
        {
            "r_net": [1.0, 0.8, -0.2, 0.1],
            "r_gross": [1.1, 0.9, -0.1, 0.2],
            "entry_state_csi_pctile": [0.9, 0.85, 0.2, 0.3],
            "entry_state_vol_pctile": [0.8, 0.7, 0.2, 0.4],
            "path_mfe_r": [2.0, 1.9, 0.4, 0.5],
            "counterfactual_exit_efficiency_realized_over_mfe": [0.6, 0.5, 0.2, 0.2],
            "cost_drag_r": [0.1, 0.1, 0.1, 0.1],
        }
    )
    _write_experiment(exp, trades)
    out = _run_agent(tmp_path, exp)
    findings = pd.read_csv(out / "demo_state_findings.csv")
    assert not findings.empty
    assert (findings["finding_type"] == "POSITIVE_EDGE_STATE").any()


def test_missing_columns_write_missing_diagnostic(tmp_path: Path) -> None:
    exp = tmp_path / "exp_missing"
    _write_experiment(exp, pd.DataFrame({"r_net": [0.1, -0.1, 0.2]}))
    out = _run_agent(tmp_path, exp)
    missing = json.loads((out / "demo_state_discovery_missing_fields.json").read_text(encoding="utf-8"))
    assert "missing_state_columns" in missing
    assert len(missing["missing_state_columns"]) > 0


def test_cost_killed_state_detected(tmp_path: Path) -> None:
    exp = tmp_path / "exp_cost"
    trades = pd.DataFrame(
        {
            "r_net": [-0.3, -0.2, -0.1],
            "r_gross": [0.3, 0.2, 0.1],
            "entry_state_vol_pctile": [0.95, 0.96, 0.94],
            "path_mfe_r": [1.2, 1.1, 1.0],
            "cost_drag_r": [0.6, 0.5, 0.4],
            "counterfactual_exit_efficiency_realized_over_mfe": [0.1, 0.2, 0.1],
        }
    )
    _write_experiment(exp, trades)
    out = _run_agent(tmp_path, exp)
    findings = pd.read_csv(out / "demo_state_findings.csv")
    assert (findings["finding_type"] == "COST_KILLED_STATE").any()


def test_exit_failure_state_detected(tmp_path: Path) -> None:
    exp = tmp_path / "exp_exit"
    trades = pd.DataFrame(
        {
            "r_net": [0.1, 0.0, -0.1],
            "r_gross": [0.2, 0.1, 0.0],
            "entry_state_csi_pctile": [0.7, 0.72, 0.71],
            "path_mfe_r": [3.0, 2.8, 2.7],
            "counterfactual_exit_efficiency_realized_over_mfe": [0.1, 0.15, 0.2],
            "cost_drag_r": [0.1, 0.1, 0.1],
        }
    )
    _write_experiment(exp, trades)
    out = _run_agent(tmp_path, exp)
    findings = pd.read_csv(out / "demo_state_findings.csv")
    assert (findings["finding_type"] == "EXIT_FAILURE_STATE").any()


def test_tail_generation_state_detected(tmp_path: Path) -> None:
    exp = tmp_path / "exp_tail"
    trades = pd.DataFrame(
        {
            "r_net": [6.0, 5.5, -0.5, 0.2],
            "r_gross": [6.1, 5.6, -0.4, 0.3],
            "entry_state_csi_pctile": [0.88, 0.9, 0.2, 0.3],
            "path_mfe_r": [7.0, 6.0, 0.5, 0.4],
            "counterfactual_exit_efficiency_realized_over_mfe": [0.8, 0.75, 0.2, 0.2],
            "cost_drag_r": [0.1, 0.1, 0.1, 0.1],
        }
    )
    _write_experiment(exp, trades)
    out = _run_agent(tmp_path, exp)
    findings = pd.read_csv(out / "demo_state_findings.csv")
    assert (findings["finding_type"] == "TAIL_GENERATION_STATE").any()


def test_outputs_json_csv_md_written(tmp_path: Path) -> None:
    exp = tmp_path / "exp_out"
    trades = pd.DataFrame({"r_net": [0.2, 0.3], "r_gross": [0.3, 0.4], "entry_state_csi_pctile": [0.8, 0.82]})
    _write_experiment(exp, trades)
    out = _run_agent(tmp_path, exp)
    assert (out / "demo_state_findings.json").exists()
    assert (out / "demo_state_findings.csv").exists()
    assert (out / "demo_state_findings.md").exists()


def test_write_db_inserts_state_findings(tmp_path: Path) -> None:
    exp = tmp_path / "exp_db"
    trades = pd.DataFrame({"r_net": [0.6, 0.5], "r_gross": [0.7, 0.6], "entry_state_csi_pctile": [0.85, 0.9]})
    _write_experiment(exp, trades)
    out = _run_agent(tmp_path, exp, extra=["--write-db"])
    db_path = tmp_path / "research.sqlite"
    conn = sqlite3.connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM state_findings").fetchone()[0]
    conn.close()
    assert count > 0
    assert (out / "demo_state_findings.csv").exists()
