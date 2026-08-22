from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pandas as pd

from orchestrator.research_memory.ingest import discover_experiment_roots, ingest_research_memory
from orchestrator.research_memory.query_engine import similar_state
from orchestrator.research_memory.recommendation_engine import generate_recommendations
from orchestrator.research_memory.report_writer import write_reports
from orchestrator.research_memory.schema import ensure_research_memory_schema


def _conn(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    ensure_research_memory_schema(conn)
    return conn


def _write_exp(root: Path, trades: pd.DataFrame, *, metrics_valid: bool = True) -> None:
    run_dir = root / "runs" / "run_1"
    run_dir.mkdir(parents=True, exist_ok=True)
    trades = trades.copy()
    if "trade_id" not in trades:
        trades["trade_id"] = [f"trade-{index}" for index in range(len(trades))]
    if "ts_signal" not in trades:
        trades["ts_signal"] = pd.date_range("2024-01-01", periods=len(trades), freq="h", tz="UTC")
    if "pnl_net" not in trades and "r_net" in trades:
        trades["pnl_net"] = trades["r_net"]
    trades.to_csv(run_dir / "trades.csv", index=False)
    (run_dir / "performance_validation.json").write_text(json.dumps({"metrics_valid": metrics_valid}), encoding="utf-8")


def _ingest(tmp_path: Path, outputs_root: Path) -> sqlite3.Connection:
    conn = _conn(tmp_path / "research.sqlite")
    ingest_research_memory(
        conn,
        outputs_root=outputs_root,
        verdicts_dir=tmp_path / "research" / "verdicts",
        state_findings_dir=tmp_path / "research" / "state_findings",
        alpha_zoo_dir=tmp_path / "research" / "alpha_zoo",
        output_dir=tmp_path / "research" / "memory",
    )
    return conn


def test_ingest_synthetic_trades_into_memory(tmp_path: Path) -> None:
    outputs = tmp_path / "outputs"
    exp = outputs / "l1_demo_parallel_stable"
    _write_exp(exp, pd.DataFrame({"trade_id": ["a"], "r_net": [0.5], "entry_state_csi_pctile": [0.8]}))
    conn = _ingest(tmp_path, outputs)
    assert conn.execute("SELECT COUNT(*) FROM research_memory_trades").fetchone()[0] == 1


def test_similar_state_query_returns_correct_aggregate(tmp_path: Path) -> None:
    outputs = tmp_path / "outputs"
    exp = outputs / "l1_demo_parallel_stable"
    _write_exp(
        exp,
        pd.DataFrame(
            {
                "trade_id": ["a", "b", "c"],
                "r_net": [1.0, 0.5, -1.0],
                "path_mfe_r": [2.0, 1.0, 0.2],
                "counterfactual_exit_efficiency_realized_over_mfe": [0.5, 0.5, 0.1],
                "entry_decision_setup_class": ["trend_pullback", "trend_pullback", "other"],
                "entry_state_csi_pctile": [0.82, 0.8, 0.1],
                "entry_state_vol_pctile": [0.76, 0.75, 0.1],
                "entry_state_spread_proxy_pctile": [0.48, 0.5, 0.9],
                "entry_state_tr_over_atr": [1.9, 1.8, 0.5],
            }
        ),
    )
    conn = _ingest(tmp_path, outputs)
    result = similar_state(conn, {"setup_class": "trend_pullback", "csi_pctile": 0.82, "vol_pctile": 0.76, "spread_pctile": 0.48, "tr_over_atr": 1.9})
    assert result["n_matches"] == 2
    assert result["ev_r_net"] == 0.75


def test_positive_bucket_creates_add_gate_recommendation(tmp_path: Path) -> None:
    outputs = tmp_path / "outputs"
    exp = outputs / "l1_demo_parallel_stable"
    _write_exp(exp, pd.DataFrame({"r_net": [0.5, 0.7, 0.4], "entry_state_csi_pctile": [0.8, 0.82, 0.78], "entry_decision_setup_class": ["trend"] * 3}))
    conn = _ingest(tmp_path, outputs)
    recs = generate_recommendations(conn, min_trades=3)
    assert any(rec["recommendation_type"] == "ADD_GATE" for rec in recs)


def test_negative_bucket_creates_avoid_state_recommendation(tmp_path: Path) -> None:
    outputs = tmp_path / "outputs"
    exp = outputs / "l1_demo_parallel_stable"
    _write_exp(exp, pd.DataFrame({"r_net": [-0.5, -0.7, -0.4], "entry_state_spread_proxy_pctile": [0.9, 0.92, 0.88], "entry_decision_setup_class": ["trend"] * 3}))
    conn = _ingest(tmp_path, outputs)
    recs = generate_recommendations(conn, min_trades=3)
    assert any(rec["recommendation_type"] == "AVOID_STATE" for rec in recs)


def test_high_mfe_low_exit_efficiency_creates_refine_exit(tmp_path: Path) -> None:
    outputs = tmp_path / "outputs"
    exp = outputs / "l1_demo_parallel_stable"
    _write_exp(
        exp,
        pd.DataFrame(
            {
                "r_net": [0.1, 0.0, -0.1],
                "path_mfe_r": [3.0, 2.8, 2.6],
                "counterfactual_exit_efficiency_realized_over_mfe": [0.1, 0.2, 0.15],
                "entry_state_csi_pctile": [0.8, 0.82, 0.78],
                "entry_decision_setup_class": ["trend"] * 3,
            }
        ),
    )
    conn = _ingest(tmp_path, outputs)
    recs = generate_recommendations(conn, min_trades=3)
    assert any(rec["recommendation_type"] == "REFINE_EXIT" for rec in recs)


def test_metrics_invalid_trades_excluded_from_recommendations(tmp_path: Path) -> None:
    outputs = tmp_path / "outputs"
    exp = outputs / "l1_demo_parallel_stable"
    _write_exp(exp, pd.DataFrame({"r_net": [1.0, 1.0, 1.0], "entry_state_csi_pctile": [0.8, 0.82, 0.78]}), metrics_valid=False)
    conn = _ingest(tmp_path, outputs)
    recs = generate_recommendations(conn, min_trades=3)
    assert conn.execute("SELECT COUNT(*) FROM research_memory_trades WHERE metrics_valid = 0").fetchone()[0] == 3
    assert recs == []


def test_recommendations_written_to_db_and_csv(tmp_path: Path) -> None:
    outputs = tmp_path / "outputs"
    exp = outputs / "l1_demo_parallel_stable"
    _write_exp(exp, pd.DataFrame({"r_net": [0.5, 0.7, 0.4], "entry_state_csi_pctile": [0.8, 0.82, 0.78]}))
    conn = _ingest(tmp_path, outputs)
    generate_recommendations(conn, min_trades=3)
    write_reports(conn, tmp_path / "research" / "memory")
    assert conn.execute("SELECT COUNT(*) FROM research_memory_recommendations").fetchone()[0] > 0
    assert (tmp_path / "research" / "memory" / "recommendations.csv").exists()


def test_tier_aware_outputs_path_scanning(tmp_path: Path) -> None:
    outputs = tmp_path / "outputs"
    exp = outputs / "tier2" / "l1_demo_parallel_stable"
    _write_exp(exp, pd.DataFrame({"r_net": [0.1]}))
    roots = discover_experiment_roots(outputs)
    assert exp in roots


def test_cli_similar_state(tmp_path: Path) -> None:
    outputs = tmp_path / "outputs"
    exp = outputs / "l1_demo_parallel_stable"
    _write_exp(exp, pd.DataFrame({"r_net": [1.0], "entry_state_csi_pctile": [0.8]}))
    db = tmp_path / "research.sqlite"
    subprocess.run(
        [sys.executable, "orchestrator/research_memory.py", "--db", str(db), "--outputs-root", str(outputs), "--output-dir", str(tmp_path / "memory"), "--write-db"],
        check=True,
        capture_output=True,
        text=True,
    )
    state = tmp_path / "state.json"
    state.write_text(json.dumps({"csi_pctile": 0.8}), encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, "orchestrator/research_memory.py", "--db", str(db), "--query", "similar_state", "--state-json", str(state)],
        check=True,
        capture_output=True,
        text=True,
    )
    assert json.loads(proc.stdout)["n_matches"] == 1
