from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from bt.validation.experiment_truth import validate_experiment_root, write_truth_report


def _write_run(root: Path, *, run_name: str = "row_00001", mutate: dict | None = None) -> Path:
    run_dir = root / "runs" / run_name
    run_dir.mkdir(parents=True)
    rows = [
        {
            "entry_ts": "2025-01-01T00:00:00+00:00",
            "exit_ts": "2025-01-01T01:00:00+00:00",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "entry_qty": 4.0,
            "qty": 4.0,
            "entry_price": 100.0,
            "exit_price": 101.0,
            "pnl_net": 4.0,
            "risk_amount": 100.0,
            "risk_budget": 100.0,
            "entry_stop_distance": 25.0,
            "risk_value_per_price_unit": 1.0,
            "r_multiple_net": 0.04,
            "max_notional": 500.0,
            "equity_used": 100000.0,
            "free_margin_post": 99000.0,
            "r_metrics_valid": True,
            "forced_liquidation": False,
            "entry_state_ts": "2025-01-01T00:00:00+00:00",
            "entry_state_funding_source_ts": "2024-12-31T16:00:00+00:00",
            "entry_state_oi_source_ts": "2024-12-31T23:55:00+00:00",
            "entry_state_csi_source": "enriched",
        }
    ]
    if mutate:
        rows[0].update(mutate)
    pd.DataFrame(rows).to_csv(run_dir / "trades.csv", index=False)
    (run_dir / "equity.csv").write_text("ts,equity\n2025-01-01T00:00:00+00:00,100000\n", encoding="utf-8")
    perf = {"total_trades": 1, "win_rate": 1.0, "ev_r_net": rows[0]["r_multiple_net"]}
    (run_dir / "performance.json").write_text(json.dumps(perf), encoding="utf-8")
    (run_dir / "run_status.json").write_text(json.dumps({"status": "PASS"}), encoding="utf-8")
    (run_dir / "config_used.yaml").write_text("risk:\n  r_per_trade: 0.005\n", encoding="utf-8")
    (run_dir / "decisions.jsonl").write_text("{}\n", encoding="utf-8")
    (run_dir / "fills.jsonl").write_text("{}\n", encoding="utf-8")
    return run_dir


def test_truth_validation_passes_consistent_run(tmp_path: Path) -> None:
    _write_run(tmp_path)

    report = validate_experiment_root(tmp_path)

    assert report.status == "PASS"
    assert report.hard_failures == 0
    assert report.runs_checked == 1


def test_truth_validation_fails_notional_cap_breach(tmp_path: Path) -> None:
    _write_run(tmp_path, mutate={"entry_qty": 7.0, "qty": 7.0})

    report = validate_experiment_root(tmp_path)

    assert report.status == "FAIL"
    assert any(issue.check == "entry_notional_cap" for issue in report.issues)


def test_truth_validation_fails_future_source_timestamp(tmp_path: Path) -> None:
    _write_run(tmp_path, mutate={"entry_state_funding_source_ts": "2025-01-01T00:01:00+00:00"})

    report = validate_experiment_root(tmp_path)

    assert report.status == "FAIL"
    assert any(issue.check == "no_lookahead_source_ts" for issue in report.issues)


def test_truth_validation_fails_requested_budget_used_as_actual_risk(tmp_path: Path) -> None:
    _write_run(tmp_path, mutate={"entry_qty": 1.0, "qty": 1.0, "risk_amount": 100.0, "entry_stop_distance": 5.0})

    report = validate_experiment_root(tmp_path)

    assert report.status == "FAIL"
    assert any(issue.check == "actual_stop_risk" for issue in report.issues)


def test_truth_validation_writes_reports(tmp_path: Path) -> None:
    _write_run(tmp_path)
    report = validate_experiment_root(tmp_path)

    json_path, md_path = write_truth_report(report, tmp_path / "summaries")

    assert json.loads(json_path.read_text(encoding="utf-8"))["status"] == "PASS"
    assert "Truth Validation Report" in md_path.read_text(encoding="utf-8")


def test_truth_validation_fails_divergent_duplicate_risk_column(tmp_path: Path) -> None:
    run_dir = _write_run(tmp_path)
    trades = pd.read_csv(run_dir / "trades.csv")
    trades["risk_amount.1"] = trades["risk_amount"] + 1.0
    trades.to_csv(run_dir / "trades.csv", index=False)

    report = validate_experiment_root(tmp_path)

    assert report.status == "FAIL"
    assert any(issue.check == "duplicate_truth_column" for issue in report.issues)
