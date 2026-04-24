from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

from bt.analytics.postmortem import run_postmortem_for_experiment
from bt.analytics.run_summary import build_run_summary_row, summarize_experiment_runs


REQUIRED_FILES = [
    "config_used.yaml",
    "performance.json",
    "equity.csv",
    "trades.csv",
    "fills.jsonl",
    "decisions.jsonl",
    "performance_by_bucket.csv",
]


def _make_run(run_dir: Path, *, strategy: str = "l1_h1_vol_floor_trend", status: str = "PASS") -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    for name in REQUIRED_FILES:
        (run_dir / name).touch()

    (run_dir / "run_status.json").write_text(json.dumps({"status": status}), encoding="utf-8")
    (run_dir / "config_used.yaml").write_text(
        "\n".join(
            [
                "strategy:",
                f"  name: {strategy}",
                "  timeframe: 15m",
                "  theta_vol: 0.7",
                "data:",
                "  symbols_subset: [BTCUSDT]",
                "  date_range:",
                "    start: '2024-01-01T00:00:00Z'",
                "    end: '2024-01-02T00:00:00Z'",
                "execution:",
                "  profile: tier2",
            ]
        ),
        encoding="utf-8",
    )
    perf = {
        "ev_r_net": 0.12,
        "ev_r_gross": 0.2,
        "win_rate_r": 0.45,
        "avg_r_win": 1.4,
        "avg_r_loss": -0.8,
        "payoff_ratio_r": 1.75,
        "max_consecutive_losses": 3,
        "total_trades": 4,
        "max_drawdown_duration_bars": 6,
        "mfe_mean_r": 1.1,
        "mae_mean_r": -0.6,
        "tail_loss_p95": 2.2,
        "tail_loss_p99": 3.1,
    }
    (run_dir / "performance.json").write_text(json.dumps(perf), encoding="utf-8")
    (run_dir / "l1_h2b_mechanism.json").write_text(
        json.dumps({"armed_setup_count": 10, "confirmed_reentry_count": 4}), encoding="utf-8"
    )
    trades = pd.DataFrame(
        [
            {
                "entry_ts": "2024-01-01T00:00:00Z",
                "exit_ts": "2024-01-01T01:00:00Z",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "qty": 1,
                "entry_price": 100,
                "exit_price": 101,
                "pnl_net": 1,
                "mfe_price": 2,
                "mae_price": -1,
                "mfe_r": 1.0,
                "mae_r": 0.5,
                "risk_amount": 2,
                "r_multiple_net": 0.5,
                "realized_r_net": 0.5,
                "holding_period_bars_signal": 4,
                "time_to_mfe_bars_signal": 2,
                "exit_reason": "stop_initial",
                "extension_to_entry_delay_signal_bars": 2,
                "vwap_distance_at_entry_atr_units": 0.9,
                "max_extension_z_before_entry": 1.4,
                "touched_vwap_before_exit": False,
                "touched_1r_before_exit": True,
            },
            {
                "entry_ts": "2024-01-01T02:00:00Z",
                "exit_ts": "2024-01-01T03:00:00Z",
                "symbol": "ETHUSDT",
                "side": "SELL",
                "qty": 2,
                "entry_price": 50,
                "exit_price": 48,
                "pnl_net": 4,
                "mfe_price": 4,
                "mae_price": -0.5,
                "mfe_r": 2.0,
                "mae_r": 0.25,
                "risk_amount": 2,
                "r_multiple_net": 2.0,
                "realized_r_net": 2.0,
                "holding_period_bars_signal": 6,
                "time_to_mfe_bars_signal": 3,
                "exit_reason": "time_stop",
                "extension_to_entry_delay_signal_bars": 1,
                "vwap_distance_at_entry_atr_units": 0.6,
                "max_extension_z_before_entry": 1.2,
                "touched_vwap_before_exit": True,
                "touched_1r_before_exit": True,
            },
        ]
    )
    trades.to_csv(run_dir / "trades.csv", index=False)

    fills = [
        {
            "symbol": "BTCUSDT",
            "ts": "2024-01-01T00:00:00Z",
            "side": "BUY",
            "metadata": {"vol_pct_t": 0.9, "signal_timeframe": "15m", "gate_pass": True},
        },
        {
            "symbol": "ETHUSDT",
            "ts": "2024-01-01T02:00:00Z",
            "side": "SELL",
            "metadata": {"vol_pct_t": 0.3, "signal_timeframe": "5m", "gate_pass": False},
        },
    ]
    with (run_dir / "fills.jsonl").open("w", encoding="utf-8") as handle:
        for row in fills:
            handle.write(json.dumps(row) + "\n")


def test_run_summary_extraction(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_a"
    _make_run(run_dir)

    row, warnings = build_run_summary_row(run_dir, completed_only=True, hypothesis_catalog={})
    assert not warnings
    assert row["ev_r_net"] == 0.12
    assert row["max_consecutive_losses"] == 3
    assert row["theta_vol"] == 0.7
    assert row["mfe_mean_r"] == 1.5
    assert row["armed_setup_count"] == 10
    assert row["confirmed_reentry_count"] == 4


def test_experiment_summary_and_missing_artifacts(tmp_path: Path) -> None:
    exp_root = tmp_path / "exp"
    good = exp_root / "runs" / "run_ok"
    bad = exp_root / "runs" / "run_bad"
    _make_run(good)
    bad.mkdir(parents=True)

    summary, warnings = summarize_experiment_runs(exp_root, completed_only=False)
    assert len(summary) == 2
    assert any("missing_or_invalid_performance_json" == w for w in warnings["warning"].tolist())


def test_completed_run_missing_mfe_mae_raises(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_bad_metrics"
    _make_run(run_dir)
    trades = pd.read_csv(run_dir / "trades.csv")
    trades = trades.drop(columns=["mfe_r", "mae_r"])
    trades.to_csv(run_dir / "trades.csv", index=False)
    try:
        build_run_summary_row(run_dir, completed_only=True, hypothesis_catalog={})
    except ValueError as exc:
        assert "mandatory path diagnostics" in str(exc)
    else:
        raise AssertionError("expected ValueError when mfe_r/mae_r columns are missing")


def test_postmortem_groupings_and_dispatch(tmp_path: Path) -> None:
    exp_root = tmp_path / "exp"
    _make_run(exp_root / "runs" / "row_00001__g0001__tier2", strategy="l1_h1_vol_floor_trend")

    outputs = run_postmortem_for_experiment(exp_root)
    assert "conditional_ev_by_vol_bucket" in outputs
    assert "conditional_ev_by_timeframe" in outputs
    assert "conditional_ev_by_symbol" in outputs
    assert "mfe_capture_summary" in outputs
    assert "cost_drag_summary" in outputs
    assert any(key.startswith("L1-H1:") for key in outputs)


def test_cli_smoke(tmp_path: Path) -> None:
    exp_root = tmp_path / "exp"
    _make_run(exp_root / "runs" / "row_00001__g0001__tier2")

    cmd = [
        sys.executable,
        "scripts/post_run_analysis.py",
        "--experiment-root",
        str(exp_root),
        "--completed-only",
        "--include-diagnostics",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr
    assert (exp_root / "summaries" / "run_summary.csv").exists()
    assert (exp_root / "summaries" / "symbol_summary.csv").exists()
    assert (exp_root / "summaries" / "exit_reason_summary.csv").exists()
    assert (exp_root / "summaries" / "diagnostics" / "manifest.json").exists()


def test_run_summary_prefers_manifest_mapping_for_shared_strategy_family(tmp_path: Path) -> None:
    exp_root = tmp_path / "exp"
    run_name = "row_00001__g00000__family_variant-L1-H11B__tier2"
    _make_run(exp_root / "runs" / run_name, strategy="l1_h11_quality_filtered_continuation")

    manifests_dir = exp_root / "manifests"
    manifests_dir.mkdir(parents=True, exist_ok=True)
    (manifests_dir / "l1_h11b_tier2_grid.csv").write_text(
        "\n".join(
            [
                "row_id,hypothesis_id,hypothesis_path,phase,tier,variant_id,config_hash,params_json,run_slug,output_dir,expected_status,enabled,notes",
                (
                    f"row_00001,L1-H11B,{Path('research/hypotheses/l1_h11b.yaml').as_posix()},"
                    "tier2,Tier2,g00000,abc123,{},"
                    f"{run_name},outputs/demo/runs/{run_name},pending,true,"
                ),
            ]
        ),
        encoding="utf-8",
    )

    summary, _ = summarize_experiment_runs(exp_root, completed_only=True)
    assert len(summary) == 1
    row = summary.iloc[0].to_dict()
    assert row["hypothesis_id"] == "L1-H11B"
    assert row["hypothesis_title"] == "Pullback Geometry / Impulse Study"
