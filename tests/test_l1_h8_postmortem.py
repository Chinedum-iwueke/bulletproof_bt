from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from bt.analytics.h8_postmortem import (
    build_h8_trade_diagnostic_rows,
    classify_failure_mode,
    pullback_depth_bucket,
    run_h8_postmortem,
)
from bt.analytics.postmortem import run_postmortem_for_experiment


REQUIRED_FILES = [
    "config_used.yaml",
    "performance.json",
    "equity.csv",
    "trades.csv",
    "fills.jsonl",
    "decisions.jsonl",
    "performance_by_bucket.csv",
]


def _make_h8_run(run_dir: Path, *, family_variant: str = "L1-H8A") -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    for name in REQUIRED_FILES:
        (run_dir / name).touch()

    (run_dir / "config_used.yaml").write_text(
        "\n".join(
            [
                "strategy:",
                "  name: l1_h8_trend_continuation_pullback",
                f"  family_variant: {family_variant}",
                "  timeframe: 15m",
            ]
        ),
        encoding="utf-8",
    )
    (run_dir / "performance.json").write_text(json.dumps({"ev_r_gross": 0.4, "ev_r_net": 0.2}), encoding="utf-8")

    trades = pd.DataFrame(
        [
            {
                "run_id": run_dir.name,
                "hypothesis_id": family_variant,
                "entry_ts": "2024-01-01T00:00:00Z",
                "exit_ts": "2024-01-01T01:00:00Z",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "r_multiple_gross": 1.2,
                "r_multiple_net": 1.0,
                "mfe_r": 2.0,
                "mae_r": 0.4,
                "hold_bars": 6,
                "time_to_mfe_bars_signal": 2,
                "max_unrealized_profit_r": 2.1,
                "fees": 0.03,
                "slippage": 0.02,
                "spread_cost": 0.01,
            },
            {
                "run_id": run_dir.name,
                "hypothesis_id": family_variant,
                "entry_ts": "2024-01-01T02:00:00Z",
                "exit_ts": "2024-01-01T03:00:00Z",
                "symbol": "ETHUSDT",
                "side": "SELL",
                "r_multiple_gross": 0.3,
                "r_multiple_net": -0.2,
                "mfe_r": 0.4,
                "mae_r": 0.8,
                "hold_bars": 5,
                "time_to_mfe_bars_signal": 4,
                "max_unrealized_profit_r": 0.5,
                "fees": 0.04,
                "slippage": 0.03,
                "spread_cost": 0.01,
            },
        ]
    )
    trades.to_csv(run_dir / "trades.csv", index=False)

    fills = [
        {
            "symbol": "BTCUSDT",
            "ts": "2024-01-01T00:00:00Z",
            "side": "BUY",
            "metadata": {
                "family_variant": family_variant,
                "signal_timeframe": "15m",
                "pullback_bars": 2,
                "pullback_depth_atr": 0.4,
                "pullback_depth_pct_of_prior_leg": 0.25,
                "pullback_reference_mode": "ema_or_vwap",
                "reference_hit": "ema",
                "adx": 28,
                "ema_fast": 100.0,
                "ema_slow": 99.0,
                "session_vwap": 99.5,
                "reclaim_strength": 0.7,
                "partial_at_r": 1.5,
            },
        },
        {
            "symbol": "ETHUSDT",
            "ts": "2024-01-01T02:00:00Z",
            "side": "SELL",
            "metadata": {
                "family_variant": family_variant,
                "signal_timeframe": "1h",
                "pullback_bars": 4,
                "pullback_depth_atr": 1.8,
                "pullback_depth_pct_of_prior_leg": 0.8,
                "pullback_reference_mode": "vwap_only",
                "reference_hit": "vwap",
                "adx": 18,
                "ema_fast": 200.0,
                "ema_slow": 201.0,
                "session_vwap": 200.5,
                "reclaim_strength": -0.1,
                "partial_at_r": 2.0,
            },
        },
    ]
    with (run_dir / "fills.jsonl").open("w", encoding="utf-8") as handle:
        for row in fills:
            handle.write(json.dumps(row) + "\n")


def test_h8_diagnostic_row_extraction_and_fields(tmp_path: Path) -> None:
    exp = tmp_path / "exp"
    _make_h8_run(exp / "runs" / "row_1")
    rows = build_h8_trade_diagnostic_rows(exp)
    assert len(rows) == 2
    assert {"pullback_depth_atr", "pullback_reference_mode", "failure_mode_label", "capture_ratio"}.issubset(rows.columns)


def test_pullback_depth_bucket_scheme() -> None:
    buckets = pullback_depth_bucket(pd.Series([0.2, 0.7, 1.2, 2.0]))
    assert buckets.astype(str).tolist() == ["0-0.5_atr", "0.5-1.0_atr", "1.0-1.5_atr", ">1.5_atr"]


def test_failure_mode_classifier_is_deterministic() -> None:
    label = classify_failure_mode(pd.Series({"adx_entry": 10, "pullback_depth_atr": 0.3, "pullback_bars_used": 1, "reclaim_strength": 0.5, "realized_r_gross": 0.2, "realized_r_net": 0.1, "mfe_r": 1.0, "capture_ratio": 0.8, "tp1_hit": False}))
    assert label == "trend_filter_weak"


def test_h8_postmortem_outputs_groupings_and_machine_readable_files(tmp_path: Path) -> None:
    exp = tmp_path / "exp"
    _make_h8_run(exp / "runs" / "row_1", family_variant="L1-H8A")
    outputs = run_h8_postmortem(exp)

    required = {
        "pullback_quality_summary",
        "ev_by_pullback_depth_bucket",
        "ev_by_pullback_bars",
        "ev_by_reference_mode",
        "continuation_strength_summary",
        "continuation_strength_by_timeframe",
        "continuation_strength_by_symbol",
        "failure_mode_summary",
        "failure_mode_by_variant",
        "failure_mode_by_timeframe",
        "runner_capture_summary",
        "runner_capture_by_variant",
        "cost_kill_summary",
        "cost_kill_by_timeframe",
        "cost_kill_by_symbol",
    }
    assert required.issubset(outputs.keys())
    for key in required:
        path = Path(outputs[key])
        assert path.exists()
        df = pd.read_csv(path)
        assert isinstance(df, pd.DataFrame)


def test_ev_groupings_include_pullback_bars_and_reference_mode(tmp_path: Path) -> None:
    exp = tmp_path / "exp"
    _make_h8_run(exp / "runs" / "row_1")
    outputs = run_h8_postmortem(exp)
    by_bars = pd.read_csv(outputs["ev_by_pullback_bars"])
    by_ref = pd.read_csv(outputs["ev_by_reference_mode"])
    assert "pullback_bars_used" in by_bars.columns
    assert "pullback_reference_mode" in by_ref.columns


def test_postmortem_dispatch_integration_for_h8(tmp_path: Path) -> None:
    exp = tmp_path / "exp"
    _make_h8_run(exp / "runs" / "row_1")
    outputs = run_postmortem_for_experiment(exp)
    assert any(key.startswith("L1-H8:") for key in outputs)
    manifest_path = Path(outputs["manifest"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert any(key.startswith("L1-H8:") for key in manifest)
