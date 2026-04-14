from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from bt.analytics.h11_postmortem import classify_failure_mode, entry_position_bucket, impulse_bucket, pullback_depth_bucket, run_h11_postmortem
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


def _make_h11_run(run_dir: Path, *, variant: str) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    for name in REQUIRED_FILES:
        (run_dir / name).touch()
    (run_dir / "config_used.yaml").write_text(
        "\n".join(
            [
                "strategy:",
                "  name: l1_h11_quality_filtered_continuation",
                f"  family_variant: {variant}",
                "  timeframe: 15m",
            ]
        ),
        encoding="utf-8",
    )
    (run_dir / "performance.json").write_text(json.dumps({"ev_r_gross": 0.3, "ev_r_net": 0.1}), encoding="utf-8")

    trades = pd.DataFrame([
        {
            "run_id": run_dir.name,
            "entry_ts": "2024-01-01T00:00:00Z",
            "exit_ts": "2024-01-01T00:20:00Z",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "r_multiple_gross": 0.4,
            "r_multiple_net": -0.1,
            "mfe_r": 1.8,
            "mae_r": 0.4,
            "exit_reason": "stop_loss",
            "fees": 0.03,
            "slippage": 0.02,
            "spread_cost": 0.01,
        },
        {
            "run_id": run_dir.name,
            "entry_ts": "2024-01-01T01:00:00Z",
            "exit_ts": "2024-01-01T01:20:00Z",
            "symbol": "ETHUSDT",
            "side": "SELL",
            "r_multiple_gross": 0.2,
            "r_multiple_net": 0.1,
            "mfe_r": 0.9,
            "mae_r": 0.7,
            "exit_reason": "trend_failure",
            "fees": 0.03,
            "slippage": 0.02,
            "spread_cost": 0.01,
        },
    ])
    trades.to_csv(run_dir / "trades.csv", index=False)

    fills = [
        {
            "symbol": "BTCUSDT",
            "ts": "2024-01-01T00:00:00Z",
            "side": "BUY",
            "metadata": {
                "family_variant": variant,
                "setup_type": "quality_filtered_continuation",
                "signal_timeframe": "15m",
                "impulse_strength_atr": 1.6,
                "swing_distance_atr": 1.6,
                "pullback_depth_atr": 0.7,
                "pull_entry_atr_low": 0.5,
                "pull_entry_atr_high": 1.0,
                "entry_position_metric": 0.6,
                "reclaim_position_metric": 0.6,
                "continuation_trigger_state": "ema20_reclaim_confirmed",
                "stop_distance": 2.0,
                "stop_price": 100.0,
                "stop_padding_atr": 0.25 if variant == "L1-H11C" else None,
                "lock_r": 1.0 if variant == "L1-H11C" else None,
                "vwap_giveback_mode": "on" if variant == "L1-H11C" else None,
            },
        },
        {
            "symbol": "ETHUSDT",
            "ts": "2024-01-01T01:00:00Z",
            "side": "SELL",
            "metadata": {
                "family_variant": variant,
                "setup_type": "quality_filtered_continuation",
                "signal_timeframe": "1h",
                "impulse_strength_atr": 0.8,
                "swing_distance_atr": 0.8,
                "pullback_depth_atr": 1.7,
                "pull_entry_atr_low": 0.5,
                "pull_entry_atr_high": 1.0,
                "entry_position_metric": 0.2,
                "reclaim_position_metric": 0.2,
                "continuation_trigger_state": "ema20_reclaim_confirmed",
                "stop_distance": 2.2,
                "stop_price": 200.0,
                "stop_padding_atr": 0.5 if variant == "L1-H11C" else None,
                "lock_r": 1.5 if variant == "L1-H11C" else None,
                "vwap_giveback_mode": "off" if variant == "L1-H11C" else None,
            },
        },
    ]
    with (run_dir / "fills.jsonl").open("w", encoding="utf-8") as handle:
        for row in fills:
            handle.write(json.dumps(row) + "\n")


def test_h11_bucketing_and_failure_mode_helpers() -> None:
    assert pullback_depth_bucket(pd.Series([0.2, 0.8, 1.2, 2.2])).astype(str).tolist() == ["0-0.5_atr", "0.5-1.0_atr", "1.0-1.5_atr", ">1.5_atr"]
    assert impulse_bucket(pd.Series([0.7, 1.2, 1.8, 2.4])).astype(str).tolist() == ["0-1.0_atr", "1.0-1.5_atr", "1.5-2.0_atr", ">2.0_atr"]
    assert entry_position_bucket(pd.Series([0.1, 0.5, 0.9])).astype(str).tolist() == ["deep_zone", "mid_zone", "early_reclaim"]
    assert classify_failure_mode(pd.Series({"realized_r_gross": 0.2, "realized_r_net": -0.1})) == "cost_killed"


def test_h11_postmortem_outputs_required_diagnostics(tmp_path: Path) -> None:
    exp = tmp_path / "exp"
    _make_h11_run(exp / "runs" / "h11a", variant="L1-H11A")
    _make_h11_run(exp / "runs" / "h11c", variant="L1-H11C")
    outputs = run_h11_postmortem(exp)
    required = {
        "pullback_quality_summary",
        "ev_by_pullback_depth_bucket",
        "pullback_quality_by_timeframe",
        "pullback_quality_by_symbol",
        "impulse_strength_summary",
        "ev_by_impulse_bucket",
        "impulse_strength_by_timeframe",
        "entry_position_summary",
        "ev_by_entry_position_bucket",
        "failure_mode_summary",
        "failure_mode_by_variant",
        "failure_mode_by_timeframe",
        "protection_discipline_summary",
        "lock_rule_effect_summary",
        "vwap_giveback_effect_summary",
        "cost_kill_summary",
        "cost_kill_by_timeframe",
        "cost_kill_by_symbol",
    }
    assert required.issubset(outputs)
    for key in required:
        assert Path(outputs[key]).exists()


def test_postmortem_dispatch_includes_h11(tmp_path: Path) -> None:
    exp = tmp_path / "exp"
    _make_h11_run(exp / "runs" / "h11a", variant="L1-H11A")
    outputs = run_postmortem_for_experiment(exp)
    assert any(key.startswith("L1-H11:") for key in outputs)
