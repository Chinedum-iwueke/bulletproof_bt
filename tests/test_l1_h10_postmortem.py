from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from bt.analytics.h10_postmortem import run_h10_postmortem
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


def _make_h10_run(run_dir: Path, *, strategy_name: str, variant: str) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    for name in REQUIRED_FILES:
        (run_dir / name).touch()

    (run_dir / "config_used.yaml").write_text(
        "\n".join(
            [
                "strategy:",
                f"  name: {strategy_name}",
                f"  family_variant: {variant}",
                "  timeframe: 5m",
            ]
        ),
        encoding="utf-8",
    )
    (run_dir / "performance.json").write_text(json.dumps({"ev_r_gross": 0.2, "ev_r_net": 0.05}), encoding="utf-8")

    trades = pd.DataFrame(
        [
            {
                "run_id": run_dir.name,
                "entry_ts": "2024-01-01T00:00:00Z",
                "exit_ts": "2024-01-01T00:10:00Z",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "r_multiple_gross": 0.3,
                "r_multiple_net": -0.1,
                "mfe_r": 2.3,
                "mae_r": 0.4,
                "holding_period_bars_signal": 2,
                "time_to_mfe_bars_signal": 2,
                "exit_reason": "take_profit",
                "fees": 0.03,
                "slippage": 0.02,
                "spread_cost": 0.01,
            },
            {
                "run_id": run_dir.name,
                "entry_ts": "2024-01-01T01:00:00Z",
                "exit_ts": "2024-01-01T01:15:00Z",
                "symbol": "ETHUSDT",
                "side": "SELL",
                "r_multiple_gross": -0.8,
                "r_multiple_net": -1.0,
                "mfe_r": 0.4,
                "mae_r": 1.2,
                "holding_period_bars_signal": 3,
                "time_to_mfe_bars_signal": 3,
                "exit_reason": "atr_stop",
                "fees": 0.05,
                "slippage": 0.03,
                "spread_cost": 0.02,
            },
        ]
    )
    trades.to_csv(run_dir / "trades.csv", index=False)

    meta = {
        "family_variant": variant,
        "setup_type": "mean_reversion_small_tp" if variant == "L1-H10A" else "breakout_scalping",
        "signal_timeframe": "5m",
        "atr_entry": 1.0,
        "stop_distance": 2.5,
        "tp_distance": 1.25,
        "tp_r": 0.5,
        "rr_ratio": 0.5,
    }
    if variant == "L1-H10A":
        meta.update({"z_vwap_t": -1.2, "z0": 1.0})
    else:
        meta.update({"breakout_atr": 0.75, "breakout_distance_atr": 0.8, "breakout_reference_price": 100.0, "adx_entry": 25.0})

    fills = [
        {"symbol": "BTCUSDT", "ts": "2024-01-01T00:00:00Z", "side": "BUY", "metadata": meta},
        {"symbol": "ETHUSDT", "ts": "2024-01-01T01:00:00Z", "side": "SELL", "metadata": meta},
    ]
    with (run_dir / "fills.jsonl").open("w", encoding="utf-8") as handle:
        for row in fills:
            handle.write(json.dumps(row) + "\n")


def test_h10_postmortem_outputs_required_files(tmp_path: Path) -> None:
    exp = tmp_path / "exp"
    _make_h10_run(exp / "runs" / "run_h10a", strategy_name="l1_h10a_mean_reversion_small_tp", variant="L1-H10A")
    _make_h10_run(exp / "runs" / "run_h10b", strategy_name="l1_h10b_breakout_scalping", variant="L1-H10B")

    outputs = run_h10_postmortem(exp)
    required = {
        "tail_potential_summary",
        "tail_potential_by_variant",
        "tail_potential_by_timeframe",
        "tail_potential_by_symbol",
        "cost_kill_summary",
        "cost_kill_by_timeframe",
        "cost_kill_by_symbol",
        "cost_kill_by_parameter_slice",
        "win_rate_stability_summary",
        "win_rate_by_timeframe",
        "win_rate_by_symbol",
        "win_rate_by_parameter_slice",
        "failure_mode_summary",
        "failure_mode_by_variant",
        "failure_mode_by_timeframe",
    }
    assert required.issubset(outputs)
    for key in required:
        assert Path(outputs[key]).exists()


def test_postmortem_dispatch_includes_h10(tmp_path: Path) -> None:
    exp = tmp_path / "exp"
    _make_h10_run(exp / "runs" / "run_h10a", strategy_name="l1_h10a_mean_reversion_small_tp", variant="L1-H10A")
    outputs = run_postmortem_for_experiment(exp)
    assert any(key.startswith("L1-H10:") for key in outputs)
