from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from bt.experiments.dataset_builder import extract_experiment_dataset


def test_dataset_extraction_preserves_known_enrichment_prefixes(tmp_path: Path) -> None:
    exp = tmp_path / "exp"
    run = exp / "runs" / "run_1"
    run.mkdir(parents=True)
    (exp / "manifests").mkdir()
    pd.DataFrame([{"row_id": "run_1", "run_slug": "run_1", "config_hash": "params-abc"}]).to_csv(
        exp / "manifests" / "grid.csv", index=False
    )
    (run / "config_used.yaml").write_text("strategy:\n  name: fixture\n", encoding="utf-8")
    (run / "performance.json").write_text(json.dumps({"net_pnl": 1.0, "trade_count": 1}), encoding="utf-8")
    (run / "run_status.json").write_text(json.dumps({"status": "PASS"}), encoding="utf-8")
    pd.DataFrame(
        [
            {
                "entry_ts": "2025-01-01T00:00:00Z",
                "signal_ts": "2024-12-31T23:59:00Z",
                "exit_ts": "2025-01-01T00:10:00Z",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "qty": 1.0,
                "entry_price": 100.0,
                "exit_price": 101.0,
                "pnl": 1.0,
                "pnl_price": 1.0,
                "pnl_net": 0.9,
                "r_multiple_net": 0.9,
                "risk_amount": 50.0,
                "sizing_notional": 1000.0,
                "notional_est": 500.0,
                "margin_required": 500.0,
                "free_margin_post": 99_000.0,
                "equity_used": 100_000.0,
                "max_gross_notional": 2_500.0,
                "current_gross_notional": 0.0,
                "remaining_gross_notional": 2_500.0,
                "gross_cap_applied": True,
                "identity_hypothesis_id": "h1",
                "entry_state_funding_rate": 0.0001,
                "entry_decision_reason_code": "fixture",
                "execution_slippage_bps": 2.0,
                "risk_initial_r": 100.0,
                "path_mfe_r": 1.2,
                "counterfactual_hold_3bars_r": 0.4,
                "label_profitable_after_costs": True,
            }
        ]
    ).to_csv(run / "trades.csv", index=False)

    extract_experiment_dataset(experiment_root=exp, overwrite=True)
    trades = pd.read_parquet(exp / "research_data" / "trades_dataset.parquet")

    for column in [
        "identity_hypothesis_id",
        "entry_state_funding_rate",
        "entry_decision_reason_code",
        "execution_slippage_bps",
        "risk_initial_r",
        "path_mfe_r",
        "counterfactual_hold_3bars_r",
        "label_profitable_after_costs",
        "notional_est",
        "actual_notional_pct_equity",
        "requested_notional_pct_equity",
        "margin_pct_equity",
        "gross_cap_applied",
    ]:
        assert column in trades.columns
    assert trades["actual_notional_pct_equity"].iloc[0] == 0.001
    assert trades["actual_entry_notional"].iloc[0] == 100.0
    assert trades["net_pnl"].iloc[0] == 0.9
    assert trades["run_net_pnl"].iloc[0] == 1.0
    assert trades["identity_trade_id"].iloc[0] == trades["trade_id"].iloc[0]
    assert trades["identity_parameter_set_id"].iloc[0] == "params-abc"
    assert pd.Timestamp(trades["identity_ts_signal"].iloc[0]) == pd.Timestamp("2024-12-31T23:59:00Z")
