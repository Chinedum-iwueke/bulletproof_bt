from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from bt.experiments.dataset_builder import extract_experiment_dataset


def test_extract_preserves_phase9_columns(tmp_path: Path) -> None:
    exp = tmp_path / "exp"
    run = exp / "runs" / "run_1"
    run.mkdir(parents=True, exist_ok=True)

    (run / "config_used.yaml").write_text("strategy:\n  name: test\n", encoding="utf-8")
    (run / "performance.json").write_text(json.dumps({"net_pnl": 1.0, "trade_count": 1}), encoding="utf-8")
    (run / "run_status.json").write_text(json.dumps({"status": "PASS"}), encoding="utf-8")

    trades = pd.DataFrame(
        [
            {
                "entry_ts": "2024-01-01T00:00:00Z",
                "exit_ts": "2024-01-01T00:15:00Z",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "qty": 1,
                "entry_price": 100,
                "exit_price": 101,
                "pnl": 1,
                "pnl_price": 1,
                "pnl_net": 1,
                "r_multiple_net": 0.5,
                "mfe_r": 1.2,
                "mae_r": 0.2,
                "identity_run_id": "run_x",
                "entry_state_csi_pctile": 0.8,
                "entry_decision_reason_code": "test_reason",
                "execution_actual_entry_price": 100,
                "path_mfe_r": 1.2,
                "counterfactual_hold_3bars_r": 0.3,
                "label_profitable_after_costs": True,
            }
        ]
    )
    trades.to_csv(run / "trades.csv", index=False)

    extract_experiment_dataset(experiment_root=exp, overwrite=True)
    out = pd.read_parquet(exp / "research_data" / "trades_dataset.parquet")

    assert "entry_state_csi_pctile" in out.columns
    assert "entry_decision_reason_code" in out.columns
    assert "execution_actual_entry_price" in out.columns
    assert "path_mfe_r" in out.columns
    assert "counterfactual_hold_3bars_r" in out.columns
    assert "label_profitable_after_costs" in out.columns
