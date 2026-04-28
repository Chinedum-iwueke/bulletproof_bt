from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from bt.analysis.ev_by_bucket import run_structural_bucket_analysis


def test_missing_fields_emit_diagnostic(tmp_path: Path) -> None:
    df = pd.DataFrame({"r_net": [0.1, -0.2, 0.3]})
    outputs = run_structural_bucket_analysis(df, tmp_path, min_trades=1)
    assert "ev_by_bucket_missing_fields" in outputs
    payload = json.loads((tmp_path / "ev_by_bucket_missing_fields.json").read_text(encoding="utf-8"))
    assert "csi" in payload["missing"]


def test_joint_bucket_outputs_multiple_buckets(tmp_path: Path) -> None:
    df = pd.DataFrame(
        {
            "r_net": [1, -1, 2, -0.5, 0.2, 0.4],
            "entry_state_csi_pctile": [0.2, 0.8, 0.9, 0.55, 0.65, 0.4],
            "entry_state_vol_pctile": [0.2, 0.8, 0.95, 0.5, 0.35, 0.1],
            "entry_state_spread_proxy_pctile": [0.2, 0.7, 0.99, 0.85, 0.4, 0.55],
            "entry_state_tr_over_atr": [0.8, 1.2, 2.4, 1.6, 0.9, 1.1],
        }
    )
    run_structural_bucket_analysis(df, tmp_path, min_trades=1)
    joint = pd.read_csv(tmp_path / "ev_by_bucket_joint_csi_vol.csv")
    keys = set(joint["bucket_key"].dropna())
    assert len([k for k in keys if k != "overall_all_trades"]) > 1
