from __future__ import annotations

import pandas as pd

from .state_bucket_analyzer import _bucket_numeric, compute_bucket_metrics


JOINT_STATE_PAIRS = [
    ("entry_state_csi_pctile", "entry_state_vol_pctile", "csi_x_vol"),
    ("entry_state_csi_pctile", "entry_state_spread_proxy_pctile", "csi_x_liquidity"),
    ("entry_state_vol_pctile", "entry_state_spread_proxy_pctile", "vol_x_liquidity"),
    ("entry_state_tr_over_atr", "entry_state_spread_proxy_pctile", "displacement_x_liquidity"),
    ("entry_decision_setup_class", "entry_state_vol_regime", "setup_x_vol"),
    ("entry_decision_setup_class", "entry_state_csi_bucket", "setup_x_csi"),
    ("entry_state_trend_state", "entry_state_vol_regime", "trend_x_vol"),
    ("entry_state_trend_state", "entry_state_liquidity_regime", "trend_x_liquidity"),
]


def analyze_joint_state_variables(trades: pd.DataFrame, *, min_bucket_trades: int) -> tuple[pd.DataFrame, list[str]]:
    out: list[pd.DataFrame] = []
    missing: list[str] = []
    for left, right, name in JOINT_STATE_PAIRS:
        if left not in trades.columns or right not in trades.columns:
            missing.append(name)
            continue
        l = trades[left]
        r = trades[right]
        lb = _bucket_numeric(l, left) if pd.api.types.is_numeric_dtype(l) else l.astype("object")
        rb = _bucket_numeric(r, right) if pd.api.types.is_numeric_dtype(r) else r.astype("object")
        key = f"__joint__{name}"
        trades[key] = lb.astype(str) + "__" + rb.astype(str)
        out.append(compute_bucket_metrics(trades, state_variable=name, bucket_col=key, min_bucket_trades=min_bucket_trades))
    if not out:
        return pd.DataFrame(), missing
    return pd.concat(out, ignore_index=True), missing
