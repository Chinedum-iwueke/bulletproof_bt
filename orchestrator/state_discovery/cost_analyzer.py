from __future__ import annotations

import pandas as pd


def detect_cost_killed_states(metrics: pd.DataFrame) -> pd.DataFrame:
    if metrics.empty:
        return metrics
    gross = pd.to_numeric(metrics.get("ev_r_gross"), errors="coerce")
    net = pd.to_numeric(metrics.get("ev_r_net"), errors="coerce")
    cost = pd.to_numeric(metrics.get("avg_cost_drag_r"), errors="coerce")
    mask = (gross > 0) & (net < 0) & (cost > 0)
    return metrics.loc[mask].copy()


def detect_exit_failure_states(metrics: pd.DataFrame) -> pd.DataFrame:
    if metrics.empty:
        return metrics
    mfe = pd.to_numeric(metrics.get("avg_mfe_r"), errors="coerce")
    eff = pd.to_numeric(metrics.get("avg_exit_efficiency"), errors="coerce")
    mask = (mfe >= 1.5) & (eff < 0.3)
    return metrics.loc[mask].copy()
