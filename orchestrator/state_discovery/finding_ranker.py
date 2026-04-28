from __future__ import annotations

from math import log1p

import pandas as pd


def _safe(v: float | None) -> float:
    try:
        return float(v) if v is not None else 0.0
    except Exception:
        return 0.0


def classify_and_rank_findings(metrics: pd.DataFrame, *, min_trades: int, include_negative_findings: bool = True) -> pd.DataFrame:
    if metrics.empty:
        return metrics
    df = metrics.copy()
    kinds: list[str] = []
    scores: list[float] = []
    for _, row in df.iterrows():
        n = int(row.get("n_trades", 0) or 0)
        ev_net = _safe(row.get("ev_r_net"))
        ev_gross = _safe(row.get("ev_r_gross"))
        tail5 = _safe(row.get("tail_5r_rate"))
        tail10 = _safe(row.get("tail_10r_rate"))
        cost = _safe(row.get("avg_cost_drag_r"))
        mfe = _safe(row.get("avg_mfe_r"))
        eff = _safe(row.get("avg_exit_efficiency"))

        positive_edge_score = ev_net * log1p(max(n, 1)) + 0.5 * _safe(row.get("tail_3r_rate")) + 1.0 * tail5 - 0.5 * abs(cost)
        tail_score = tail5 * log1p(max(n, 1)) + tail10 * 2.0 + mfe
        cost_killed_score = max(ev_gross - ev_net, 0) + cost + max(mfe - ev_net, 0)

        if n < min_trades:
            kind = "FRAGILE_STATE"
            score = positive_edge_score
        elif ev_gross > 0 and ev_net < 0 and cost > 0:
            kind = "COST_KILLED_STATE"
            score = cost_killed_score
        elif mfe >= 1.5 and eff < 0.3:
            kind = "EXIT_FAILURE_STATE"
            score = mfe - eff
        elif tail5 >= 0.05 or tail10 >= 0.01:
            kind = "TAIL_GENERATION_STATE"
            score = tail_score
        elif ev_net >= 0.05:
            kind = "POSITIVE_EDGE_STATE"
            score = positive_edge_score
        elif ev_net <= -0.05:
            kind = "NEGATIVE_EDGE_STATE"
            score = abs(ev_net) * log1p(max(n, 1))
        else:
            kind = "STRUCTURELESS"
            score = 0.0

        kinds.append(kind)
        scores.append(score)

    df["finding_type"] = kinds
    df["finding_score"] = scores
    if not include_negative_findings:
        df = df[df["finding_type"] != "NEGATIVE_EDGE_STATE"]
    return df.sort_values(["finding_score", "n_trades"], ascending=[False, False]).reset_index(drop=True)
