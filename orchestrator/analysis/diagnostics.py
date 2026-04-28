from __future__ import annotations

from statistics import median
from typing import Any


def _to_float(value: Any) -> float | None:
    if value in (None, "", "None"):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _vals(rows: list[dict[str, Any]], key: str) -> list[float]:
    out: list[float] = []
    for row in rows:
        v = _to_float(row.get(key))
        if v is not None:
            out.append(v)
    return out


def _dataset_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    evs = _vals(rows, "ev_r_net")
    trades = _vals(rows, "n_trades")
    robustness = _vals(rows, "robustness_score")
    positives = [v for v in evs if v > 0]
    return {
        "run_count": len(rows),
        "best_ev_r_net": max(evs) if evs else None,
        "median_ev_r_net": median(evs) if evs else None,
        "positive_run_count": len(positives),
        "positive_run_pct": (len(positives) / len(evs) * 100.0) if evs else None,
        "trade_count_median": median(trades) if trades else None,
        "trade_count_max": max(trades) if trades else None,
        "best_robustness_score": max(robustness) if robustness else None,
    }


def _promotion_candidates(rows: list[dict[str, Any]], dataset: str, min_ev: float, min_trades: int) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for row in rows:
        ev = _to_float(row.get("ev_r_net"))
        trades = _to_float(row.get("n_trades"))
        dd = _to_float(row.get("max_drawdown"))
        tail_10 = _to_float(row.get("tail_10r_count"))
        if ev is None or trades is None:
            continue
        if ev < min_ev or trades < min_trades:
            continue
        if dd is not None and abs(dd) > 3.0:
            continue
        if tail_10 is not None and tail_10 <= 0 and trades < (min_trades * 2):
            continue
        item = {
            "dataset": dataset,
            "run_id": row.get("run_id"),
            "ev_r_net": ev,
            "n_trades": trades,
            "max_drawdown": dd,
            "robustness_score": _to_float(row.get("robustness_score")),
        }
        candidates.append(item)
    candidates.sort(key=lambda x: (x.get("robustness_score") or -999.0), reverse=True)
    return candidates


def _salvage_candidates(stable_rows: list[dict[str, Any]], vol_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for dataset, rows in (("stable", stable_rows), ("volatile", vol_rows)):
        for row in rows:
            ev_gross = _to_float(row.get("ev_r_gross"))
            ev_net = _to_float(row.get("ev_r_net"))
            if ev_gross is not None and ev_net is not None and ev_gross > 0 and ev_net <= 0:
                items.append({"dataset": dataset, "run_id": row.get("run_id"), "type": "cost_drag", "detail": "gross positive, net non-positive"})

            mfe = _to_float(row.get("mfe_mean_r")) or _to_float(row.get("mfe_r"))
            realized = ev_net
            if mfe is not None and realized is not None and mfe > 0 and realized <= 0:
                items.append({"dataset": dataset, "run_id": row.get("run_id"), "type": "poor_exit_capture", "detail": "MFE positive but realized EV weak"})

    stable_best = max(_vals(stable_rows, "ev_r_net"), default=None)
    vol_best = max(_vals(vol_rows, "ev_r_net"), default=None)
    if stable_best is not None and vol_best is not None:
        if stable_best - vol_best > 0.05:
            items.append({"dataset": "stable", "type": "dataset_dominance", "detail": "stable clearly better than volatile"})
        elif vol_best - stable_best > 0.05:
            items.append({"dataset": "volatile", "type": "dataset_dominance", "detail": "volatile clearly better than stable"})

    return items


def _scrap_evidence(stable_rows: list[dict[str, Any]], vol_rows: list[dict[str, Any]], salvage: list[dict[str, Any]]) -> list[str]:
    evidence: list[str] = []
    all_evs = _vals(stable_rows, "ev_r_net") + _vals(vol_rows, "ev_r_net")
    if all_evs and max(all_evs) <= 0:
        evidence.append("all_ev_non_positive")
    tails = _vals(stable_rows, "tail_10r_count") + _vals(vol_rows, "tail_10r_count")
    if tails and max(tails) <= 0:
        evidence.append("no_right_tail")
    if not salvage:
        evidence.append("no_salvage_patterns")
    gross = _vals(stable_rows, "ev_r_gross") + _vals(vol_rows, "ev_r_gross")
    net = _vals(stable_rows, "ev_r_net") + _vals(vol_rows, "ev_r_net")
    if gross and net and max(gross) > 0 and max(net) <= 0:
        evidence.append("costs_destroy_signal")
    return evidence


def _failure_mode(comparison: dict[str, Any], scrap: list[str], salvage: list[dict[str, Any]], promotions: list[dict[str, Any]]) -> str:
    if promotions and len(promotions) >= 2:
        return "robust_positive_ev"
    if promotions and len(promotions) == 1:
        return "fragile_positive_ev"
    if any(s.get("type") == "cost_drag" for s in salvage):
        return "cost_drag"
    if any(s.get("type") == "poor_exit_capture" for s in salvage):
        return "poor_exit_capture"
    stable_best = comparison["stable"].get("best_ev_r_net")
    vol_best = comparison["volatile"].get("best_ev_r_net")
    if stable_best is not None and vol_best is not None and abs(stable_best - vol_best) > 0.05:
        return "wrong_regime"
    med_trades = comparison["stable"].get("trade_count_median") or comparison["volatile"].get("trade_count_median")
    if med_trades is not None and med_trades < 30:
        return "insufficient_sample"
    if "all_ev_non_positive" in scrap:
        return "entry_no_edge"
    return "inconclusive"


def compute_diagnostics(
    stable_rows: list[dict[str, Any]],
    vol_rows: list[dict[str, Any]],
    *,
    promotion_min_ev: float,
    promotion_min_trades: int,
) -> dict[str, Any]:
    comparison = {
        "stable": _dataset_summary(stable_rows),
        "volatile": _dataset_summary(vol_rows),
    }
    promotion = _promotion_candidates(stable_rows, "stable", promotion_min_ev, promotion_min_trades)
    promotion.extend(_promotion_candidates(vol_rows, "volatile", promotion_min_ev, promotion_min_trades))
    salvage = _salvage_candidates(stable_rows, vol_rows)
    scrap = _scrap_evidence(stable_rows, vol_rows, salvage)
    failure_mode = _failure_mode(comparison, scrap, salvage, promotion)

    return {
        "dataset_comparison": comparison,
        "promotion_candidates": promotion,
        "salvage_candidates": salvage,
        "scrap_evidence": scrap,
        "failure_mode": failure_mode,
    }
