from __future__ import annotations

from typing import Any


def _to_float(value: Any) -> float | None:
    if value in (None, "", "None"):
        return None
    try:
        return float(value)
    except Exception:
        return None


def score_runs(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    scored: list[dict[str, Any]] = []
    for row in rows:
        r = dict(row)
        notes: list[str] = []

        ev_net = _to_float(r.get("ev_r_net"))
        ev_gross = _to_float(r.get("ev_r_gross"))
        n_trades = _to_float(r.get("n_trades"))
        win_rate = _to_float(r.get("win_rate"))
        max_drawdown = _to_float(r.get("max_drawdown"))
        avg_r_win = _to_float(r.get("avg_r_win"))
        avg_r_loss = _to_float(r.get("avg_r_loss"))
        tail_5r_count = _to_float(r.get("tail_5r_count"))
        tail_10r_count = _to_float(r.get("tail_10r_count"))

        drag = None
        if ev_gross is not None and ev_net is not None:
            drag = ev_gross - ev_net
            r["gross_to_net_drag"] = drag
            notes.append("gross_to_net_drag")

        payoff_ratio = None
        if avg_r_win is not None and avg_r_loss is not None and avg_r_loss != 0:
            payoff_ratio = avg_r_win / abs(avg_r_loss)
            r["payoff_ratio"] = payoff_ratio
            notes.append("payoff_ratio")

        score = 0.0
        available = []

        if ev_net is not None:
            score += 3.0 * ev_net
            available.append("ev_r_net")
        if tail_5r_count is not None:
            score += 0.5 * tail_5r_count
            available.append("tail_5r_count")
        if tail_10r_count is not None:
            score += 1.0 * tail_10r_count
            available.append("tail_10r_count")
        if payoff_ratio is not None:
            score += 0.5 * payoff_ratio
            available.append("payoff_ratio")
        if max_drawdown is not None:
            score -= 1.5 * abs(max_drawdown)
            available.append("max_drawdown")
        if drag is not None:
            score -= 0.25 * max(drag, 0.0)
            available.append("cost_drag_penalty")
        if n_trades is not None:
            score += min(n_trades / 100.0, 1.0)
            available.append("sample_size_bonus")

        if win_rate is not None:
            available.append("win_rate")

        r["robustness_score"] = score
        r["score_components_available"] = available
        r["score_notes"] = notes
        scored.append(r)

    return scored
