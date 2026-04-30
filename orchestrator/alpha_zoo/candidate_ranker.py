from __future__ import annotations
import math


def rank_candidates(candidates: list[dict]) -> list[dict]:
    for c in candidates:
        m = c.setdefault("zoo_metadata", {})
        missing = []
        p, t, cost = c.get("performance", {}), c.get("tail", {}), c.get("cost", {})
        def g(src, k):
            v = src.get(k)
            if v is None:
                missing.append(k)
                return 0.0
            return float(v)
        rank = 3.0*g(p,"ev_r_net") + 0.75*math.log1p(max(g(p,"n_trades"),0)) + 0.75*g(t,"tail_5r_rate") + 1.25*g(t,"tail_10r_rate") + 0.5*g(p,"payoff_ratio") - 1.0*abs(g(p,"max_drawdown")) - 0.5*abs(g(cost,"avg_cost_drag_r"))
        lucky_penalty = 0.0
        tail5 = t.get("tail_5r_count") or 0
        if (p.get("ev_r_net") or 0)>0 and tail5<=1 and (p.get("max_r",0) or 0) > 4 * ((p.get("p95_r",0) or 0)+1e-6):
            lucky_penalty = 1.5
            m["fragile_one_lucky_trade"] = True
        m["rank_score"] = rank
        m["promotion_score"] = rank - lucky_penalty
        m["missing_score_components"] = sorted(set(missing))
    return sorted(candidates, key=lambda x: x.get("zoo_metadata",{}).get("promotion_score",-999), reverse=True)
