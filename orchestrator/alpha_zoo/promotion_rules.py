from __future__ import annotations


def apply_promotion_rules(candidates: list[dict], min_trades: int = 50) -> list[dict]:
    for c in candidates:
        z = c.setdefault("zoo_metadata", {})
        if z.get("candidate_status") == "REDUNDANT":
            continue
        p, t = c.get("performance", {}), c.get("tail", {})
        ev = p.get("ev_r_net") or 0
        n = p.get("n_trades") or 0
        net_killed = (p.get("ev_r_gross") or 0) > 0 and ev <= 0
        if net_killed:
            z["candidate_status"] = "REFINE"
            z["recommended_action"] = "REFINE"
        elif ev > 0 and n >= min_trades and not z.get("fragile_one_lucky_trade"):
            z["candidate_status"] = "PROMOTE_TIER3"
            z["recommended_action"] = "PROMOTE_TIER3"
        elif ev > 0 and n < min_trades:
            z["candidate_status"] = "WATCHLIST"
            z["recommended_action"] = "WATCHLIST"
        elif t.get("tail_5r_count", 0) > 0 or c.get("state_profile", {}).get("positive_state_conditions"):
            z["candidate_status"] = "KEEP_RESEARCH"
            z["recommended_action"] = "KEEP_RESEARCH"
        else:
            z["candidate_status"] = "REJECTED"
            z["recommended_action"] = "REJECTED"
    return candidates
