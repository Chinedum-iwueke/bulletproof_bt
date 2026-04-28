"""Trade row enrichment for path/counterfactual/labels."""
from __future__ import annotations

from typing import Any


def enrich_trade_row(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    r_net = _f(out.get("r_net", out.get("realized_r_net", out.get("r_multiple_net"))))
    r_gross = _f(out.get("r_gross", out.get("realized_r_gross", out.get("r_multiple_gross"))))
    mfe_r = _f(out.get("path_mfe_r", out.get("mfe_r")))
    mae_r = _f(out.get("path_mae_r", out.get("mae_r")))
    bars_held = _i(out.get("path_bars_held", out.get("holding_period_bars_signal")))

    out["path_mfe_r"] = mfe_r
    out["path_mae_r"] = mae_r
    out["path_bars_held"] = bars_held
    out["path_holding_time_minutes"] = _f(out.get("path_holding_time_minutes", out.get("holding_period_minutes")))

    for k in (1, 3, 5, 10, 30):
        out.setdefault(f"path_r_{k}bar" if k == 1 else f"path_r_{k}bars", None)
        out[f"counterfactual_hold_{k}bar_r" if k == 1 else f"counterfactual_hold_{k}bars_r"] = out.get(
            f"path_r_{k}bar" if k == 1 else f"path_r_{k}bars"
        )

    out["path_touched_2r"] = bool(mfe_r is not None and mfe_r >= 2.0)
    out["path_touched_3r"] = bool(mfe_r is not None and mfe_r >= 3.0)
    out["path_touched_5r"] = bool(mfe_r is not None and mfe_r >= 5.0)
    out["path_touched_10r"] = bool(mfe_r is not None and mfe_r >= 10.0)

    out["counterfactual_exit_efficiency_realized_over_mfe"] = (r_net / mfe_r) if (r_net is not None and mfe_r and mfe_r > 0) else None
    cost_drag = (r_gross - r_net) if (r_gross is not None and r_net is not None) else None
    out["cost_drag_r"] = cost_drag
    out["counterfactual_cost_drag_r"] = cost_drag
    out["counterfactual_fee_drag_r"] = _f(out.get("fee_drag_r"))
    out["counterfactual_slippage_drag_r"] = _f(out.get("slippage_drag_r"))
    out["counterfactual_spread_drag_r"] = _f(out.get("spread_drag_r"))

    out["label_reached_3r"] = bool(mfe_r is not None and mfe_r >= 3.0)
    out["label_reached_5r"] = bool(mfe_r is not None and mfe_r >= 5.0)
    out["label_tail_trade_ge_10r"] = bool(mfe_r is not None and mfe_r >= 10.0)
    out["label_closed_positive"] = bool(r_net is not None and r_net > 0)
    out["label_closed_negative"] = bool(r_net is not None and r_net < 0)
    out["label_profitable_after_costs"] = bool(r_net is not None and r_net > 0)
    out["label_success_1r_before_neg_1r"] = bool(mfe_r is not None and mfe_r >= 1.0 and (mae_r is None or mae_r < 1.0))
    out["label_success_2r_before_neg_1r"] = bool(mfe_r is not None and mfe_r >= 2.0 and (mae_r is None or mae_r < 1.0))

    if mfe_r is not None and mae_r is not None and mfe_r >= 2 and mae_r < 1:
        out["label_entry_quality_bucket"] = "high"
    elif mfe_r is not None and mfe_r >= 1:
        out["label_entry_quality_bucket"] = "medium"
    else:
        out["label_entry_quality_bucket"] = "low"

    eff = out["counterfactual_exit_efficiency_realized_over_mfe"]
    if eff is None:
        out["label_exit_efficiency_bucket"] = None
    elif eff >= 0.7:
        out["label_exit_efficiency_bucket"] = "excellent"
    elif eff >= 0.4:
        out["label_exit_efficiency_bucket"] = "decent"
    elif eff >= 0.1:
        out["label_exit_efficiency_bucket"] = "poor"
    else:
        out["label_exit_efficiency_bucket"] = "failed"
    out.setdefault("label_structure_class", out.get("entry_decision_setup_class"))
    out.setdefault("label_market_regime_class", out.get("entry_state_vol_regime"))
    return out


def _f(v: Any) -> float | None:
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def _i(v: Any) -> int | None:
    try:
        return None if v is None else int(v)
    except (TypeError, ValueError):
        return None
