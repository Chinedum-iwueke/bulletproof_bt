from __future__ import annotations

from typing import Any

ALLOWED_VERDICTS = [
    "SCRAP",
    "REFINE_ENTIRE_GRID",
    "REFINE_ENTRY",
    "REFINE_EXIT",
    "REFINE_GATE",
    "REFINE_TIMEFRAME",
    "REFINE_UNIVERSE",
    "ADD_STATE_FILTER",
    "PROMOTE_SINGLE_RUN_TIER3",
    "PROMOTE_MULTIPLE_RUNS_TIER3",
    "PROMOTE_FAMILY_TIER3",
    "PROMOTE_FORWARD_TEST",
    "ADD_TO_ALPHA_ZOO",
    "INCONCLUSIVE_NEEDS_MORE_DATA",
]


def compute_preliminary_verdict(diagnostics: dict[str, Any], *, min_trades: int) -> dict[str, Any]:
    promotions = diagnostics.get("promotion_candidates", [])
    salvage = diagnostics.get("salvage_candidates", [])
    scrap = diagnostics.get("scrap_evidence", [])
    failure_mode = diagnostics.get("failure_mode")

    if len(promotions) >= 3:
        verdict = "PROMOTE_FAMILY_TIER3"
        reason = "multiple robust promotion candidates"
    elif len(promotions) == 2:
        verdict = "PROMOTE_MULTIPLE_RUNS_TIER3"
        reason = "at least two promotion candidates"
    elif len(promotions) == 1:
        candidate = promotions[0]
        trades = candidate.get("n_trades") or 0
        if trades >= (min_trades * 2):
            verdict = "PROMOTE_SINGLE_RUN_TIER3"
            reason = "single strong candidate with adequate sample"
        else:
            verdict = "INCONCLUSIVE_NEEDS_MORE_DATA"
            reason = "single candidate but weak sample"
    elif any(item.get("type") == "poor_exit_capture" for item in salvage):
        verdict = "REFINE_EXIT"
        reason = "salvage indicates exit capture issues"
    elif any(item.get("type") == "cost_drag" for item in salvage):
        verdict = "REFINE_TIMEFRAME"
        reason = "cost drag dominates net performance"
    elif any(item.get("type") == "dataset_dominance" for item in salvage):
        verdict = "REFINE_UNIVERSE"
        reason = "one dataset/regime dominates"
    elif "all_ev_non_positive" in scrap and "no_salvage_patterns" in scrap:
        verdict = "SCRAP"
        reason = "all runs non-positive without salvage structure"
    elif failure_mode == "insufficient_sample":
        verdict = "INCONCLUSIVE_NEEDS_MORE_DATA"
        reason = "sample size insufficient"
    else:
        verdict = "REFINE_ENTIRE_GRID"
        reason = "mixed evidence; full-grid refinement suggested"

    return {
        "preliminary_verdict": verdict,
        "preliminary_reason": reason,
        "allowed_verdicts": ALLOWED_VERDICTS,
    }
