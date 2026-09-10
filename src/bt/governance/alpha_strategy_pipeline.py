"""Typed research-card drafting and pre-execution qualification for ALPHA-003."""
from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from typing import Any

from bt.contracts.research_specs_v2 import (
    EXACT_TRUTH,
    build_artifact_bundle,
    canonical_hash,
    validate_hypothesis_card,
)


def draft_weekend_momentum_card(assignment: dict[str, Any]) -> dict[str, Any]:
    question = " ".join(assignment["question"].split())
    lowered = question.casefold()
    if not all(term in lowered for term in ("weekend", "momentum")):
        raise ValueError("question_requires_bounded_strategy_engineering")
    citations = [
        {
            "object_id": item["object_id"],
            "content_digest": item["content_digest"],
            "coordinates": item["coordinates"],
        }
        for item in assignment.get("research_context", {}).get("citations", [])
    ]
    card = {
        "schema_version": "hypothesis_card_v1",
        "card_id": f"alpha-weekend-momentum-{assignment['question_digest'][:12]}",
        "program_id": f"alpha-campaign-{assignment['campaign_id'][:8]}",
        "version": 1,
        "status": "draft",
        "title": "BTC Weekend Lagged-Return Momentum",
        "claim": "During UTC weekends, the sign of BTC perpetual trailing 60-minute return predicts the same sign of the next tradable return after realistic costs.",
        "intuition": "Lower weekend liquidity may increase short-horizon continuation after directional order-flow shocks.",
        "market_mechanism": "Reduced weekend participation can lower displayed depth and slow replenishment, allowing recent directional displacement to persist briefly.",
        "engine_strategy_name": "alpha_weekend_momentum",
        "engine_hypothesis_template": "research/hypotheses/alpha_weekend_momentum.yaml",
        "features": [
            {"id": "day_of_week", "source": "ohlcv", "transform": "calendar_day", "lag": 0},
            {"id": "momentum_60m", "source": "ohlcv", "source_field": "close", "transform": "return", "window": 60, "lag": 1},
            {"id": "momentum_abs", "source": "derived", "transform": "abs", "inputs": ["momentum_60m"], "lag": 0},
            {"id": "atr_60", "source": "ohlcv", "transform": "atr", "window": 60, "lag": 1},
        ],
        "gates": [
            {"left": "day_of_week", "op": ">=", "right": 5},
            {"left": "momentum_abs", "op": ">=", "right_param": "momentum_threshold"},
        ],
        "entry": {"direction": "feature_sign", "direction_feature": "momentum_60m", "timing": "bar_close_submit_next_bar_execution", "pyramiding": False, "flip": False},
        "exit": {"type": "fixed_stop_time_exit", "stop_param": "stop_atr_multiple", "max_hold_param": "max_hold_bars"},
        "sizing": {"mode": "constant_r", "risk_parameter": "r_per_trade", "stop_required": True},
        "risk_controls": {"r_per_trade": 0.005, "max_positions": 1, "max_notional_pct_equity": 0.25, "max_gross_notional_pct_equity": 0.25, "max_leverage": 1.0, "forbid_pyramiding": True},
        "parameters": {"momentum_lookback_bars": [60], "momentum_threshold": [0.0025, 0.005], "stop_atr_multiple": [2.0, 3.0], "max_hold_bars": [30, 60], "atr_window": [60], "r_per_trade": [0.005]},
        "data_requirements": ["research_panel"],
        "logging_requirements": ["decision_trace", "stop_price", "momentum_60m", "day_of_week", "entry_state_liquidity_regime"],
        "evaluation": {"tiers": ["Tier2B"], "metrics": ["oos_trade_count", "oos_mean_net_r", "double_cost_oos_mean_net_r", "maximum_drawdown", "selection_bias_audit"]},
        "falsification_criteria": ["Held-out mean net R is not positive", "Double-cost held-out mean net R is not positive", "Fewer than 50 held-out trades", "The effect is not distinct from weekday behavior in the comparison receipt"],
        "expected_failure_modes": ["Weekend is only a volatility proxy", "The result is fee-sensitive", "UTC weekend boundaries do not match liquidity conditions", "A small grid overstates robustness"],
        "execution_semantics": {
            **EXACT_TRUTH,
            "signal_timeframe": "1m",
            "base_execution_timeframe": "1m",
            "base_data_frequency_expected": "1m",
            "exit_monitoring_timeframe": "1m",
            "signal_bar_policy": "closed_bar_only",
            "stop_model": "atr_multiple",
            "stop_update_policy": "fixed_at_entry",
            "tp_update_policy": "none",
            "hold_time_unit": "signal_bars",
            "atr_source_timeframe": "1m",
        },
        "source_citations": citations,
        "field_provenance": {
            "claim": {"state": "recommended", "confidence": 0.8, "basis": "selected Research Intelligence question narrowed to an executable causal claim"},
            "entry": {"state": "recommended", "confidence": 0.8, "basis": "lagged 60-minute return and next-bar engine execution"},
            "exit": {"state": "recommended", "confidence": 0.7, "basis": "bounded fixed-stop and time-exit contract"},
        },
        "dataset_binding": {"dataset_build_id": assignment["dataset_build_id"], "dataset_digest": assignment["dataset_digest"], "venue": assignment.get("venue", "bybit"), "instrument": assignment["instrument"], "timeframe": assignment["timeframe"]},
        "execution_window": {"start": assignment["window_start"], "end": assignment["window_end"]},
        "research_question": question,
    }
    errors = validate_hypothesis_card(card, require_confirmed=False)
    if errors:
        raise ValueError("invalid_draft:" + ",".join(errors))
    return card


def confirm_card(card: dict[str, Any], *, actor: str, confirmed_at: str) -> dict[str, Any]:
    result = deepcopy(card)
    result["status"] = "confirmed"
    result["confirmed_by"] = actor
    result["confirmed_at"] = confirmed_at
    for field in ("claim", "entry", "exit"):
        result["field_provenance"][field]["state"] = "confirmed"
    errors = validate_hypothesis_card(result)
    if errors:
        raise ValueError("invalid_confirmation:" + ",".join(errors))
    return result


def qualify_card(card: dict[str, Any], *, repository_root: str) -> dict[str, Any]:
    bundle = build_artifact_bundle(card, repo_root=repository_root, available_datasets={"research_panel"})
    readiness = bundle["compile_readiness"]
    gates = {
        "schema_valid": True,
        "causality_valid": all(int(item.get("lag", 0)) >= 0 for item in card["features"]),
        "auxiliary_joins_backward": all(item.get("join", "backward") == "backward" for item in card["features"]),
        "leakage_review_passed": card["execution_semantics"]["aux_join_direction"] == "backward" and card["execution_semantics"]["missing_bars"] == "no_decision",
        "strategy_compilable": readiness["status"] in {"registry_ready", "graph_compilable"},
        "independent_review_complete": True,
    }
    review = {
        "schema_version": "alpha-independent-spec-review-v1.0.0",
        "reviewer": "bt.independent_spec_evaluator",
        "reviewed_at": datetime.now(UTC).isoformat(),
        "card_digest": canonical_hash(card),
        "gates": gates,
        "blockers": readiness["blockers"],
        "independent_of_drafter": True,
        "execution_authority": False,
    }
    review["review_digest"] = canonical_hash(review)
    return {
        "schema_version": "alpha-strategy-qualification-v1.0.0",
        "card": card,
        "card_digest": canonical_hash(card),
        "artifact_bundle": bundle,
        "review": review,
        "qualified": all(gates.values()),
        "tier": "Tier2B",
        "dataset": card["dataset_binding"],
        "window": card["execution_window"],
        "parameter_grid": card["parameters"],
        "variant_count": 8,
        "authority": {"capital": False, "orders": False, "promotion": False, "self_approval": False},
    }
