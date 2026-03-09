"""Standardized hypothesis-contract runner."""
from __future__ import annotations

from typing import Any, Callable

from bt.hypotheses.contract import HypothesisContract
from bt.hypotheses.exceptions import MissingRequiredTierError
from bt.hypotheses.logging import make_log_row


def run_hypothesis_contract(
    contract: HypothesisContract,
    *,
    executor: Callable[[dict[str, Any], str], dict[str, Any]],
    symbol: str,
    timeframe: str,
    start_ts: str,
    end_ts: str,
    available_tiers: set[str],
    execution_model_name: str = "engine_default",
<<<<<<< codex/implement-engine-capabilities-for-hypothesis-testing-jqv1dd
    execution_workflow: str = "all_tiers",
    promotion_predicate: Callable[[dict[str, Any]], bool] | None = None,
) -> list[dict[str, Any]]:
    """Execute hypothesis variants across required tiers with strict tier enforcement.

    Supported workflows:
    - ``all_tiers``: execute every required tier for every variant.
    - ``sequential``: execute required tiers in order and only promote variants to
      subsequent tiers if ``promotion_predicate`` returns ``True`` on the prior
      tier result. Non-promoted tiers are logged as explicit ``skipped`` rows.
    """
    missing = [tier for tier in contract.required_tiers() if tier not in available_tiers]
    if missing:
        raise MissingRequiredTierError(f"missing required tiers: {missing}")
    if execution_workflow not in {"all_tiers", "sequential"}:
        raise ValueError("execution_workflow must be one of: all_tiers, sequential")
    if execution_workflow == "sequential" and promotion_predicate is None:
        raise ValueError("promotion_predicate is required when execution_workflow='sequential'")

    rows: list[dict[str, Any]] = []
    for spec in contract.to_run_specs():
        should_continue = True
        for tier_index, tier in enumerate(contract.required_tiers()):
            if execution_workflow == "sequential" and tier_index > 0 and not should_continue:
                base = {
                    "run_id": f"{spec['hypothesis_id']}::{spec['grid_id']}::{tier}",
                    "hypothesis_id": spec["hypothesis_id"],
                    "title": spec["title"],
                    "contract_version": spec["contract_version"],
                    "grid_id": spec["grid_id"],
                    "config_hash": spec["config_hash"],
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "tier": tier,
                    "execution_model_name": execution_model_name,
                    "params_json": spec["params"],
                    "indicators_json": list(contract.required_indicators()),
                    "gates_json": list(contract.schema.gates),
                }
                rows.append(
                    make_log_row(
                        base,
                        {},
                        status="skipped",
                        failure_reason="variant_not_promoted_from_prior_tier",
                    )
                )
                continue

=======
) -> list[dict[str, Any]]:
    """Execute all variants across all required tiers with strict enforcement."""
    missing = [tier for tier in contract.required_tiers() if tier not in available_tiers]
    if missing:
        raise MissingRequiredTierError(f"missing required tiers: {missing}")

    rows: list[dict[str, Any]] = []
    for spec in contract.to_run_specs():
        for tier in contract.required_tiers():
>>>>>>> main
            result = executor(spec, tier)
            base = {
                "run_id": f"{spec['hypothesis_id']}::{spec['grid_id']}::{tier}",
                "hypothesis_id": spec["hypothesis_id"],
                "title": spec["title"],
                "contract_version": spec["contract_version"],
                "grid_id": spec["grid_id"],
                "config_hash": spec["config_hash"],
                "symbol": symbol,
                "timeframe": timeframe,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "tier": tier,
                "execution_model_name": execution_model_name,
                "params_json": spec["params"],
                "indicators_json": list(contract.required_indicators()),
                "gates_json": list(contract.schema.gates),
            }
            rows.append(make_log_row(base, result))
<<<<<<< codex/implement-engine-capabilities-for-hypothesis-testing-jqv1dd
            if execution_workflow == "sequential" and tier_index < len(contract.required_tiers()) - 1:
                should_continue = bool(promotion_predicate(result))
=======
>>>>>>> main
    return rows
