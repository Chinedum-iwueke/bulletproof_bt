"""Standardized hypothesis-contract runner."""
from __future__ import annotations

from typing import Any, Callable

from bt.hypotheses.contract import HypothesisContract
from bt.hypotheses.exceptions import MissingRequiredTierError
from bt.hypotheses.logging import make_log_row


def resolve_phase_tiers(contract: HypothesisContract, phase: str) -> tuple[str, ...]:
    required = contract.required_tiers()
    if phase == "tier2":
        return tuple(t for t in required if t == "Tier2")
    if phase == "tier3":
        return tuple(t for t in required if t == "Tier3")
    if phase == "validate":
        return required
    raise ValueError("phase must be one of: tier2, tier3, validate")


def validation_status(contract: HypothesisContract, observed_tiers: set[str]) -> str:
    missing = [tier for tier in contract.required_tiers() if tier not in observed_tiers]
    return "validated" if not missing else "incomplete"


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
    phase: str = "validate",
) -> list[dict[str, Any]]:
    tiers_to_run = resolve_phase_tiers(contract, phase)
    if not tiers_to_run:
        raise MissingRequiredTierError(f"phase '{phase}' did not resolve to any required tiers")
    missing = [tier for tier in tiers_to_run if tier not in available_tiers]
    if missing:
        raise MissingRequiredTierError(f"missing required tiers for phase '{phase}': {missing}")

    rows: list[dict[str, Any]] = []
    for spec in contract.to_run_specs():
        for tier in tiers_to_run:
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
                "validation_status": validation_status(contract, {tier}),
            }
            rows.append(make_log_row(base, result))
    return rows
