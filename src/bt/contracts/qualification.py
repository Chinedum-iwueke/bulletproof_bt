"""Deterministic backtest-to-demo qualification and portable strategy IR."""
from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json
from typing import Any


QUALIFICATION_VERSION = "deployment_qualification_v1"
PORTABLE_IR_VERSION = "portable_strategy_ir_v1"


def _hash(value: Any) -> str:
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def normalize_portable_ir(strategy_spec: dict[str, Any]) -> dict[str, Any]:
    required = ("strategy_spec_id", "feature_graph", "gate_graph", "entry", "exit_state_machine", "parameter_defaults", "execution_semantics", "truth_contract")
    missing = [key for key in required if key not in strategy_spec]
    if missing:
        raise ValueError(f"portable_ir_missing:{','.join(missing)}")
    return {
        "schema_version": PORTABLE_IR_VERSION,
        "strategy_spec_id": strategy_spec["strategy_spec_id"],
        "strategy_spec_hash": _hash(strategy_spec),
        "features": deepcopy(strategy_spec["feature_graph"]),
        "gates": deepcopy(strategy_spec["gate_graph"]),
        "entry": deepcopy(strategy_spec["entry"]),
        "exit": deepcopy(strategy_spec["exit_state_machine"]),
        "parameters": deepcopy(strategy_spec["parameter_defaults"]),
        "parameter_grid": deepcopy(strategy_spec.get("parameter_grid", {})),
        "data_requirements": sorted(set(strategy_spec.get("data_requirements", []))),
        "risk_policy": deepcopy(strategy_spec.get("risk_policy", {})),
        "execution_semantics": deepcopy(strategy_spec["execution_semantics"]),
        "truth_contract": deepcopy(strategy_spec["truth_contract"]),
        "compiler_version": strategy_spec.get("compiler_version"),
    }


def classify_targets(portable_ir: dict[str, Any]) -> dict[str, Any]:
    private_sources = sorted({str(item.get("source")) for item in portable_ir["features"] if item.get("source") not in {"ohlcv", "derived"}})
    unsupported_datasets = sorted(f"dataset:{item}" for item in portable_ir.get("data_requirements", []) if item != "ohlcv")
    unsupported_transforms = sorted({str(item.get("transform")) for item in portable_ir["features"] if item.get("transform") not in {"identity", "return", "sma", "ema", "atr", "true_range", "true_range_over", "half_range_over_close"}})
    unresolved = []
    semantics = portable_ir.get("execution_semantics", {})
    if semantics.get("signal_bar_policy", "closed_bar_only") != "closed_bar_only":
        unresolved.append("signal_bar_not_closed_only")
    if semantics.get("interpolation") != "forbidden":
        unresolved.append("interpolation_not_forbidden")
    if semantics.get("intrabar_semantics") not in {None, "disabled"}:
        unresolved.append("unresolved_intrabar_semantics")
    if semantics.get("higher_timeframe_data") is True and semantics.get("higher_timeframe_confirmation") != "confirmed_closed_bar":
        unresolved.append("unconfirmed_higher_timeframe_data")
    if portable_ir.get("risk_policy", {}).get("portfolio_state_required") is True:
        unresolved.append("portfolio_state_required")
    simulation_blockers = [*private_sources, *unsupported_datasets, *unsupported_transforms, *unresolved]
    visualization_blockers = [*private_sources, *unsupported_datasets, *unresolved]
    exit_spec = portable_ir.get("exit", {})
    stop_param, hold_param = str(exit_spec.get("stop_param", "")), str(exit_spec.get("max_hold_param", ""))
    exit_portable = exit_spec.get("type") == "fixed_stop_time_exit" and isinstance(portable_ir.get("parameters", {}).get(stop_param), (int, float)) and isinstance(portable_ir.get("parameters", {}).get(hold_param), (int, float))
    if not exit_portable:
        simulation_blockers.append("exit_not_simulation_portable")
    return {
        "visualization": "compatible" if not visualization_blockers else "unsupported",
        "simulation": "compatible" if not simulation_blockers else "unsupported",
        "visualization_blockers": visualization_blockers,
        "simulation_blockers": simulation_blockers,
    }


def build_backtest_demo_qualification(*, portable_ir: dict[str, Any], evidence: dict[str, Any], approval: dict[str, Any]) -> dict[str, Any]:
    rules = [
        ("reproducible_lineage", bool(evidence.get("code_hash") and evidence.get("data_snapshot_id") and evidence.get("config_hash"))),
        ("truth_certification", evidence.get("truth_certification") == "PASS"),
        ("experiment_contract", not bool(evidence.get("blocking_contract_errors"))),
        ("execution_assumptions", all(evidence.get(key) is not None for key in ("fees_bps", "slippage_bps", "spread_bps", "timing_model", "leverage", "liquidation_model"))),
        ("sample_threshold", int(evidence.get("trade_count", 0)) >= int(evidence.get("minimum_trade_count", 30))),
        ("coverage_threshold", float(evidence.get("coverage_days", 0)) >= float(evidence.get("minimum_coverage_days", 90))),
        ("holdout_evidence", evidence.get("holdout_required") is False or evidence.get("holdout_status") == "PASS"),
        ("cost_survival", evidence.get("cost_survival") == "PASS"),
        ("ruin_limit", float(evidence.get("risk_of_ruin", 1)) <= float(evidence.get("maximum_risk_of_ruin", 0.05))),
        ("supported_symbols", bool(evidence.get("symbols")) and bool(evidence.get("exchange_product_type"))),
        ("critical_verdicts", int(evidence.get("unresolved_critical_verdicts", 0)) == 0),
    ]
    targets = classify_targets(portable_ir)
    rules.append(("simulation_compatible", targets["simulation"] == "compatible"))
    expected = {"strategy_spec_hash": portable_ir["strategy_spec_hash"], "risk_policy_hash": _hash(portable_ir.get("risk_policy", {})), "config_hash": str(evidence.get("config_hash", ""))}
    hash_approval = all(approval.get(key) == value for key, value in expected.items()) and bool(approval.get("approved_by") and approval.get("approved_at"))
    rules.append(("exact_hash_approval", hash_approval))
    observed = [{"rule": name, "status": "pass" if passed else "block", "observed": evidence.get(name), "required": True} for name, passed in rules]
    blockers = [item["rule"] for item in observed if item["status"] == "block"]
    next_tests = {
        "truth_certification": "Run Backtest Truth Certification and retain the PASS artifact.",
        "execution_assumptions": "Declare fees, spread, slippage, timing, leverage, and liquidation assumptions.",
        "sample_threshold": "Run a longer sample or explain why the strategy frequency requires another threshold.",
        "holdout_evidence": "Complete the declared holdout or walk-forward experiment.",
        "cost_survival": "Run the approved cost and slippage stress grid.",
        "ruin_limit": "Reduce risk or revise the user-approved ruin threshold.",
        "exact_hash_approval": "Approve the exact strategy, risk, and run-config hashes.",
        "simulation_compatible": "Resolve strategy semantics that the engine cannot simulate through the portable contract.",
    }
    snapshot = {
        "schema_version": QUALIFICATION_VERSION,
        "qualification_id": evidence.get("qualification_id"),
        "program_id": evidence.get("program_id"),
        "stage_from": "backtest",
        "stage_to": "demo",
        "status": "qualified" if not blockers else "blocked",
        "strategy_spec_id": portable_ir["strategy_spec_id"],
        **expected,
        "code_hash": evidence.get("code_hash"),
        "data_snapshot_id": evidence.get("data_snapshot_id"),
        "experiment_run_id": evidence.get("experiment_run_id"),
        "rules": observed,
        "blockers": blockers,
        "required_next_tests": [next_tests.get(item, f"Resolve qualification rule: {item}.") for item in blockers],
        "target_compatibility": targets,
        "limitations": list(evidence.get("limitations", [])),
        "approval": deepcopy(approval),
    }
    return {**snapshot, "snapshot_hash": _hash(snapshot)}
