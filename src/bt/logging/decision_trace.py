"""Strategy decision trace dataclass and flattening helpers."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
import json


@dataclass
class StrategyDecisionTrace:
    reason_code: str | None = None
    setup_class: str | None = None
    hypothesis_branch: str | None = None
    conditions_bool_map: dict[str, bool] = field(default_factory=dict)
    blockers_bool_map: dict[str, bool] = field(default_factory=dict)
    permission_layer_state: dict[str, Any] = field(default_factory=dict)
    score: float | None = None
    rank: float | None = None
    parameter_combination: str | None = None
    gate_thresholds: dict[str, float] = field(default_factory=dict)
    gate_values: dict[str, float] = field(default_factory=dict)
    gate_margins: dict[str, float] = field(default_factory=dict)
    most_binding_gate: str | None = None

    def to_metadata(self) -> dict[str, Any]:
        return {"decision_trace": self.__dict__.copy()}


def flatten_decision_trace(payload: dict[str, Any] | StrategyDecisionTrace | None) -> dict[str, Any]:
    if isinstance(payload, StrategyDecisionTrace):
        data = payload.__dict__
    elif isinstance(payload, dict):
        data = payload
    else:
        data = {}
    return {
        "entry_decision_reason_code": data.get("reason_code"),
        "entry_decision_setup_class": data.get("setup_class"),
        "entry_decision_hypothesis_branch": data.get("hypothesis_branch"),
        "entry_decision_parameter_combination": data.get("parameter_combination"),
        "entry_decision_conditions_json": json.dumps(data.get("conditions_bool_map", {}), sort_keys=True),
        "entry_decision_blockers_json": json.dumps(data.get("blockers_bool_map", {}), sort_keys=True),
        "entry_decision_permission_json": json.dumps(data.get("permission_layer_state", {}), sort_keys=True),
        "entry_decision_score": data.get("score"),
        "entry_decision_rank": data.get("rank"),
        "entry_decision_most_binding_gate": data.get("most_binding_gate"),
        "entry_decision_gate_margins_json": json.dumps(data.get("gate_margins", {}), sort_keys=True),
        "entry_gate_thresholds_json": json.dumps(data.get("gate_thresholds", {}), sort_keys=True),
        "entry_gate_values_json": json.dumps(data.get("gate_values", {}), sort_keys=True),
    }


def make_decision_trace(
    reason_code: str,
    setup_class: str,
    hypothesis_branch: str | None = None,
    conditions_bool_map: dict | None = None,
    blockers_bool_map: dict | None = None,
    permission_layer_state: dict | None = None,
    score: float | None = None,
    rank: float | None = None,
    parameter_combination: dict | None = None,
    gate_values: dict | None = None,
    gate_thresholds: dict | None = None,
    gate_margins: dict | None = None,
    most_binding_gate: str | None = None,
) -> dict[str, Any]:
    def _safe(v: Any) -> Any:
        try:
            json.dumps(v)
            return v
        except TypeError:
            return str(v)

    return {
        "reason_code": _safe(reason_code),
        "setup_class": _safe(setup_class),
        "hypothesis_branch": _safe(hypothesis_branch),
        "conditions_bool_map": {str(k): bool(v) for k, v in (conditions_bool_map or {}).items()},
        "blockers_bool_map": {str(k): bool(v) for k, v in (blockers_bool_map or {}).items()},
        "permission_layer_state": {str(k): _safe(v) for k, v in (permission_layer_state or {}).items()},
        "score": score,
        "rank": rank,
        "parameter_combination": json.dumps(parameter_combination or {}, sort_keys=True),
        "gate_values": {str(k): _safe(v) for k, v in (gate_values or {}).items()},
        "gate_thresholds": {str(k): _safe(v) for k, v in (gate_thresholds or {}).items()},
        "gate_margins": {str(k): _safe(v) for k, v in (gate_margins or {}).items()},
        "most_binding_gate": _safe(most_binding_gate),
    }
