"""Trade schema coverage checks for research-grade logging."""
from __future__ import annotations

from typing import Any

GROUP_REQUIREMENTS: dict[str, dict[str, Any]] = {
    "identity": {"required": ["identity_run_id", "identity_trade_id", "identity_symbol", "identity_ts_entry_fill"]},
    "entry_state": {"prefix": "entry_state_", "expected_minimum": 10},
    "decision_trace": {"prefix": "entry_decision_", "expected_minimum": 5},
    "execution": {"prefix": "execution_", "expected_minimum": 8},
    "path": {"prefix": "path_", "expected_minimum": 10},
    "counterfactual": {"prefix": "counterfactual_", "expected_minimum": 3},
    "label": {"prefix": "label_", "expected_minimum": 8},
}


def schema_coverage(columns: list[str]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    warnings: list[str] = []
    for group, spec in GROUP_REQUIREMENTS.items():
        if "required" in spec:
            req = spec["required"]
            present = sum(1 for c in req if c in columns)
            expected = len(req)
            cov = present / expected if expected else 1.0
            result[group] = {"present": present, "expected": expected, "coverage": cov}
            if present < expected:
                warnings.append(f"{group} missing required columns: {[c for c in req if c not in columns]}")
        else:
            prefix = spec["prefix"]
            expected_minimum = int(spec["expected_minimum"])
            present = sum(1 for c in columns if c.startswith(prefix))
            cov = min(1.0, present / expected_minimum) if expected_minimum else 1.0
            result[group] = {"present": present, "expected_minimum": expected_minimum, "coverage": cov}
            if present < expected_minimum:
                warnings.append(f"{group} below expected minimum: present={present} expected_minimum={expected_minimum}")
    result["warnings"] = warnings
    return result
