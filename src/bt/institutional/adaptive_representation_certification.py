"""Independent native certification for adaptive multi-asset representations."""

from __future__ import annotations

from typing import Any

from bt.experiments.adaptive_representation import (
    materialize_adaptive_representation,
)
from bt.institutional.receipt import ProducerReceipt, build_receipt, digest


def adaptive_representation_certification_receipt(
    *,
    cases: list[dict[str, Any]],
    source_commit: str,
) -> ProducerReceipt:
    """Certify reasoning outputs by deterministic replay and terminal BT-009 evidence."""
    if len(cases) < 3 or len(cases) > 20:
        raise ValueError("adaptive representation certification requires 3-20 cases")
    results = []
    timeframes: set[str] = set()
    operations: set[str] = set()
    mixed_group_replay = False
    terminal_multi_asset = False
    for case in cases:
        required = {
            "case_key",
            "plan",
            "panels",
            "dataset_digests",
            "strategy_execution_semantics",
            "terminal_evidence",
        }
        if set(case) != required:
            raise ValueError("certification case fields are not exact")
        plan = case["plan"]
        materialized = materialize_adaptive_representation(plan, case["panels"])
        outputs = materialized.receipt["output_fields"]
        semantics = case["strategy_execution_semantics"]
        if (
            semantics.get("adaptive_representation_plan_digest") != digest(plan)
            or semantics.get("adaptive_representation_fields") != outputs
            or semantics.get("required_extra_columns") != outputs
        ):
            raise ValueError("strategy consumer is not bound to the adaptive plan")
        dataset_digests = case["dataset_digests"]
        if set(dataset_digests) != set(plan["instruments"]) or any(
            not isinstance(value, str) or len(value) != 64
            for value in dataset_digests.values()
        ):
            raise ValueError("case dataset digests do not cover the frozen basket")
        groups = {
            group
            for member in plan["basket_members"]
            for group in member["legacy_groups"]
        }
        case_operations = {item["operation"] for item in plan["transformations"]}
        cross_asset = any(
            len(item["input_fields"]) > 1
            for item in plan["transformations"]
        )
        mixed = len(plan["instruments"]) > 1 and groups >= {"stable", "volatile"}
        mixed_group_replay = mixed_group_replay or (mixed and cross_asset)
        timeframes.add(plan["research_timeframe"])
        operations.update(case_operations)
        terminal = case["terminal_evidence"]
        if terminal is not None:
            if (
                set(terminal)
                != {
                    "bt009_receipt_digest",
                    "bundle_digest",
                    "outcome",
                    "instrument_count",
                }
                or terminal["outcome"]
                not in {"positive", "negative", "invalid", "failed"}
                or terminal["instrument_count"] != len(plan["instruments"])
                or any(
                    not isinstance(terminal[key], str) or len(terminal[key]) != 64
                    for key in ("bt009_receipt_digest", "bundle_digest")
                )
            ):
                raise ValueError("terminal BT-009 evidence is malformed")
            terminal_multi_asset = terminal_multi_asset or len(plan["instruments"]) > 1
        results.append(
            {
                "case_key": case["case_key"],
                "plan_digest": digest(plan),
                "dataset_digests": dataset_digests,
                "basket": plan["instruments"],
                "groups": sorted(groups),
                "research_timeframe": plan["research_timeframe"],
                "operations": sorted(case_operations),
                "cross_asset_operation": cross_asset,
                "representation_receipt_digest": materialized.receipt[
                    "receipt_digest"
                ],
                "aligned_rows": materialized.receipt["aligned_rows"],
                "terminal_evidence": terminal,
            }
        )
    capability_checks = {
        "mixed_stable_volatile_basket_replayed": mixed_group_replay,
        "multiple_hypothesis_specific_timeframes_replayed": len(timeframes) >= 2,
        "scale_safe_return_representation_replayed": bool(
            operations & {"simple_return", "log_return"}
        ),
        "train_only_fractional_difference_replayed": "fractional_difference"
        in operations,
        "local_scale_or_volatility_representation_replayed": bool(
            operations & {"rolling_zscore", "realized_volatility"}
        ),
        "strategy_consumers_bound_to_exact_outputs": True,
        "terminal_multi_asset_bt009_outcome_retained": terminal_multi_asset,
    }
    status = "qualified" if all(capability_checks.values()) else "not_qualified"
    result = {
        "schema_version": "alpha009-adaptive-representation-certification-v1.0.0",
        "status": status,
        "case_count": len(results),
        "cases": results,
        "capability_checks": capability_checks,
        "timeframes": sorted(timeframes),
        "operations": sorted(operations),
        "capital_or_order_authority": False,
        "claim_boundary": (
            "Qualification proves outcome-blind basket and representation selection can "
            "be deterministically replayed and consumed by a retained no-capital BT-009 "
            "attempt. It does not prove alpha, shadow fitness, or capital authority."
        ),
    }
    inputs = [
        {
            "case_key": item["case_key"],
            "plan_digest": item["plan_digest"],
            "dataset_digests": item["dataset_digests"],
        }
        for item in results
    ]
    return build_receipt(
        milestone="ALPHA-009",
        producer=(
            "bt.institutional.adaptive_representation."
            "adaptive_representation_certification_receipt"
        ),
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs=inputs,
        dataset_digest=digest([item["dataset_digests"] for item in results]),
        configuration={
            "required_cases": 3,
            "outcome_selection": False,
            "terminal_multi_asset_bt009_required": True,
        },
        artifacts={
            "representation_receipts": [
                item["representation_receipt_digest"] for item in results
            ],
            "terminal_evidence": [
                item["terminal_evidence"]
                for item in results
                if item["terminal_evidence"] is not None
            ],
        },
        result=result,
    )
