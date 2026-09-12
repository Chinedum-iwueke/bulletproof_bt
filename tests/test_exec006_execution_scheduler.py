from datetime import UTC, datetime, timedelta

import pytest

from bt.institutional.execution_scheduler import (
    EXECUTION_SCHEDULE_SPECIFICATION,
    ExecutionScheduleError,
    authorize_schedule_action,
    build_execution_schedule,
    execution_schedule_receipt,
    next_schedule_action,
)
from bt.institutional.receipt import build_receipt, digest, verify_receipt

NOW = datetime(2026, 9, 12, 12, tzinfo=UTC)
DATASET = "a" * 64
COMMIT = "b" * 40


def dependency(milestone: str, result: dict | None = None):
    return build_receipt(
        milestone=milestone,
        producer=f"test.{milestone.lower()}",
        producer_version="1.0.0",
        source_commit=COMMIT,
        inputs={},
        dataset_digest=DATASET,
        configuration={},
        artifacts={},
        result=result
        or {
            "qualified": True,
            "calibration": {"strata": [{"pessimistic_cost_bps": 5.0}]},
        },
    )


def intent(**updates):
    value = {
        "intent_id": "parent-1",
        "candidate_digest": "c" * 64,
        "symbol": "BTCUSDT",
        "side": "buy",
        "reduce_only": False,
        "total_quantity": 4,
        "reference_price": 50000,
        "start_at": (NOW + timedelta(seconds=10)).isoformat(),
        "end_at": (NOW + timedelta(seconds=40)).isoformat(),
        "slice_count": 4,
        "projected_interval_volumes": [100, 100, 100, 100],
    }
    return value | updates


def policy(**updates):
    return {
        "allowed_algorithms": ["market", "limit", "twap", "vwap"],
        "maximum_horizon_seconds": 300,
        "maximum_slices": 8,
        "minimum_child_quantity": 0.1,
        "maximum_participation_rate": 0.1,
        "clock_tolerance_seconds": 5,
        "child_lifetime_seconds": 10,
        "algorithm_cost_multipliers": {
            "market": 1.5,
            "limit": 0.8,
            "twap": 1.0,
            "vwap": 0.9,
        },
    } | updates


@pytest.mark.parametrize("algorithm", ["market", "limit", "twap", "vwap"])
def test_deterministic_baselines_preserve_quantity(algorithm):
    parent = intent(
        slice_count=4 if algorithm in {"twap", "vwap"} else 1,
        projected_interval_volumes=[100] * (4 if algorithm in {"twap", "vwap"} else 1),
        limit_price=49900 if algorithm == "limit" else None,
    )
    profile = (
        {"available_at": NOW.isoformat(), "bucket_weights": [1, 2, 3, 4]}
        if algorithm == "vwap"
        else None
    )
    first = build_execution_schedule(
        parent_intent=parent,
        algorithm=algorithm,
        policy=policy(),
        known_at=NOW,
        volume_profile=profile,
    )
    second = build_execution_schedule(
        parent_intent=parent,
        algorithm=algorithm,
        policy=policy(),
        known_at=NOW,
        volume_profile=profile,
    )
    assert first == second
    assert sum(child["target_quantity"] for child in first["children"]) == 4
    assert first["order_authority"] is False


def test_vwap_rejects_future_profile_and_participation_conflict():
    with pytest.raises(ExecutionScheduleError, match="future"):
        build_execution_schedule(
            parent_intent=intent(),
            algorithm="vwap",
            policy=policy(),
            known_at=NOW,
            volume_profile={
                "available_at": (NOW + timedelta(seconds=1)).isoformat(),
                "bucket_weights": [1, 1, 1, 1],
            },
        )
    with pytest.raises(ExecutionScheduleError, match="participation"):
        build_execution_schedule(
            parent_intent=intent(projected_interval_volumes=[1] * 4),
            algorithm="twap",
            policy=policy(),
            known_at=NOW,
        )


def test_scheduler_waits_cancels_carries_partial_fills_and_fails_on_drift():
    schedule = build_execution_schedule(
        parent_intent=intent(), algorithm="twap", policy=policy(), known_at=NOW
    )
    first = schedule["children"][0]
    assert (
        next_schedule_action(
            schedule=schedule, now=NOW, cumulative_filled_quantity=0, child_states={}
        )["action"]
        == "wait"
    )
    proposed = next_schedule_action(
        schedule=schedule,
        now=NOW + timedelta(seconds=10),
        cumulative_filled_quantity=0,
        child_states={},
    )
    assert proposed["action"] == "propose_submit"
    assert (
        next_schedule_action(
            schedule=schedule,
            now=NOW + timedelta(seconds=21),
            cumulative_filled_quantity=0.5,
            child_states={first["child_id"]: "partially_filled"},
        )["action"]
        == "cancel"
    )
    carried = next_schedule_action(
        schedule=schedule,
        now=NOW + timedelta(seconds=20),
        cumulative_filled_quantity=0.5,
        child_states={first["child_id"]: "cancelled"},
    )
    assert carried["order"]["quantity"] == 1.5
    assert (
        next_schedule_action(
            schedule=schedule,
            now=NOW + timedelta(seconds=17),
            cumulative_filled_quantity=0,
            child_states={},
        )["reason"]
        == "schedule_clock_drift"
    )


def test_receipt_binds_dependencies_and_requires_fresh_exact_risk_receipt():
    receipt = execution_schedule_receipt(
        parent_intent=intent(),
        algorithm="twap",
        policy=policy(),
        known_at=NOW,
        exec004_receipt=dependency("EXEC-004"),
        exec005_receipt=dependency("EXEC-005"),
        dataset_digest=DATASET,
        source_commit=COMMIT,
    )
    assert verify_receipt(receipt)
    assert receipt.result["execution_schedule_schema_digest"] == digest(
        EXECUTION_SCHEDULE_SPECIFICATION
    )
    assert receipt.result["pessimistic_cost_comparison_bps"]["limit"] == 4.0
    schedule = receipt.result["schedule"]
    action = next_schedule_action(
        schedule=schedule,
        now=NOW + timedelta(seconds=10),
        cumulative_filled_quantity=0,
        child_states={},
    )
    order = action["order"]
    risk_result = {
        "allowed": True,
        "state_version": 7,
        "known_at": (NOW + timedelta(seconds=10)).isoformat(),
        "decision": "allow",
        "order_binding_digest": digest(
            {
                "symbol": order["symbol"],
                "side": order["side"],
                "quantity": order["quantity"],
                "reference_price": order["reference_price"],
                "reduce_only": order["reduce_only"],
            }
        ),
        "decision_digest": "d" * 64,
    }
    risk = dependency("RISK-005", risk_result)
    authorized = authorize_schedule_action(
        action=action,
        risk_receipt=risk,
        expected_state_version=7,
        now=NOW + timedelta(seconds=11),
        maximum_age_seconds=5,
    )
    assert authorized["action"] == "submit" and authorized["risk_authorized"] is True
    with pytest.raises(ExecutionScheduleError, match="state version"):
        authorize_schedule_action(
            action=action,
            risk_receipt=risk,
            expected_state_version=8,
            now=NOW + timedelta(seconds=11),
            maximum_age_seconds=5,
        )
    tampered = dict(action)
    tampered["order"] = action["order"] | {"quantity": 2}
    with pytest.raises(ExecutionScheduleError, match="does not match"):
        authorize_schedule_action(
            action=tampered,
            risk_receipt=risk,
            expected_state_version=7,
            now=NOW + timedelta(seconds=11),
            maximum_age_seconds=5,
        )


def test_schedule_tamper_and_unknown_state_fail_closed():
    schedule = build_execution_schedule(
        parent_intent=intent(), algorithm="twap", policy=policy(), known_at=NOW
    )
    with pytest.raises(ExecutionScheduleError, match="digest"):
        next_schedule_action(
            schedule=schedule | {"total_quantity": 40},
            now=NOW + timedelta(seconds=10),
            cumulative_filled_quantity=0,
            child_states={},
        )
    with pytest.raises(ExecutionScheduleError, match="child state"):
        next_schedule_action(
            schedule=schedule,
            now=NOW + timedelta(seconds=10),
            cumulative_filled_quantity=0,
            child_states={schedule["children"][0]["child_id"]: "mystery"},
        )
