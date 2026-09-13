from datetime import UTC, datetime, timedelta

import pytest

from bt.institutional.execution_degradation import (
    EXECUTION_DEGRADATION_SPECIFICATION,
    ExecutionDegradationError,
    evaluate_execution_degradation,
    execution_degradation_receipt,
)
from bt.institutional.receipt import build_receipt, digest, verify_receipt

NOW = datetime(2026, 9, 12, 12, tzinfo=UTC)
COMMIT = "a" * 40
DATASET = digest({"fixture": "exec009"})
CONFIG = {
    "thresholds": {
        "fill_rate": 0.8,
        "ack_latency_ms": 250,
        "fill_latency_ms": 1000,
        "implementation_shortfall_bps": 8,
        "adverse_selection_bps": 6,
        "queue_model_error_bps": 4,
        "reject_rate": 0.05,
        "reconciliation_breaks": 0,
    },
    "consecutive_breaches": 2,
    "recovery_observations": 3,
    "observation_expiry_seconds": 300,
}
CANDIDATE = digest({"candidate": "one"})
LIFECYCLE = {
    "status": "demo",
    "candidate_digest": CANDIDATE,
    "record_digest": digest({"candidate_lifecycle": "one"}),
}


def dependency(milestone: str, **result):
    producers = {
        "EXEC-005": "bt.institutional.execution_calibration.execution_calibration_receipt",
        "EXEC-008": "bt.institutional.adapter_certification.adapter_certification_receipt",
        "SHADOW-002": "bt.institutional.shadow_monitoring.shadow_monitoring_receipt",
    }
    if milestone == "EXEC-008":
        result = {"venue": "bybit", "environment": "demo", **result}
    return build_receipt(
        milestone=milestone,
        producer=producers[milestone],
        producer_version="1.0.0",
        source_commit=COMMIT,
        inputs={},
        dataset_digest=DATASET,
        configuration={},
        artifacts={},
        result={"qualified": True, **result},
    )


def observation(number: int, **overrides):
    values = {
        "observation_id": f"obs-{number}",
        "venue": "bybit",
        "environment": "demo",
        "strategy_id": "weekend-momentum",
        "listing_id": "bybit:linear:BTCUSDT",
        "order_type": "limit",
        "size_bucket": "micro",
        "regime": "normal",
        "observed_at": (NOW - timedelta(seconds=30 - number)).isoformat(),
        "available_at": (NOW - timedelta(seconds=20 - number)).isoformat(),
        "source_digest": digest({"observation": number}),
        "service_available": True,
        "venue_rule_current": True,
        "fill_rate": 0.95,
        "ack_latency_ms": 100,
        "fill_latency_ms": 500,
        "implementation_shortfall_bps": 3,
        "adverse_selection_bps": 2,
        "queue_model_error_bps": 1,
        "reject_rate": 0.01,
        "reconciliation_breaks": 0,
    }
    values.update(overrides)
    return values


def evaluate(items, **overrides):
    values = {
        "observations": items,
        "known_at": NOW,
        "prior_status": "monitoring",
        "candidate_lifecycle": LIFECYCLE,
        "configuration": CONFIG,
    }
    values.update(overrides)
    return evaluate_execution_degradation(**values)


def test_healthy_evidence_continues_without_action_authority():
    result = evaluate([observation(1), observation(2)])
    assert result["status"] == "monitoring"
    assert result["recommended_action"] == "continue_monitoring"
    assert result["continued_execution_eligible"] is True
    assert result["authority"] == EXECUTION_DEGRADATION_SPECIFICATION["authority"]
    assert not result["automatic_execution_change"]


def test_resolved_single_breach_does_not_create_a_false_alarm():
    result = evaluate([observation(1, queue_model_error_bps=7), observation(2)])
    assert result["status"] == "monitoring"
    assert result["recommended_action"] == "continue_monitoring"
    assert result["maximum_breach_streak"] == 1
    assert result["diagnoses"] == ["model"]


def test_current_single_breach_requests_observation_without_restriction():
    result = evaluate([observation(1, queue_model_error_bps=7)])
    assert result["status"] == "monitoring"
    assert result["recommended_action"] == "observe_and_recalibrate"
    assert result["continued_execution_eligible"] is False


def test_exact_duplicate_observation_cannot_manufacture_confirmation():
    item = observation(1, implementation_shortfall_bps=12)
    result = evaluate([item, item])
    assert result["observation_count"] == 1
    assert result["maximum_breach_streak"] == 1
    assert result["recommended_action"] == "observe_and_recalibrate"


def test_confirmed_strategy_degradation_requests_shadow_fallback():
    result = evaluate(
        [
            observation(1, implementation_shortfall_bps=12),
            observation(2, adverse_selection_bps=9),
        ]
    )
    assert result["status"] == "shadow_fallback"
    assert result["recommended_action"] == "shadow_fallback_review"
    assert result["diagnoses"] == ["strategy"]
    assert len(result["incidents"]) == 2
    assert all(len(item["incident_digest"]) == 64 for item in result["incidents"])


def test_venue_and_infrastructure_are_not_collapsed_into_strategy_drift():
    result = evaluate(
        [
            observation(1, reject_rate=0.2),
            observation(2, reconciliation_breaks=1),
        ]
    )
    assert result["status"] == "restricted"
    assert result["recommended_action"] == "route_restriction_review"
    assert result["diagnoses"] == ["infrastructure", "venue"]


def test_outage_fails_closed_and_recovery_never_reactivates_automatically():
    failed = evaluate([observation(1, service_available=False)])
    assert failed["status"] == "killed"
    assert failed["recommended_action"] == "freeze_and_kill_review"
    recovered = evaluate(
        [observation(1), observation(2), observation(3)], prior_status="killed"
    )
    assert recovered["status"] == "killed"
    assert recovered["recommended_action"] == "independent_restore_review"
    assert recovered["recovery_evidence_complete"] is True
    assert recovered["automatic_reactivation"] is False


def test_future_evidence_is_excluded_and_identity_reuse_is_rejected():
    future = observation(2)
    future["available_at"] = (NOW + timedelta(seconds=1)).isoformat()
    result = evaluate([observation(1), future])
    assert result["future_observations_excluded"] == 1
    changed = observation(1, fill_rate=0.1)
    with pytest.raises(ExecutionDegradationError, match="identity"):
        evaluate([observation(1), changed])


def test_receipt_binds_dependencies_and_verifies_exactly():
    receipt = execution_degradation_receipt(
        exec005_receipt=dependency("EXEC-005"),
        exec008_receipt=dependency("EXEC-008"),
        shadow002_receipt=dependency("SHADOW-002", candidate_digest=CANDIDATE),
        governance_policy_digest=digest({"gov003": "active"}),
        platform_observability_digest=digest({"plat005": "current"}),
        candidate_lifecycle=LIFECYCLE,
        observations=[observation(1), observation(2)],
        known_at=NOW,
        prior_status="monitoring",
        dataset_digest=DATASET,
        source_commit=COMMIT,
        configuration=CONFIG,
    )
    assert receipt.milestone == "EXEC-009"
    assert verify_receipt(receipt)
    assert receipt.result["degradation_schema_digest"] == digest(
        EXECUTION_DEGRADATION_SPECIFICATION
    )


def test_receipt_rejects_a_different_shadow_candidate():
    with pytest.raises(ExecutionDegradationError, match="candidate binding"):
        execution_degradation_receipt(
            exec005_receipt=dependency("EXEC-005"),
            exec008_receipt=dependency("EXEC-008"),
            shadow002_receipt=dependency(
                "SHADOW-002", candidate_digest=digest({"candidate": "other"})
            ),
            governance_policy_digest=digest({"gov003": "active"}),
            platform_observability_digest=digest({"plat005": "current"}),
            candidate_lifecycle=LIFECYCLE,
            observations=[observation(1)],
            known_at=NOW,
            prior_status="monitoring",
            dataset_digest=DATASET,
            source_commit=COMMIT,
            configuration=CONFIG,
        )


def test_receipt_binds_independently_versioned_dependency_dataset():
    drifted = build_receipt(
        milestone="EXEC-005",
        producer="bt.institutional.execution_calibration.execution_calibration_receipt",
        producer_version="1.0.0",
        source_commit=COMMIT,
        inputs={},
        dataset_digest=digest({"fixture": "different"}),
        configuration={},
        artifacts={},
        result={"qualified": True},
    )
    receipt = execution_degradation_receipt(
        exec005_receipt=drifted,
        exec008_receipt=dependency("EXEC-008"),
        shadow002_receipt=dependency("SHADOW-002", candidate_digest=CANDIDATE),
        governance_policy_digest=digest({"gov003": "active"}),
        platform_observability_digest=digest({"plat005": "current"}),
        candidate_lifecycle=LIFECYCLE,
        observations=[observation(1)],
        known_at=NOW,
        prior_status="monitoring",
        dataset_digest=DATASET,
        source_commit=COMMIT,
        configuration=CONFIG,
    )
    assert (
        receipt.result["dependency_dataset_digests"]["exec005"]
        == drifted.dataset_digest
    )


def test_receipt_rejects_wrong_dependency_producer():
    forged = build_receipt(
        milestone="EXEC-005",
        producer="fixture.exec005",
        producer_version="1.0.0",
        source_commit=COMMIT,
        inputs={},
        dataset_digest=DATASET,
        configuration={},
        artifacts={},
        result={"qualified": True},
    )
    with pytest.raises(ExecutionDegradationError, match="exact EXEC-005"):
        execution_degradation_receipt(
            exec005_receipt=forged,
            exec008_receipt=dependency("EXEC-008"),
            shadow002_receipt=dependency("SHADOW-002", candidate_digest=CANDIDATE),
            governance_policy_digest=digest({"gov003": "active"}),
            platform_observability_digest=digest({"plat005": "current"}),
            candidate_lifecycle=LIFECYCLE,
            observations=[observation(1)],
            known_at=NOW,
            prior_status="monitoring",
            dataset_digest=DATASET,
            source_commit=COMMIT,
            configuration=CONFIG,
        )


def test_receipt_rejects_adapter_qualification_for_another_venue():
    with pytest.raises(ExecutionDegradationError, match="observed venue"):
        execution_degradation_receipt(
            exec005_receipt=dependency("EXEC-005"),
            exec008_receipt=dependency("EXEC-008", venue="binance"),
            shadow002_receipt=dependency("SHADOW-002", candidate_digest=CANDIDATE),
            governance_policy_digest=digest({"gov003": "active"}),
            platform_observability_digest=digest({"plat005": "current"}),
            candidate_lifecycle=LIFECYCLE,
            observations=[observation(1)],
            known_at=NOW,
            prior_status="monitoring",
            dataset_digest=DATASET,
            source_commit=COMMIT,
            configuration=CONFIG,
        )


def test_public_institutional_api_exports_exec009_contract():
    from bt import institutional

    assert (
        institutional.EXECUTION_DEGRADATION_SCHEMA_VERSION
        == "exec009-execution-degradation-v1.1.0"
    )
    assert institutional.execution_degradation_receipt is execution_degradation_receipt
