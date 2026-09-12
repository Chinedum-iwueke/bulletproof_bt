from datetime import UTC, datetime, timedelta

import pytest

from bt.institutional.receipt import digest, verify_receipt
from bt.institutional.shadow_monitoring import (
    SHADOW_MONITORING_SPECIFICATION,
    ShadowMonitoringError,
    monitor_shadow_candidate,
    shadow_monitoring_receipt,
)

NOW = datetime(2026, 9, 12, 20, tzinfo=UTC)
CANDIDATE = digest({"candidate": "shadow002"})
DATASET = digest({"dataset": "prospective-feed"})


def replay(**overrides):
    value = {
        "success": True,
        "sealed": True,
        "sealed_at": (NOW - timedelta(minutes=2)).isoformat(),
        "journal_digest": digest({"journal": "sealed"}),
        "bindings": {"candidate_digest": CANDIDATE},
        "capital_or_order_authority": False,
    }
    value.update(overrides)
    return value


def config(**overrides):
    value = {
        "observation_expiry_seconds": 120,
        "journal_expiry_seconds": 3600,
        "review_interval_seconds": 300,
        "maximum_data_lag_seconds": 5,
        "maximum_prediction_drift": 0.20,
        "maximum_cost_drift_bps": 8,
        "maximum_drawdown_fraction": 0.10,
        "demotion_after_consecutive_breaches": 2,
        "recovery_observations": 2,
    }
    value.update(overrides)
    return value


def observation(index=0, **overrides):
    observed = NOW - timedelta(seconds=30 - index * 5)
    value = {
        "observation_id": f"obs-{index}",
        "observed_at": observed.isoformat(),
        "available_at": (observed + timedelta(seconds=1)).isoformat(),
        "source_digest": digest({"observation": index}),
        "data_lag_seconds": 1,
        "service_available": True,
        "prediction_drift": 0.02,
        "cost_drift_bps": 1,
        "drawdown_fraction": 0.01,
    }
    value.update(overrides)
    return value


def monitor(observations=None, **overrides):
    value = {
        "candidate_digest": CANDIDATE,
        "shadow001_replay": replay(),
        "observations": [observation()] if observations is None else observations,
        "known_at": NOW,
        "prior_status": "monitoring",
        "last_reviewed_at": NOW - timedelta(seconds=60),
        "configuration": config(),
    }
    value.update(overrides)
    return monitor_shadow_candidate(**value)


def test_healthy_current_candidate_continues_without_authority():
    result = monitor()
    assert result["recommended_action"] == "continue_monitoring"
    assert result["qualified_for_continued_shadow"] is True
    assert not any(result["authority"].values())


@pytest.mark.parametrize(
    ("overrides", "failure"),
    [
        ({"service_available": False}, "service_outage"),
        ({"data_lag_seconds": 6}, "data_lag"),
        ({"prediction_drift": 0.21}, "prediction_drift"),
        ({"cost_drift_bps": 9}, "cost_drift"),
        ({"drawdown_fraction": 0.11}, "drawdown"),
    ],
)
def test_single_deterioration_freezes(overrides, failure):
    result = monitor([observation(**overrides)])
    assert result["status"] == "frozen"
    assert result["recommended_action"] == "freeze_and_review"
    assert failure in result["failures"]


def test_consecutive_breaches_propose_demotion():
    result = monitor(
        [
            observation(0, service_available=False),
            observation(1, service_available=False),
        ]
    )
    assert result["status"] == "demotion_pending"
    assert result["recommended_action"] == "demotion_review_required"


def test_stale_and_missing_observations_fail_closed():
    stale = observation(
        observed_at=(NOW - timedelta(hours=1)).isoformat(),
        available_at=(NOW - timedelta(hours=1) + timedelta(seconds=1)).isoformat(),
    )
    assert monitor([stale])["recommended_action"] == "freeze_and_review"
    assert monitor([])["recommended_action"] == "freeze_and_review"


def test_future_observation_is_excluded_without_lookahead():
    future = observation(
        observed_at=(NOW + timedelta(seconds=1)).isoformat(),
        available_at=(NOW + timedelta(seconds=2)).isoformat(),
    )
    result = monitor([observation(), future])
    assert result["future_observations_excluded"] == 1
    assert result["observation_count"] == 1


def test_identity_collision_is_rejected():
    with pytest.raises(ShadowMonitoringError, match="reused"):
        monitor([observation(), observation(1, observation_id="obs-0")])


def test_invalid_or_expired_journal_is_rejected():
    with pytest.raises(ShadowMonitoringError, match="sealed"):
        monitor(shadow001_replay=replay(sealed=False))
    with pytest.raises(ShadowMonitoringError, match="expired"):
        monitor(
            shadow001_replay=replay(sealed_at=(NOW - timedelta(hours=2)).isoformat())
        )


def test_candidate_binding_mismatch_is_rejected():
    with pytest.raises(ShadowMonitoringError, match="binding"):
        monitor(shadow001_replay=replay(bindings={"candidate_digest": "a" * 64}))


def test_scheduled_review_is_visible_and_deterministic():
    result = monitor(last_reviewed_at=NOW - timedelta(seconds=300))
    assert result["review_due"] is True
    assert result["recommended_action"] == "scheduled_re_evaluation_required"


def test_healthy_recovery_never_reactivates_automatically():
    result = monitor([observation(), observation(1)], prior_status="frozen")
    assert result["status"] == "frozen"
    assert result["recommended_action"] == "independent_reactivation_review_required"
    assert result["automatic_reactivation"] is False


def test_incomplete_recovery_streak_remains_explicit():
    result = monitor([observation()], prior_status="frozen")
    assert "recovery_streak_incomplete" in result["failures"]


def test_breach_followed_by_apparent_recovery_cannot_silently_clear():
    result = monitor(
        [
            observation(0, prediction_drift=0.5),
            observation(1),
        ]
    )
    assert result["status"] == "frozen"
    assert result["recommended_action"] == "independent_reactivation_review_required"


def test_demotion_streak_is_not_erased_by_later_healthy_sample():
    result = monitor(
        [
            observation(0, service_available=False),
            observation(1, service_available=False),
            observation(2),
        ]
    )
    assert result["status"] == "demotion_pending"


def test_receipt_binds_journal_schema_and_monitoring_dossier():
    receipt = shadow_monitoring_receipt(
        candidate_digest=CANDIDATE,
        shadow001_replay=replay(),
        observations=[observation()],
        known_at=NOW,
        prior_status="monitoring",
        last_reviewed_at=NOW - timedelta(seconds=60),
        configuration=config(),
        dataset_digest=DATASET,
        source_commit="a" * 40,
    )
    assert verify_receipt(receipt)
    assert receipt.milestone == "SHADOW-002"
    assert receipt.result["shadow_monitoring_schema_digest"] == digest(
        SHADOW_MONITORING_SPECIFICATION
    )


@pytest.mark.parametrize(
    "override",
    [
        {"recovery_observations": 0},
        {"demotion_after_consecutive_breaches": 0},
        {"maximum_drawdown_fraction": 1.1},
    ],
)
def test_invalid_policy_is_rejected(override):
    with pytest.raises(ShadowMonitoringError):
        monitor(configuration=config(**override))
