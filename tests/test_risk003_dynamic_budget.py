from datetime import UTC, datetime, timedelta

import pytest

from bt.institutional.receipt import build_receipt, digest, verify_receipt
from bt.institutional.risk_budget import (
    RISK_BUDGET_SPECIFICATION,
    DynamicRiskBudgetError,
    dynamic_risk_budget,
    dynamic_risk_budget_receipt,
)

NOW = datetime(2026, 9, 12, 18, tzinfo=UTC)
DATASET = digest({"dataset": "risk003-fixture"})


def config(**overrides):
    values = {
        "base_risk_fraction": 0.01,
        "minimum_risk_fraction": 0.001,
        "maximum_risk_fraction": 0.012,
        "static_conservative_fraction": 0.0025,
        "target_annualized_volatility": 0.20,
        "uncertainty_penalty": 0.75,
        "confidence_floor": 0.50,
        "hysteresis_fraction": 0.0002,
        "maximum_upward_step": 0.001,
        "recovery_observations": 2,
        "expiry_seconds": 300,
        "regime_multipliers": {"normal": 1.0, "elevated": 0.6, "crisis": 0.2},
    }
    values.update(overrides)
    return values


def state(index=0, **overrides):
    observed = NOW - timedelta(seconds=90 - index * 10)
    values = {
        "state_id": f"state-{index}",
        "observed_at": observed.isoformat(),
        "available_at": (observed + timedelta(seconds=1)).isoformat(),
        "regime": "normal",
        "annualized_volatility": 0.20,
        "model_confidence": 0.95,
        "uncertainty": 0.10,
        "source_digest": digest({"state": index}),
    }
    values.update(overrides)
    return values


def dependency(milestone, producer, result):
    return build_receipt(
        milestone=milestone,
        producer=producer,
        producer_version="1.0.0",
        source_commit="a" * 40,
        inputs={},
        dataset_digest=DATASET,
        configuration={},
        artifacts=result,
        result=result,
    )


def dependencies():
    return {
        "risk001_receipt": dependency("RISK-001", "bt.institutional.risk.stress_dossier_receipt", {"admissible": True}),
        "risk002_receipt": dependency("RISK-002", "bt.institutional.risk.venue_rule_receipt", {"allowed": True}),
        "ml004_receipt": dependency("ML-004", "bt.institutional.ml.calibration_receipt", {"qualified": True}),
        "port004_receipt": dependency(
            "PORT-004", "bt.institutional.capacity.capacity_dossier_receipt",
            {"qualified": True, "capacity_supported_notional": 100_000},
        ),
    }


def calculate(states=None, **overrides):
    values = {
        "states": [state()] if states is None else states,
        "known_at": NOW,
        "prior_risk_fraction": 0.008,
        "requested_capital": 120_000,
        "capacity_supported_notional": 100_000,
        "configuration": config(),
    }
    values.update(overrides)
    return dynamic_risk_budget(**values)


def test_normal_state_produces_bounded_capacity_capped_budget():
    result = calculate()
    assert result["qualified"] is True
    assert result["budget_capital"] == 100_000
    assert 0.001 <= result["effective_risk_fraction"] <= 0.012
    assert result["effective_risk_notional"] <= 1_200
    assert not any(result["authority"].values())


def test_volatility_spike_and_crisis_reduce_budget_immediately():
    result = calculate(
        states=[state(), state(1, regime="crisis", annualized_volatility=0.80)]
    )
    assert result["budget_trajectory"][-1]["action"] == "reduce_immediately"
    assert result["budget_trajectory"][-1]["effective_risk_fraction"] == 0.002


def test_more_uncertainty_never_increases_budget():
    low = calculate(states=[state(uncertainty=0.1)])
    high = calculate(states=[state(uncertainty=0.9)])
    assert high["effective_risk_fraction"] <= low["effective_risk_fraction"]


def test_lower_confidence_never_increases_budget():
    high = calculate(states=[state(model_confidence=0.95)])
    low = calculate(states=[state(model_confidence=0.55)])
    assert low["effective_risk_fraction"] <= high["effective_risk_fraction"]


def test_recovery_requires_streak_and_is_step_limited():
    states = [
        state(0, regime="crisis", annualized_volatility=0.8),
        state(1),
        state(2),
    ]
    result = calculate(states=states)
    trajectory = result["budget_trajectory"]
    assert trajectory[1]["action"] == "hold_pending_recovery"
    assert trajectory[2]["action"] == "bounded_recovery"
    assert trajectory[2]["effective_risk_fraction"] - trajectory[1]["effective_risk_fraction"] <= 0.001


def test_hysteresis_holds_small_changes():
    result = calculate(
        states=[state(uncertainty=0.10), state(1, uncertainty=0.11)],
        prior_risk_fraction=0.009,
    )
    assert result["budget_trajectory"][-1]["action"] == "hold_hysteresis"


def test_expired_state_uses_static_conservative_fallback():
    stale = state(
        observed_at=(NOW - timedelta(hours=1)).isoformat(),
        available_at=(NOW - timedelta(hours=1) + timedelta(seconds=1)).isoformat(),
    )
    result = calculate(states=[stale])
    assert result["decision"] == "static_conservative_fallback"
    assert result["effective_risk_fraction"] <= 0.0025
    assert "risk_state_expired" in result["failures"]


def test_zero_capacity_uses_static_conservative_fallback_with_zero_notional():
    result = calculate(capacity_supported_notional=0)
    assert result["decision"] == "static_conservative_fallback"
    assert result["effective_risk_notional"] == 0


def test_future_state_is_excluded_without_lookahead():
    future = state(
        state_id="future",
        observed_at=(NOW + timedelta(seconds=10)).isoformat(),
        available_at=(NOW + timedelta(seconds=11)).isoformat(),
    )
    result = calculate(states=[state(), future])
    assert result["future_states_excluded"] == 1
    assert len(result["budget_trajectory"]) == 1


def test_state_identity_collision_is_rejected():
    with pytest.raises(DynamicRiskBudgetError, match="reused"):
        calculate(states=[state(), state(1, state_id="state-0")])


def test_unknown_regime_fails_closed():
    result = calculate(states=[state(regime="unknown")])
    assert result["decision"] == "static_conservative_fallback"
    assert "unknown_regime:unknown" in result["failures"]


@pytest.mark.parametrize(
    "override",
    [
        {"minimum_risk_fraction": 0.02},
        {"recovery_observations": 0},
        {"regime_multipliers": {}},
        {"uncertainty_penalty": 1.1},
    ],
)
def test_invalid_policy_is_rejected(override):
    with pytest.raises(DynamicRiskBudgetError):
        calculate(configuration=config(**override))


def test_receipt_binds_all_dependencies_and_schema():
    receipt = dynamic_risk_budget_receipt(
        **dependencies(),
        states=[state()],
        known_at=NOW,
        prior_risk_fraction=0.008,
        requested_capital=100_000,
        configuration=config(),
        dataset_digest=DATASET,
        source_commit="b" * 40,
    )
    assert verify_receipt(receipt)
    assert receipt.milestone == "RISK-003"
    assert receipt.result["risk_budget_schema_digest"] == digest(RISK_BUDGET_SPECIFICATION)
    assert set(receipt.result["dependency_receipts"]) == {"risk001", "risk002", "ml004", "port004"}


def test_unqualified_or_tampered_dependency_is_rejected():
    deps = dependencies()
    deps["ml004_receipt"] = dependency("ML-004", "bt.institutional.ml.calibration_receipt", {"qualified": False})
    with pytest.raises(DynamicRiskBudgetError, match="ML-004"):
        dynamic_risk_budget_receipt(
            **deps,
            states=[state()], known_at=NOW, prior_risk_fraction=0.008,
            requested_capital=100_000, configuration=config(), dataset_digest=DATASET,
            source_commit="b" * 40,
        )
