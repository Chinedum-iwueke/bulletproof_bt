from datetime import UTC, datetime, timedelta

import pytest

from bt.execution.model_registry import declared_classic_bundle, validate_model_bundle_document
from bt.institutional.execution_calibration import (
    CALIBRATION_SPECIFICATION,
    ExecutionCalibrationError,
    calibrate_execution_quality,
    execution_calibration_receipt,
)
from bt.institutional.receipt import build_receipt, digest, verify_receipt

NOW = datetime(2026, 9, 12, 12, tzinfo=UTC)
COMMIT = "a" * 40
DATASET = digest({"dataset": "exec005"})


def observation(index: int, role: str, **overrides):
    submitted = NOW + timedelta(seconds=index * 10)
    value = {
        "observation_id": f"o-{role}-{index}",
        "venue_id": "bybit",
        "listing_id": "BTCUSDT",
        "order_type": "limit",
        "side": "buy" if index % 2 == 0 else "sell",
        "size_bucket": "small",
        "regime": "normal",
        "sample_role": role,
        "submitted_at": submitted.isoformat(),
        "acknowledged_at": (submitted + timedelta(milliseconds=20 + index)).isoformat(),
        "first_fill_at": (submitted + timedelta(milliseconds=80 + index)).isoformat(),
        "terminal_at": (submitted + timedelta(milliseconds=100 + index)).isoformat(),
        "available_at": (submitted + timedelta(milliseconds=110 + index)).isoformat(),
        "arrival_mid": 100.0,
        "modeled_fill_price": 100.01 if index % 2 == 0 else 99.99,
        "observed_fill_price": 100.012 if index % 2 == 0 else 99.988,
        "future_mid": 100.005 if index % 2 == 0 else 99.995,
        "quantity": 1.0,
        "filled_quantity": 1.0,
        "censored": False,
        "source_event_digest": digest({"event": role, "index": index}),
    }
    value.update(overrides)
    return value


def observations(count=3):
    return [observation(i, role) for role in ("calibration", "holdout") for i in range(count)]


def config(**overrides):
    value = {
        "minimum_observations_per_role": 2,
        "maximum_censoring_rate": 0.25,
        "maximum_holdout_cost_shift_bps": 5.0,
        "declared_pessimistic_cost_bps": 4.0,
    }
    value.update(overrides)
    return value


def dependency(milestone):
    return build_receipt(
        milestone=milestone,
        producer="fixture",
        producer_version="1.0.0",
        source_commit=COMMIT,
        inputs={},
        dataset_digest=DATASET,
        configuration={},
        artifacts={},
        result={"qualified": True},
    ).as_dict()


def bundle():
    return declared_classic_bundle(
        profile="tier2",
        parameters={"taker_fee_bps": 5, "slippage_bps": 2, "spread_bps": 1, "delay_bars": 1},
    ).document()


def test_qualified_calibration_has_uncertainty_and_strata():
    result = calibrate_execution_quality(observations=observations(), known_at=NOW + timedelta(minutes=5), configuration=config())
    assert result["qualified"] is True
    assert result["decision"] == "use_empirical_calibration"
    assert result["strata"][0]["holdout"]["fill_rate"]["lower"] < 1
    assert result["strata"][0]["holdout"]["implementation_shortfall_bps"]["p95"] > 0


def test_future_available_observation_is_excluded():
    rows = observations() + [observation(99, "holdout", available_at=(NOW + timedelta(hours=2)).isoformat())]
    result = calibrate_execution_quality(observations=rows, known_at=NOW + timedelta(minutes=5), configuration=config())
    assert result["future_observations_excluded"] == 1
    assert result["observations"] == 6


def test_future_only_sample_cannot_supply_required_holdout():
    rows = [observation(i, "calibration") for i in range(3)] + [observation(50, "holdout")]
    with pytest.raises(ExecutionCalibrationError, match="calibration and holdout"):
        calibrate_execution_quality(observations=rows, known_at=NOW + timedelta(minutes=5), configuration=config())


def test_duplicate_identity_is_idempotent_but_conflict_fails():
    row = observation(0, "calibration")
    rows = [row, dict(row)] + [observation(i, "holdout") for i in range(3)]
    result = calibrate_execution_quality(observations=rows, known_at=NOW + timedelta(minutes=5), configuration=config(minimum_observations_per_role=1))
    assert result["observations"] == 4
    conflict = dict(row, arrival_mid=101)
    with pytest.raises(ExecutionCalibrationError, match="identity"):
        calibrate_execution_quality(observations=[row, conflict, observation(0, "holdout")], known_at=NOW + timedelta(minutes=5), configuration=config(minimum_observations_per_role=1))


def test_partial_fill_and_censoring_are_measured_not_discarded():
    rows = observations()
    rows[-1] = observation(2, "holdout", filled_quantity=0.5, terminal_at=None, censored=True)
    result = calibrate_execution_quality(observations=rows, known_at=NOW + timedelta(minutes=5), configuration=config(maximum_censoring_rate=0.5))
    holdout = result["strata"][0]["holdout"]
    assert holdout["partial_fill_rate"]["estimate"] == pytest.approx(1 / 3)
    assert holdout["censoring_rate"]["estimate"] == pytest.approx(1 / 3)


def test_excessive_censoring_forces_pessimistic_fallback():
    rows = observations()
    for index in range(3):
        rows[3 + index] = observation(index, "holdout", terminal_at=None, censored=True)
    result = calibrate_execution_quality(observations=rows, known_at=NOW + timedelta(minutes=5), configuration=config())
    assert result["qualified"] is False
    assert "excessive_censoring" in result["strata"][0]["reasons"]


def test_sparse_stratum_fails_without_weakening_threshold():
    result = calibrate_execution_quality(observations=observations(1), known_at=NOW + timedelta(minutes=5), configuration=config())
    assert result["decision"] == "use_pessimistic_fallback"
    assert result["strata"][0]["reasons"] == ["sparse_sample"]


def test_holdout_regime_shift_forces_fallback():
    rows = observations()
    for index in range(3):
        rows[3 + index] = observation(index, "holdout", observed_fill_price=100.2 if index % 2 == 0 else 99.8)
    result = calibrate_execution_quality(observations=rows, known_at=NOW + timedelta(minutes=5), configuration=config(maximum_holdout_cost_shift_bps=2))
    assert "holdout_regime_shift" in result["strata"][0]["reasons"]


def test_pessimistic_cost_never_improves_empirical_cost():
    result = calibrate_execution_quality(observations=observations(), known_at=NOW + timedelta(minutes=5), configuration=config(declared_pessimistic_cost_bps=20))
    stratum = result["strata"][0]
    assert stratum["pessimistic_cost_bps"] == 20
    assert stratum["pessimistic_cost_bps"] >= stratum["holdout"]["implementation_shortfall_bps"]["p95"]


def test_invalid_temporal_order_fails_closed():
    row = observation(0, "calibration", acknowledged_at=(NOW - timedelta(seconds=1)).isoformat())
    with pytest.raises(ExecutionCalibrationError, match="timestamps precede"):
        calibrate_execution_quality(observations=[row, observation(0, "holdout")], known_at=NOW + timedelta(minutes=5), configuration=config(minimum_observations_per_role=1))


def test_fill_requires_price_and_fill_timestamp():
    row = observation(0, "calibration", observed_fill_price=None)
    with pytest.raises(ExecutionCalibrationError, match="fill requires"):
        calibrate_execution_quality(observations=[row, observation(0, "holdout")], known_at=NOW + timedelta(minutes=5), configuration=config(minimum_observations_per_role=1))


def test_censored_terminal_record_is_rejected():
    row = observation(0, "calibration", censored=True)
    with pytest.raises(ExecutionCalibrationError, match="censored"):
        calibrate_execution_quality(observations=[row, observation(0, "holdout")], known_at=NOW + timedelta(minutes=5), configuration=config(minimum_observations_per_role=1))


def test_calibration_is_deterministic_under_input_order():
    rows = observations()
    first = calibrate_execution_quality(observations=rows, known_at=NOW + timedelta(minutes=5), configuration=config())
    second = calibrate_execution_quality(observations=reversed(rows), known_at=NOW + timedelta(minutes=5), configuration=config())
    assert first["calibration_digest"] == second["calibration_digest"]


def test_receipt_binds_dependencies_prior_bundle_and_no_authority():
    receipt = execution_calibration_receipt(
        exec001_receipt=dependency("EXEC-001"),
        exec002_receipt=dependency("EXEC-002"),
        exec004_receipt=dependency("EXEC-004"),
        shadow001_receipt=dependency("SHADOW-001"),
        market_model_bundle=bundle(),
        observations=observations(),
        known_at=NOW + timedelta(minutes=5),
        source_commit=COMMIT,
        dataset_digest=DATASET,
        configuration=config(),
    )
    assert verify_receipt(receipt)
    assert receipt.result["calibration_schema_digest"] == digest(CALIBRATION_SPECIFICATION)
    assert receipt.result["prior_market_model_bundle_digest"] == bundle()["bundle_digest"]
    assert receipt.result["qualified"] is True
    assert not any(receipt.authority.values())
    validate_model_bundle_document(receipt.result["empirical_market_model_bundle"])


def test_receipt_rejects_wrong_dependency_and_tampered_bundle():
    kwargs = dict(
        exec001_receipt=dependency("EXEC-001"),
        exec002_receipt=dependency("EXEC-002"),
        exec004_receipt=dependency("EXEC-004"),
        shadow001_receipt=dependency("SHADOW-001"),
        market_model_bundle=bundle(),
        observations=observations(),
        known_at=NOW + timedelta(minutes=5),
        source_commit=COMMIT,
        dataset_digest=DATASET,
        configuration=config(),
    )
    with pytest.raises(ExecutionCalibrationError, match="EXEC-004"):
        execution_calibration_receipt(**(kwargs | {"exec004_receipt": dependency("EXEC-003")}))
    tampered = bundle()
    tampered["name"] = "tampered"
    with pytest.raises(Exception, match="digest"):
        execution_calibration_receipt(**(kwargs | {"market_model_bundle": tampered}))
