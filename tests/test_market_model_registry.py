from __future__ import annotations

from dataclasses import replace

import pytest

from bt.execution.model_registry import (
    CalibrationProvenance,
    MarketModelBundle,
    MarketModelCard,
    MarketModelError,
    assert_pessimistic_cost_order,
    declared_classic_bundle,
)


def test_classic_bundle_is_deterministic_and_declares_capability_boundaries() -> None:
    parameters = {"taker_fee_bps": 6.0, "slippage_bps": 2.0, "spread_bps": 1.0, "delay_bars": 1}
    first = declared_classic_bundle(profile="tier2", parameters=parameters)
    second = declared_classic_bundle(profile="tier2", parameters=dict(reversed(list(parameters.items()))))

    assert first.digest == second.digest
    assert len(first.digest) == 64
    assert first.require("fee").parameters["taker_fee_bps"] == 6.0
    assert first.require("fill").parameters["partial_fill"] is False

    with pytest.raises(MarketModelError, match="funding cashflows are not charged"):
        first.require("funding")
    with pytest.raises(MarketModelError, match="borrow availability"):
        first.require("borrow")
    with pytest.raises(MarketModelError, match="capacity is diagnostic-only"):
        first.require("capacity")


def test_empirical_model_requires_digest_bound_calibration_and_finite_diagnostics() -> None:
    calibration = CalibrationProvenance(
        source="empirical",
        dataset_digest=None,
        sample_start="2025-01-01T00:00:00Z",
        sample_end="2025-02-01T00:00:00Z",
        method="robust median by venue tier",
        fit_diagnostics={"mae_bps": 0.2},
        holdout_diagnostics={"mae_bps": 0.3},
    )
    card = MarketModelCard(
        model_id="observed-fees",
        version="1.0.0",
        kind="fee",
        support_status="supported",
        implementation="bt.execution.fees.FeeModel",
        applicability={"venues": ("bybit",)},
        timestamp_semantics="fee schedule known before order",
        parameters={"taker_fee_bps": 5.5},
        uncertainty={"holdout_mae_bps": 0.3},
        stress_ranges={"taker_fee_bps": (5.5, 12.0)},
        calibration=calibration,
        fallback="reject unknown venue",
    )
    with pytest.raises(MarketModelError, match="dataset digest"):
        card.document()

    invalid = replace(
        card,
        calibration=replace(calibration, dataset_digest="a" * 64, holdout_diagnostics={"mae": float("nan")}),
    )
    with pytest.raises(MarketModelError, match="finite"):
        invalid.document()


def test_registry_rejects_duplicate_model_identity() -> None:
    model = declared_classic_bundle(
        profile="tier1",
        parameters={"taker_fee_bps": 4.0, "slippage_bps": 0.5, "spread_bps": 0.0, "delay_bars": 0},
    ).models[0]
    with pytest.raises(MarketModelError, match="duplicate identities"):
        MarketModelBundle(name="duplicate", version="1", models=(model, model)).document()


def test_pessimistic_stress_is_monotone_and_non_negative() -> None:
    assert_pessimistic_cost_order(baseline_cost=10.0, stressed_cost=15.0)
    assert_pessimistic_cost_order(baseline_cost=10.0, stressed_cost=10.0)
    with pytest.raises(MarketModelError, match="cannot reduce"):
        assert_pessimistic_cost_order(baseline_cost=10.0, stressed_cost=9.99)
    with pytest.raises(MarketModelError, match="non-negative"):
        assert_pessimistic_cost_order(baseline_cost=-1.0, stressed_cost=2.0)
