from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from bt.institutional.capacity import (
    CAPACITY_SPECIFICATION,
    PortfolioCapacityError,
    capacity_dossier_receipt,
    portfolio_capacity,
)
from bt.institutional.receipt import build_receipt, digest, verify_receipt

NOW = datetime(2026, 9, 12, 12, tzinfo=UTC)
COMMIT = "a" * 40
DATASET = "b" * 64


def port003():
    result = {
        "valid": True,
        "selection": {
            "weights": {"carry": 0.6, "trend": 0.4},
            "metrics": {"expected_return": 0.08},
        },
    }
    return build_receipt(
        milestone="PORT-003",
        producer="bt.institutional.construction.construction_dossier_receipt",
        producer_version="1.0.0",
        source_commit=COMMIT,
        inputs={},
        dataset_digest=DATASET,
        configuration={},
        artifacts=result,
        result=result,
    )


def stratum(**overrides):
    value = {
        "venue_id": "bybit",
        "order_type": "limit",
        "size_bucket": "small",
        "regime": "normal",
        "pessimistic_cost_bps": 4.0,
        "holdout": {
            "implementation_shortfall_bps": {
                "p95": 3.0,
                "mean_interval_95": {"estimate": 2.5, "lower": 2.0, "upper": 3.5},
            }
        },
    }
    value.update(overrides)
    return value


def exec005(*, qualified=True):
    calibration = {"qualified": qualified, "strata": [stratum()]}
    result = {"qualified": qualified, "calibration": calibration}
    return build_receipt(
        milestone="EXEC-005",
        producer="bt.institutional.execution_calibration.execution_calibration_receipt",
        producer_version="1.0.0",
        source_commit=COMMIT,
        inputs={},
        dataset_digest=DATASET,
        configuration={},
        artifacts=result,
        result=result,
    )


def snapshot(candidate: str, **overrides):
    value = {
        "snapshot_id": f"snapshot-{candidate}",
        "candidate_id": candidate,
        "venue_id": "bybit",
        "listing_id": "BTCUSDT" if candidate == "carry" else "ETHUSDT",
        "order_type": "limit",
        "size_bucket": "small",
        "regime": "normal",
        "observed_at": (NOW - timedelta(seconds=30)).isoformat(),
        "available_at": (NOW - timedelta(seconds=20)).isoformat(),
        "average_daily_dollar_volume": 1_000_000,
        "executable_depth_notional": 100_000,
        "reference_trade_notional": 10_000,
        "spread_bps": 1.0,
        "source_digest": digest({"liquidity": candidate}),
    }
    value.update(overrides)
    return value


def config(**overrides):
    value = {
        "stale_after_seconds": 300,
        "rebalance_horizon_days": 1,
        "maximum_participation_rate": 0.05,
        "maximum_depth_fraction": 0.5,
        "maximum_liquidation_days": 3,
        "maximum_cost_bps": 10,
        "maximum_portfolio_cost_bps": 5,
        "explicit_fee_bps": 0.5,
        "stress_volume_haircut": 0.5,
        "stress_spread_multiplier": 2,
        "stress_impact_multiplier": 1.5,
        "scale_grid": [0.5, 1.0, 2.0],
    }
    value.update(overrides)
    return value


def direct(**overrides):
    values = {
        "target_weights": {"carry": 0.6, "trend": 0.4},
        "prior_weights": {"carry": 0.5, "trend": 0.5},
        "liquidity_snapshots": [snapshot("carry"), snapshot("trend")],
        "execution_calibration": {"qualified": True, "strata": [stratum()]},
        "known_at": NOW,
        "requested_capital": 100_000,
        "configuration": config(),
    }
    values.update(overrides)
    return portfolio_capacity(**values)


def test_qualified_result_reports_turnover_cost_and_capacity() -> None:
    result = direct()
    assert result["qualified"] is True
    assert result["turnover"]["gross_l1"] == pytest.approx(0.2)
    assert result["turnover"]["one_way"] == pytest.approx(0.1)
    assert result["capacity_supported_notional"] == 100_000
    assert result["stressed_capital_capacity"] <= result["base_capital_capacity"]
    assert result["requested_economics"]["stress_net_expected_return"] <= result["requested_economics"][
        "base_net_expected_return"
    ]


def test_cost_curve_is_deterministic_and_cost_dollars_are_monotone() -> None:
    first = direct()
    second = direct(liquidity_snapshots=reversed([snapshot("carry"), snapshot("trend")]))
    assert first["capacity_digest"] == second["capacity_digest"]
    assert [row["base_cost"] for row in first["cost_capacity_curve"]] == sorted(
        row["base_cost"] for row in first["cost_capacity_curve"]
    )
    assert all(row["stress_cost"] >= row["base_cost"] for row in first["cost_capacity_curve"])


def test_stale_liquidity_forces_zero_capacity() -> None:
    old = (NOW - timedelta(hours=1)).isoformat()
    result = direct(liquidity_snapshots=[snapshot("carry", observed_at=old), snapshot("trend", observed_at=old)])
    assert result["qualified"] is False
    assert result["capacity_supported_notional"] == 0
    assert "stale_liquidity:carry" in result["failures"]


def test_future_available_snapshot_is_excluded() -> None:
    result = direct(
        liquidity_snapshots=[
            snapshot("carry"),
            snapshot("trend"),
            snapshot("carry", snapshot_id="future", available_at=(NOW + timedelta(seconds=1)).isoformat()),
        ]
    )
    assert result["future_snapshots_excluded"] == 1


def test_missing_candidate_liquidity_forces_zero_capacity() -> None:
    result = direct(liquidity_snapshots=[snapshot("carry")])
    assert result["decision"] == "abstain_zero_executable_capacity"
    assert "missing_liquidity:trend" in result["failures"]


def test_unmatched_execution_stratum_forces_zero_capacity() -> None:
    result = direct(execution_calibration={"qualified": True, "strata": [stratum(regime="stress")]})
    assert "execution_stratum_unavailable:carry" in result["failures"]


def test_unqualified_execution_calibration_cannot_admit_capital() -> None:
    result = direct(execution_calibration={"qualified": False, "strata": [stratum()]})
    assert result["capacity_supported_notional"] == 0
    assert "execution_calibration_unqualified" in result["failures"]


def test_participation_shock_reduces_capacity() -> None:
    ordinary = direct(configuration=config(stress_volume_haircut=0.1))
    severe = direct(configuration=config(stress_volume_haircut=0.8))
    assert severe["stressed_capital_capacity"] < ordinary["stressed_capital_capacity"]


def test_excessive_requested_scale_fails_capacity_gate() -> None:
    result = direct(requested_capital=200_000)
    assert result["qualified"] is False
    assert "stress_capacity_breached" in result["failures"]


def test_portfolio_cost_limit_is_enforced() -> None:
    result = direct(configuration=config(maximum_portfolio_cost_bps=0.01))
    assert result["qualified"] is False
    assert "portfolio_cost_limit_breached" in result["failures"]


def test_snapshot_identity_conflict_fails_closed() -> None:
    conflict = snapshot("carry", average_daily_dollar_volume=2_000_000)
    with pytest.raises(PortfolioCapacityError, match="identity"):
        direct(liquidity_snapshots=[snapshot("carry"), conflict, snapshot("trend")])


def test_invalid_weights_and_grid_fail_closed() -> None:
    with pytest.raises(PortfolioCapacityError, match="sum to one"):
        direct(target_weights={"carry": 0.7, "trend": 0.4})
    with pytest.raises(PortfolioCapacityError, match="scale_grid"):
        direct(configuration=config(scale_grid=[1.0, 0.5]))
    with pytest.raises(PortfolioCapacityError, match="contain 1.0"):
        direct(configuration=config(scale_grid=[0.5, 2.0]))


def test_receipt_binds_dependencies_and_has_no_authority() -> None:
    receipt = capacity_dossier_receipt(
        port003_receipt=port003(),
        exec005_receipt=exec005(),
        prior_weights={"carry": 0.5, "trend": 0.5},
        liquidity_snapshots=[snapshot("carry"), snapshot("trend")],
        known_at=NOW,
        requested_capital=100_000,
        configuration=config(),
        dataset_digest=DATASET,
        source_commit=COMMIT,
    )
    assert verify_receipt(receipt)
    assert receipt.result["qualified"] is True
    assert receipt.result["dependency_receipts"]["port003"] == port003().receipt_digest
    assert not any(receipt.authority.values())
    assert CAPACITY_SPECIFICATION["unsupported_action"] == "zero executable capacity and abstain"


def test_wrong_or_tampered_dependencies_fail_closed() -> None:
    wrong = port003().as_dict()
    wrong["milestone"] = "PORT-002"
    with pytest.raises(PortfolioCapacityError, match="PORT-003"):
        capacity_dossier_receipt(
            port003_receipt=wrong,
            exec005_receipt=exec005(),
            prior_weights={"carry": 0.5, "trend": 0.5},
            liquidity_snapshots=[snapshot("carry"), snapshot("trend")],
            known_at=NOW,
            requested_capital=100_000,
            configuration=config(),
            dataset_digest=DATASET,
            source_commit=COMMIT,
        )
    tampered = deepcopy(exec005().as_dict())
    tampered["result"]["qualified"] = False
    with pytest.raises(PortfolioCapacityError, match="EXEC-005"):
        capacity_dossier_receipt(
            port003_receipt=port003(),
            exec005_receipt=tampered,
            prior_weights={"carry": 0.5, "trend": 0.5},
            liquidity_snapshots=[snapshot("carry"), snapshot("trend")],
            known_at=NOW,
            requested_capital=100_000,
            configuration=config(),
            dataset_digest=DATASET,
            source_commit=COMMIT,
        )
