from __future__ import annotations

from copy import deepcopy

import pytest

from bt.institutional.construction import PortfolioConstructionError, construction_dossier_receipt
from bt.institutional.receipt import build_receipt, digest, verify_receipt

COMMIT = "a" * 40
DATASET = "b" * 64


def _dependency():
    result = {"schema_version": "port002-dependency-dossier-v1.0.0", "qualified": True}
    return build_receipt(
        milestone="PORT-002",
        producer="bt.institutional.portfolio.dependency_dossier_receipt",
        producer_version="1.0.0",
        source_commit=COMMIT,
        inputs={"candidates": ["carry", "trend", "value"]},
        dataset_digest=DATASET,
        configuration={"cluster_threshold": 0.8},
        artifacts=result,
        result=result,
    )


def _risk():
    result = {"schema_version": "risk001-stress-dossier-v1.0.0", "admissible": True}
    return build_receipt(
        milestone="RISK-001",
        producer="bt.institutional.risk.stress_dossier_receipt",
        producer_version="1.0.0",
        source_commit=COMMIT,
        inputs={"portfolio": "fixture"},
        dataset_digest=DATASET,
        configuration={"scenario_limit": 0.2},
        artifacts=result,
        result=result,
    )


def _solver(**overrides):
    values = {
        "name": "projected-gradient-robust-mean-variance",
        "version": "1.0.0",
        "risk_aversion": 4.0,
        "uncertainty_penalty": 0.25,
        "covariance_shrinkage": 0.20,
        "step_size": 0.5,
        "iterations": 500,
    }
    values.update(overrides)
    return values


def _receipt(**overrides):
    values = {
        "dependency_receipt": _dependency(),
        "risk_receipt": _risk(),
        "expected_returns": {"carry": 0.035, "trend": 0.08, "value": 0.045},
        "uncertainty": {"carry": 0.003, "trend": 0.004, "value": 0.003},
        "covariance": [[0.04, 0.006, 0.004], [0.006, 0.09, 0.008], [0.004, 0.008, 0.05]],
        "prior_weights": {"carry": 0.34, "trend": 0.33, "value": 0.33},
        "lower_bounds": {"carry": 0.10, "trend": 0.10, "value": 0.10},
        "upper_bounds": {"carry": 0.60, "trend": 0.60, "value": 0.60},
        "solver": _solver(),
        "weight_increment": 0.01,
        "maximum_turnover": 0.80,
        "maximum_sensitivity_l1": 0.50,
        "dataset_digest": DATASET,
        "source_commit": COMMIT,
    }
    values.update(overrides)
    return construction_dossier_receipt(**values)


def test_robust_optimizer_beats_benchmarks_and_is_deterministic() -> None:
    first = _receipt()
    second = _receipt()
    assert first == second
    assert verify_receipt(first)
    result = first.result
    assert result["selection"]["method"] == "robust_optimizer"
    assert result["selection"]["fallback_used"] is False
    assert result["optimized"]["deterministic_replay"] is True
    assert result["rounding"]["sum"] == 1
    assert sum(result["selection"]["weights"].values()) == pytest.approx(1)
    assert first.authority == {"allocation": False, "capital": False, "orders": False, "promotion": False}


def test_turnover_or_sensitivity_breach_uses_deterministic_benchmark_fallback() -> None:
    receipt = _receipt(maximum_turnover=0.01, maximum_sensitivity_l1=0.01)
    assert receipt.result["selection"]["fallback_used"] is True
    assert receipt.result["selection"]["method"].startswith("fallback:")
    assert "turnover_limit_breached" in receipt.result["failures"]
    assert sum(receipt.result["selection"]["weights"].values()) == pytest.approx(1)


def test_singular_covariance_is_regularized_without_hidden_solver_change() -> None:
    singular = [[0.04, 0.04, 0.02], [0.04, 0.04, 0.02], [0.02, 0.02, 0.03]]
    receipt = _receipt(covariance=singular, solver=_solver(covariance_shrinkage=0.25))
    assert receipt.result["valid"] is True
    assert receipt.result["covariance"]["shrinkage"] == 0.25
    assert receipt.result["solver_digest"] == digest(_solver(covariance_shrinkage=0.25))


def test_infeasible_bounds_and_indefinite_covariance_fail_closed() -> None:
    with pytest.raises(PortfolioConstructionError, match="bounds are infeasible"):
        _receipt(
            lower_bounds={"carry": 0.40, "trend": 0.40, "value": 0.40},
            upper_bounds={"carry": 0.60, "trend": 0.60, "value": 0.60},
        )
    with pytest.raises(PortfolioConstructionError, match="positive semidefinite"):
        _receipt(covariance=[[0.01, 0.04, 0], [0.04, 0.01, 0], [0, 0, 0.01]], solver=_solver(covariance_shrinkage=0))


def test_rounding_is_deterministic_and_respects_bounds() -> None:
    receipt = _receipt(weight_increment=0.05)
    weights = receipt.result["selection"]["weights"]
    assert all(round(value / 0.05) == pytest.approx(value / 0.05) for value in weights.values())
    assert all(0.10 <= value <= 0.60 for value in weights.values())
    assert sum(weights.values()) == pytest.approx(1)


def test_dependency_and_risk_receipts_are_mandatory_and_immutable() -> None:
    invalid_dependency = _dependency().as_dict()
    invalid_dependency["result"] = deepcopy(invalid_dependency["result"])
    invalid_dependency["result"]["qualified"] = False
    with pytest.raises(PortfolioConstructionError, match="qualified PORT-002"):
        _receipt(dependency_receipt=invalid_dependency)
    invalid_risk = _risk().as_dict()
    invalid_risk["receipt_digest"] = "0" * 64
    with pytest.raises(PortfolioConstructionError, match="admissible RISK-001"):
        _receipt(risk_receipt=invalid_risk)


def test_unknown_solver_identity_is_rejected() -> None:
    with pytest.raises(PortfolioConstructionError, match="solver identity"):
        _receipt(solver=_solver(name="mystery-optimizer"))
