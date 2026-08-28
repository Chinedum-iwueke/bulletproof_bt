"""PORT-003 native benchmark ladder and robust construction producer."""

from __future__ import annotations

from math import sqrt
from typing import Any

import numpy as np

from .receipt import ProducerReceipt, build_receipt, digest, verify_receipt


class PortfolioConstructionError(ValueError):
    """Portfolio construction inputs cannot produce trustworthy evidence."""


def _rounded(value: float) -> float:
    return round(float(value), 12)


def _project_capped_simplex(values: np.ndarray, lower: np.ndarray, upper: np.ndarray) -> np.ndarray:
    if lower.sum() > 1 + 1e-12 or upper.sum() < 1 - 1e-12:
        raise PortfolioConstructionError("weight bounds are infeasible")
    low = float(np.min(values - upper)) - 1.0
    high = float(np.max(values - lower)) + 1.0
    for _ in range(100):
        middle = (low + high) / 2
        projected = np.clip(values - middle, lower, upper)
        if projected.sum() > 1:
            low = middle
        else:
            high = middle
    result = np.clip(values - high, lower, upper)
    residual = 1.0 - float(result.sum())
    for index in range(len(result)):
        room = upper[index] - result[index] if residual > 0 else result[index] - lower[index]
        adjustment = np.sign(residual) * min(abs(residual), float(room))
        result[index] += adjustment
        residual -= adjustment
    if abs(residual) > 1e-10:
        raise PortfolioConstructionError("simplex projection did not converge")
    return result


def _round_weights(
    weights: np.ndarray,
    candidate_ids: list[str],
    increment: float,
    lower: np.ndarray,
    upper: np.ndarray,
) -> np.ndarray:
    units = int(round(1 / increment))
    if units <= 0 or abs(units * increment - 1) > 1e-10:
        raise PortfolioConstructionError("weight increment must divide one exactly")
    floor_units = np.ceil(lower / increment - 1e-12).astype(int)
    cap_units = np.floor(upper / increment + 1e-12).astype(int)
    target = np.floor(weights / increment + 1e-12).astype(int)
    target = np.clip(target, floor_units, cap_units)
    remaining = units - int(target.sum())
    fractions = weights / increment - np.floor(weights / increment)
    while remaining > 0:
        eligible = [index for index in range(len(target)) if target[index] < cap_units[index]]
        if not eligible:
            raise PortfolioConstructionError("rounding cannot satisfy upper bounds")
        chosen = sorted(eligible, key=lambda index: (-fractions[index], candidate_ids[index]))[0]
        target[chosen] += 1
        remaining -= 1
    while remaining < 0:
        eligible = [index for index in range(len(target)) if target[index] > floor_units[index]]
        if not eligible:
            raise PortfolioConstructionError("rounding cannot satisfy lower bounds")
        chosen = sorted(eligible, key=lambda index: (fractions[index], candidate_ids[index]))[0]
        target[chosen] -= 1
        remaining += 1
    return target.astype(float) * increment


def _metrics(weights: np.ndarray, expected: np.ndarray, covariance: np.ndarray) -> dict[str, float]:
    variance = max(0.0, float(weights @ covariance @ weights))
    volatility = sqrt(variance)
    expected_return = float(weights @ expected)
    return {
        "expected_return": _rounded(expected_return),
        "volatility": _rounded(volatility),
        "return_to_risk": _rounded(expected_return / volatility) if volatility else 0.0,
        "hhi": _rounded(float(weights @ weights)),
    }


def _objective(
    weights: np.ndarray,
    expected: np.ndarray,
    covariance: np.ndarray,
    risk_aversion: float,
    uncertainty: np.ndarray,
    uncertainty_penalty: float,
) -> float:
    return float(
        weights @ expected
        - risk_aversion * (weights @ covariance @ weights)
        - uncertainty_penalty * (weights @ uncertainty)
    )


def construction_dossier_receipt(
    *,
    dependency_receipt: ProducerReceipt | dict[str, Any],
    risk_receipt: ProducerReceipt | dict[str, Any],
    expected_returns: dict[str, float],
    uncertainty: dict[str, float],
    covariance: list[list[float]],
    prior_weights: dict[str, float],
    lower_bounds: dict[str, float],
    upper_bounds: dict[str, float],
    solver: dict[str, Any],
    weight_increment: float,
    maximum_turnover: float,
    maximum_sensitivity_l1: float,
    dataset_digest: str,
    source_commit: str,
) -> ProducerReceipt:
    """Compare benchmarks with a deterministic robust optimizer and fallback."""
    dependency = dependency_receipt.as_dict() if isinstance(dependency_receipt, ProducerReceipt) else dependency_receipt
    risk = risk_receipt.as_dict() if isinstance(risk_receipt, ProducerReceipt) else risk_receipt
    if not verify_receipt(dependency) or dependency["milestone"] != "PORT-002" or not dependency["result"]["qualified"]:
        raise PortfolioConstructionError("PORT-003 requires a qualified PORT-002 receipt")
    if not verify_receipt(risk) or risk["milestone"] != "RISK-001" or not risk["result"]["admissible"]:
        raise PortfolioConstructionError("PORT-003 requires an admissible RISK-001 receipt")
    candidate_ids = sorted(expected_returns)
    mappings = (uncertainty, prior_weights, lower_bounds, upper_bounds)
    if len(candidate_ids) < 2 or any(set(mapping) != set(candidate_ids) for mapping in mappings):
        raise PortfolioConstructionError("all construction mappings must share at least two candidates")
    matrix = np.asarray(covariance, dtype=np.float64)
    size = len(candidate_ids)
    if matrix.shape != (size, size) or not np.isfinite(matrix).all() or not np.allclose(matrix, matrix.T, atol=1e-12):
        raise PortfolioConstructionError("covariance must be finite, square and symmetric")
    expected = np.asarray([expected_returns[key] for key in candidate_ids], dtype=np.float64)
    errors = np.asarray([uncertainty[key] for key in candidate_ids], dtype=np.float64)
    prior = np.asarray([prior_weights[key] for key in candidate_ids], dtype=np.float64)
    lower = np.asarray([lower_bounds[key] for key in candidate_ids], dtype=np.float64)
    upper = np.asarray([upper_bounds[key] for key in candidate_ids], dtype=np.float64)
    if not all(np.isfinite(value).all() for value in (expected, errors, prior, lower, upper)):
        raise PortfolioConstructionError("construction inputs must be finite")
    if np.any(errors < 0) or np.any(lower < 0) or np.any(upper > 1) or np.any(lower > upper):
        raise PortfolioConstructionError("uncertainty and weight bounds are invalid")
    if lower.sum() > 1 + 1e-12 or upper.sum() < 1 - 1e-12:
        raise PortfolioConstructionError("weight bounds are infeasible")
    if abs(float(prior.sum()) - 1) > 1e-9 or np.any(prior < lower - 1e-12) or np.any(prior > upper + 1e-12):
        raise PortfolioConstructionError("prior weights are infeasible")
    if solver.get("name") != "projected-gradient-robust-mean-variance" or solver.get("version") != "1.0.0":
        raise PortfolioConstructionError("solver identity is not supported")
    risk_aversion = float(solver["risk_aversion"])
    uncertainty_penalty = float(solver["uncertainty_penalty"])
    shrinkage = float(solver["covariance_shrinkage"])
    step_size = float(solver["step_size"])
    iterations = int(solver["iterations"])
    if not (risk_aversion > 0 and uncertainty_penalty >= 0 and 0 <= shrinkage <= 1 and step_size > 0 and iterations > 0):
        raise PortfolioConstructionError("solver parameters are invalid")
    if maximum_turnover < 0 or maximum_sensitivity_l1 < 0:
        raise PortfolioConstructionError("turnover and sensitivity limits cannot be negative")

    robust_covariance = (1 - shrinkage) * matrix + shrinkage * np.diag(np.diag(matrix))
    eigenvalues = np.linalg.eigvalsh(robust_covariance)
    if eigenvalues.min() < -1e-10:
        raise PortfolioConstructionError("covariance is not positive semidefinite")
    equal = _project_capped_simplex(np.full(size, 1 / size), lower, upper)
    inverse_volatility = 1 / np.sqrt(np.maximum(np.diag(robust_covariance), 1e-16))
    risk_budget = _project_capped_simplex(inverse_volatility / inverse_volatility.sum(), lower, upper)

    def solve(means: np.ndarray) -> np.ndarray:
        weights = risk_budget.copy()
        for _ in range(iterations):
            gradient = means - 2 * risk_aversion * (robust_covariance @ weights) - uncertainty_penalty * errors
            weights = _project_capped_simplex(weights + step_size * gradient, lower, upper)
        return weights

    optimized = solve(expected)
    repeat = solve(expected)
    deterministic = bool(np.array_equal(optimized, repeat))
    optimized = _round_weights(optimized, candidate_ids, weight_increment, lower, upper)
    turnover = float(np.abs(optimized - prior).sum())
    sensitivity_runs = [solve(expected - errors), solve(expected + errors)]
    sensitivity_l1 = max(float(np.abs(run - optimized).sum()) for run in sensitivity_runs)
    benchmark_vectors = {"equal_weight": equal, "risk_budget": risk_budget}
    benchmark_ladder = {
        name: {
            "weights": {key: _rounded(vector[index]) for index, key in enumerate(candidate_ids)},
            "metrics": _metrics(vector, expected, robust_covariance),
            "robust_objective": _rounded(_objective(vector, expected, robust_covariance, risk_aversion, errors, uncertainty_penalty)),
        }
        for name, vector in benchmark_vectors.items()
    }
    optimized_objective = _objective(optimized, expected, robust_covariance, risk_aversion, errors, uncertainty_penalty)
    best_benchmark_name = max(benchmark_ladder, key=lambda name: benchmark_ladder[name]["robust_objective"])
    best_benchmark = benchmark_vectors[best_benchmark_name]
    failures = []
    if not deterministic:
        failures.append("solver_nondeterminism")
    if turnover > maximum_turnover + 1e-12:
        failures.append("turnover_limit_breached")
    if sensitivity_l1 > maximum_sensitivity_l1 + 1e-12:
        failures.append("sensitivity_limit_breached")
    if optimized_objective <= benchmark_ladder[best_benchmark_name]["robust_objective"] + 1e-12:
        failures.append("optimizer_did_not_improve_benchmark")
    selected_method = "robust_optimizer" if not failures else "fallback:" + best_benchmark_name
    selected = optimized if not failures else best_benchmark
    selected = _round_weights(selected, candidate_ids, weight_increment, lower, upper)
    if abs(float(selected.sum()) - 1) > 1e-10 or np.any(selected < lower - 1e-12) or np.any(selected > upper + 1e-12):
        raise PortfolioConstructionError("selected construction is infeasible after rounding")
    result = {
        "schema_version": "port003-construction-dossier-v1.0.0",
        "dependency_receipt_digest": dependency["receipt_digest"],
        "risk_receipt_digest": risk["receipt_digest"],
        "solver": {key: solver[key] for key in sorted(solver)},
        "solver_digest": digest(solver),
        "candidate_ids": candidate_ids,
        "benchmark_ladder": benchmark_ladder,
        "optimized": {
            "weights": {key: _rounded(optimized[index]) for index, key in enumerate(candidate_ids)},
            "metrics": _metrics(optimized, expected, robust_covariance),
            "robust_objective": _rounded(optimized_objective),
            "turnover_l1": _rounded(turnover),
            "sensitivity_l1": _rounded(sensitivity_l1),
            "deterministic_replay": deterministic,
        },
        "selection": {
            "method": selected_method,
            "weights": {key: _rounded(selected[index]) for index, key in enumerate(candidate_ids)},
            "metrics": _metrics(selected, expected, robust_covariance),
            "fallback_used": bool(failures),
        },
        "covariance": {
            "input_digest": digest(covariance),
            "shrinkage": shrinkage,
            "minimum_eigenvalue": _rounded(float(eigenvalues.min())),
        },
        "rounding": {"weight_increment": weight_increment, "sum": _rounded(float(selected.sum()))},
        "failures": failures,
        "valid": True,
        "claim": "construction evidence only; no allocation, capital or order authority",
    }
    return build_receipt(
        milestone="PORT-003",
        producer="bt.institutional.construction.construction_dossier_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={
            "dependency_receipt": dependency,
            "risk_receipt": risk,
            "expected_returns": expected_returns,
            "uncertainty": uncertainty,
            "covariance": covariance,
            "prior_weights": prior_weights,
            "lower_bounds": lower_bounds,
            "upper_bounds": upper_bounds,
        },
        dataset_digest=dataset_digest,
        configuration={
            "solver": solver,
            "weight_increment": weight_increment,
            "maximum_turnover": maximum_turnover,
            "maximum_sensitivity_l1": maximum_sensitivity_l1,
        },
        artifacts=result,
        result=result,
    )
