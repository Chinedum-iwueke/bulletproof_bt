"""PORT-002 native dependency, diversification and concentration producer."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from math import atanh, sqrt, tanh
from typing import Any

import numpy as np

from bt.portfolio_engine.candidate_service import validate_portfolio_candidate_dossier

from .receipt import ProducerReceipt, build_receipt


class PortfolioDependencyError(ValueError):
    """Dependency evidence is malformed or cannot support the requested claim."""


def _rounded(value: float) -> float:
    return round(float(value), 12)


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PortfolioDependencyError("timestamps must be timezone-aware")
    return parsed.astimezone(UTC)


def _correlation_interval(correlation: float, observations: int) -> list[float]:
    if observations <= 3 or abs(correlation) >= 1:
        return [-1.0, 1.0]
    center = atanh(correlation)
    radius = 1.96 / sqrt(observations - 3)
    return [_rounded(tanh(center - radius)), _rounded(tanh(center + radius))]


def _components(matrix: np.ndarray, threshold: float) -> list[list[int]]:
    remaining = set(range(len(matrix)))
    groups: list[list[int]] = []
    while remaining:
        stack = [min(remaining)]
        component: set[int] = set()
        while stack:
            current = stack.pop()
            if current in component:
                continue
            component.add(current)
            remaining.discard(current)
            stack.extend(
                index
                for index in sorted(remaining, reverse=True)
                if matrix[current, index] >= threshold
            )
        groups.append(sorted(component))
    return groups


def dependency_dossier_receipt(
    *,
    candidate_dossier: dict[str, Any],
    observations: dict[str, list[dict[str, Any]]],
    exposures: dict[str, list[str]],
    evaluated_at: str,
    stale_after_seconds: int,
    minimum_overlap: int,
    minimum_state_overlap: int,
    tail_quantile: float,
    cluster_threshold: float,
    maximum_cluster_weight: float,
    maximum_exposure_weight: float,
    maximum_hhi: float,
    dataset_digest: str,
    source_commit: str,
) -> ProducerReceipt:
    """Measure portfolio dependency without allocating capital or changing weights."""
    validate_portfolio_candidate_dossier(candidate_dossier)
    if candidate_dossier["decision"] != "candidate":
        raise PortfolioDependencyError("PORT-002 requires an admitted PORT-001 dossier")
    weights = candidate_dossier["proposal"]["weights"]
    candidate_ids = sorted(weights)
    if set(observations) != set(candidate_ids):
        raise PortfolioDependencyError("observations must exactly match PORT-001 candidates")
    if not 0 < tail_quantile < 0.5 or not 0 < cluster_threshold <= 1:
        raise PortfolioDependencyError("tail and clustering parameters are invalid")
    if min(stale_after_seconds, minimum_overlap, minimum_state_overlap) <= 0:
        raise PortfolioDependencyError("freshness and overlap limits must be positive")
    for limit in (maximum_cluster_weight, maximum_exposure_weight, maximum_hhi):
        if not 0 < limit <= 1:
            raise PortfolioDependencyError("concentration limits must be in (0, 1]")

    evaluated = _parse_time(evaluated_at)
    normalized: dict[str, dict[str, tuple[float, str]]] = {}
    for candidate_id in candidate_ids:
        series: dict[str, tuple[float, str]] = {}
        for item in observations[candidate_id]:
            timestamp = _parse_time(str(item["timestamp"]))
            key = timestamp.isoformat().replace("+00:00", "Z")
            value = float(item["net_return"])
            state = str(item.get("state", "unknown")).strip()
            if key in series or not np.isfinite(value) or not state:
                raise PortfolioDependencyError("observations must be unique, finite and state-tagged")
            series[key] = (value, state)
        normalized[candidate_id] = series
    common = sorted(set.intersection(*(set(series) for series in normalized.values())))
    reasons: list[str] = []
    if len(common) < minimum_overlap:
        reasons.append("insufficient_common_history")
    if not common or (evaluated - _parse_time(common[-1])).total_seconds() > stale_after_seconds:
        reasons.append("stale_input_window")
    states: dict[str, list[str]] = defaultdict(list)
    for timestamp in common:
        labels = {normalized[candidate_id][timestamp][1] for candidate_id in candidate_ids}
        if len(labels) != 1:
            reasons.append("inconsistent_state_labels")
            break
        states[next(iter(labels))].append(timestamp)
    sparse_states = sorted(state for state, stamps in states.items() if len(stamps) < minimum_state_overlap)
    if sparse_states:
        reasons.append("sparse_state_history:" + ",".join(sparse_states))

    pairwise: list[dict[str, Any]] = []
    joint = np.eye(len(candidate_ids), dtype=np.float64)
    if not reasons:
        for left_index, left in enumerate(candidate_ids):
            for right_index in range(left_index + 1, len(candidate_ids)):
                right = candidate_ids[right_index]
                left_values = np.asarray([normalized[left][stamp][0] for stamp in common])
                right_values = np.asarray([normalized[right][stamp][0] for stamp in common])
                correlation = float(np.corrcoef(left_values, right_values)[0, 1])
                if not np.isfinite(correlation):
                    reasons.append("undefined_dependency:" + left + ":" + right)
                    continue
                lower_left = left_values <= np.quantile(left_values, tail_quantile)
                lower_right = right_values <= np.quantile(right_values, tail_quantile)
                lower_tail = float(np.mean(lower_left & lower_right) / tail_quantile)
                shared = sorted(set(exposures.get(left, [])) & set(exposures.get(right, [])))
                union = set(exposures.get(left, [])) | set(exposures.get(right, []))
                exposure_dependency = len(shared) / len(union) if union else 0.0
                by_state: dict[str, Any] = {}
                state_max = 0.0
                for state, stamps in sorted(states.items()):
                    x = np.asarray([normalized[left][stamp][0] for stamp in stamps])
                    y = np.asarray([normalized[right][stamp][0] for stamp in stamps])
                    state_correlation = float(np.corrcoef(x, y)[0, 1])
                    state_max = max(state_max, abs(state_correlation))
                    by_state[state] = {
                        "observations": len(stamps),
                        "correlation": _rounded(state_correlation),
                        "confidence_interval_95": _correlation_interval(state_correlation, len(stamps)),
                    }
                score = min(1.0, max(abs(correlation), lower_tail, exposure_dependency, state_max))
                joint[left_index, right_index] = joint[right_index, left_index] = score
                pairwise.append(
                    {
                        "left": left,
                        "right": right,
                        "observations": len(common),
                        "correlation": _rounded(correlation),
                        "confidence_interval_95": _correlation_interval(correlation, len(common)),
                        "lower_tail_dependence": _rounded(lower_tail),
                        "shared_exposures": shared,
                        "exposure_dependency": _rounded(exposure_dependency),
                        "state_dependence": by_state,
                        "joint_dependency": _rounded(score),
                    }
                )

    clusters: list[dict[str, Any]] = []
    cluster_max = 1.0
    exposure_weights: dict[str, float] = defaultdict(float)
    hhi = sum(float(weight) ** 2 for weight in weights.values())
    if not reasons:
        for number, component in enumerate(_components(joint, cluster_threshold), start=1):
            members = [candidate_ids[index] for index in component]
            weight = sum(float(weights[member]) for member in members)
            clusters.append({"cluster": number, "members": members, "weight": _rounded(weight)})
        cluster_max = max(item["weight"] for item in clusters)
        for candidate_id, weight in weights.items():
            for exposure in sorted(set(exposures.get(candidate_id, []))):
                exposure_weights[exposure] += float(weight)
        if cluster_max > maximum_cluster_weight:
            reasons.append("cluster_concentration_limit_breached")
        if exposure_weights and max(exposure_weights.values()) > maximum_exposure_weight:
            reasons.append("shared_exposure_limit_breached")
        if hhi > maximum_hhi:
            reasons.append("candidate_hhi_limit_breached")

    diversification: dict[str, Any] = {}
    stress: dict[str, Any] = {}
    if common and not any(reason.startswith("undefined_dependency") for reason in reasons):
        matrix = np.asarray(
            [[normalized[candidate][stamp][0] for stamp in common] for candidate in candidate_ids]
        )
        volatilities = matrix.std(axis=1, ddof=1)
        weight_vector = np.asarray([weights[candidate] for candidate in candidate_ids])
        covariance = np.cov(matrix, ddof=1)
        portfolio_volatility = sqrt(max(0.0, float(weight_vector @ covariance @ weight_vector)))
        weighted_standalone = float(weight_vector @ volatilities)
        diversification = {
            "hhi": _rounded(hhi),
            "effective_candidate_count": _rounded(1 / hhi),
            "portfolio_volatility": _rounded(portfolio_volatility),
            "weighted_standalone_volatility": _rounded(weighted_standalone),
            "diversification_ratio": _rounded(weighted_standalone / portfolio_volatility) if portfolio_volatility else None,
        }
        stressed_correlation = np.where(np.eye(len(candidate_ids)), 1.0, np.maximum(joint, cluster_threshold))
        stressed_covariance = np.outer(volatilities, volatilities) * stressed_correlation
        stress_volatility = sqrt(max(0.0, float(weight_vector @ stressed_covariance @ weight_vector)))
        stress = {
            "name": "correlation_convergence",
            "portfolio_volatility": _rounded(stress_volatility),
            "volatility_multiplier": _rounded(stress_volatility / portfolio_volatility) if portfolio_volatility else None,
            "assumption": "off-diagonal dependence floored at cluster threshold",
        }

    result = {
        "schema_version": "port002-dependency-dossier-v1.0.0",
        "port001_dossier_digest": candidate_dossier["dossier_digest"],
        "evaluated_at": evaluated.isoformat().replace("+00:00", "Z"),
        "window": {"first": common[0] if common else None, "last": common[-1] if common else None, "overlap": len(common)},
        "pairwise": pairwise,
        "clusters": clusters,
        "concentration": {
            "cluster_max_weight": _rounded(cluster_max),
            "exposure_weights": {key: _rounded(value) for key, value in sorted(exposure_weights.items())},
            "limits": {
                "maximum_cluster_weight": maximum_cluster_weight,
                "maximum_exposure_weight": maximum_exposure_weight,
                "maximum_hhi": maximum_hhi,
            },
        },
        "diversification": diversification,
        "stress": stress,
        "failures": sorted(set(reasons)),
        "qualified": not reasons,
        "claim": "dependency evidence only; no target weights or allocation authority",
    }
    configuration = {
        "stale_after_seconds": stale_after_seconds,
        "minimum_overlap": minimum_overlap,
        "minimum_state_overlap": minimum_state_overlap,
        "tail_quantile": tail_quantile,
        "cluster_threshold": cluster_threshold,
        "maximum_cluster_weight": maximum_cluster_weight,
        "maximum_exposure_weight": maximum_exposure_weight,
        "maximum_hhi": maximum_hhi,
    }
    return build_receipt(
        milestone="PORT-002",
        producer="bt.institutional.portfolio.dependency_dossier_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={"candidate_dossier": candidate_dossier, "observations": normalized, "exposures": exposures},
        dataset_digest=dataset_digest,
        configuration=configuration,
        artifacts=result,
        result=result,
    )
