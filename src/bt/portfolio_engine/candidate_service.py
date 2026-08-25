"""Deterministic, non-allocating portfolio-candidate risk service."""

from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import numpy as np

SCHEMA_VERSION = "portfolio-candidate-dossier-v1.0.0"
POLICY_VERSION = "portfolio-candidate-policy-v1.0.0"


class PortfolioCandidateError(ValueError):
    """Candidate evidence cannot produce a trustworthy portfolio proposal."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _is_sha256(value: object) -> bool:
    text = str(value)
    return len(text) == 64 and all(char in "0123456789abcdef" for char in text)


def _finite(value: Any, *, field: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise PortfolioCandidateError(f"{field} must be finite")
    return result


def _rounded(value: float) -> float:
    return round(float(value), 12)


@dataclass(frozen=True)
class PortfolioCandidatePolicy:
    portfolio_notional: float
    min_candidates: int = 2
    min_overlap_observations: int = 20
    max_candidate_weight: float = 0.60
    max_family_weight: float = 0.70
    max_hhi: float = 0.52
    max_weighted_dependency: float = 0.75
    max_stress_loss: float = 0.12
    cost_stress_multiplier: float = 3.0
    capacity_stress_haircut: float = 0.50

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": POLICY_VERSION,
            "portfolio_notional": _finite(
                self.portfolio_notional, field="portfolio_notional"
            ),
            "min_candidates": int(self.min_candidates),
            "min_overlap_observations": int(self.min_overlap_observations),
            "max_candidate_weight": _finite(
                self.max_candidate_weight, field="max_candidate_weight"
            ),
            "max_family_weight": _finite(
                self.max_family_weight, field="max_family_weight"
            ),
            "max_hhi": _finite(self.max_hhi, field="max_hhi"),
            "max_weighted_dependency": _finite(
                self.max_weighted_dependency, field="max_weighted_dependency"
            ),
            "max_stress_loss": _finite(
                self.max_stress_loss, field="max_stress_loss"
            ),
            "cost_stress_multiplier": _finite(
                self.cost_stress_multiplier, field="cost_stress_multiplier"
            ),
            "capacity_stress_haircut": _finite(
                self.capacity_stress_haircut, field="capacity_stress_haircut"
            ),
        }


def _validate_policy(policy: dict[str, Any]) -> None:
    if policy["portfolio_notional"] <= 0:
        raise PortfolioCandidateError("portfolio_notional must be positive")
    if policy["min_candidates"] < 2 or policy["min_overlap_observations"] < 2:
        raise PortfolioCandidateError("candidate and overlap minima must be at least two")
    for field in (
        "max_candidate_weight",
        "max_family_weight",
        "max_hhi",
        "max_weighted_dependency",
        "max_stress_loss",
        "capacity_stress_haircut",
    ):
        if not 0 < policy[field] <= 1:
            raise PortfolioCandidateError(f"{field} must be in (0, 1]")
    if policy["cost_stress_multiplier"] < 1:
        raise PortfolioCandidateError("cost_stress_multiplier must be at least one")


def _normalize_candidate(
    candidate: dict[str, Any], *, validate_digest: bool = True
) -> dict[str, Any]:
    required_digests = ("candidate_digest", "evidence_digest", "shadow_digest")
    for field in required_digests:
        if not _is_sha256(candidate.get(field)):
            raise PortfolioCandidateError(f"{field} must be a lowercase sha256")
    candidate_id = str(candidate.get("candidate_id", "")).strip()
    family = str(candidate.get("family", "")).strip()
    if not candidate_id or not family:
        raise PortfolioCandidateError("candidate_id and family are required")
    if candidate.get("eligibility") != "portfolio_eligible":
        raise PortfolioCandidateError(
            f"candidate {candidate_id} is not portfolio_eligible"
        )
    observations = candidate.get("observations")
    if not isinstance(observations, list):
        raise PortfolioCandidateError(f"candidate {candidate_id} observations are required")
    normalized_observations: list[dict[str, Any]] = []
    seen: set[str] = set()
    for observation in observations:
        timestamp = str(observation.get("timestamp", ""))
        try:
            parsed = datetime.fromisoformat(timestamp)
        except ValueError as exc:
            raise PortfolioCandidateError(
                f"candidate {candidate_id} has an invalid observation timestamp"
            ) from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise PortfolioCandidateError(
                f"candidate {candidate_id} observation timestamps must be timezone-aware"
            )
        canonical_timestamp = parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")
        if canonical_timestamp in seen:
            raise PortfolioCandidateError(
                f"candidate {candidate_id} has duplicate observation timestamps"
            )
        seen.add(canonical_timestamp)
        normalized_observations.append(
            {
                "timestamp": canonical_timestamp,
                "net_return": _finite(
                    observation.get("net_return"), field="observation net_return"
                ),
            }
        )
    normalized_observations.sort(key=lambda item: item["timestamp"])
    dependencies = candidate.get("dependencies") or {}
    normalized_dependencies = {
        key: sorted({str(value) for value in dependencies.get(key, [])})
        for key in ("instruments", "venues", "data", "models", "infrastructure")
    }
    scenarios = candidate.get("scenario_returns") or {}
    if not scenarios:
        raise PortfolioCandidateError(
            f"candidate {candidate_id} requires scenario_returns"
        )
    normalized = {
        "candidate_id": candidate_id,
        "candidate_digest": candidate["candidate_digest"],
        "evidence_digest": candidate["evidence_digest"],
        "shadow_digest": candidate["shadow_digest"],
        "family": family,
        "eligibility": "portfolio_eligible",
        "forecast_horizon": str(candidate.get("forecast_horizon", "")).strip(),
        "expected_net_return": _finite(
            candidate.get("expected_net_return"), field="expected_net_return"
        ),
        "uncertainty": _finite(candidate.get("uncertainty"), field="uncertainty"),
        "turnover": _finite(candidate.get("turnover"), field="turnover"),
        "cost_bps": _finite(candidate.get("cost_bps"), field="cost_bps"),
        "capacity_notional": _finite(
            candidate.get("capacity_notional"), field="capacity_notional"
        ),
        "observations": normalized_observations,
        "scenario_returns": {
            str(key): _finite(value, field=f"scenario_returns.{key}")
            for key, value in sorted(scenarios.items())
        },
        "dependencies": normalized_dependencies,
    }
    if not normalized["forecast_horizon"]:
        raise PortfolioCandidateError(
            f"candidate {candidate_id} requires forecast_horizon"
        )
    if normalized["uncertainty"] < 0 or normalized["turnover"] < 0:
        raise PortfolioCandidateError("uncertainty and turnover cannot be negative")
    if normalized["cost_bps"] < 0 or normalized["capacity_notional"] <= 0:
        raise PortfolioCandidateError("cost must be non-negative and capacity positive")
    core = {key: value for key, value in normalized.items() if key != "candidate_digest"}
    if validate_digest and _digest(core) != normalized["candidate_digest"]:
        raise PortfolioCandidateError(
            f"candidate {candidate_id} digest does not match canonical evidence"
        )
    return normalized


def finalize_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    """Add the content digest expected by the portfolio service."""
    draft = dict(candidate)
    draft.pop("candidate_digest", None)
    normalized = _normalize_candidate(
        draft | {"candidate_digest": "0" * 64}, validate_digest=False
    )
    core = {key: value for key, value in normalized.items() if key != "candidate_digest"}
    return core | {"candidate_digest": _digest(core)}


def _shared_dependency_matrix(candidates: list[dict[str, Any]]) -> list[list[float]]:
    matrix: list[list[float]] = []
    keys = ("instruments", "venues", "data", "models", "infrastructure")
    for left in candidates:
        row: list[float] = []
        for right in candidates:
            if left["candidate_id"] == right["candidate_id"]:
                row.append(1.0)
                continue
            overlaps = 0
            populated = 0
            for key in keys:
                union = set(left["dependencies"][key]) | set(right["dependencies"][key])
                if union:
                    populated += 1
                    if set(left["dependencies"][key]) & set(right["dependencies"][key]):
                        overlaps += 1
            row.append(_rounded(overlaps / populated) if populated else 1.0)
        matrix.append(row)
    return matrix


def _allocate(
    candidates: list[dict[str, Any]],
    risk_scales: np.ndarray,
    policy: dict[str, Any],
) -> tuple[dict[str, float], list[str]]:
    caps: dict[str, float] = {}
    for candidate in candidates:
        capacity_cap = (
            candidate["capacity_notional"]
            * policy["capacity_stress_haircut"]
            / policy["portfolio_notional"]
        )
        caps[candidate["candidate_id"]] = min(
            policy["max_candidate_weight"], capacity_cap
        )
    reasons: list[str] = []
    if sum(caps.values()) < 1.0 - 1e-10:
        return {}, ["infeasible_candidate_and_capacity_caps"]
    family_capacity: dict[str, float] = defaultdict(float)
    for candidate in candidates:
        family_capacity[candidate["family"]] += caps[candidate["candidate_id"]]
    if sum(min(value, policy["max_family_weight"]) for value in family_capacity.values()) < 1.0 - 1e-10:
        return {}, ["infeasible_family_caps"]

    scores = {
        candidate["candidate_id"]: 1.0 / max(float(risk_scale), 1e-12)
        for candidate, risk_scale in zip(candidates, risk_scales, strict=True)
    }
    weights = {candidate["candidate_id"]: 0.0 for candidate in candidates}
    families = {candidate["candidate_id"]: candidate["family"] for candidate in candidates}
    remaining = 1.0
    for _ in range(len(candidates) * 4):
        family_weights: dict[str, float] = defaultdict(float)
        for candidate_id, weight in weights.items():
            family_weights[families[candidate_id]] += weight
        eligible = [
            candidate_id
            for candidate_id in sorted(weights)
            if weights[candidate_id] < caps[candidate_id] - 1e-12
            and family_weights[families[candidate_id]]
            < policy["max_family_weight"] - 1e-12
        ]
        if not eligible or remaining <= 1e-12:
            break
        score_total = sum(scores[candidate_id] for candidate_id in eligible)
        additions: dict[str, float] = {}
        for candidate_id in eligible:
            additions[candidate_id] = min(
                remaining * scores[candidate_id] / score_total,
                caps[candidate_id] - weights[candidate_id],
            )
        additions_by_family: dict[str, list[str]] = defaultdict(list)
        for candidate_id in eligible:
            additions_by_family[families[candidate_id]].append(candidate_id)
        for family, candidate_ids in additions_by_family.items():
            requested = sum(additions[candidate_id] for candidate_id in candidate_ids)
            family_room = policy["max_family_weight"] - family_weights[family]
            if requested > family_room:
                scale = family_room / requested
                for candidate_id in candidate_ids:
                    additions[candidate_id] *= scale
        progress = sum(additions.values())
        if progress <= 1e-12:
            break
        for candidate_id, addition in additions.items():
            weights[candidate_id] += addition
        remaining -= progress
    if remaining > 1e-9:
        reasons.append("deterministic_baseline_could_not_satisfy_caps")
        return {}, reasons
    return {key: _rounded(value) for key, value in weights.items()}, reasons


def evaluate_portfolio_candidates(
    candidates: Iterable[dict[str, Any]],
    policy: PortfolioCandidatePolicy,
    *,
    evaluated_at: str,
    registered_evidence_digests: set[str],
) -> dict[str, Any]:
    """Produce a deterministic risk dossier without granting allocation authority."""
    raw_candidates = list(candidates)
    normalized_policy = policy.as_dict()
    rejection_reasons: list[str] = []
    normalized: list[dict[str, Any]] = []
    try:
        parsed = datetime.fromisoformat(evaluated_at)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError
        canonical_time = parsed.isoformat().replace("+00:00", "Z")
        _validate_policy(normalized_policy)
        normalized = sorted(
            (_normalize_candidate(candidate) for candidate in raw_candidates),
            key=lambda item: item["candidate_id"],
        )
        if len(normalized) < normalized_policy["min_candidates"]:
            raise PortfolioCandidateError("candidate set is smaller than min_candidates")
        if len({item["candidate_id"] for item in normalized}) != len(normalized):
            raise PortfolioCandidateError("candidate_id values must be unique")
        missing_registrations = sorted(
            {
                digest
                for item in normalized
                for digest in (item["evidence_digest"], item["shadow_digest"])
                if digest not in registered_evidence_digests
            }
        )
        if missing_registrations:
            raise PortfolioCandidateError(
                "candidate evidence is absent from the supplied registry snapshot: "
                + ",".join(missing_registrations)
            )
        horizons = {item["forecast_horizon"] for item in normalized}
        if len(horizons) != 1:
            raise PortfolioCandidateError("forecast horizons must match")
        timestamps = [
            tuple(item["timestamp"] for item in candidate["observations"])
            for candidate in normalized
        ]
        if len(set(timestamps)) != 1:
            raise PortfolioCandidateError(
                "candidates require identical overlapping observation timestamps"
            )
        if len(timestamps[0]) < normalized_policy["min_overlap_observations"]:
            raise PortfolioCandidateError("insufficient overlapping observations")
        scenario_sets = {tuple(item["scenario_returns"]) for item in normalized}
        if len(scenario_sets) != 1:
            raise PortfolioCandidateError("candidate scenario sets must match")
    except (PortfolioCandidateError, TypeError, ValueError) as exc:
        canonical_time = evaluated_at
        rejection_reasons.append(str(exc) or "invalid_evaluation_input")

    candidate_set_digest = _digest(normalized if normalized else raw_candidates)
    dependency: dict[str, Any] = {
        "candidate_ids": [item["candidate_id"] for item in normalized],
        "overlap_observations": 0,
        "covariance": [],
        "correlation": [],
        "shared_dependency": [],
    }
    proposal: dict[str, Any] = {"weights": {}, "notionals": {}}
    stress: dict[str, Any] = {}
    constraints: list[dict[str, Any]] = []
    if not rejection_reasons:
        returns = np.asarray(
            [
                [observation["net_return"] for observation in item["observations"]]
                for item in normalized
            ],
            dtype=np.float64,
        )
        volatilities = returns.std(axis=1, ddof=1)
        if not np.isfinite(volatilities).all() or np.any(volatilities <= 1e-12):
            rejection_reasons.append("zero_or_invalid_candidate_volatility")
        else:
            covariance = np.cov(returns, ddof=1)
            correlation = np.corrcoef(returns)
            if not np.isfinite(covariance).all() or not np.isfinite(correlation).all():
                rejection_reasons.append("invalid_dependency_estimate")
            else:
                shared = _shared_dependency_matrix(normalized)
                dependency = {
                    "candidate_ids": [item["candidate_id"] for item in normalized],
                    "overlap_observations": returns.shape[1],
                    "estimator": "sample_covariance_exact_overlap",
                    "covariance": [
                        [_rounded(value) for value in row] for row in covariance.tolist()
                    ],
                    "correlation": [
                        [_rounded(value) for value in row] for row in correlation.tolist()
                    ],
                    "shared_dependency": shared,
                }
                shared_array = np.asarray(shared, dtype=np.float64)
                joint_dependency = np.maximum(np.abs(correlation), shared_array)
                dependency["joint_dependency"] = [
                    [_rounded(value) for value in row]
                    for row in joint_dependency.tolist()
                ]
                risk_scales = volatilities + np.asarray(
                    [item["uncertainty"] for item in normalized], dtype=np.float64
                )
                weights, allocation_rejections = _allocate(
                    normalized, risk_scales, normalized_policy
                )
                rejection_reasons.extend(allocation_rejections)
                if weights:
                    weight_vector = np.asarray(
                        [weights[item["candidate_id"]] for item in normalized]
                    )
                    hhi = float(np.sum(np.square(weight_vector)))
                    weighted_dependency = float(
                        weight_vector @ joint_dependency @ weight_vector
                    )
                    family_weights: dict[str, float] = defaultdict(float)
                    for item in normalized:
                        family_weights[item["family"]] += weights[item["candidate_id"]]
                    scenario_returns = {
                        scenario: _rounded(
                            sum(
                                weights[item["candidate_id"]]
                                * item["scenario_returns"][scenario]
                                for item in normalized
                            )
                        )
                        for scenario in normalized[0]["scenario_returns"]
                    }
                    baseline_net = sum(
                        weights[item["candidate_id"]] * item["expected_net_return"]
                        for item in normalized
                    )
                    incremental_cost = sum(
                        weights[item["candidate_id"]]
                        * item["cost_bps"]
                        * item["turnover"]
                        * (normalized_policy["cost_stress_multiplier"] - 1.0)
                        / 10_000.0
                        for item in normalized
                    )
                    cost_stress_return = baseline_net - incremental_cost
                    stress = {
                        "baseline_expected_net_return": _rounded(baseline_net),
                        "cost_stress_return": _rounded(cost_stress_return),
                        "scenario_returns": scenario_returns,
                        "worst_return": _rounded(
                            min([cost_stress_return, *scenario_returns.values()])
                        ),
                        "capacity_haircut": normalized_policy[
                            "capacity_stress_haircut"
                        ],
                    }
                    checks = {
                        "weights_sum_to_one": abs(sum(weights.values()) - 1.0) <= 1e-9,
                        "candidate_concentration": max(weights.values())
                        <= normalized_policy["max_candidate_weight"] + 1e-10,
                        "family_concentration": max(family_weights.values())
                        <= normalized_policy["max_family_weight"] + 1e-10,
                        "hhi": hhi <= normalized_policy["max_hhi"] + 1e-10,
                        "weighted_dependency": weighted_dependency
                        <= normalized_policy["max_weighted_dependency"] + 1e-10,
                        "stress_loss": stress["worst_return"]
                        >= -normalized_policy["max_stress_loss"] - 1e-10,
                    }
                    constraints = [
                        {
                            "name": name,
                            "passed": passed,
                            "binding": not passed,
                        }
                        for name, passed in checks.items()
                    ]
                    rejection_reasons.extend(
                        f"constraint_failed:{name}"
                        for name, passed in checks.items()
                        if not passed
                    )
                    proposal = {
                        "method": "capacity_capped_inverse_volatility_baseline",
                        "weights": weights,
                        "notionals": {
                            key: _rounded(value * normalized_policy["portfolio_notional"])
                            for key, value in weights.items()
                        },
                        "family_weights": {
                            key: _rounded(value)
                            for key, value in sorted(family_weights.items())
                        },
                        "hhi": _rounded(hhi),
                        "weighted_dependency": _rounded(weighted_dependency),
                    }

    dossier: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "evaluated_at": canonical_time,
        "service": {
            "name": "bulletproof-portfolio-candidate-risk",
            "version": "1.0.0",
            "determinism": "same canonical inputs produce the same dossier digest",
        },
        "authority": {
            "portfolio_allocation": "prohibited",
            "capital": "prohibited",
            "orders": "prohibited",
            "self_promotion": "prohibited",
            "permitted_use": "portfolio_candidate_review_only",
        },
        "candidate_set_digest": candidate_set_digest,
        "candidate_digests": [item["candidate_digest"] for item in normalized],
        "policy": normalized_policy,
        "policy_digest": _digest(normalized_policy),
        "dependency": dependency,
        "proposal": proposal,
        "stress": stress,
        "constraint_evaluations": constraints,
        "decision": "candidate" if not rejection_reasons else "rejected",
        "rejection_reasons": sorted(set(rejection_reasons)),
        "allocated": False,
    }
    dossier["dossier_digest"] = _digest(dossier)
    return dossier


def validate_portfolio_candidate_dossier(dossier: dict[str, Any]) -> None:
    if dossier.get("schema_version") != SCHEMA_VERSION:
        raise PortfolioCandidateError("unsupported portfolio candidate dossier schema")
    supplied = dossier.get("dossier_digest")
    core = {key: value for key, value in dossier.items() if key != "dossier_digest"}
    if not _is_sha256(supplied) or supplied != _digest(core):
        raise PortfolioCandidateError("portfolio candidate dossier digest mismatch")
    authority = dossier.get("authority") or {}
    if any(
        authority.get(key) != "prohibited"
        for key in ("portfolio_allocation", "capital", "orders", "self_promotion")
    ) or dossier.get("allocated") is not False:
        raise PortfolioCandidateError("portfolio candidate dossier exceeds its authority")
