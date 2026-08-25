from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from bt.portfolio_engine import (
    PortfolioCandidateError,
    PortfolioCandidatePolicy,
    evaluate_portfolio_candidates,
    finalize_candidate,
    validate_portfolio_candidate_dossier,
)


def candidate(
    candidate_id: str,
    *,
    family: str,
    phase: float,
    capacity: float = 2_000_000,
) -> dict:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    returns = [
        0.004 * (((index + int(phase)) % 7) - 3)
        + 0.002 * (((index * (int(phase) + 2)) % 5) - 2)
        for index in range(40)
    ]
    return finalize_candidate(
        {
            "candidate_id": candidate_id,
            "evidence_digest": ("1" if candidate_id == "trend" else "2") * 64,
            "shadow_digest": ("3" if candidate_id == "trend" else "4") * 64,
            "family": family,
            "eligibility": "portfolio_eligible",
            "forecast_horizon": "1d",
            "expected_net_return": 0.006,
            "uncertainty": 0.003,
            "turnover": 1.0,
            "cost_bps": 8.0,
            "capacity_notional": capacity,
            "observations": [
                {
                    "timestamp": (start + timedelta(days=index)).isoformat(),
                    "net_return": value,
                }
                for index, value in enumerate(returns)
            ],
            "scenario_returns": {
                "correlation_convergence": -0.04 + phase * 0.002,
                "liquidity_withdrawal": -0.06 + phase * 0.002,
                "regime_reversal": -0.03 - phase * 0.001,
            },
            "dependencies": {
                "instruments": ["BTCUSDT"],
                "venues": ["bybit" if candidate_id == "trend" else "binance"],
                "data": [f"dataset-{candidate_id}"],
                "models": [family],
                "infrastructure": ["vm1-research"],
            },
        }
    )


def policy(**overrides: object) -> PortfolioCandidatePolicy:
    values = {
        "portfolio_notional": 1_000_000,
        "max_candidate_weight": 0.65,
        "max_family_weight": 0.70,
        "max_hhi": 0.55,
        "max_weighted_dependency": 1.0,
    }
    values.update(overrides)
    return PortfolioCandidatePolicy(**values)


def registry(candidates: list[dict]) -> set[str]:
    return {
        digest
        for item in candidates
        for digest in (item["evidence_digest"], item["shadow_digest"])
    }


def canonical_digest(value: dict) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()


def test_candidate_dossier_is_deterministic_non_allocating_and_schema_valid() -> None:
    candidates = [
        candidate("trend", family="momentum", phase=1),
        candidate("carry", family="carry", phase=2),
    ]
    first = evaluate_portfolio_candidates(
        candidates,
        policy(),
        evaluated_at="2026-08-25T12:00:00Z",
        registered_evidence_digests=registry(candidates),
    )
    second = evaluate_portfolio_candidates(
        list(reversed(candidates)),
        policy(),
        evaluated_at="2026-08-25T12:00:00Z",
        registered_evidence_digests=registry(candidates),
    )
    assert first == second
    assert first["decision"] == "candidate"
    assert first["allocated"] is False
    assert sum(first["proposal"]["weights"].values()) == pytest.approx(1.0)
    assert first["dependency"]["overlap_observations"] == 40
    validate_portfolio_candidate_dossier(first)
    schema = json.loads(
        (
            Path(__file__).parents[1]
            / "schemas/portfolio-candidate-dossier-v1.schema.json"
        ).read_text(encoding="utf-8")
    )
    Draft202012Validator(schema).validate(first)


def test_missing_overlap_fails_closed_instead_of_assuming_diversification() -> None:
    one = candidate("trend", family="momentum", phase=1)
    two = candidate("carry", family="carry", phase=2)
    draft = deepcopy(two)
    draft.pop("candidate_digest")
    draft["observations"] = draft["observations"][1:]
    two = finalize_candidate(draft)
    dossier = evaluate_portfolio_candidates(
        [one, two],
        policy(),
        evaluated_at="2026-08-25T12:00:00Z",
        registered_evidence_digests=registry([one, two]),
    )
    assert dossier["decision"] == "rejected"
    assert dossier["proposal"]["weights"] == {}
    assert dossier["rejection_reasons"] == [
        "candidates require identical overlapping observation timestamps"
    ]


def test_capacity_and_family_infeasibility_are_explainable() -> None:
    too_small = [
        candidate("trend", family="momentum", phase=1, capacity=100_000),
        candidate("carry", family="carry", phase=2, capacity=100_000),
    ]
    capacity_dossier = evaluate_portfolio_candidates(
        too_small,
        policy(),
        evaluated_at="2026-08-25T12:00:00Z",
        registered_evidence_digests=registry(too_small),
    )
    assert capacity_dossier["rejection_reasons"] == [
        "infeasible_candidate_and_capacity_caps"
    ]

    same_family = [
        candidate("trend", family="momentum", phase=1),
        candidate("carry", family="momentum", phase=2),
    ]
    family_dossier = evaluate_portfolio_candidates(
        same_family,
        policy(),
        evaluated_at="2026-08-25T12:00:00Z",
        registered_evidence_digests=registry(same_family),
    )
    assert family_dossier["rejection_reasons"] == ["infeasible_family_caps"]


def test_concentration_and_stress_constraints_reject_without_relaxation() -> None:
    candidates = [
        candidate("trend", family="momentum", phase=1),
        candidate("carry", family="carry", phase=2),
    ]
    concentration = evaluate_portfolio_candidates(
        candidates,
        policy(max_hhi=0.49),
        evaluated_at="2026-08-25T12:00:00Z",
        registered_evidence_digests=registry(candidates),
    )
    assert "constraint_failed:hhi" in concentration["rejection_reasons"]
    assert concentration["allocated"] is False

    stress = evaluate_portfolio_candidates(
        candidates,
        policy(max_stress_loss=0.02),
        evaluated_at="2026-08-25T12:00:00Z",
        registered_evidence_digests=registry(candidates),
    )
    assert "constraint_failed:stress_loss" in stress["rejection_reasons"]


def test_corrupt_candidate_and_dossier_digests_fail_closed() -> None:
    one = candidate("trend", family="momentum", phase=1)
    two = candidate("carry", family="carry", phase=2)
    one["expected_net_return"] = 10.0
    dossier = evaluate_portfolio_candidates(
        [one, two],
        policy(),
        evaluated_at="2026-08-25T12:00:00Z",
        registered_evidence_digests=registry([one, two]),
    )
    assert dossier["decision"] == "rejected"
    assert "digest does not match" in dossier["rejection_reasons"][0]

    valid = evaluate_portfolio_candidates(
        [candidate("trend", family="momentum", phase=1), two],
        policy(),
        evaluated_at="2026-08-25T12:00:00Z",
        registered_evidence_digests=registry(
            [candidate("trend", family="momentum", phase=1), two]
        ),
    )
    valid["allocated"] = True
    with pytest.raises(PortfolioCandidateError, match="digest mismatch"):
        validate_portfolio_candidate_dossier(valid)

    valid = evaluate_portfolio_candidates(
        [candidate("trend", family="momentum", phase=1), two],
        policy(),
        evaluated_at="2026-08-25T12:00:00Z",
        registered_evidence_digests=registry(
            [candidate("trend", family="momentum", phase=1), two]
        ),
    )
    valid["authority"]["capital"] = "allowed"
    core = {key: value for key, value in valid.items() if key != "dossier_digest"}
    valid["dossier_digest"] = canonical_digest(core)
    with pytest.raises(PortfolioCandidateError, match="exceeds its authority"):
        validate_portfolio_candidate_dossier(valid)


def test_dependency_constraint_can_reject_a_concentrated_joint_state() -> None:
    candidates = [
        candidate("trend", family="momentum", phase=1),
        candidate("carry", family="carry", phase=2),
    ]
    dossier = evaluate_portfolio_candidates(
        candidates,
        policy(max_weighted_dependency=0.45),
        evaluated_at="2026-08-25T12:00:00Z",
        registered_evidence_digests=registry(candidates),
    )
    assert "constraint_failed:weighted_dependency" in dossier["rejection_reasons"]


def test_unregistered_evidence_is_rejected_before_risk_computation() -> None:
    candidates = [
        candidate("trend", family="momentum", phase=1),
        candidate("carry", family="carry", phase=2),
    ]
    dossier = evaluate_portfolio_candidates(
        candidates,
        policy(),
        evaluated_at="2026-08-25T12:00:00Z",
        registered_evidence_digests=set(),
    )
    assert dossier["decision"] == "rejected"
    assert "absent from the supplied registry" in dossier["rejection_reasons"][0]


def test_family_cap_is_respected_when_multiple_candidates_share_a_family() -> None:
    candidates = [
        candidate("trend", family="momentum", phase=1),
        candidate("breakout", family="momentum", phase=2),
        candidate("carry", family="carry", phase=3),
    ]
    dossier = evaluate_portfolio_candidates(
        candidates,
        policy(max_family_weight=0.60, max_hhi=0.60),
        evaluated_at="2026-08-25T12:00:00Z",
        registered_evidence_digests=registry(candidates),
    )
    assert dossier["decision"] == "candidate"
    assert dossier["proposal"]["family_weights"]["momentum"] <= 0.60
