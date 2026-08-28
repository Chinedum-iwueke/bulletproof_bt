from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from bt.institutional.portfolio import PortfolioDependencyError, dependency_dossier_receipt
from bt.institutional.receipt import digest, verify_receipt
from bt.portfolio_engine import PortfolioCandidatePolicy, evaluate_portfolio_candidates, finalize_candidate

COMMIT = "8" * 40
DATASET = "9" * 64


def _candidate(candidate_id: str, family: str, values: list[float]) -> dict:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    return finalize_candidate(
        {
            "candidate_id": candidate_id,
            "evidence_digest": digest({"evidence": candidate_id}),
            "shadow_digest": digest({"shadow": candidate_id}),
            "family": family,
            "eligibility": "portfolio_eligible",
            "forecast_horizon": "1d",
            "expected_net_return": 0.004,
            "uncertainty": 0.002,
            "turnover": 1.0,
            "cost_bps": 5.0,
            "capacity_notional": 2_000_000,
            "observations": [
                {"timestamp": (start + timedelta(days=index)).isoformat(), "net_return": value}
                for index, value in enumerate(values)
            ],
            "scenario_returns": {"correlation_convergence": -0.04, "liquidity_withdrawal": -0.05},
            "dependencies": {"instruments": [candidate_id], "venues": ["bybit"], "data": [candidate_id], "models": [family], "infrastructure": ["vm1"]},
        }
    )


def _inputs() -> tuple[dict, dict[str, list[dict]], dict[str, list[str]]]:
    values = [0.001 * ((index % 9) - 4) + 0.0002 * (index % 3) for index in range(60)]
    candidates = [
        _candidate("carry", "carry", [value * (-0.35) + 0.0003 * (index % 5) for index, value in enumerate(values)]),
        _candidate("trend", "momentum", values),
    ]
    registry = {value for item in candidates for value in (item["evidence_digest"], item["shadow_digest"])}
    dossier = evaluate_portfolio_candidates(
        candidates,
        PortfolioCandidatePolicy(portfolio_notional=1_000_000, max_candidate_weight=0.65, max_family_weight=0.70, max_hhi=0.60, max_weighted_dependency=1.0),
        evaluated_at="2026-03-02T00:00:00Z",
        registered_evidence_digests=registry,
    )
    observations: dict[str, list[dict]] = {}
    for candidate in candidates:
        observations[candidate["candidate_id"]] = [
            item | {"state": "risk_on" if index < 30 else "risk_off"}
            for index, item in enumerate(candidate["observations"])
        ]
    exposures = {"carry": ["BTC", "bybit"], "trend": ["BTC", "binance"]}
    return dossier, observations, exposures


def _receipt(**overrides):
    dossier, observations, exposures = _inputs()
    values = {
        "candidate_dossier": dossier,
        "observations": observations,
        "exposures": exposures,
        "evaluated_at": "2026-03-02T00:00:00Z",
        "stale_after_seconds": 172800,
        "minimum_overlap": 40,
        "minimum_state_overlap": 20,
        "tail_quantile": 0.10,
        "cluster_threshold": 0.80,
        "maximum_cluster_weight": 1.0,
        "maximum_exposure_weight": 1.0,
        "maximum_hhi": 0.55,
        "dataset_digest": DATASET,
        "source_commit": COMMIT,
    }
    values.update(overrides)
    return dependency_dossier_receipt(**values)


def test_port002_is_deterministic_state_aware_and_non_allocating() -> None:
    first = _receipt()
    second = _receipt()
    assert first == second
    assert verify_receipt(first)
    assert first.result["qualified"] is True
    pair = first.result["pairwise"][0]
    assert set(pair["state_dependence"]) == {"risk_off", "risk_on"}
    assert pair["lower_tail_dependence"] >= 0
    assert first.result["diversification"]["effective_candidate_count"] > 1
    assert first.authority == {"allocation": False, "capital": False, "orders": False, "promotion": False}


def test_sparse_history_and_stale_window_fail_closed() -> None:
    sparse = _receipt(minimum_state_overlap=31)
    assert sparse.result["qualified"] is False
    assert "sparse_state_history:risk_off,risk_on" in sparse.result["failures"]
    stale = _receipt(evaluated_at="2026-04-01T00:00:00Z", stale_after_seconds=60)
    assert stale.result["qualified"] is False
    assert "stale_input_window" in stale.result["failures"]


def test_shared_exposure_and_cluster_concentration_are_retained() -> None:
    receipt = _receipt(maximum_exposure_weight=0.75, maximum_cluster_weight=0.55)
    assert receipt.result["qualified"] is False
    assert "shared_exposure_limit_breached" in receipt.result["failures"]
    assert receipt.result["concentration"]["exposure_weights"]["BTC"] == 1.0
    assert receipt.result["clusters"]


def test_correlation_break_stress_is_more_conservative() -> None:
    receipt = _receipt()
    base = receipt.result["diversification"]["portfolio_volatility"]
    stressed = receipt.result["stress"]["portfolio_volatility"]
    assert stressed >= base
    assert receipt.result["stress"]["name"] == "correlation_convergence"


def test_candidate_digest_and_candidate_set_mismatch_are_rejected() -> None:
    dossier, observations, exposures = _inputs()
    corrupt = deepcopy(dossier)
    corrupt["allocated"] = True
    with pytest.raises(Exception, match="digest mismatch"):
        _receipt(candidate_dossier=corrupt)
    observations.pop("carry")
    with pytest.raises(PortfolioDependencyError, match="exactly match"):
        _receipt(candidate_dossier=dossier, observations=observations, exposures=exposures)
