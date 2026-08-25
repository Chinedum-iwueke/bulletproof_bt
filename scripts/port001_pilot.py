"""Exercise PORT-001 candidate construction and fail-closed risk paths."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

from bt.logging.formatting import write_json_deterministic
from bt.portfolio_engine import (
    PortfolioCandidatePolicy,
    evaluate_portfolio_candidates,
    finalize_candidate,
    validate_portfolio_candidate_dossier,
)


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def candidate(
    candidate_id: str,
    *,
    family: str,
    phase: int,
    source_commit: str,
    capacity: float = 3_000_000,
) -> dict:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    returns = [
        0.003 * (((index + phase) % 7) - 3)
        + 0.0015 * (((index * (phase + 2)) % 5) - 2)
        for index in range(60)
    ]
    return finalize_candidate(
        {
            "candidate_id": candidate_id,
            "evidence_digest": digest(
                f"{source_commit}:{candidate_id}:research".encode("ascii")
            ),
            "shadow_digest": digest(
                f"{source_commit}:{candidate_id}:shadow".encode("ascii")
            ),
            "family": family,
            "eligibility": "portfolio_eligible",
            "forecast_horizon": "1d",
            "expected_net_return": 0.004 + phase * 0.0002,
            "uncertainty": 0.002 + phase * 0.0001,
            "turnover": 0.8 + phase * 0.1,
            "cost_bps": 7.0 + phase,
            "capacity_notional": capacity,
            "observations": [
                {
                    "timestamp": (start + timedelta(days=index)).isoformat(),
                    "net_return": value,
                }
                for index, value in enumerate(returns)
            ],
            "scenario_returns": {
                "correlation_convergence": -0.035 - phase * 0.002,
                "liquidity_withdrawal": -0.045 - phase * 0.002,
                "regime_reversal": -0.025 - phase * 0.002,
            },
            "dependencies": {
                "instruments": ["BTCUSDT"],
                "venues": ["bybit" if phase % 2 else "binance"],
                "data": [f"point-in-time-{candidate_id}"],
                "models": [family],
                "infrastructure": ["vm1-research"],
            },
        }
    )


def registry(candidates: list[dict]) -> set[str]:
    return {
        evidence_digest
        for item in candidates
        for evidence_digest in (item["evidence_digest"], item["shadow_digest"])
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    source_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    evaluated_at = "2026-08-25T18:00:00Z"
    policy = PortfolioCandidatePolicy(
        portfolio_notional=1_500_000,
        min_overlap_observations=40,
        max_candidate_weight=0.50,
        max_family_weight=0.60,
        max_hhi=0.42,
        max_weighted_dependency=0.85,
        max_stress_loss=0.10,
    )
    candidates = [
        candidate("trend", family="momentum", phase=1, source_commit=source_commit),
        candidate("carry", family="carry", phase=2, source_commit=source_commit),
        candidate(
            "reversion", family="mean-reversion", phase=3, source_commit=source_commit
        ),
    ]
    registered = registry(candidates)
    accepted = evaluate_portfolio_candidates(
        candidates,
        policy,
        evaluated_at=evaluated_at,
        registered_evidence_digests=registered,
    )
    reproduced = evaluate_portfolio_candidates(
        list(reversed(candidates)),
        policy,
        evaluated_at=evaluated_at,
        registered_evidence_digests=registered,
    )
    validate_portfolio_candidate_dossier(accepted)

    missing = deepcopy(candidates)
    draft = missing[1] | {"observations": missing[1]["observations"][1:]}
    draft.pop("candidate_digest")
    missing[1] = finalize_candidate(draft)
    missing_overlap = evaluate_portfolio_candidates(
        missing,
        policy,
        evaluated_at=evaluated_at,
        registered_evidence_digests=registry(missing),
    )
    capacity_candidates = [
        candidate(
            "trend",
            family="momentum",
            phase=1,
            source_commit=source_commit,
            capacity=100_000,
        ),
        candidate(
            "carry",
            family="carry",
            phase=2,
            source_commit=source_commit,
            capacity=100_000,
        ),
    ]
    infeasible = evaluate_portfolio_candidates(
        capacity_candidates,
        policy,
        evaluated_at=evaluated_at,
        registered_evidence_digests=registry(capacity_candidates),
    )
    stress_rejected = evaluate_portfolio_candidates(
        candidates,
        PortfolioCandidatePolicy(
            portfolio_notional=1_500_000,
            min_overlap_observations=40,
            max_candidate_weight=0.50,
            max_family_weight=0.60,
            max_hhi=0.42,
            max_weighted_dependency=0.85,
            max_stress_loss=0.02,
        ),
        evaluated_at=evaluated_at,
        registered_evidence_digests=registered,
    )
    for filename, dossier in (
        ("portfolio-candidate-dossier.json", accepted),
        ("missing-overlap-rejection.json", missing_overlap),
        ("infeasible-capacity-rejection.json", infeasible),
        ("stress-rejection.json", stress_rejected),
    ):
        write_json_deterministic(args.output / filename, dossier)
    report = {
        "schema_version": "port001-pilot-report-v1.0.0",
        "source_commit": source_commit,
        "success": (
            accepted == reproduced
            and accepted["decision"] == "candidate"
            and missing_overlap["decision"] == "rejected"
            and infeasible["decision"] == "rejected"
            and stress_rejected["decision"] == "rejected"
            and accepted["allocated"] is False
        ),
        "candidate_set_digest": accepted["candidate_set_digest"],
        "dossier_digest": accepted["dossier_digest"],
        "deterministic_reproduction": accepted == reproduced,
        "proposed_weights": accepted["proposal"]["weights"],
        "dependency": {
            "overlap_observations": accepted["dependency"]["overlap_observations"],
            "weighted_dependency": accepted["proposal"]["weighted_dependency"],
        },
        "stress": accepted["stress"],
        "rejected_paths": {
            "missing_overlap": missing_overlap["rejection_reasons"],
            "infeasible_capacity": infeasible["rejection_reasons"],
            "stress": stress_rejected["rejection_reasons"],
        },
        "capital_or_order_authority": False,
        "production_resources_touched": False,
    }
    report["report_digest"] = digest(
        json.dumps(report, sort_keys=True, separators=(",", ":")).encode("ascii")
    )
    write_json_deterministic(args.output / "port001-report.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
