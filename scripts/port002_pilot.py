#!/usr/bin/env python3
"""Build one deterministic no-capital PORT-002 producer receipt."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from bt.institutional.portfolio import dependency_dossier_receipt
from bt.institutional.receipt import digest, verify_receipt
from bt.portfolio_engine import PortfolioCandidatePolicy, evaluate_portfolio_candidates, finalize_candidate


def candidate(candidate_id: str, family: str, returns: list[float]) -> dict:
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
                for index, value in enumerate(returns)
            ],
            "scenario_returns": {"correlation_convergence": -0.04, "liquidity_withdrawal": -0.05},
            "dependencies": {"instruments": [candidate_id], "venues": ["bybit"], "data": [candidate_id], "models": [family], "infrastructure": ["vm1"]},
        }
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    trend = [0.001 * ((index % 9) - 4) + 0.0002 * (index % 3) for index in range(60)]
    candidates = [
        candidate("carry", "carry", [value * -0.35 + 0.0003 * (index % 5) for index, value in enumerate(trend)]),
        candidate("trend", "momentum", trend),
    ]
    registry = {value for item in candidates for value in (item["evidence_digest"], item["shadow_digest"])}
    dossier = evaluate_portfolio_candidates(
        candidates,
        PortfolioCandidatePolicy(portfolio_notional=1_000_000, max_candidate_weight=0.65, max_family_weight=0.70, max_hhi=0.60, max_weighted_dependency=1.0),
        evaluated_at="2026-03-02T00:00:00Z",
        registered_evidence_digests=registry,
    )
    observations = {
        item["candidate_id"]: [observation | {"state": "risk_on" if index < 30 else "risk_off"} for index, observation in enumerate(item["observations"])]
        for item in candidates
    }
    receipt = dependency_dossier_receipt(
        candidate_dossier=dossier,
        observations=observations,
        exposures={"carry": ["BTC", "bybit"], "trend": ["BTC", "binance"]},
        evaluated_at="2026-03-02T00:00:00Z",
        stale_after_seconds=172800,
        minimum_overlap=40,
        minimum_state_overlap=20,
        tail_quantile=0.10,
        cluster_threshold=0.80,
        maximum_cluster_weight=1.0,
        maximum_exposure_weight=1.0,
        maximum_hhi=0.55,
        dataset_digest=digest(observations),
        source_commit=args.source_commit,
    )
    report = {
        "schema_version": "port002-native-pilot-v1.0.0",
        "success": verify_receipt(receipt) and receipt.result["qualified"],
        "capital_or_order_authority": False,
        "receipt": receipt.as_dict(),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="ascii")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
