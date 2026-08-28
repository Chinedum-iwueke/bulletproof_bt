#!/usr/bin/env python3
"""Build one deterministic no-capital PORT-003 producer receipt."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from bt.institutional.construction import construction_dossier_receipt
from bt.institutional.receipt import build_receipt, digest, verify_receipt


def dependency_receipt(source_commit: str, dataset_digest: str):
    result = {"schema_version": "port002-dependency-dossier-v1.0.0", "qualified": True}
    return build_receipt(
        milestone="PORT-002",
        producer="bt.institutional.portfolio.dependency_dossier_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={"candidates": ["carry", "trend", "value"]},
        dataset_digest=dataset_digest,
        configuration={"cluster_threshold": 0.8},
        artifacts=result,
        result=result,
    )


def risk_receipt(source_commit: str, dataset_digest: str):
    result = {"schema_version": "risk001-stress-dossier-v1.0.0", "admissible": True}
    return build_receipt(
        milestone="RISK-001",
        producer="bt.institutional.risk.stress_dossier_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={"portfolio": "port003-pilot"},
        dataset_digest=dataset_digest,
        configuration={"scenario_limit": 0.2},
        artifacts=result,
        result=result,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    dataset_digest = digest({"dataset": "port003-deterministic-fixture-v1"})
    solver = {
        "name": "projected-gradient-robust-mean-variance",
        "version": "1.0.0",
        "risk_aversion": 4.0,
        "uncertainty_penalty": 0.25,
        "covariance_shrinkage": 0.20,
        "step_size": 0.5,
        "iterations": 500,
    }
    receipt = construction_dossier_receipt(
        dependency_receipt=dependency_receipt(args.source_commit, dataset_digest),
        risk_receipt=risk_receipt(args.source_commit, dataset_digest),
        expected_returns={"carry": 0.035, "trend": 0.08, "value": 0.045},
        uncertainty={"carry": 0.003, "trend": 0.004, "value": 0.003},
        covariance=[[0.04, 0.006, 0.004], [0.006, 0.09, 0.008], [0.004, 0.008, 0.05]],
        prior_weights={"carry": 0.34, "trend": 0.33, "value": 0.33},
        lower_bounds={"carry": 0.10, "trend": 0.10, "value": 0.10},
        upper_bounds={"carry": 0.60, "trend": 0.60, "value": 0.60},
        solver=solver,
        weight_increment=0.01,
        maximum_turnover=0.80,
        maximum_sensitivity_l1=0.50,
        dataset_digest=dataset_digest,
        source_commit=args.source_commit,
    )
    report = {
        "schema_version": "port003-native-pilot-v1.0.0",
        "success": verify_receipt(receipt) and receipt.result["valid"],
        "capital_or_order_authority": False,
        "solver_specification": solver,
        "solver_specification_digest": digest(solver),
        "receipt": receipt.as_dict(),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="ascii")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
