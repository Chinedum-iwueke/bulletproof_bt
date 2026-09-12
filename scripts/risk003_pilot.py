#!/usr/bin/env python3
"""Run one deterministic no-authority RISK-003 budget pilot."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from bt.institutional.receipt import build_receipt, digest, verify_receipt
from bt.institutional.risk_budget import (
    RISK_BUDGET_SPECIFICATION,
    dynamic_risk_budget,
    dynamic_risk_budget_receipt,
)


def dependency(milestone: str, producer: str, commit: str, dataset: str, result: dict):
    return build_receipt(
        milestone=milestone,
        producer=producer,
        producer_version="1.0.0",
        source_commit=commit,
        inputs={},
        dataset_digest=dataset,
        configuration={},
        artifacts=result,
        result=result,
    )


def state(index: int, now: datetime, *, regime: str = "normal", volatility: float = 0.20, uncertainty: float = 0.10):
    observed = now - timedelta(seconds=90 - index * 10)
    return {
        "state_id": f"risk-state-{index}",
        "observed_at": observed.isoformat(),
        "available_at": (observed + timedelta(seconds=1)).isoformat(),
        "regime": regime,
        "annualized_volatility": volatility,
        "model_confidence": 0.95,
        "uncertainty": uncertainty,
        "source_digest": digest({"risk-state": index, "regime": regime, "volatility": volatility, "uncertainty": uncertainty}),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    now = datetime(2026, 9, 12, 18, tzinfo=UTC)
    dataset = digest({"dataset": "risk003-dynamic-budget-fixture-v1"})
    configuration = {
        "base_risk_fraction": 0.01,
        "minimum_risk_fraction": 0.001,
        "maximum_risk_fraction": 0.012,
        "static_conservative_fraction": 0.0025,
        "target_annualized_volatility": 0.20,
        "uncertainty_penalty": 0.75,
        "confidence_floor": 0.50,
        "hysteresis_fraction": 0.0002,
        "maximum_upward_step": 0.001,
        "recovery_observations": 2,
        "expiry_seconds": 300,
        "regime_multipliers": {"normal": 1.0, "elevated": 0.6, "crisis": 0.2},
    }
    receipt = dynamic_risk_budget_receipt(
        risk001_receipt=dependency(
            "RISK-001", "bt.institutional.risk.stress_dossier_receipt", args.source_commit,
            dataset, {"admissible": True},
        ),
        risk002_receipt=dependency(
            "RISK-002", "bt.institutional.risk.venue_rule_receipt", args.source_commit,
            dataset, {"allowed": True},
        ),
        ml004_receipt=dependency(
            "ML-004", "bt.institutional.ml.calibration_receipt", args.source_commit,
            dataset, {"qualified": True},
        ),
        port004_receipt=dependency(
            "PORT-004", "bt.institutional.capacity.capacity_dossier_receipt", args.source_commit,
            dataset, {"qualified": True, "capacity_supported_notional": 100_000},
        ),
        states=[
            state(0, now),
            state(1, now, regime="crisis", volatility=0.8),
            state(2, now),
            state(3, now),
        ],
        known_at=now,
        prior_risk_fraction=0.008,
        requested_capital=120_000,
        configuration=configuration,
        dataset_digest=dataset,
        source_commit=args.source_commit,
    )
    low_uncertainty = dynamic_risk_budget(
        states=[state(10, now, uncertainty=0.1)], known_at=now,
        prior_risk_fraction=0.008, requested_capital=120_000,
        capacity_supported_notional=100_000, configuration=configuration,
    )
    high_uncertainty = dynamic_risk_budget(
        states=[state(10, now, uncertainty=0.9)], known_at=now,
        prior_risk_fraction=0.008, requested_capital=120_000,
        capacity_supported_notional=100_000, configuration=configuration,
    )
    stale_state = state(20, now)
    stale_state["observed_at"] = (now - timedelta(hours=1)).isoformat()
    stale_state["available_at"] = (now - timedelta(hours=1) + timedelta(seconds=1)).isoformat()
    stale = dynamic_risk_budget(
        states=[stale_state], known_at=now, prior_risk_fraction=0.008,
        requested_capital=120_000, capacity_supported_notional=100_000,
        configuration=configuration,
    )
    result = receipt.result
    report = {
        "schema_version": "risk003-native-pilot-v1.0.0",
        "success": bool(
            verify_receipt(receipt)
            and result["qualified"]
            and high_uncertainty["effective_risk_fraction"] <= low_uncertainty["effective_risk_fraction"]
            and stale["decision"] == "static_conservative_fallback"
            and not any(result["authority"].values())
        ),
        "capital_or_order_authority": False,
        "risk_budget_specification": RISK_BUDGET_SPECIFICATION,
        "risk_budget_specification_digest": digest(RISK_BUDGET_SPECIFICATION),
        "receipt": receipt.as_dict(),
        "uncertainty_monotonic": high_uncertainty["effective_risk_fraction"] <= low_uncertainty["effective_risk_fraction"],
        "stale_input": {
            "decision": stale["decision"],
            "effective_risk_fraction": stale["effective_risk_fraction"],
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="ascii")
    args.output.chmod(0o600)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
