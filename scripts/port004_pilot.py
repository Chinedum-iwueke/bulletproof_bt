#!/usr/bin/env python3
"""Run one deterministic no-authority PORT-004 capacity pilot."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from bt.execution.model_registry import declared_classic_bundle
from bt.institutional.capacity import CAPACITY_SPECIFICATION, capacity_dossier_receipt, portfolio_capacity
from bt.institutional.construction import construction_dossier_receipt
from bt.institutional.execution_calibration import execution_calibration_receipt
from bt.institutional.receipt import build_receipt, digest, verify_receipt


def dependency(milestone: str, commit: str, dataset: str, result: dict | None = None):
    payload = result or {"qualified": True, "admissible": True}
    return build_receipt(
        milestone=milestone,
        producer=f"port004.fixture.{milestone.lower()}",
        producer_version="1.0.0",
        source_commit=commit,
        inputs={},
        dataset_digest=dataset,
        configuration={},
        artifacts=payload,
        result=payload,
    )


def observation(index: int, role: str, now: datetime) -> dict:
    submitted = now - timedelta(minutes=5) + timedelta(seconds=index * 10)
    side = "buy" if index % 2 == 0 else "sell"
    return {
        "observation_id": f"{role}-{index}",
        "venue_id": "bybit",
        "listing_id": "portfolio-perpetual-panel",
        "order_type": "limit",
        "side": side,
        "size_bucket": "small",
        "regime": "normal",
        "sample_role": role,
        "submitted_at": submitted.isoformat(),
        "acknowledged_at": (submitted + timedelta(milliseconds=20 + index)).isoformat(),
        "first_fill_at": (submitted + timedelta(milliseconds=80 + index)).isoformat(),
        "terminal_at": (submitted + timedelta(milliseconds=100 + index)).isoformat(),
        "available_at": (submitted + timedelta(milliseconds=120 + index)).isoformat(),
        "arrival_mid": 60_000,
        "modeled_fill_price": 60_001 if side == "buy" else 59_999,
        "observed_fill_price": 60_001.2 if side == "buy" else 59_998.8,
        "future_mid": 60_000.5 if side == "buy" else 59_999.5,
        "quantity": 0.01,
        "filled_quantity": 0.01,
        "censored": False,
        "source_event_digest": digest({"execution": role, "index": index}),
    }


def snapshot(candidate: str, now: datetime, *, stale: bool = False) -> dict:
    observed = now - timedelta(hours=1) if stale else now - timedelta(seconds=30)
    return {
        "snapshot_id": f"liquidity-{candidate}-{'stale' if stale else 'current'}",
        "candidate_id": candidate,
        "venue_id": "bybit",
        "listing_id": f"{candidate.upper()}USDT",
        "order_type": "limit",
        "size_bucket": "small",
        "regime": "normal",
        "observed_at": observed.isoformat(),
        "available_at": (now - timedelta(seconds=20)).isoformat(),
        "average_daily_dollar_volume": 2_000_000,
        "executable_depth_notional": 200_000,
        "reference_trade_notional": 10_000,
        "spread_bps": 1.0,
        "source_digest": digest({"liquidity": candidate, "observed": observed.isoformat()}),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    now = datetime(2026, 9, 12, 16, tzinfo=UTC)
    dataset = digest({"dataset": "port004-turnover-capacity-fixture-v1"})
    construction = construction_dossier_receipt(
        dependency_receipt=dependency("PORT-002", args.source_commit, dataset, {"qualified": True}),
        risk_receipt=dependency("RISK-001", args.source_commit, dataset, {"admissible": True}),
        expected_returns={"carry": 0.035, "trend": 0.08, "value": 0.045},
        uncertainty={"carry": 0.003, "trend": 0.004, "value": 0.003},
        covariance=[[0.04, 0.006, 0.004], [0.006, 0.09, 0.008], [0.004, 0.008, 0.05]],
        prior_weights={"carry": 0.34, "trend": 0.33, "value": 0.33},
        lower_bounds={"carry": 0.10, "trend": 0.10, "value": 0.10},
        upper_bounds={"carry": 0.60, "trend": 0.60, "value": 0.60},
        solver={
            "name": "projected-gradient-robust-mean-variance",
            "version": "1.0.0",
            "risk_aversion": 4.0,
            "uncertainty_penalty": 0.25,
            "covariance_shrinkage": 0.20,
            "step_size": 0.5,
            "iterations": 500,
        },
        weight_increment=0.01,
        maximum_turnover=0.80,
        maximum_sensitivity_l1=0.50,
        dataset_digest=dataset,
        source_commit=args.source_commit,
    )
    calibration_config = {
        "minimum_observations_per_role": 3,
        "maximum_censoring_rate": 0.25,
        "maximum_holdout_cost_shift_bps": 2,
        "declared_pessimistic_cost_bps": 5,
    }
    observations = [observation(index, role, now) for role in ("calibration", "holdout") for index in range(4)]
    calibration = execution_calibration_receipt(
        exec001_receipt=dependency("EXEC-001", args.source_commit, dataset).as_dict(),
        exec002_receipt=dependency("EXEC-002", args.source_commit, dataset).as_dict(),
        exec004_receipt=dependency("EXEC-004", args.source_commit, dataset).as_dict(),
        shadow001_receipt=dependency("SHADOW-001", args.source_commit, dataset).as_dict(),
        market_model_bundle=declared_classic_bundle(
            profile="tier2",
            parameters={"taker_fee_bps": 5, "slippage_bps": 2, "spread_bps": 1, "delay_bars": 1},
        ).document(),
        observations=observations,
        known_at=now,
        source_commit=args.source_commit,
        dataset_digest=dataset,
        configuration=calibration_config,
    )
    capacity_config = {
        "stale_after_seconds": 300,
        "rebalance_horizon_days": 1,
        "maximum_participation_rate": 0.05,
        "maximum_depth_fraction": 0.5,
        "maximum_liquidation_days": 3,
        "maximum_cost_bps": 12,
        "maximum_portfolio_cost_bps": 5,
        "explicit_fee_bps": 0.5,
        "stress_volume_haircut": 0.5,
        "stress_spread_multiplier": 2,
        "stress_impact_multiplier": 1.5,
        "scale_grid": [0.5, 1.0, 2.0],
    }
    candidates = sorted(construction.result["selection"]["weights"])
    liquidity = [snapshot(candidate, now) for candidate in candidates]
    prior = {"carry": 0.34, "trend": 0.33, "value": 0.33}
    receipt = capacity_dossier_receipt(
        port003_receipt=construction,
        exec005_receipt=calibration,
        prior_weights=prior,
        liquidity_snapshots=liquidity,
        known_at=now,
        requested_capital=100_000,
        configuration=capacity_config,
        dataset_digest=dataset,
        source_commit=args.source_commit,
    )
    stale = portfolio_capacity(
        target_weights=construction.result["selection"]["weights"],
        prior_weights=prior,
        liquidity_snapshots=[snapshot(candidate, now, stale=True) for candidate in candidates],
        execution_calibration=calibration.result["calibration"],
        known_at=now,
        requested_capital=100_000,
        configuration=capacity_config,
    )
    result = receipt.result
    report = {
        "schema_version": "port004-native-pilot-v1.0.0",
        "success": bool(
            verify_receipt(receipt)
            and result["qualified"]
            and result["stressed_capital_capacity"] <= result["base_capital_capacity"]
            and stale["decision"] == "abstain_zero_executable_capacity"
        ),
        "capital_or_order_authority": False,
        "capacity_specification": CAPACITY_SPECIFICATION,
        "capacity_specification_digest": digest(CAPACITY_SPECIFICATION),
        "receipt": receipt.as_dict(),
        "stale_liquidity": {
            "decision": stale["decision"],
            "capacity_supported_notional": stale["capacity_supported_notional"],
            "failures": stale["failures"],
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="ascii")
    args.output.chmod(0o600)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
