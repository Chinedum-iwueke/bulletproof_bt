#!/usr/bin/env python3
"""Run a deterministic no-authority EXEC-005 calibration pilot."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from bt.execution.model_registry import declared_classic_bundle
from bt.institutional.execution_calibration import (
    CALIBRATION_SPECIFICATION,
    calibrate_execution_quality,
    execution_calibration_receipt,
)
from bt.institutional.receipt import build_receipt, digest


def dependency(milestone: str, commit: str, dataset: str) -> dict:
    return build_receipt(
        milestone=milestone,
        producer=f"exec005.fixture.{milestone.lower()}",
        producer_version="1.0.0",
        source_commit=commit,
        inputs={},
        dataset_digest=dataset,
        configuration={},
        artifacts={},
        result={"qualified": True},
    ).as_dict()


def observation(index: int, role: str, *, future: bool = False) -> dict:
    submitted = datetime(2026, 9, 12, 12, tzinfo=UTC) + timedelta(seconds=index * 10)
    side = "buy" if index % 2 == 0 else "sell"
    return {
        "observation_id": f"{role}-{index}",
        "venue_id": "bybit",
        "listing_id": "BTCUSDT",
        "order_type": "limit",
        "side": side,
        "size_bucket": "small",
        "regime": "normal",
        "sample_role": role,
        "submitted_at": submitted.isoformat(),
        "acknowledged_at": (submitted + timedelta(milliseconds=20 + index)).isoformat(),
        "first_fill_at": (submitted + timedelta(milliseconds=80 + index)).isoformat(),
        "terminal_at": (submitted + timedelta(milliseconds=100 + index)).isoformat(),
        "available_at": (submitted + timedelta(hours=3) if future else submitted + timedelta(milliseconds=120)).isoformat(),
        "arrival_mid": 60000,
        "modeled_fill_price": 60001 if side == "buy" else 59999,
        "observed_fill_price": 60001.2 if side == "buy" else 59998.8,
        "future_mid": 60000.5 if side == "buy" else 59999.5,
        "quantity": 0.01,
        "filled_quantity": 0.01,
        "censored": False,
        "source_event_digest": digest({"event": role, "index": index}),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    dataset = digest({"dataset": "exec005-observed-vs-modeled-fixture-v1"})
    known_at = datetime(2026, 9, 12, 12, 10, tzinfo=UTC)
    observations = [observation(index, role) for role in ("calibration", "holdout") for index in range(4)]
    observations.append(observation(99, "holdout", future=True))
    configuration = {
        "minimum_observations_per_role": 3,
        "maximum_censoring_rate": 0.25,
        "maximum_holdout_cost_shift_bps": 2,
        "declared_pessimistic_cost_bps": 5,
    }
    prior = declared_classic_bundle(
        profile="tier2",
        parameters={"taker_fee_bps": 5, "slippage_bps": 2, "spread_bps": 1, "delay_bars": 1},
    ).document()
    receipt = execution_calibration_receipt(
        exec001_receipt=dependency("EXEC-001", args.source_commit, dataset),
        exec002_receipt=dependency("EXEC-002", args.source_commit, dataset),
        exec004_receipt=dependency("EXEC-004", args.source_commit, dataset),
        shadow001_receipt=dependency("SHADOW-001", args.source_commit, dataset),
        market_model_bundle=prior,
        observations=observations,
        known_at=known_at,
        source_commit=args.source_commit,
        dataset_digest=dataset,
        configuration=configuration,
    )
    sparse = calibrate_execution_quality(
        observations=[observation(0, "calibration"), observation(0, "holdout")],
        known_at=known_at,
        configuration=configuration,
    )
    report = {
        "schema_version": "exec005-native-pilot-v1.0.0",
        "success": receipt.result["qualified"] and sparse["decision"] == "use_pessimistic_fallback",
        "capital_or_order_authority": False,
        "calibration_specification": CALIBRATION_SPECIFICATION,
        "calibration_specification_digest": receipt.result["calibration_schema_digest"],
        "receipt": receipt.as_dict(),
        "future_observations_excluded": receipt.result["calibration"]["future_observations_excluded"],
        "sparse_evidence": {
            "decision": sparse["decision"],
            "reasons": sparse["strata"][0]["reasons"],
            "pessimistic_cost_bps": sparse["strata"][0]["pessimistic_cost_bps"],
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
