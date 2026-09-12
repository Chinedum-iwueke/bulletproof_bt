#!/usr/bin/env python3
"""Run the deterministic, no-authority EXEC-006 qualification replay."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from bt.institutional.execution_scheduler import (
    EXECUTION_SCHEDULE_SPECIFICATION,
    build_execution_schedule,
    execution_schedule_receipt,
    next_schedule_action,
)
from bt.institutional.receipt import build_receipt, digest, verify_receipt


def dependency(milestone: str, producer: str, commit: str, dataset: str):
    return build_receipt(
        milestone=milestone,
        producer=producer,
        producer_version="1.0.0",
        source_commit=commit,
        inputs={},
        dataset_digest=dataset,
        configuration={},
        artifacts={},
        result={"qualified": True},
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    now = datetime(2026, 9, 12, 22, tzinfo=UTC)
    dataset = digest({"dataset": "exec006-qualification-fixture-v1"})
    parent = {
        "intent_id": "exec006-parent-1",
        "candidate_digest": digest({"candidate": "fixture"}),
        "symbol": "BTCUSDT",
        "side": "buy",
        "reduce_only": False,
        "total_quantity": 0.04,
        "reference_price": 50_000,
        "start_at": (now + timedelta(seconds=10)).isoformat(),
        "end_at": (now + timedelta(seconds=40)).isoformat(),
        "slice_count": 4,
        "projected_interval_volumes": [10, 20, 30, 40],
    }
    policy = {
        "allowed_algorithms": ["market", "limit", "twap", "vwap"],
        "maximum_horizon_seconds": 300,
        "maximum_slices": 8,
        "minimum_child_quantity": 0.001,
        "maximum_participation_rate": 0.01,
        "clock_tolerance_seconds": 5,
        "child_lifetime_seconds": 10,
        "algorithm_cost_multipliers": {
            "market": 1.5,
            "limit": 0.8,
            "twap": 1.0,
            "vwap": 0.9,
        },
    }
    profile = {"available_at": now.isoformat(), "bucket_weights": [1, 2, 3, 4]}
    receipt = execution_schedule_receipt(
        parent_intent=parent,
        algorithm="vwap",
        policy=policy,
        known_at=now,
        volume_profile=profile,
        exec004_receipt=dependency(
            "EXEC-004",
            "bt.institutional.oms.oms_reconciliation_receipt",
            args.source_commit,
            dataset,
        ),
        exec005_receipt=build_receipt(
            milestone="EXEC-005",
            producer="bt.institutional.execution_calibration.execution_calibration_receipt",
            producer_version="1.0.0",
            source_commit=args.source_commit,
            inputs={},
            dataset_digest=dataset,
            configuration={},
            artifacts={},
            result={
                "qualified": True,
                "calibration": {"strata": [{"pessimistic_cost_bps": 5.0}]},
            },
        ),
        dataset_digest=dataset,
        source_commit=args.source_commit,
    )
    schedule = receipt.result["schedule"]
    first_action = next_schedule_action(
        schedule=schedule,
        now=now + timedelta(seconds=10),
        cumulative_filled_quantity=0,
        child_states={},
    )
    repeat = build_execution_schedule(
        parent_intent=parent,
        algorithm="vwap",
        policy=policy,
        known_at=now,
        volume_profile=profile,
    )
    report = {
        "schema_version": "exec006-native-pilot-v1.0.0",
        "success": bool(
            verify_receipt(receipt)
            and schedule == repeat
            and first_action["action"] == "propose_submit"
            and first_action["risk_authorized"] is False
        ),
        "evidence_class": "deterministic_qualification_fixture",
        "capital_or_order_authority": False,
        "execution_schedule_specification": EXECUTION_SCHEDULE_SPECIFICATION,
        "execution_schedule_specification_digest": digest(
            EXECUTION_SCHEDULE_SPECIFICATION
        ),
        "receipt": receipt.as_dict(),
        "first_action": first_action,
    }
    report["report_digest"] = digest(report)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="ascii"
    )
    args.output.chmod(0o600)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
