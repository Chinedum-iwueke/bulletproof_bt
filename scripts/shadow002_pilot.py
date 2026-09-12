#!/usr/bin/env python3
"""Run deterministic SHADOW-002 monitoring, deterioration and recovery paths."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from bt.institutional.receipt import digest, verify_receipt
from bt.institutional.shadow_monitoring import (
    SHADOW_MONITORING_SPECIFICATION,
    monitor_shadow_candidate,
    shadow_monitoring_receipt,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    now = datetime(2026, 9, 12, 20, tzinfo=UTC)
    candidate = digest({"candidate": "shadow002-prospective-pilot"})
    dataset = digest({"dataset": "shadow002-prospective-pilot"})
    replay = {
        "success": True,
        "sealed": True,
        "sealed_at": (now - timedelta(minutes=2)).isoformat(),
        "journal_digest": digest({"journal": "shadow001-sealed"}),
        "bindings": {"candidate_digest": candidate},
        "capital_or_order_authority": False,
    }
    configuration = {
        "observation_expiry_seconds": 120,
        "journal_expiry_seconds": 3600,
        "review_interval_seconds": 300,
        "maximum_data_lag_seconds": 5,
        "maximum_prediction_drift": 0.20,
        "maximum_cost_drift_bps": 8,
        "maximum_drawdown_fraction": 0.10,
        "demotion_after_consecutive_breaches": 2,
        "recovery_observations": 2,
    }

    def obs(index: int, **updates):
        observed = now - timedelta(seconds=30 - index * 5)
        value = {
            "observation_id": f"obs-{index}",
            "observed_at": observed.isoformat(),
            "available_at": (observed + timedelta(seconds=1)).isoformat(),
            "source_digest": digest({"observation": index}),
            "data_lag_seconds": 1,
            "service_available": True,
            "prediction_drift": 0.02,
            "cost_drift_bps": 1,
            "drawdown_fraction": 0.01,
        }
        value.update(updates)
        return value

    receipt = shadow_monitoring_receipt(
        candidate_digest=candidate,
        shadow001_replay=replay,
        observations=[obs(0)],
        known_at=now,
        prior_status="monitoring",
        last_reviewed_at=now - timedelta(seconds=60),
        configuration=configuration,
        dataset_digest=dataset,
        source_commit=args.source_commit,
    )
    deterioration = monitor_shadow_candidate(
        candidate_digest=candidate,
        shadow001_replay=replay,
        observations=[obs(0, service_available=False), obs(1, service_available=False)],
        known_at=now,
        prior_status="monitoring",
        last_reviewed_at=now - timedelta(seconds=60),
        configuration=configuration,
    )
    recovery = monitor_shadow_candidate(
        candidate_digest=candidate,
        shadow001_replay=replay,
        observations=[obs(0), obs(1)],
        known_at=now,
        prior_status="frozen",
        last_reviewed_at=now - timedelta(seconds=60),
        configuration=configuration,
    )
    stale = obs(9)
    stale["observed_at"] = (now - timedelta(hours=1)).isoformat()
    stale["available_at"] = (
        now - timedelta(hours=1) + timedelta(seconds=1)
    ).isoformat()
    stale_result = monitor_shadow_candidate(
        candidate_digest=candidate,
        shadow001_replay=replay,
        observations=[stale],
        known_at=now,
        prior_status="monitoring",
        last_reviewed_at=now - timedelta(seconds=60),
        configuration=configuration,
    )
    report = {
        "schema_version": "shadow002-native-pilot-v1.0.0",
        "success": verify_receipt(receipt)
        and deterioration["recommended_action"] == "demotion_review_required"
        and recovery["automatic_reactivation"] is False
        and stale_result["recommended_action"] == "freeze_and_review",
        "capital_or_order_authority": False,
        "shadow_monitoring_specification": SHADOW_MONITORING_SPECIFICATION,
        "shadow_monitoring_specification_digest": digest(
            SHADOW_MONITORING_SPECIFICATION
        ),
        "receipt": receipt.as_dict(),
        "deterioration_action": deterioration["recommended_action"],
        "recovery_action": recovery["recommended_action"],
        "stale_action": stale_result["recommended_action"],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="ascii"
    )
    args.output.chmod(0o600)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
