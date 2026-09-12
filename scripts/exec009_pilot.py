#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

from bt.institutional.execution_degradation import (
    EXECUTION_DEGRADATION_SPECIFICATION,
    execution_degradation_receipt,
)
from bt.institutional.receipt import build_receipt, digest, verify_receipt


def dependency(milestone: str, dataset_digest: str, source_commit: str) -> dict:
    return build_receipt(
        milestone=milestone,
        producer=f"bt.exec009.fixture.{milestone.lower()}",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={"fixture": "exec009-bounded-replay"},
        dataset_digest=dataset_digest,
        configuration={},
        artifacts={},
        result={"qualified": True},
    ).as_dict()


def observation(number: int, now: datetime, **overrides) -> dict:
    value = {
        "observation_id": f"exec009-{number}",
        "venue": "bybit",
        "environment": "demo",
        "strategy_id": "btc-weekend-lagged-return-momentum",
        "listing_id": "bybit:linear:BTCUSDT",
        "order_type": "limit",
        "size_bucket": "micro",
        "regime": "normal",
        "observed_at": (now - timedelta(seconds=20 - number)).isoformat(),
        "available_at": (now - timedelta(seconds=10 - number)).isoformat(),
        "source_digest": digest({"observation": number}),
        "service_available": True,
        "venue_rule_current": True,
        "fill_rate": 0.95,
        "ack_latency_ms": 100,
        "fill_latency_ms": 500,
        "implementation_shortfall_bps": 3,
        "adverse_selection_bps": 2,
        "queue_model_error_bps": 1,
        "reject_rate": 0.01,
        "reconciliation_breaks": 0,
    }
    value.update(overrides)
    return value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source_commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True
    ).strip()
    dataset_digest = digest({"fixture": "exec009-degradation-replay-v1"})
    now = datetime.now(UTC)
    config = {
        "thresholds": {
            "fill_rate": 0.8,
            "ack_latency_ms": 250,
            "fill_latency_ms": 1000,
            "implementation_shortfall_bps": 8,
            "adverse_selection_bps": 6,
            "queue_model_error_bps": 4,
            "reject_rate": 0.05,
            "reconciliation_breaks": 0,
        },
        "consecutive_breaches": 2,
        "recovery_observations": 3,
        "observation_expiry_seconds": 300,
    }
    lifecycle = {
        "status": "demo",
        "candidate_digest": digest({"candidate": "btc-weekend-lagged-return-momentum"}),
        "record_digest": digest(
            {"candidate_lifecycle": "btc-weekend-lagged-return-momentum"}
        ),
    }
    dependencies = {
        name: dependency(name, dataset_digest, source_commit)
        for name in ("EXEC-005", "EXEC-008")
    }
    dependencies["SHADOW-002"] = build_receipt(
        milestone="SHADOW-002",
        producer="bt.exec009.fixture.shadow002",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={"fixture": "exec009-bounded-replay"},
        dataset_digest=dataset_digest,
        configuration={},
        artifacts={},
        result={"candidate_digest": lifecycle["candidate_digest"]},
    ).as_dict()

    def produce(items: list[dict], prior_status: str = "monitoring") -> dict:
        return execution_degradation_receipt(
            exec005_receipt=dependencies["EXEC-005"],
            exec008_receipt=dependencies["EXEC-008"],
            shadow002_receipt=dependencies["SHADOW-002"],
            governance_policy_digest=digest({"gov003": "active"}),
            platform_observability_digest=digest({"plat005": "current"}),
            candidate_lifecycle=lifecycle,
            observations=items,
            known_at=now,
            prior_status=prior_status,
            dataset_digest=dataset_digest,
            source_commit=source_commit,
            configuration=config,
        ).as_dict()

    healthy = produce([observation(1, now), observation(2, now)])
    degraded = produce(
        [
            observation(3, now, implementation_shortfall_bps=12),
            observation(4, now, adverse_selection_bps=9),
        ]
    )
    outage = produce([observation(5, now, service_available=False)])
    recovery = produce(
        [observation(6, now), observation(7, now), observation(8, now)],
        prior_status="killed",
    )
    receipts = [healthy, degraded, outage, recovery]
    success = (
        all(verify_receipt(item) for item in receipts)
        and healthy["result"]["recommended_action"] == "continue_monitoring"
        and degraded["result"]["recommended_action"] == "shadow_fallback_review"
        and outage["result"]["recommended_action"] == "freeze_and_kill_review"
        and recovery["result"]["recommended_action"] == "independent_restore_review"
        and not any(outage["authority"].values())
    )
    report = {
        "schema_version": "exec009-pilot-report-v1.0.0",
        "success": success,
        "degradation_specification": EXECUTION_DEGRADATION_SPECIFICATION,
        "degradation_specification_digest": digest(EXECUTION_DEGRADATION_SPECIFICATION),
        "receipt": degraded,
        "scenario_receipt_digests": {
            "healthy": healthy["receipt_digest"],
            "confirmed_degradation": degraded["receipt_digest"],
            "critical_outage": outage["receipt_digest"],
            "recovery": recovery["receipt_digest"],
        },
        "decisions": {
            "healthy": healthy["result"]["recommended_action"],
            "confirmed_degradation": degraded["result"]["recommended_action"],
            "critical_outage": outage["result"]["recommended_action"],
            "recovery": recovery["result"]["recommended_action"],
        },
        "capital_or_order_authority": False,
    }
    report["report_digest"] = digest(report)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="ascii"
    )
    args.output.chmod(0o600)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
