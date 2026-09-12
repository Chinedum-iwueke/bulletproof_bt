#!/usr/bin/env python3
"""Run the deterministic no-authority EXEC-004 OMS pilot."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from bt.institutional.oms import (
    OMS_SPECIFICATION,
    client_order_id,
    oms_reconciliation_receipt,
    reconcile_oms,
    replay_oms,
)
from bt.institutional.receipt import build_receipt, digest


def dependency(milestone: str, commit: str, dataset: str) -> dict:
    return build_receipt(
        milestone=milestone, producer=f"exec004.fixture.{milestone.lower()}", producer_version="1.0.0",
        source_commit=commit, inputs={}, dataset_digest=dataset, configuration={}, artifacts={}, result={"qualified": True},
    ).as_dict()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    now = datetime(2026, 9, 12, 1, 0, tzinfo=UTC)
    dataset = digest({"dataset": "exec004-oms-fixture-v1"})
    command = {
        "command_id": "command-001", "idempotency_key": "campaign:trial:decision:1", "action": "submit",
        "venue_id": "bybit", "listing_id": "BTCUSDT", "side": "buy", "quantity": "0.001",
        "order_type": "limit", "limit_price": "60000", "intent_digest": "a" * 64,
        "approval_digest": "b" * 64, "risk_decision_digest": "c" * 64, "issued_at": now.isoformat(),
    }
    client_id = client_order_id(command["idempotency_key"], "bybit")
    events = [
        {"event_id": "event-001", "command_id": "command-001", "sequence": 1, "event_type": "submission_unknown", "occurred_at": now.isoformat(), "available_at": now.isoformat()},
    ]
    snapshot = {
        "observed_at": now.isoformat(), "available_at": (now + timedelta(seconds=1)).isoformat(),
        "orders": [{"client_order_id": client_id, "venue_order_id": "venue-001", "state": "acknowledged", "cumulative_fill_quantity": "0"}],
        "positions": {"BTCUSDT": "0"}, "balances": {"USDT": "100"},
    }
    receipt = oms_reconciliation_receipt(
        exec001_receipt=dependency("EXEC-001", args.source_commit, dataset),
        exec003_receipt=dependency("EXEC-003", args.source_commit, dataset),
        risk002_receipt=dependency("RISK-002", args.source_commit, dataset),
        commands=[command, command], events=events, venue_snapshot=snapshot,
        local_positions={"BTCUSDT": "0"}, local_balances={"USDT": "100"},
        known_at=now + timedelta(seconds=2), source_commit=args.source_commit, dataset_digest=dataset,
        configuration={"maximum_snapshot_age_seconds": 30, "quantity_tolerance": "0", "balance_tolerance": "0"},
    )
    stale = dict(snapshot)
    stale_at = (now - timedelta(minutes=5)).isoformat()
    stale["observed_at"] = stale_at
    stale["available_at"] = stale_at
    adverse = reconcile_oms(
        journal=replay_oms(commands=[command], events=events), venue_snapshot=stale,
        local_positions={"BTCUSDT": "0"}, local_balances={"USDT": "100"}, known_at=now,
    )
    report = {
        "schema_version": "exec004-native-pilot-v1.0.0", "success": receipt.result["qualified"],
        "capital_or_order_authority": False, "oms_specification": OMS_SPECIFICATION,
        "oms_specification_digest": receipt.result["oms_schema_digest"], "receipt": receipt.as_dict(),
        "adversarial_stale_snapshot": {"decision": adverse["decision"], "submission_allowed": adverse["submission_allowed"]},
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["success"] and not adverse["submission_allowed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
