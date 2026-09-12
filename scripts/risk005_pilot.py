#!/usr/bin/env python3
"""Run the deterministic, no-authority RISK-005 qualification fixture."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from bt.institutional.receipt import build_receipt, digest, verify_receipt
from bt.institutional.realtime_risk import (
    REALTIME_RISK_SPECIFICATION,
    realtime_risk_decision_receipt,
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    now = datetime(2026, 9, 12, 20, tzinfo=UTC)
    dataset = digest({"dataset": "risk005-qualification-fixture-v1"})
    candidate = digest({"candidate": "risk005-fixture"})
    dependencies = {
        "RISK-002": dependency(
            "RISK-002",
            "bt.institutional.risk.venue_rule_receipt",
            args.source_commit,
            dataset,
            {"allowed": True},
        ),
        "RISK-003": dependency(
            "RISK-003",
            "bt.institutional.risk_budget.dynamic_risk_budget_receipt",
            args.source_commit,
            dataset,
            {"qualified": True, "budget_capital": 1_000.0},
        ),
        "RISK-004": dependency(
            "RISK-004",
            "bt.institutional.candidate_admission.candidate_admission_receipt",
            args.source_commit,
            dataset,
            {
                "eligible_for_authority_review": True,
                "candidate_digest": candidate,
                "requested_action": "allocate",
                "expires_at": (now + timedelta(hours=1)).isoformat(),
            },
        ),
        "EXEC-001": dependency(
            "EXEC-001",
            "bt.institutional.execution.execution_journal_receipt",
            args.source_commit,
            dataset,
            {"reconstructable": True},
        ),
        "EXEC-004": dependency(
            "EXEC-004",
            "bt.institutional.oms.oms_reconciliation_receipt",
            args.source_commit,
            dataset,
            {"qualified": True, "reconciliation": {"submission_allowed": True}},
        ),
    }
    policy = {
        "snapshot_expiry_seconds": 1.0,
        "decision_deadline_ms": 50.0,
        "allowed_symbols": ["BTCUSDT"],
        "maximum_order_quantity": 1.0,
        "maximum_order_notional": 1_000.0,
        "maximum_open_orders": 5,
        "maximum_gross_notional": 1_000.0,
        "maximum_daily_loss": 100.0,
    }
    state = {
        "state_id": "qualification-state-7",
        "version": 7,
        "observed_at": (now - timedelta(milliseconds=10)).isoformat(),
        "available_at": (now - timedelta(milliseconds=8)).isoformat(),
        "positions": {"BTCUSDT": 0.0},
        "connector_healthy": True,
        "reconciliation_healthy": True,
        "kill_active": False,
        "critical_incidents": 0,
        "open_orders": 0,
        "gross_notional": 0.0,
        "daily_pnl": 0.0,
    }
    intent = {
        "intent_id": "qualification-intent-1",
        "candidate_digest": candidate,
        "received_at": (now - timedelta(milliseconds=5)).isoformat(),
        "expected_state_version": 7,
        "symbol": "BTCUSDT",
        "side": "buy",
        "quantity": 0.01,
        "price": 10_000.0,
        "reduce_only": False,
    }
    allowed = realtime_risk_decision_receipt(
        intent=intent,
        state=state,
        dependency_receipts=dependencies,
        policy=policy,
        known_at=now,
        dataset_digest=dataset,
        source_commit=args.source_commit,
    )
    denied = realtime_risk_decision_receipt(
        intent=intent | {"intent_id": "qualification-intent-2", "quantity": 2.0},
        state=state,
        dependency_receipts=dependencies,
        policy=policy,
        known_at=now,
        dataset_digest=dataset,
        source_commit=args.source_commit,
    )
    reduce_only = realtime_risk_decision_receipt(
        intent=intent
        | {
            "intent_id": "qualification-intent-3",
            "side": "sell",
            "quantity": 0.05,
            "reduce_only": True,
        },
        state=state | {"kill_active": True, "positions": {"BTCUSDT": 1.0}},
        dependency_receipts=dependencies,
        policy=policy,
        known_at=now,
        dataset_digest=dataset,
        source_commit=args.source_commit,
    )
    report = {
        "schema_version": "risk005-native-pilot-v1.0.0",
        "success": bool(
            verify_receipt(allowed)
            and verify_receipt(denied)
            and verify_receipt(reduce_only)
            and allowed.result["decision"] == "allow"
            and denied.result["decision"] == "deny"
            and reduce_only.result["decision"] == "reduce_only_exit"
        ),
        "capital_or_order_authority": False,
        "evidence_class": "deterministic_qualification_fixture",
        "realtime_risk_specification": REALTIME_RISK_SPECIFICATION,
        "realtime_risk_specification_digest": digest(REALTIME_RISK_SPECIFICATION),
        "decisions": {
            "allow": allowed.as_dict(),
            "deny": denied.as_dict(),
            "reduce_only_exit": reduce_only.as_dict(),
        },
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
