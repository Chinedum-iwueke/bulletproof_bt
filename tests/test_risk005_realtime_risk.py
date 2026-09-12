from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from bt.institutional.realtime_risk import (
    RealtimeRiskError,
    realtime_risk_decision_receipt,
    require_realtime_risk_authorization,
)
from bt.institutional.receipt import build_receipt, verify_receipt

NOW = datetime(2026, 9, 12, 12, 0, tzinfo=UTC)
DATASET = "1" * 64
COMMIT = "2" * 40
CANDIDATE = "3" * 64


def _dependency(milestone: str, result: dict[str, object]):
    return build_receipt(
        milestone=milestone,
        producer=f"tests.{milestone.lower()}",
        producer_version="1.0.0",
        source_commit=COMMIT,
        inputs={"milestone": milestone},
        dataset_digest=DATASET,
        configuration={},
        artifacts={},
        result=result,
    )


def _dependencies():
    return {
        "RISK-002": _dependency("RISK-002", {"allowed": True}),
        "RISK-003": _dependency(
            "RISK-003", {"qualified": True, "budget_capital": 1_000.0}
        ),
        "RISK-004": _dependency(
            "RISK-004",
            {
                "eligible_for_authority_review": True,
                "candidate_digest": CANDIDATE,
                "requested_action": "allocate",
                "expires_at": (NOW + timedelta(hours=1)).isoformat(),
            },
        ),
        "EXEC-001": _dependency("EXEC-001", {"reconstructable": True}),
        "EXEC-004": _dependency(
            "EXEC-004",
            {"qualified": True, "reconciliation": {"submission_allowed": True}},
        ),
    }


def _intent(**changes):
    value = {
        "intent_id": "intent-1",
        "candidate_digest": CANDIDATE,
        "received_at": (NOW - timedelta(milliseconds=5)).isoformat(),
        "expected_state_version": 7,
        "symbol": "BTCUSDT",
        "side": "buy",
        "quantity": 0.01,
        "price": 10_000.0,
        "reduce_only": False,
    }
    value.update(changes)
    return value


def _state(**changes):
    value = {
        "state_id": "state-7",
        "version": 7,
        "observed_at": (NOW - timedelta(milliseconds=10)).isoformat(),
        "available_at": (NOW - timedelta(milliseconds=8)).isoformat(),
        "positions": {"BTCUSDT": 0.0},
        "connector_healthy": True,
        "reconciliation_healthy": True,
        "kill_active": False,
        "critical_incidents": 0,
        "open_orders": 0,
        "gross_notional": 0.0,
        "daily_pnl": 0.0,
    }
    value.update(changes)
    return value


def _policy(**changes):
    value = {
        "snapshot_expiry_seconds": 1.0,
        "decision_deadline_ms": 50.0,
        "allowed_symbols": ["BTCUSDT"],
        "maximum_order_quantity": 1.0,
        "maximum_order_notional": 1_000.0,
        "maximum_open_orders": 5,
        "maximum_gross_notional": 1_000.0,
        "maximum_daily_loss": 100.0,
    }
    value.update(changes)
    return value


def _decision(*, intent=None, state=None, dependencies=None, policy=None):
    return realtime_risk_decision_receipt(
        intent=intent or _intent(),
        state=state or _state(),
        dependency_receipts=dependencies or _dependencies(),
        policy=policy or _policy(),
        known_at=NOW,
        dataset_digest=DATASET,
        source_commit=COMMIT,
    )


def test_allows_only_exact_current_order_and_is_deterministic() -> None:
    first = _decision()
    second = _decision()
    assert first == second
    assert verify_receipt(first)
    assert first.result["decision"] == "allow"
    assert first.result["authority"] == {
        "allocation": False,
        "capital": False,
        "orders": False,
        "promotion": False,
    }
    accepted = require_realtime_risk_authorization(
        receipt=first,
        order={
            "symbol": "BTCUSDT",
            "side": "buy",
            "quantity": 0.01,
            "reference_price": 10_000.0,
            "reduce_only": False,
        },
        expected_state_version=7,
        now=NOW + timedelta(milliseconds=100),
        maximum_age_seconds=1.0,
    )
    assert accepted["intent_id"] == "intent-1"


@pytest.mark.parametrize(
    ("state", "intent", "reason"),
    [
        (
            {
                "observed_at": (NOW - timedelta(seconds=2, milliseconds=1)).isoformat(),
                "available_at": (NOW - timedelta(seconds=2)).isoformat(),
            },
            {},
            "stale_state",
        ),
        (
            {},
            {"received_at": (NOW - timedelta(seconds=1)).isoformat()},
            "decision_deadline_exceeded",
        ),
        ({"connector_healthy": False}, {}, "connector_unhealthy"),
        ({"reconciliation_healthy": False}, {}, "reconciliation_unhealthy"),
        ({"critical_incidents": 1}, {}, "critical_incident"),
        ({"open_orders": 5}, {}, "open_order_limit"),
        ({"gross_notional": 950.0}, {}, "gross_notional_limit"),
        ({"daily_pnl": -100.0}, {}, "daily_loss_limit"),
        ({}, {"quantity": 2.0}, "order_quantity_limit"),
        ({}, {"quantity": 0.2}, "order_notional_limit"),
        ({}, {"symbol": "ETHUSDT"}, "symbol_not_allowed"),
    ],
)
def test_fail_closed_conditions(state, intent, reason) -> None:
    receipt = _decision(state=_state(**state), intent=_intent(**intent))
    assert receipt.result["decision"] == "deny"
    assert receipt.result["effective_quantity"] == 0.0
    assert reason in receipt.result["reasons"]


def test_state_version_race_is_rejected_before_decision() -> None:
    with pytest.raises(RealtimeRiskError, match="version race"):
        _decision(state=_state(version=8))


def test_kill_state_allows_only_a_genuine_reduce_only_exit() -> None:
    exit_receipt = _decision(
        intent=_intent(side="sell", quantity=0.05, reduce_only=True),
        state=_state(kill_active=True, positions={"BTCUSDT": 1.0}),
    )
    assert exit_receipt.result["decision"] == "reduce_only_exit"
    assert exit_receipt.result["allowed"] is True

    false_exit = _decision(
        intent=_intent(side="buy", quantity=0.05, reduce_only=True),
        state=_state(kill_active=True, positions={"BTCUSDT": 1.0}),
    )
    assert false_exit.result["decision"] == "deny"
    assert "reduce_only_does_not_reduce" in false_exit.result["reasons"]


def test_dependency_failure_and_expired_admission_deny() -> None:
    dependencies = _dependencies()
    dependencies["EXEC-004"] = _dependency(
        "EXEC-004", {"qualified": True, "reconciliation": {"submission_allowed": False}}
    )
    receipt = _decision(dependencies=dependencies)
    assert receipt.result["decision"] == "deny"
    assert "oms_not_reconciled" in receipt.result["reasons"]

    dependencies = _dependencies()
    dependencies["RISK-004"] = _dependency(
        "RISK-004",
        {
            "eligible_for_authority_review": True,
            "candidate_digest": CANDIDATE,
            "requested_action": "allocate",
            "expires_at": NOW.isoformat(),
        },
    )
    assert (
        "candidate_not_admitted"
        in _decision(dependencies=dependencies).result["reasons"]
    )


def test_tampering_binding_or_staleness_cannot_authorize_submission() -> None:
    receipt = _decision().as_dict()
    tampered = deepcopy(receipt)
    tampered["result"]["allowed"] = False
    with pytest.raises(RealtimeRiskError, match="exact RISK-005"):
        require_realtime_risk_authorization(
            receipt=tampered,
            order={},
            expected_state_version=7,
            now=NOW,
            maximum_age_seconds=1.0,
        )

    with pytest.raises(RealtimeRiskError, match="does not match"):
        require_realtime_risk_authorization(
            receipt=receipt,
            order={
                "symbol": "BTCUSDT",
                "side": "buy",
                "quantity": 0.02,
                "reference_price": 10_000.0,
                "reduce_only": False,
            },
            expected_state_version=7,
            now=NOW,
            maximum_age_seconds=1.0,
        )

    with pytest.raises(RealtimeRiskError, match="stale"):
        require_realtime_risk_authorization(
            receipt=receipt,
            order={
                "symbol": "BTCUSDT",
                "side": "buy",
                "quantity": 0.01,
                "reference_price": 10_000.0,
                "reduce_only": False,
            },
            expected_state_version=7,
            now=NOW + timedelta(seconds=2),
            maximum_age_seconds=1.0,
        )
