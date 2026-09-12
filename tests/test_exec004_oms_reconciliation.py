from datetime import UTC, datetime, timedelta

import pytest

from bt.institutional.oms import OmsError, client_order_id, oms_reconciliation_receipt, reconcile_oms, replay_oms
from bt.institutional.receipt import build_receipt, digest, verify_receipt

NOW = datetime(2026, 9, 12, tzinfo=UTC)
COMMIT = "a" * 40
DATASET = digest({"dataset": "exec004"})


def command(**overrides):
    value = {"command_id": "c1", "idempotency_key": "trial:1", "action": "submit", "venue_id": "bybit", "listing_id": "BTCUSDT", "side": "buy", "quantity": "1", "order_type": "limit", "limit_price": "100", "intent_digest": "d" * 64, "approval_digest": "e" * 64, "risk_decision_digest": "f" * 64, "issued_at": NOW.isoformat()}
    value.update(overrides)
    return value


def event(event_type="acknowledged", sequence=1, **overrides):
    value = {"event_id": f"e{sequence}", "command_id": "c1", "sequence": sequence, "event_type": event_type, "occurred_at": NOW.isoformat(), "available_at": NOW.isoformat(), "venue_order_id": "v1"}
    value.update(overrides)
    return value


def snapshot(state="acknowledged", fill="0", **overrides):
    value = {"observed_at": NOW.isoformat(), "available_at": NOW.isoformat(), "orders": [{"client_order_id": client_order_id("trial:1", "bybit"), "venue_order_id": "v1", "state": state, "cumulative_fill_quantity": fill}], "positions": {"BTCUSDT": "0"}, "balances": {"USDT": "100"}}
    value.update(overrides)
    return value


def reconcile(journal, remote=None, **overrides):
    values = {"journal": journal, "venue_snapshot": remote or snapshot(), "local_positions": {"BTCUSDT": "0"}, "local_balances": {"USDT": "100"}, "known_at": NOW}
    values.update(overrides)
    return reconcile_oms(**values)


def dependency(milestone):
    return build_receipt(milestone=milestone, producer="fixture", producer_version="1.0.0", source_commit=COMMIT, inputs={}, dataset_digest=DATASET, configuration={}, artifacts={}, result={}).as_dict()


def test_duplicate_command_is_suppressed_and_client_id_is_stable():
    result = replay_oms(commands=[command(), command(command_id="retry-c1")], events=[event()])
    assert result["duplicate_commands_suppressed"] == 1
    assert len(result["orders"]) == 1
    assert result["orders"][0]["client_order_id"] == client_order_id("trial:1", "bybit")


def test_idempotency_key_content_conflict_fails_closed():
    with pytest.raises(OmsError, match="idempotency key"):
        replay_oms(commands=[command(), command(command_id="c2", quantity="2")], events=[])


def test_partial_fill_then_cancel_race_is_monotonic():
    events = [event("acknowledged", 1), event("partially_filled", 2, execution_id="x1", fill_quantity="0.4", fill_price="100"), event("cancel_requested", 3), event("partially_filled", 4, execution_id="x2", fill_quantity="0.1", fill_price="100"), event("cancelled", 5)]
    order = replay_oms(commands=[command()], events=events)["orders"][0]
    assert order["state"] == "cancelled"
    assert order["cumulative_fill_quantity"] == "0.5"


def test_duplicate_fill_execution_is_not_applied_twice():
    events = [event("acknowledged", 1), event("partially_filled", 2, execution_id="x1", fill_quantity="0.4", fill_price="100"), event("partially_filled", 3, event_id="e3", execution_id="x1", fill_quantity="0.4", fill_price="100")]
    result = replay_oms(commands=[command()], events=events)
    assert result["orders"][0]["cumulative_fill_quantity"] == "0.4"
    assert result["duplicate_fills_suppressed"] == 1


def test_duplicate_broker_event_is_suppressed_exactly_once():
    acknowledged = event()
    result = replay_oms(commands=[command()], events=[acknowledged, acknowledged])
    assert result["duplicate_events_suppressed"] == 1
    assert len(result["events"]) == 1


def test_execution_identity_content_conflict_fails_closed():
    events = [event("acknowledged", 1), event("partially_filled", 2, execution_id="x1", fill_quantity="0.4", fill_price="100"), event("partially_filled", 3, execution_id="x1", fill_quantity="0.3", fill_price="100")]
    with pytest.raises(OmsError, match="execution_id"):
        replay_oms(commands=[command()], events=events)


def test_cancel_requires_a_known_target_order():
    cancel = command(command_id="cancel", idempotency_key="cancel:1", action="cancel", target_client_order_id="missing")
    with pytest.raises(OmsError, match="cancel command"):
        replay_oms(commands=[cancel], events=[])


def test_cancel_command_can_reference_the_canonical_client_order():
    cancel = command(
        command_id="cancel",
        idempotency_key="cancel:1",
        action="cancel",
        target_client_order_id=client_order_id("trial:1", "bybit"),
    )
    result = replay_oms(commands=[command(), cancel], events=[event()])
    assert len(result["commands"]) == 2
    assert result["orders"][0]["state"] == "acknowledged"


def test_overfill_and_noncontiguous_events_fail_closed():
    with pytest.raises(OmsError, match="exceeds"):
        replay_oms(commands=[command()], events=[event("filled", 1, execution_id="x", fill_quantity="2", fill_price="100")])
    with pytest.raises(OmsError, match="contiguous"):
        replay_oms(commands=[command()], events=[event(sequence=2)])


def test_terminal_state_rejects_late_cancel():
    with pytest.raises(OmsError, match="terminal"):
        replay_oms(commands=[command()], events=[event("filled", 1, execution_id="x", fill_quantity="1", fill_price="100"), event("cancelled", 2)])


def test_unknown_submission_is_recovered_by_client_identity():
    journal = replay_oms(commands=[command()], events=[event("submission_unknown", 1, venue_order_id=None)])
    result = reconcile(journal)
    assert result["qualified"] is True
    assert result["recovered_ambiguous_submissions"] == [client_order_id("trial:1", "bybit")]


def test_unknown_submission_without_remote_match_freezes():
    journal = replay_oms(commands=[command()], events=[event("submission_unknown", 1, venue_order_id=None)])
    remote = snapshot(orders=[])
    result = reconcile(journal, remote)
    assert result["decision"] == "freeze_and_investigate"
    assert result["submission_allowed"] is False
    assert result["discrepancies"][0]["category"] == "ambiguous_submission"


def test_unknown_external_order_and_state_drift_freeze():
    journal = replay_oms(commands=[command()], events=[event()])
    unknown = {"client_order_id": "external", "venue_order_id": "v2", "state": "acknowledged", "cumulative_fill_quantity": "0"}
    local_remote = snapshot(state="filled")["orders"][0]
    remote = snapshot(orders=[local_remote, unknown])
    result = reconcile(journal, remote)
    assert {item["category"] for item in result["discrepancies"]} == {"unknown_venue_order", "order_state"}
    assert not result["submission_allowed"]


def test_stale_snapshot_and_balance_or_position_break_freeze():
    journal = replay_oms(commands=[command()], events=[event()])
    stale_at = (NOW - timedelta(minutes=1)).isoformat()
    remote = snapshot(observed_at=stale_at, available_at=stale_at, positions={"BTCUSDT": "1"}, balances={"USDT": "99"})
    result = reconcile(journal, remote)
    assert {item["category"] for item in result["discrepancies"]} == {"stale_snapshot", "position_quantity", "balance_quantity"}


def test_snapshot_future_availability_fails_point_in_time_guard():
    journal = replay_oms(commands=[command()], events=[event()])
    future = (NOW + timedelta(seconds=1)).isoformat()
    with pytest.raises(OmsError, match="point-in-time"):
        reconcile(journal, snapshot(observed_at=future, available_at=future))


def test_restart_replay_is_digest_identical():
    commands = [command()]
    events = [event()]
    assert replay_oms(commands=commands, events=events)["journal_digest"] == replay_oms(commands=commands, events=events)["journal_digest"]


def test_receipt_binds_dependencies_and_has_no_authority():
    receipt = oms_reconciliation_receipt(exec001_receipt=dependency("EXEC-001"), exec003_receipt=dependency("EXEC-003"), risk002_receipt=dependency("RISK-002"), commands=[command()], events=[event()], venue_snapshot=snapshot(), local_positions={"BTCUSDT": "0"}, local_balances={"USDT": "100"}, known_at=NOW, source_commit=COMMIT, dataset_digest=DATASET, configuration={})
    assert verify_receipt(receipt)
    assert receipt.result["qualified"] is True
    assert not any(receipt.authority.values())


def test_receipt_rejects_wrong_dependency():
    with pytest.raises(OmsError, match="EXEC-001"):
        oms_reconciliation_receipt(exec001_receipt=dependency("EXEC-002"), exec003_receipt=dependency("EXEC-003"), risk002_receipt=dependency("RISK-002"), commands=[], events=[], venue_snapshot=snapshot(orders=[]), local_positions={}, local_balances={}, known_at=NOW, source_commit=COMMIT, dataset_digest=DATASET, configuration={})
