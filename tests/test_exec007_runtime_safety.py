from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from bt.exec.cli import connector_bridge
from bt.exec.services.kill_switch import KillSwitch
from bt.institutional.receipt import build_receipt
from bt.institutional.runtime_safety import (
    RuntimeSafetyError,
    contain_runtime,
    initialize_safety_state,
    load_safety_state,
    recover_runtime,
    runtime_safety_receipt,
    verify_safety_journal,
)

NOW = datetime(2026, 9, 12, 12, tzinfo=UTC)
DATASET = "a" * 64
COMMIT = "b" * 40


def receipt(milestone: str, result: dict | None = None):
    return build_receipt(
        milestone=milestone,
        producer=f"bt.test.{milestone.lower()}",
        producer_version="1.0.0",
        source_commit=COMMIT,
        inputs={"milestone": milestone},
        dataset_digest=DATASET,
        configuration={},
        artifacts={},
        result=result or {"qualified": True},
    )


def authority(state: dict, *, age: int = 0, actor: str = "founder-operator") -> dict:
    return {
        "decision_type": "execution-recovery",
        "action": "resume",
        "object_digest": state["state_digest"],
        "actor": actor,
        "effective_roles": ["founder"],
        "outcome": "authorized",
        "created_at": (NOW - timedelta(seconds=age)).isoformat(),
        "record_digest": "c" * 64,
    }


def test_containment_is_durable_idempotent_and_seen_by_running_switch(tmp_path) -> None:
    state_path = tmp_path / "state.json"
    journal_path = tmp_path / "journal.jsonl"
    initial = initialize_safety_state(
        state_path=state_path, journal_path=journal_path, actor="operator", now=NOW
    )
    assert initial["status"] == "frozen"
    with pytest.raises(RuntimeSafetyError, match="frozen"):
        from bt.institutional.runtime_safety import require_runtime_ready

        require_runtime_ready(state_path)

    killed = contain_runtime(
        state_path=state_path,
        journal_path=journal_path,
        action="kill",
        request_id="kill-1",
        actor="local-controller",
        reason="control plane partition drill",
        now=NOW,
    )
    duplicate = contain_runtime(
        state_path=state_path,
        journal_path=journal_path,
        action="kill",
        request_id="kill-1",
        actor="local-controller",
        reason="control plane partition drill",
        now=NOW,
    )
    assert duplicate == killed
    assert len(verify_safety_journal(journal_path)) == 2
    switch = KillSwitch(state_path=str(state_path), journal_path=str(journal_path))
    assert switch.state().freeze_new_orders
    assert switch.state().reason == "control plane partition drill"


def test_corruption_fails_closed(tmp_path) -> None:
    state_path = tmp_path / "state.json"
    journal_path = tmp_path / "journal.jsonl"
    initialize_safety_state(
        state_path=state_path, journal_path=journal_path, actor="operator", now=NOW
    )
    state_path.write_text("{}", encoding="ascii")
    with pytest.raises(RuntimeSafetyError):
        load_safety_state(state_path)
    assert (
        KillSwitch(state_path=str(state_path), journal_path=str(journal_path))
        .state()
        .freeze_new_orders
    )

    journal_path.write_text("not-json\n", encoding="ascii")
    with pytest.raises(RuntimeSafetyError, match="corrupt"):
        verify_safety_journal(journal_path)


def test_ready_state_with_divergent_journal_fails_closed(tmp_path) -> None:
    from bt.institutional.runtime_safety import require_runtime_ready

    state_path = tmp_path / "state.json"
    journal_path = tmp_path / "journal.jsonl"
    state = initialize_safety_state(
        state_path=state_path, journal_path=journal_path, actor="operator", now=NOW
    )
    clean = receipt(
        "EXEC-004",
        {"qualified": True, "reconciliation": {"submission_allowed": True}},
    )
    recover_runtime(
        state_path=state_path,
        journal_path=journal_path,
        request_id="recover",
        actor="founder-operator",
        reason="reviewed clean reconciliation",
        now=NOW,
        exec004_receipt=clean,
        authority_record=authority(state),
    )
    journal_path.write_text(
        journal_path.read_text(encoding="ascii").splitlines()[0] + "\n",
        encoding="ascii",
    )
    with pytest.raises(RuntimeSafetyError, match="diverge"):
        require_runtime_ready(state_path, journal_path)
    switch = KillSwitch(state_path=str(state_path), journal_path=str(journal_path))
    assert switch.state().freeze_new_orders
    assert switch.state().reason == "execution_safety_state_unavailable"


def test_human_recovery_requires_fresh_bound_authority_and_clean_reconciliation(
    tmp_path,
) -> None:
    state_path = tmp_path / "state.json"
    journal_path = tmp_path / "journal.jsonl"
    state = initialize_safety_state(
        state_path=state_path, journal_path=journal_path, actor="operator", now=NOW
    )
    clean = receipt(
        "EXEC-004",
        {"qualified": True, "reconciliation": {"submission_allowed": True}},
    )
    with pytest.raises(RuntimeSafetyError, match="stale"):
        recover_runtime(
            state_path=state_path,
            journal_path=journal_path,
            request_id="recover-stale",
            actor="founder-operator",
            reason="reviewed clean reconciliation",
            now=NOW,
            exec004_receipt=clean,
            authority_record=authority(state, age=901),
        )
    bad = receipt(
        "EXEC-004",
        {"qualified": False, "reconciliation": {"submission_allowed": False}},
    )
    with pytest.raises(RuntimeSafetyError, match="clean EXEC-004"):
        recover_runtime(
            state_path=state_path,
            journal_path=journal_path,
            request_id="recover-bad",
            actor="founder-operator",
            reason="reviewed reconciliation",
            now=NOW,
            exec004_receipt=bad,
            authority_record=authority(state),
        )
    ready = recover_runtime(
        state_path=state_path,
        journal_path=journal_path,
        request_id="recover-1",
        actor="founder-operator",
        reason="reviewed clean reconciliation",
        now=NOW,
        exec004_receipt=clean,
        authority_record=authority(state),
    )
    assert ready["status"] == "ready"


def test_receipt_requires_exact_dependencies_and_all_drills(tmp_path) -> None:
    state_path = tmp_path / "state.json"
    journal_path = tmp_path / "journal.jsonl"
    initialize_safety_state(
        state_path=state_path, journal_path=journal_path, actor="operator", now=NOW
    )
    dependencies = {
        "EXEC-004": receipt("EXEC-004"),
        "EXEC-006": receipt("EXEC-006"),
        "RISK-005": receipt("RISK-005"),
    }
    produced = runtime_safety_receipt(
        dependency_receipts=dependencies,
        events=verify_safety_journal(journal_path),
        drill_results={
            "partition": True,
            "corrupt_state": True,
            "runaway_orders": True,
            "human_recovery": True,
        },
        dataset_digest=DATASET,
        source_commit=COMMIT,
        configuration={},
    )
    assert produced.milestone == "EXEC-007"
    assert produced.result["qualified"] is True
    with pytest.raises(RuntimeSafetyError, match="drills"):
        runtime_safety_receipt(
            dependency_receipts=dependencies,
            events=verify_safety_journal(journal_path),
            drill_results={"partition": True},
            dataset_digest=DATASET,
            source_commit=COMMIT,
            configuration={},
        )


def test_persistent_freeze_writes_journal(tmp_path) -> None:
    state_path = tmp_path / "state.json"
    journal_path = tmp_path / "journal.jsonl"
    clean = receipt(
        "EXEC-004", {"qualified": True, "reconciliation": {"submission_allowed": True}}
    )
    state = initialize_safety_state(
        state_path=state_path, journal_path=journal_path, actor="operator", now=NOW
    )
    recover_runtime(
        state_path=state_path,
        journal_path=journal_path,
        request_id="recover",
        actor="founder-operator",
        reason="reviewed clean reconciliation",
        now=NOW,
        exec004_receipt=clean,
        authority_record=authority(state),
    )
    switch = KillSwitch(state_path=str(state_path), journal_path=str(journal_path))
    assert not switch.state().freeze_new_orders
    switch.freeze(reason="runaway order detector", ts=pd.Timestamp(NOW))
    assert switch.state().freeze_new_orders
    assert verify_safety_journal(journal_path)[-1]["event_type"] == "runtime_freeze"


def test_emergency_kill_cancels_runaway_orders_without_normal_live_approval(
    tmp_path, monkeypatch
) -> None:
    state_path = tmp_path / "state.json"
    journal_path = tmp_path / "journal.jsonl"
    initialize_safety_state(
        state_path=state_path, journal_path=journal_path, actor="operator", now=NOW
    )

    class Order:
        id = "venue-order-1"
        symbol = "BTCUSDT"
        metadata = {"client_order_id": "client-1"}

    class Adapter:
        cancelled = []

        def start(self):
            return None

        def stop(self):
            return None

        def set_live_mutations_enabled(self, value):
            assert value is True

        def fetch_open_orders(self):
            return [Order()]

        def cancel_order(self, request):
            self.cancelled.append(request.order_id)

        def fetch_positions(self):
            return []

    adapter = Adapter()
    monkeypatch.setattr(connector_bridge, "_adapter", lambda payload: adapter)
    result = connector_bridge.execute(
        {
            "action": "emergency_freeze",
            "venue": "bybit",
            "environment": "live",
            "product_type": "perpetual",
            "safety_state_path": str(state_path),
            "safety_journal_path": str(journal_path),
            "request_id": "runaway-1",
            "reason": "runaway order drill",
        }
    )
    assert result["cancelled_order_ids"] == ["venue-order-1"]
    assert adapter.cancelled == ["venue-order-1"]
    assert load_safety_state(state_path)["status"] == "killed"
