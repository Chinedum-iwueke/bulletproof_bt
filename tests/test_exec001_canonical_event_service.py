from datetime import UTC, datetime, timedelta

import pytest

from bt.institutional.execution import (
    CanonicalEventError,
    CanonicalEventJournal,
    canonical_event,
    execution_journal_receipt,
    verify_event,
)
from bt.institutional.receipt import build_receipt, digest, verify_receipt

COMMIT = "a" * 40
DATASET = digest({"dataset": "exec001-fixture"})
T0 = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)


def dependency(milestone: str) -> dict:
    receipt = build_receipt(
        milestone=milestone,
        producer=f"fixture.{milestone.lower()}",
        producer_version="1.0.0",
        source_commit=COMMIT,
        inputs={"milestone": milestone},
        dataset_digest=DATASET,
        configuration={},
        artifacts={},
        result={"qualified": True},
    )
    return receipt.as_dict()


def event(sequence: int, *, event_id: str | None = None, received: int | None = None, correction_of: str | None = None):
    return canonical_event(
        event_id=event_id or f"event-{sequence}",
        source="bybit",
        stream="BTCUSDT:orders",
        kind="order_update",
        instrument_id="BTCUSDT-PERP",
        event_time=T0 + timedelta(seconds=sequence),
        receive_time=T0 + timedelta(seconds=received if received is not None else sequence + 1),
        source_sequence=sequence,
        correction_of=correction_of,
        payload={"state": "filled", "sequence": sequence},
    )


def test_event_is_dual_clock_digest_bound() -> None:
    item = event(1)
    assert verify_event(item)
    changed = item.as_dict()
    changed["payload"]["state"] = "cancelled"
    assert not verify_event(changed)


def test_journal_is_idempotent_and_rejects_identity_collision(tmp_path) -> None:
    journal = CanonicalEventJournal(tmp_path / "events.sqlite")
    item = event(1)
    assert journal.append(item)["status"] == "appended"
    assert journal.append(item)["status"] == "duplicate"
    collision = canonical_event(
        event_id=item.event_id,
        source=item.source,
        stream=item.stream,
        kind=item.kind,
        event_time=T0 + timedelta(seconds=1),
        receive_time=T0 + timedelta(seconds=2),
        source_sequence=1,
        payload={"state": "rejected"},
    )
    with pytest.raises(CanonicalEventError, match="reused"):
        journal.append(collision)
    journal.close()


def test_disorder_and_gaps_are_explicit_and_replay_is_deterministic(tmp_path) -> None:
    journal = CanonicalEventJournal(tmp_path / "events.sqlite")
    assert journal.append(event(1))["late"] is False
    assert journal.append(event(3))["gap"] is True
    assert journal.append(event(2, received=5))["late"] is True
    first = journal.replay()
    second = journal.replay()
    assert first == second
    assert first["sequence_gaps"] == []
    journal.close()


def test_point_in_time_replay_uses_receive_clock(tmp_path) -> None:
    journal = CanonicalEventJournal(tmp_path / "events.sqlite")
    journal.append(event(1, received=2))
    journal.append(event(2, received=20))
    early = journal.replay(known_at=T0 + timedelta(seconds=10))
    late = journal.replay(known_at=T0 + timedelta(seconds=30))
    assert early["event_count"] == 1
    assert late["event_count"] == 2
    journal.close()


def test_correction_preserves_history_and_supersedes_projection(tmp_path) -> None:
    journal = CanonicalEventJournal(tmp_path / "events.sqlite")
    original = event(1)
    journal.append(original)
    correction = event(2, event_id="correction-2", correction_of=original.event_id)
    journal.append(correction)
    replay = journal.replay()
    assert replay["event_count"] == 2
    assert replay["active_event_count"] == 1
    assert replay["events"][0]["event_id"] == "correction-2"
    journal.close()


def test_unknown_correction_and_excessive_future_drift_fail_closed(tmp_path) -> None:
    journal = CanonicalEventJournal(tmp_path / "events.sqlite")
    with pytest.raises(CanonicalEventError, match="absent"):
        journal.append(event(2, correction_of="missing"))
    with pytest.raises(CanonicalEventError, match="drift"):
        canonical_event(
            event_id="future",
            source="bybit",
            stream="market",
            kind="trade",
            event_time=T0 + timedelta(seconds=20),
            receive_time=T0,
            source_sequence=1,
            payload={},
        )
    journal.close()


def test_receipt_binds_dependencies_and_reconstruction() -> None:
    receipt = execution_journal_receipt(
        events=[event(1), event(2)],
        data_reference_receipt=dependency("DATA-001"),
        market_catalog_receipt=dependency("DATA-002"),
        source_commit=COMMIT,
        dataset_digest=DATASET,
        configuration={"maximum_future_drift_seconds": 5},
    )
    assert verify_receipt(receipt)
    assert receipt.result["reconstructable"] is True
    assert receipt.result["event_count"] == 2
    assert receipt.authority["orders"] is False


def test_receipt_rejects_wrong_dependency() -> None:
    with pytest.raises(CanonicalEventError, match="DATA-001"):
        execution_journal_receipt(
            events=[event(1)],
            data_reference_receipt=dependency("DATA-003"),
            market_catalog_receipt=dependency("DATA-002"),
            source_commit=COMMIT,
            dataset_digest=DATASET,
            configuration={},
        )
