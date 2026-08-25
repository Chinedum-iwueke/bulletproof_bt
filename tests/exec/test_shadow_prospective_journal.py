from __future__ import annotations

import json
from pathlib import Path

import pytest

from bt.exec.shadow import JournalError, ProspectiveJournal, replay_journal


def bindings() -> dict[str, str]:
    return {
        "candidate_digest": "1" * 64,
        "dataset_digest": "2" * 64,
        "strategy_digest": "3" * 64,
        "cost_model_digest": "4" * 64,
        "source_commit": "5" * 40,
    }


def test_journal_is_digest_bound_idempotent_and_replayable(tmp_path: Path) -> None:
    path = tmp_path / "prospective_journal.jsonl"
    journal = ProspectiveJournal(path, run_id="shadow-1", bindings=bindings())
    assert journal.append(
        "decision", {"signal": "long"}, event_id="decision:1",
        observed_at="2026-08-25T10:00:00Z",
    )
    assert not journal.append(
        "decision", {"signal": "long"}, event_id="decision:1",
        observed_at="2026-08-25T10:00:00Z",
    )
    journal.append(
        "fill",
        {"fee_cost": 1.25, "slippage_cost": 0.5, "spread_cost": 0.25},
        event_id="fill:1",
        observed_at="2026-08-25T10:00:01Z",
    )
    report = journal.seal()

    assert report["success"] is True
    assert report["event_count"] == 4
    assert report["event_counts"] == {
        "session_started": 1, "decision": 1, "fill": 1, "session_sealed": 1,
    }
    assert report["cost_totals"] == {
        "fee_cost": 1.25, "slippage_cost": 0.5, "spread_cost": 0.25,
    }
    assert report["capital_or_order_authority"] is False


def test_unsealed_journal_resumes_after_disconnect(tmp_path: Path) -> None:
    path = tmp_path / "journal.jsonl"
    first = ProspectiveJournal(path, run_id="shadow-restart", bindings=bindings())
    first.append("heartbeat", {"healthy": True}, event_id="heartbeat:1")
    first.close()

    resumed = ProspectiveJournal(path, run_id="shadow-restart", bindings=bindings())
    resumed.append("reconciliation", {"parity": True}, event_id="reconcile:1")
    report = resumed.seal()
    assert report["success"]
    assert report["event_count"] == 4


def test_replay_rejects_tamper_out_of_order_and_naive_clock(tmp_path: Path) -> None:
    path = tmp_path / "journal.jsonl"
    journal = ProspectiveJournal(path, run_id="shadow-invalid", bindings=bindings())
    with pytest.raises(JournalError, match="timezone-aware"):
        journal.append("heartbeat", {}, observed_at="2026-08-25T10:00:00")
    journal.append("heartbeat", {}, observed_at="2026-08-25T10:00:00Z")
    journal.close()

    records = [json.loads(line) for line in path.read_text().splitlines()]
    records[1]["sequence"] = 9
    path.write_text("\n".join(json.dumps(item) for item in records) + "\n")
    with pytest.raises(JournalError, match="record digest mismatch|out-of-order"):
        replay_journal(path)


def test_bindings_and_seal_are_immutable(tmp_path: Path) -> None:
    path = tmp_path / "journal.jsonl"
    journal = ProspectiveJournal(path, run_id="shadow-sealed", bindings=bindings())
    journal.seal()
    changed = bindings() | {"dataset_digest": "9" * 64}
    with pytest.raises(JournalError, match="bindings"):
        replay_journal(path, expected_bindings=changed)
    with pytest.raises(JournalError, match="sealed"):
        ProspectiveJournal(path, run_id="shadow-sealed", bindings=bindings())
