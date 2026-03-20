from __future__ import annotations

import pandas as pd

from bt.exec.state import RuntimeCheckpoint, RuntimeSessionState
from bt.exec.state.recovery import build_recovery_plan
from bt.exec.state.sqlite_store import SQLiteExecutionStateStore


def test_recovery_no_prior_session(tmp_path) -> None:
    store = SQLiteExecutionStateStore(path=str(tmp_path / "state.sqlite"))
    plan = build_recovery_plan(store=store, mode="shadow", restart_policy="resume")
    assert plan.disposition.value == "no_prior_state"


def test_recovery_resume_with_checkpoint(tmp_path) -> None:
    store = SQLiteExecutionStateStore(path=str(tmp_path / "state.sqlite"))
    now = pd.Timestamp("2026-01-01T00:00:00Z")
    store.record_session_liveness(
        RuntimeSessionState(
            run_id="old-run",
            mode="paper_simulated",
            restart_policy="resume",
            status="stopped",
            started_at=now,
            updated_at=now,
            ended_at=now,
        )
    )
    store.persist_checkpoint(
        RuntimeCheckpoint(
            ts=now,
            run_id="old-run",
            sequence=5,
            last_bar_ts=now,
            next_client_order_seq=9,
        )
    )
    plan = build_recovery_plan(store=store, mode="paper_simulated", restart_policy="resume")
    assert plan.disposition.value == "resume"
    assert plan.checkpoint is not None
    assert plan.checkpoint.sequence == 5


def test_recovery_incomplete_or_policy_degrade(tmp_path) -> None:
    store = SQLiteExecutionStateStore(path=str(tmp_path / "state.sqlite"))
    now = pd.Timestamp("2026-01-01T00:00:00Z")
    store.record_session_liveness(
        RuntimeSessionState(
            run_id="old-run",
            mode="shadow",
            restart_policy="resume",
            status="failed",
            started_at=now,
            updated_at=now,
        )
    )
    incomplete = build_recovery_plan(store=store, mode="shadow", restart_policy="resume")
    assert incomplete.disposition.value == "incomplete_prior_state"

    reconcile = build_recovery_plan(store=store, mode="shadow", restart_policy="reconcile_only")
    assert reconcile.disposition.value == "start_fresh"
