from __future__ import annotations

import pandas as pd

from bt.exec.state.models import RecoveryDisposition, RecoveryPlan
from bt.exec.state.store import ExecutionStateStore


def build_recovery_plan(*, store: ExecutionStateStore, mode: str, restart_policy: str) -> RecoveryPlan:
    latest_session = store.load_latest_session(mode=mode)
    if latest_session is None:
        return RecoveryPlan(
            disposition=RecoveryDisposition.NO_PRIOR_STATE,
            restart_policy=restart_policy,
            message="No prior session exists for mode; starting fresh.",
        )

    if restart_policy == "fresh":
        return RecoveryPlan(
            disposition=RecoveryDisposition.START_FRESH,
            restart_policy=restart_policy,
            message=f"restart_policy=fresh ignores prior run {latest_session.run_id}.",
        )

    if restart_policy == "reconcile_only":
        return RecoveryPlan(
            disposition=RecoveryDisposition.START_FRESH,
            restart_policy=restart_policy,
            message="reconcile_only is not implemented in Phase 2; falling back to conservative fresh start.",
        )

    checkpoint = store.load_latest_checkpoint(latest_session.run_id)
    if checkpoint is None:
        return RecoveryPlan(
            disposition=RecoveryDisposition.INCOMPLETE_PRIOR_STATE,
            restart_policy=restart_policy,
            message=f"Session {latest_session.run_id} exists but has no checkpoint; starting fresh.",
        )

    try:
        if checkpoint.sequence < 0:
            raise ValueError("negative sequence")
        _ = checkpoint.ts.tz_convert("UTC")
    except Exception:
        return RecoveryPlan(
            disposition=RecoveryDisposition.CORRUPT_PRIOR_STATE,
            restart_policy=restart_policy,
            message=f"Session {latest_session.run_id} checkpoint is corrupt; starting fresh.",
        )

    return RecoveryPlan(
        disposition=RecoveryDisposition.RESUME,
        restart_policy=restart_policy,
        checkpoint=checkpoint,
        message=f"Resuming from prior run {latest_session.run_id} checkpoint sequence {checkpoint.sequence}.",
    )


def should_skip_bar(*, checkpoint_bar_ts: pd.Timestamp | None, bar_ts: pd.Timestamp) -> bool:
    if checkpoint_bar_ts is None:
        return False
    return bar_ts <= checkpoint_bar_ts
