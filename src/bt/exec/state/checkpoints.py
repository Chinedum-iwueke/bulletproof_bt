from __future__ import annotations

import pandas as pd

from bt.core.types import Order, Position
from bt.exec.adapters.base import BalanceSnapshot
from bt.exec.state.models import RuntimeCheckpoint
from bt.exec.state.store import ExecutionStateStore




class CheckpointCadence:
    def __init__(self, *, interval_seconds: int) -> None:
        self._interval_seconds = max(1, int(interval_seconds))
        self._last_ts: pd.Timestamp | None = None

    def should_checkpoint(self, ts: pd.Timestamp) -> bool:
        if self._last_ts is None:
            self._last_ts = ts
            return True
        if (ts - self._last_ts).total_seconds() >= float(self._interval_seconds):
            self._last_ts = ts
            return True
        return False

def build_runtime_checkpoint(
    *,
    run_id: str,
    sequence: int,
    last_bar_ts: pd.Timestamp | None,
    next_client_order_seq: int,
    open_orders: list[Order],
    positions: list[Position],
    balances: BalanceSnapshot | None,
    mode: str,
) -> RuntimeCheckpoint:
    return RuntimeCheckpoint(
        ts=pd.Timestamp.now(tz="UTC"),
        run_id=run_id,
        sequence=sequence,
        last_bar_ts=last_bar_ts,
        next_client_order_seq=next_client_order_seq,
        open_orders=open_orders,
        positions=positions,
        balances=balances,
        metadata={"mode": mode},
    )


def save_checkpoint(*, store: ExecutionStateStore, checkpoint: RuntimeCheckpoint) -> None:
    store.persist_checkpoint(checkpoint)
    store.persist_positions_snapshot(run_id=checkpoint.run_id, ts=checkpoint.ts, positions=checkpoint.positions)
    if checkpoint.balances is not None:
        store.persist_balance_snapshot(run_id=checkpoint.run_id, snapshot=checkpoint.balances)


def load_latest_checkpoint(*, store: ExecutionStateStore, run_id: str) -> RuntimeCheckpoint | None:
    return store.load_latest_checkpoint(run_id)
