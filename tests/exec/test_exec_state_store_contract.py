from __future__ import annotations

import pandas as pd

from bt.core.enums import OrderState, OrderType, PositionState, Side
from bt.core.types import Fill, Order, Position
from bt.exec.adapters import BalanceSnapshot
from bt.exec.state import ExecutionCheckpoint, ExecutionStateStore, OrderLifecycleRecord, ProcessedEventRecord, RuntimeSessionState


class InMemoryContractStore:
    def __init__(self) -> None:
        self._checkpoint: ExecutionCheckpoint | None = None
        self._dedupe: set[str] = set()

    def persist_order_lifecycle_event(self, record: OrderLifecycleRecord) -> None:
        _ = record

    def persist_broker_event(self, record: object) -> None:
        _ = record

    def persist_processed_event(self, record: ProcessedEventRecord) -> None:
        self._dedupe.add(record.dedupe_key)

    def mark_broker_event_processed(self, dedupe_key: str, ts: pd.Timestamp) -> None:
        _ = ts
        self._dedupe.add(dedupe_key)

    def has_processed_event(self, *, run_id: str, dedupe_key: str) -> bool:
        _ = run_id
        return dedupe_key in self._dedupe

    def has_processed_broker_event(self, dedupe_key: str) -> bool:
        return dedupe_key in self._dedupe

    def persist_checkpoint(self, checkpoint: ExecutionCheckpoint) -> None:
        self._checkpoint = checkpoint

    def load_latest_checkpoint(self, run_id: str) -> ExecutionCheckpoint | None:
        _ = run_id
        return self._checkpoint

    def query_local_fill_history(self, *, run_id: str, limit: int = 200) -> list[Fill]:
        _ = run_id, limit
        return []

    def query_open_local_orders(self, *, run_id: str) -> list[Order]:
        _ = run_id
        return [
            Order(
                id="order-1",
                ts_submitted=pd.Timestamp("2026-01-01T00:00:00Z"),
                symbol="BTCUSDT",
                side=Side.BUY,
                qty=1.0,
                order_type=OrderType.LIMIT,
                limit_price=100.0,
                state=OrderState.NEW,
            )
        ]

    def query_latest_local_positions_snapshot(self, *, run_id: str) -> list[Position]:
        _ = run_id
        return [
            Position(
                symbol="BTCUSDT",
                state=PositionState.OPEN,
                side=Side.BUY,
                qty=1.0,
                avg_entry_price=100.0,
                realized_pnl=0.0,
                unrealized_pnl=0.0,
                mae_price=None,
                mfe_price=None,
                opened_ts=pd.Timestamp("2026-01-01T00:00:00Z"),
                closed_ts=None,
            )
        ]

    def persist_positions_snapshot(self, *, run_id: str, ts: pd.Timestamp, positions: list[Position]) -> None:
        _ = run_id, ts, positions

    def query_latest_balance_snapshot(self, *, run_id: str) -> BalanceSnapshot | None:
        _ = run_id
        return BalanceSnapshot(ts=pd.Timestamp("2026-01-01T00:00:00Z"), balances={"USDT": 1000.0})

    def persist_balance_snapshot(self, *, run_id: str, snapshot: BalanceSnapshot) -> None:
        _ = run_id, snapshot

    def record_session_liveness(self, session: RuntimeSessionState) -> None:
        _ = session

    def mark_session_final_status(self, *, run_id: str, status: str, ts: pd.Timestamp, error: str | None = None) -> None:
        _ = run_id, status, ts, error

    def load_latest_session(self, *, mode: str) -> RuntimeSessionState | None:
        _ = mode
        return None


def test_state_store_protocol_importability_and_shape() -> None:
    store = InMemoryContractStore()
    assert isinstance(store, ExecutionStateStore)

    checkpoint = ExecutionCheckpoint(
        ts=pd.Timestamp("2026-01-01T00:00:00Z"),
        run_id="run-1",
        sequence=1,
        last_bar_ts=pd.Timestamp("2026-01-01T00:00:00Z"),
        next_client_order_seq=1,
    )
    store.persist_checkpoint(checkpoint)
    assert store.load_latest_checkpoint("run-1") == checkpoint
