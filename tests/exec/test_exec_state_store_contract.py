from __future__ import annotations

import pandas as pd

from bt.core.enums import OrderState, OrderType, PositionState, Side
from bt.core.types import Order, Position
from bt.exec.adapters import BalanceSnapshot
from bt.exec.state import BrokerEventRecord, ExecutionCheckpoint, ExecutionStateStore, OrderLifecycleRecord


class InMemoryContractStore:
    def __init__(self) -> None:
        self._checkpoint: ExecutionCheckpoint | None = None
        self._dedupe: set[str] = set()

    def persist_order_lifecycle_event(self, record: OrderLifecycleRecord) -> None:
        _ = record

    def persist_broker_event(self, record: BrokerEventRecord) -> None:
        _ = record

    def persist_checkpoint(self, checkpoint: ExecutionCheckpoint) -> None:
        self._checkpoint = checkpoint

    def load_latest_checkpoint(self, run_id: str) -> ExecutionCheckpoint | None:
        _ = run_id
        return self._checkpoint

    def mark_broker_event_processed(self, dedupe_key: str, ts: pd.Timestamp) -> None:
        _ = ts
        self._dedupe.add(dedupe_key)

    def has_processed_broker_event(self, dedupe_key: str) -> bool:
        return dedupe_key in self._dedupe

    def query_open_local_orders(self) -> list[Order]:
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

    def query_latest_local_positions_snapshot(self) -> list[Position]:
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

    def query_latest_balance_snapshot(self) -> BalanceSnapshot | None:
        return BalanceSnapshot(ts=pd.Timestamp("2026-01-01T00:00:00Z"), balances={"USDT": 1000.0})


def test_state_store_protocol_importability_and_shape() -> None:
    store = InMemoryContractStore()
    assert isinstance(store, ExecutionStateStore)

    checkpoint = ExecutionCheckpoint(
        ts=pd.Timestamp("2026-01-01T00:00:00Z"),
        run_id="run-1",
        sequence=1,
    )
    store.persist_checkpoint(checkpoint)
    assert store.load_latest_checkpoint("run-1") == checkpoint
