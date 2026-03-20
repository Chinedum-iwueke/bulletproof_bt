from __future__ import annotations

import pandas as pd

from bt.core.enums import OrderState, OrderType, PositionState, Side
from bt.core.types import Order, Position
from bt.exec.adapters.base import BalanceSnapshot
from bt.exec.state import OrderLifecycleRecord, ProcessedEventRecord, RuntimeCheckpoint, RuntimeSessionState
from bt.exec.state.sqlite_store import SQLiteExecutionStateStore


def test_sqlite_store_bootstrap_and_checkpoint_roundtrip(tmp_path) -> None:
    db = SQLiteExecutionStateStore(path=str(tmp_path / "state.sqlite"))
    run_id = "run-1"
    now = pd.Timestamp("2026-01-01T00:00:00Z")
    order = Order(
        id="sim-1",
        ts_submitted=now,
        symbol="BTCUSDT",
        side=Side.BUY,
        qty=1.0,
        order_type=OrderType.MARKET,
        limit_price=None,
        state=OrderState.SUBMITTED,
        metadata={},
    )
    position = Position(
        symbol="BTCUSDT",
        state=PositionState.OPEN,
        side=Side.BUY,
        qty=1.0,
        avg_entry_price=100.0,
        realized_pnl=0.0,
        unrealized_pnl=0.0,
        mae_price=100.0,
        mfe_price=100.0,
        opened_ts=now,
        closed_ts=None,
    )
    session = RuntimeSessionState(
        run_id=run_id,
        mode="paper_simulated",
        restart_policy="resume",
        status="running",
        started_at=now,
        updated_at=now,
    )
    db.record_session_liveness(session)
    db.persist_order_lifecycle_event(
        OrderLifecycleRecord(ts=now, run_id=run_id, order_id=order.id, event_type="acknowledged", payload={"order": db._order_to_dict(order)})
    )
    db.persist_processed_event(ProcessedEventRecord(ts=now, run_id=run_id, dedupe_key="fill-1", source="broker"))
    checkpoint = RuntimeCheckpoint(
        ts=now,
        run_id=run_id,
        sequence=1,
        last_bar_ts=now,
        next_client_order_seq=4,
        open_orders=[order],
        positions=[position],
        balances=BalanceSnapshot(ts=now, balances={"USDT": 1000.0}),
    )
    db.persist_checkpoint(checkpoint)
    db.persist_positions_snapshot(run_id=run_id, ts=now, positions=[position])
    db.persist_balance_snapshot(run_id=run_id, snapshot=BalanceSnapshot(ts=now, balances={"USDT": 1000.0}))

    loaded = db.load_latest_checkpoint(run_id)
    assert loaded is not None
    assert loaded.next_client_order_seq == 4
    assert db.has_processed_event(run_id=run_id, dedupe_key="fill-1")
    assert len(db.query_open_local_orders(run_id=run_id)) == 1
    assert len(db.query_latest_local_positions_snapshot(run_id=run_id)) == 1
    assert db.query_latest_balance_snapshot(run_id=run_id) is not None
