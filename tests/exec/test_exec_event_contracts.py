from __future__ import annotations

import pandas as pd
import pytest

from bt.core.enums import OrderState, OrderType, PositionState, Side
from bt.core.types import Fill, Order, Position
from bt.exec.adapters import BalanceSnapshot
from bt.exec.events.broker_events import (
    BrokerConnectionStatus,
    BrokerConnectionStatusEvent,
    BrokerOrderAcknowledgedEvent,
    BrokerOrderFilledEvent,
    BrokerOrderPartiallyFilledEvent,
)
from bt.exec.events.runtime_events import RuntimeHeartbeatEvent, RuntimeLifecycleEvent, RuntimeLifecycleKind


UTC_NOW = pd.Timestamp("2026-01-01T00:00:00Z")


def _sample_order() -> Order:
    return Order(
        id="order-1",
        ts_submitted=UTC_NOW,
        symbol="BTCUSDT",
        side=Side.BUY,
        qty=1.0,
        order_type=OrderType.MARKET,
        limit_price=None,
        state=OrderState.NEW,
    )


def _sample_fill() -> Fill:
    return Fill(
        order_id="order-1",
        ts=UTC_NOW,
        symbol="BTCUSDT",
        side=Side.BUY,
        qty=0.5,
        price=100.0,
        fee=0.1,
        slippage=0.0,
    )


def test_runtime_and_broker_events_construct_with_utc_timestamps() -> None:
    order = _sample_order()
    fill = _sample_fill()

    heartbeat = RuntimeHeartbeatEvent(ts=UTC_NOW, sequence=1)
    lifecycle = RuntimeLifecycleEvent(ts=UTC_NOW, kind=RuntimeLifecycleKind.STARTUP)
    ack = BrokerOrderAcknowledgedEvent(ts=UTC_NOW, broker_event_id="evt-1", order=order)
    partial = BrokerOrderPartiallyFilledEvent(
        ts=UTC_NOW,
        broker_event_id="evt-2",
        fill=fill,
        leaves_qty=0.5,
    )
    filled = BrokerOrderFilledEvent(ts=UTC_NOW, broker_event_id="evt-3", fill=fill)
    status = BrokerConnectionStatusEvent(
        ts=UTC_NOW,
        broker_event_id="evt-4",
        status=BrokerConnectionStatus.CONNECTED,
    )

    assert heartbeat.sequence == 1
    assert lifecycle.kind == RuntimeLifecycleKind.STARTUP
    assert ack.order.id == "order-1"
    assert partial.leaves_qty == pytest.approx(0.5)
    assert filled.fill.order_id == "order-1"
    assert status.status == BrokerConnectionStatus.CONNECTED


def test_balance_snapshot_requires_utc() -> None:
    BalanceSnapshot(ts=UTC_NOW, balances={"USDT": 1000.0})
    with pytest.raises(ValueError, match="timezone-aware UTC"):
        BalanceSnapshot(ts=pd.Timestamp("2026-01-01"), balances={"USDT": 1000.0})


def test_position_snapshot_compatible_with_core_position_type() -> None:
    position = Position(
        symbol="BTCUSDT",
        state=PositionState.OPEN,
        side=Side.BUY,
        qty=1.0,
        avg_entry_price=100.0,
        realized_pnl=0.0,
        unrealized_pnl=0.0,
        mae_price=None,
        mfe_price=None,
        opened_ts=UTC_NOW,
        closed_ts=None,
    )
    assert position.symbol == "BTCUSDT"
