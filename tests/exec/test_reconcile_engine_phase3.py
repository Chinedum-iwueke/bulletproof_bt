from __future__ import annotations

import pandas as pd

from bt.core.enums import OrderState, OrderType, PositionState, Side
from bt.core.types import Fill, Order, Position
from bt.exec.adapters.base import BalanceSnapshot
from bt.exec.reconcile import ReconciliationEngine, ReconciliationInputs, ReconciliationPolicy, ReconciliationScope


TS = pd.Timestamp("2026-01-01T00:00:00Z")


def _order(order_id: str, *, qty: float = 1.0) -> Order:
    return Order(id=order_id, ts_submitted=TS, symbol="BTCUSDT", side=Side.BUY, qty=qty, order_type=OrderType.MARKET, limit_price=None, state=OrderState.SUBMITTED)


def _fill(order_id: str, qty: float) -> Fill:
    return Fill(order_id=order_id, ts=TS, symbol="BTCUSDT", side=Side.BUY, qty=qty, price=100.0, fee=0.0, slippage=0.0)


def _position(qty: float) -> Position:
    return Position(symbol="BTCUSDT", state=PositionState.OPEN, side=Side.BUY, qty=qty, avg_entry_price=100.0, realized_pnl=0.0, unrealized_pnl=0.0, mae_price=None, mfe_price=None, opened_ts=TS, closed_ts=None)


def _inputs(**kwargs):
    base = dict(
        run_id="run-1",
        ts=TS,
        local_open_orders=[_order("o1")],
        adapter_open_orders=[_order("o1")],
        adapter_completed_orders=[],
        local_fills=[_fill("o1", 1.0)],
        adapter_fills=[_fill("o1", 1.0)],
        local_positions=[_position(1.0)],
        adapter_positions=[_position(1.0)],
        local_balances=BalanceSnapshot(ts=TS, balances={"USDT": 1000.0}),
        adapter_balances=BalanceSnapshot(ts=TS, balances={"USDT": 1000.0}),
        scope=ReconciliationScope(compare_orders=True, compare_fills=True, compare_positions=True, compare_balances=True),
        material_fill_qty_tolerance=0.0,
        material_position_qty_tolerance=0.0,
        material_balance_tolerance=0.0,
    )
    base.update(kwargs)
    return ReconciliationInputs(**base)


def test_reconcile_no_mismatch() -> None:
    result = ReconciliationEngine().reconcile(inputs=_inputs(), policy=ReconciliationPolicy.LOG_ONLY)
    assert result.mismatches == []


def test_reconcile_mismatch_and_policy_freeze() -> None:
    result = ReconciliationEngine().reconcile(
        inputs=_inputs(adapter_fills=[_fill("o1", 0.5)]),
        policy=ReconciliationPolicy.FREEZE_ON_MATERIAL,
    )
    assert result.material_mismatch_count > 0
    assert result.decision.action.value == "freeze"


def test_reconcile_warn_policy() -> None:
    result = ReconciliationEngine().reconcile(
        inputs=_inputs(adapter_open_orders=[]),
        policy=ReconciliationPolicy.WARN,
    )
    assert result.decision.action.value == "warn"
