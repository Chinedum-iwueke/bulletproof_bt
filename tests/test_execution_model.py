from __future__ import annotations

import pandas as pd

from bt.core.enums import OrderState, OrderType, Side
from bt.core.types import Bar, Order
from bt.execution.execution_model import ExecutionModel
from bt.execution.fees import FeeModel
from bt.execution.slippage import SlippageModel


def _bar(*, ts: pd.Timestamp, symbol: str) -> Bar:
    return Bar(
        ts=ts,
        symbol=symbol,
        open=100,
        high=110,
        low=90,
        close=100,
        volume=1000,
    )


def _order(*, ts: pd.Timestamp, symbol: str, order_type: OrderType) -> Order:
    return Order(
        id="order-1",
        ts_submitted=ts,
        symbol=symbol,
        side=Side.BUY,
        qty=1.0,
        order_type=order_type,
        limit_price=None,
        state=OrderState.NEW,
        metadata={},
    )


def test_fee_and_slippage_applied_with_delay_and_worst_case_fill() -> None:
    fee_model = FeeModel(maker_fee_bps=0.0, taker_fee_bps=10.0)
    slippage_model = SlippageModel(k=1.0)
    model = ExecutionModel(
        fee_model=fee_model,
        slippage_model=slippage_model,
        delay_bars=1,
    )

    t0 = pd.Timestamp("2024-01-01T00:00:00Z")
    t1 = pd.Timestamp("2024-01-01T01:00:00Z")
    bar = _bar(ts=t0, symbol="BTC")
    order = _order(ts=t0, symbol="BTC", order_type=OrderType.MARKET)

    updated_orders, fills = model.process(ts=t0, bars_by_symbol={"BTC": bar}, open_orders=[order])
    assert len(fills) == 0
    assert updated_orders[0].state == OrderState.SUBMITTED

    updated_orders, fills = model.process(ts=t1, bars_by_symbol={"BTC": bar}, open_orders=updated_orders)
    assert len(fills) == 1
    fill = fills[0]
    assert fill.price >= 110
    assert fill.fee > 0
    assert fill.slippage >= 0
    assert updated_orders[0].state == OrderState.FILLED


def test_missing_bar_preserves_delay_and_prevents_fill() -> None:
    fee_model = FeeModel(maker_fee_bps=0.0, taker_fee_bps=1.0)
    slippage_model = SlippageModel(k=1.0)
    model = ExecutionModel(
        fee_model=fee_model,
        slippage_model=slippage_model,
        delay_bars=1,
    )

    t0 = pd.Timestamp("2024-01-02T00:00:00Z")
    t1 = pd.Timestamp("2024-01-02T01:00:00Z")
    t2 = pd.Timestamp("2024-01-02T02:00:00Z")
    order = _order(ts=t0, symbol="ETH", order_type=OrderType.MARKET)

    updated_orders, fills = model.process(ts=t0, bars_by_symbol={}, open_orders=[order])
    assert len(fills) == 0
    assert updated_orders[0].metadata["delay_remaining"] == 1

    bar = _bar(ts=t1, symbol="ETH")
    updated_orders, fills = model.process(ts=t1, bars_by_symbol={"ETH": bar}, open_orders=updated_orders)
    assert len(fills) == 0
    assert updated_orders[0].metadata["delay_remaining"] == 0

    updated_orders, fills = model.process(ts=t2, bars_by_symbol={"ETH": bar}, open_orders=updated_orders)
    assert len(fills) == 1
    assert updated_orders[0].state == OrderState.FILLED


def test_limit_order_not_supported() -> None:
    fee_model = FeeModel(maker_fee_bps=0.0, taker_fee_bps=1.0)
    slippage_model = SlippageModel(k=1.0)
    model = ExecutionModel(
        fee_model=fee_model,
        slippage_model=slippage_model,
        delay_bars=0,
    )

    t0 = pd.Timestamp("2024-01-03T00:00:00Z")
    order = _order(ts=t0, symbol="BTC", order_type=OrderType.LIMIT)

    try:
        model.process(ts=t0, bars_by_symbol={"BTC": _bar(ts=t0, symbol="BTC")}, open_orders=[order])
    except NotImplementedError:
        assert True
    else:
        raise AssertionError("Expected NotImplementedError for limit orders.")


def test_entry_fill_qty_is_clipped_to_actual_fill_stop_risk_budget() -> None:
    model = ExecutionModel(
        fee_model=FeeModel(maker_fee_bps=0.0, taker_fee_bps=0.0),
        slippage_model=SlippageModel(k=0.0),
        delay_bars=0,
    )
    ts = pd.Timestamp("2024-01-04T00:00:00Z")
    order = Order(
        id="order-risk",
        ts_submitted=ts,
        symbol="BTC",
        side=Side.BUY,
        qty=100.0,
        order_type=OrderType.MARKET,
        limit_price=None,
        state=OrderState.NEW,
        metadata={
            "risk_budget": 500.0,
            "stop_price": 100.0,
            "risk_value_per_price_unit": 1.0,
        },
    )
    bar = Bar(ts=ts, symbol="BTC", open=105.0, high=110.0, low=104.0, close=106.0, volume=1000.0)

    updated_orders, fills = model.process(ts=ts, bars_by_symbol={"BTC": bar}, open_orders=[order])

    assert len(fills) == 1
    fill = fills[0]
    assert fill.price == 110.0
    assert fill.qty == 50.0
    assert fill.qty * abs(fill.price - 100.0) <= 500.0
    assert fill.metadata["risk_fill_qty_clipped"] is True
    assert updated_orders[0].qty == 50.0


def test_close_only_fill_is_not_risk_clipped() -> None:
    model = ExecutionModel(
        fee_model=FeeModel(maker_fee_bps=0.0, taker_fee_bps=0.0),
        slippage_model=SlippageModel(k=0.0),
        delay_bars=0,
    )
    ts = pd.Timestamp("2024-01-05T00:00:00Z")
    order = Order(
        id="order-close",
        ts_submitted=ts,
        symbol="BTC",
        side=Side.SELL,
        qty=100.0,
        order_type=OrderType.MARKET,
        limit_price=None,
        state=OrderState.NEW,
        metadata={
            "close_only": True,
            "risk_budget": 500.0,
            "stop_price": 100.0,
        },
    )
    bar = Bar(ts=ts, symbol="BTC", open=105.0, high=110.0, low=90.0, close=106.0, volume=1000.0)

    _, fills = model.process(ts=ts, bars_by_symbol={"BTC": bar}, open_orders=[order])

    assert len(fills) == 1
    assert fills[0].qty == 100.0
    assert "risk_fill_qty_clipped" not in fills[0].metadata
