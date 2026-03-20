from __future__ import annotations

import pandas as pd

from bt.core.enums import OrderState, OrderType, PositionState, Side
from bt.core.types import Fill, Order, Position
from bt.exec.adapters import (
    AdapterHealth,
    AdapterHealthStatus,
    BalanceSnapshot,
    BrokerAdapter,
    BrokerOrderAmendRequest,
    BrokerOrderCancelRequest,
    BrokerOrderRequest,
    MarketDataAdapter,
)


class DummyMarketDataAdapter:
    def start(self) -> None:
        return None

    def stop(self) -> None:
        return None

    def subscribe_closed_bars(self, symbols: list[str], timeframe: str) -> None:
        _ = (symbols, timeframe)

    def iter_events(self) -> list[object]:
        return []

    def get_health(self) -> AdapterHealth:
        return AdapterHealth(source="market", ts=pd.Timestamp("2026-01-01T00:00:00Z"), status=AdapterHealthStatus.HEALTHY)


class DummyBrokerAdapter:
    def start(self) -> None:
        return None

    def stop(self) -> None:
        return None

    def iter_events(self) -> list[object]:
        return []

    def submit_order(self, request: BrokerOrderRequest) -> str:
        _ = request
        return "req-1"

    def cancel_order(self, request: BrokerOrderCancelRequest) -> None:
        _ = request

    def amend_order(self, request: BrokerOrderAmendRequest) -> None:
        _ = request

    def fetch_open_orders(self) -> list[Order]:
        return [
            Order(
                id="order-1",
                ts_submitted=pd.Timestamp("2026-01-01T00:00:00Z"),
                symbol="BTCUSDT",
                side=Side.BUY,
                qty=1.0,
                order_type=OrderType.MARKET,
                limit_price=None,
                state=OrderState.NEW,
            )
        ]

    def fetch_completed_orders(self, limit: int = 200) -> list[Order]:
        _ = limit
        return []

    def fetch_positions(self) -> list[Position]:
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

    def fetch_balances(self) -> BalanceSnapshot:
        return BalanceSnapshot(ts=pd.Timestamp("2026-01-01T00:00:00Z"), balances={"USDT": 1000.0})

    def fetch_recent_fills_or_executions(self, limit: int = 200) -> list[Fill]:
        _ = limit
        return [
            Fill(
                order_id="order-1",
                ts=pd.Timestamp("2026-01-01T00:00:00Z"),
                symbol="BTCUSDT",
                side=Side.BUY,
                qty=1.0,
                price=100.0,
                fee=0.1,
                slippage=0.0,
            )
        ]

    def get_health(self) -> AdapterHealth:
        return AdapterHealth(source="broker", ts=pd.Timestamp("2026-01-01T00:00:00Z"), status=AdapterHealthStatus.HEALTHY)


def test_adapter_protocol_importability_and_runtime_shape() -> None:
    market = DummyMarketDataAdapter()
    broker = DummyBrokerAdapter()

    assert isinstance(market, MarketDataAdapter)
    assert isinstance(broker, BrokerAdapter)
