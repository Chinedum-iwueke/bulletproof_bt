from __future__ import annotations

import copy

import pandas as pd

from bt.core.enums import OrderState, OrderType, Side
from bt.core.types import Bar, Fill, Order, Position
from bt.exec.adapters.base import AdapterHealth, AdapterHealthStatus, BalanceSnapshot, BrokerOrderAmendRequest, BrokerOrderCancelRequest, BrokerOrderRequest
from bt.exec.events.broker_events import BrokerEvent, BrokerOrderAcknowledgedEvent, BrokerOrderFilledEvent
from bt.execution.execution_model import ExecutionModel


class SimulatedBrokerAdapter:
    def __init__(self, *, execution_model: ExecutionModel) -> None:
        self._execution_model = execution_model
        self._open_orders: list[Order] = []
        self._completed_orders: list[Order] = []
        self._fill_history: list[Fill] = []
        self._events: list[BrokerEvent] = []
        self._started = False
        self._seq = 0
        self._balance_snapshot = BalanceSnapshot(ts=pd.Timestamp.now(tz="UTC"), balances={})

    def start(self) -> None:
        self._started = True

    def stop(self) -> None:
        self._started = False

    def iter_events(self) -> list[BrokerEvent]:
        events = list(self._events)
        self._events.clear()
        return events

    def submit_order(self, request: BrokerOrderRequest) -> str:
        self._seq += 1
        order_id = f"sim-{self._seq}"
        now = pd.Timestamp.now(tz="UTC")
        order = Order(
            id=order_id,
            ts_submitted=now,
            symbol=request.symbol,
            side=Side(request.side),
            qty=float(request.qty),
            order_type=OrderType(request.order_type),
            limit_price=request.limit_price,
            state=OrderState.SUBMITTED,
            metadata={**request.metadata, "client_order_id": request.client_order_id},
        )
        self._open_orders.append(order)
        self._events.append(BrokerOrderAcknowledgedEvent(ts=now, broker_event_id=f"ack-{order_id}", order=order))
        return order_id

    def cancel_order(self, request: BrokerOrderCancelRequest) -> None:
        _ = request
        raise NotImplementedError("cancel_order is intentionally unsupported in simulated adapter")

    def amend_order(self, request: BrokerOrderAmendRequest) -> None:
        _ = request
        raise NotImplementedError("amend_order is intentionally unsupported in simulated adapter")

    def process_bar(self, *, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar]) -> list[BrokerEvent]:
        updated, fills = self._execution_model.process(ts=ts, bars_by_symbol=bars_by_symbol, open_orders=self._open_orders)
        self._open_orders = [o for o in updated if o.state not in {OrderState.FILLED, OrderState.CANCELLED, OrderState.REJECTED}]
        self._completed_orders.extend([o for o in updated if o.state in {OrderState.FILLED, OrderState.CANCELLED, OrderState.REJECTED}])
        generated: list[BrokerEvent] = []
        for fill in fills:
            self._fill_history.append(fill)
            generated.append(BrokerOrderFilledEvent(ts=fill.ts, broker_event_id=f"fill-{fill.order_id}-{fill.ts.value}", fill=fill))
        self._events.extend(generated)
        return generated

    def fetch_open_orders(self) -> list[Order]:
        return list(self._open_orders)

    def fetch_completed_orders(self, limit: int = 200) -> list[Order]:
        if limit <= 0:
            return []
        return list(self._completed_orders[-limit:])

    def fetch_positions(self) -> list[Position]:
        return []

    def fetch_balances(self) -> BalanceSnapshot:
        return copy.deepcopy(self._balance_snapshot)

    def fetch_recent_fills_or_executions(self, limit: int = 200) -> list[Fill]:
        if limit <= 0:
            return []
        return list(self._fill_history[-limit:])

    def get_health(self) -> AdapterHealth:
        return AdapterHealth(source="simulated", ts=pd.Timestamp.now(tz="UTC"), status=AdapterHealthStatus.HEALTHY if self._started else AdapterHealthStatus.DEGRADED)
