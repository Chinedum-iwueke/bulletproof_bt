from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from bt.core.enums import OrderState, Side
from bt.core.types import Fill, Order, OrderIntent
from bt.exec.adapters.base import BrokerAdapter, BrokerOrderRequest
from bt.exec.adapters.simulated import SimulatedBrokerAdapter
from bt.exec.events.broker_events import (
    BrokerOrderAcknowledgedEvent,
    BrokerOrderCancelledEvent,
    BrokerOrderFilledEvent,
    BrokerOrderPartiallyFilledEvent,
)
from bt.exec.lifecycle import (
    OrderLifecycleState,
    accumulate_fills,
    build_client_order_id,
    fill_identity_key,
    lifecycle_dedupe_key,
    should_process_once,
    validate_transition,
)
from bt.exec.logging.schemas import FillArtifactRecord, OrderArtifactRecord
from bt.exec.services.portfolio_runner import PortfolioRunner
from bt.exec.state import ExecutionStateStore, OrderLifecycleRecord, ProcessedEventRecord
from bt.institutional.realtime_risk import require_realtime_risk_authorization


@dataclass(frozen=True)
class SubmitResult:
    request_id: str
    order_id: str


class ExecutionRouter:
    def __init__(
        self,
        *,
        run_id: str,
        mode: str,
        adapter: BrokerAdapter,
        portfolio_runner: PortfolioRunner,
        store: ExecutionStateStore | None,
        save_processed_event_ids: bool,
    ) -> None:
        self._run_id = run_id
        self._mode = mode
        self._adapter = adapter
        self._portfolio_runner = portfolio_runner
        self._store = store
        self._save_processed_event_ids = save_processed_event_ids
        self._order_states: dict[str, OrderLifecycleState] = {}
        self._order_requested_qty: dict[str, float] = {}
        self._fills_by_order: dict[str, list[Fill]] = {}
        self._local_open_orders: dict[str, Order] = {}
        self._local_fills: list[Fill] = []

    def submit_order(self, *, order_seq: int, intent: OrderIntent, ts: pd.Timestamp) -> SubmitResult:
        if self._mode in {"demo_broker", "live_broker"}:
            metadata = dict(intent.metadata)
            require_realtime_risk_authorization(
                receipt=metadata.get("risk005_receipt", {}),
                order={
                    "symbol": intent.symbol,
                    "side": intent.side.value,
                    "quantity": abs(float(intent.qty)),
                    "reference_price": metadata.get("risk_reference_price", intent.limit_price),
                    "reduce_only": metadata.get("reduce_only") is True,
                },
                expected_state_version=metadata.get("risk_state_version"),
                now=ts.to_pydatetime(),
                maximum_age_seconds=metadata.get("risk_decision_max_age_seconds", 1.0),
            )
        request = BrokerOrderRequest(
            client_order_id=build_client_order_id(order_seq=order_seq),
            symbol=intent.symbol,
            side=intent.side.value,
            qty=abs(float(intent.qty)),
            order_type=intent.order_type.value,
            limit_price=intent.limit_price,
            reduce_only=bool(intent.metadata.get("reduce_only", False)),
            metadata=dict(intent.metadata),
        )
        request_id = self._adapter.submit_order(request)
        order_id = request_id
        self._record_lifecycle(
            ts=ts,
            order_id=order_id,
            next_state=OrderLifecycleState.PENDING_SUBMIT,
            payload={"request_id": request_id, "client_order_id": request.client_order_id},
        )
        self._record_lifecycle(
            ts=ts,
            order_id=order_id,
            next_state=OrderLifecycleState.SUBMITTED,
            payload={"request_id": request_id, "client_order_id": request.client_order_id},
        )
        for evt in self._adapter.iter_events():
            if isinstance(evt, BrokerOrderAcknowledgedEvent):
                order_id = evt.order.id
                self._record_lifecycle(
                    ts=evt.ts,
                    order_id=order_id,
                    next_state=OrderLifecycleState.ACKNOWLEDGED,
                    payload={"order": _order_dict(evt.order), "request_id": request_id},
                )
                self._local_open_orders[order_id] = evt.order
                self._order_requested_qty[order_id] = evt.order.qty
            elif isinstance(evt, (BrokerOrderPartiallyFilledEvent, BrokerOrderFilledEvent, BrokerOrderCancelledEvent)):
                self._consume_broker_event(evt)

        return SubmitResult(request_id=request_id, order_id=order_id)

    def process_bar(self, *, ts: pd.Timestamp, bars_by_symbol: dict[str, object]) -> list[FillArtifactRecord]:
        if self._mode != "paper_simulated":
            return []
        if not isinstance(self._adapter, SimulatedBrokerAdapter):
            return []
        self._adapter.process_bar(ts=ts, bars_by_symbol=bars_by_symbol)
        fills: list[Fill] = []
        artifacts: list[FillArtifactRecord] = []
        for evt in self._adapter.iter_events():
            if not isinstance(evt, BrokerOrderFilledEvent):
                continue
            fill_key = fill_identity_key(evt.fill)
            if self._save_processed_event_ids and self._store is not None:
                if not should_process_once(store=self._store, run_id=self._run_id, dedupe_key=fill_key):
                    continue
                self._store.persist_processed_event(ProcessedEventRecord(ts=evt.ts, run_id=self._run_id, dedupe_key=fill_key, source="fill"))
            fills.append(evt.fill)
            self._local_fills.append(evt.fill)
            artifacts.append(FillArtifactRecord.from_fill(evt.fill))
            by_order = self._fills_by_order.setdefault(evt.fill.order_id, [])
            by_order.append(evt.fill)
            requested = self._order_requested_qty.get(evt.fill.order_id, evt.fill.qty)
            agg = accumulate_fills(order_id=evt.fill.order_id, requested_qty=requested, fills=by_order)
            next_state = OrderLifecycleState.FILLED if agg.is_terminal else OrderLifecycleState.PARTIALLY_FILLED
            self._record_lifecycle(ts=evt.ts, order_id=evt.fill.order_id, next_state=next_state, payload={"fill": _fill_dict(evt.fill)})
            if next_state == OrderLifecycleState.FILLED:
                self._local_open_orders.pop(evt.fill.order_id, None)

        if fills:
            self._portfolio_runner.apply_fills(fills)
        return artifacts

    def process_broker_events(self) -> list[FillArtifactRecord]:
        artifacts: list[FillArtifactRecord] = []
        for evt in self._adapter.iter_events():
            maybe = self._consume_broker_event(evt)
            if maybe is not None:
                artifacts.append(maybe)
        return artifacts

    def current_open_orders(self) -> list[Order]:
        return list(self._local_open_orders.values())

    def local_fills(self) -> list[Fill]:
        return list(self._local_fills)

    def _record_lifecycle(self, *, ts: pd.Timestamp, order_id: str, next_state: OrderLifecycleState, payload: dict[str, object]) -> None:
        previous = self._order_states.get(order_id)
        transition = validate_transition(previous, next_state)
        if not transition.valid:
            return
        self._order_states[order_id] = next_state
        if self._store is None:
            return
        dedupe_key = lifecycle_dedupe_key(order_id=order_id, event_type=next_state.value)
        if self._save_processed_event_ids and not should_process_once(store=self._store, run_id=self._run_id, dedupe_key=dedupe_key):
            return
        if self._save_processed_event_ids:
            self._store.persist_processed_event(ProcessedEventRecord(ts=ts, run_id=self._run_id, dedupe_key=dedupe_key, source="lifecycle"))
        self._store.persist_order_lifecycle_event(
            OrderLifecycleRecord(ts=ts, run_id=self._run_id, order_id=order_id, event_type=next_state.value, payload=payload)
        )

    def _consume_broker_event(self, evt: object) -> FillArtifactRecord | None:
        if isinstance(evt, BrokerOrderAcknowledgedEvent):
            self._local_open_orders[evt.order.id] = evt.order
            self._order_requested_qty[evt.order.id] = evt.order.qty
            self._record_lifecycle(
                ts=evt.ts,
                order_id=evt.order.id,
                next_state=OrderLifecycleState.ACKNOWLEDGED,
                payload={"order": _order_dict(evt.order), "metadata": evt.metadata},
            )
            return None
        if isinstance(evt, BrokerOrderCancelledEvent):
            self._record_lifecycle(
                ts=evt.ts,
                order_id=evt.order_id,
                next_state=OrderLifecycleState.CANCELLED,
                payload={"reason": evt.reason, "metadata": evt.metadata},
            )
            self._local_open_orders.pop(evt.order_id, None)
            return None
        if not isinstance(evt, (BrokerOrderPartiallyFilledEvent, BrokerOrderFilledEvent)):
            return None
        fill_key = fill_identity_key(evt.fill)
        if self._save_processed_event_ids and self._store is not None:
            if not should_process_once(store=self._store, run_id=self._run_id, dedupe_key=fill_key):
                return None
            self._store.persist_processed_event(ProcessedEventRecord(ts=evt.ts, run_id=self._run_id, dedupe_key=fill_key, source="fill"))
        self._local_fills.append(evt.fill)
        by_order = self._fills_by_order.setdefault(evt.fill.order_id, [])
        by_order.append(evt.fill)
        requested = self._order_requested_qty.get(evt.fill.order_id, evt.fill.qty)
        agg = accumulate_fills(order_id=evt.fill.order_id, requested_qty=requested, fills=by_order)
        next_state = OrderLifecycleState.FILLED if (isinstance(evt, BrokerOrderFilledEvent) or agg.is_terminal) else OrderLifecycleState.PARTIALLY_FILLED
        self._record_lifecycle(ts=evt.ts, order_id=evt.fill.order_id, next_state=next_state, payload={"fill": _fill_dict(evt.fill)})
        if next_state == OrderLifecycleState.FILLED:
            self._local_open_orders.pop(evt.fill.order_id, None)
        self._portfolio_runner.apply_fills([evt.fill])
        return FillArtifactRecord.from_fill(evt.fill)


def build_submitted_order_artifact(*, ts: pd.Timestamp, symbol: str, side: Side, qty: float, result: SubmitResult) -> OrderArtifactRecord:
    return OrderArtifactRecord(
        ts=ts,
        event="submitted",
        order_id=result.order_id,
        symbol=symbol,
        side=side.value,
        qty=qty,
        payload={"request_id": result.request_id},
    )


def _fill_dict(fill: Fill) -> dict[str, object]:
    return {
        "order_id": fill.order_id,
        "ts": fill.ts.isoformat(),
        "symbol": fill.symbol,
        "side": fill.side.value,
        "qty": fill.qty,
        "price": fill.price,
        "fee": fill.fee,
        "slippage": fill.slippage,
        "metadata": fill.metadata,
    }


def _order_dict(order: Order) -> dict[str, object]:
    return {
        "id": order.id,
        "ts_submitted": order.ts_submitted.isoformat(),
        "symbol": order.symbol,
        "side": order.side.value,
        "qty": order.qty,
        "order_type": order.order_type.value,
        "limit_price": order.limit_price,
        "state": order.state.value if isinstance(order.state, OrderState) else str(order.state),
        "metadata": order.metadata,
    }
