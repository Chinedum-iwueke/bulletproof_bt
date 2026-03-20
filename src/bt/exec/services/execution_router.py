from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Fill, Order, OrderIntent
from bt.exec.adapters.base import BrokerOrderRequest
from bt.exec.adapters.simulated import SimulatedBrokerAdapter
from bt.exec.events.broker_events import BrokerOrderAcknowledgedEvent, BrokerOrderFilledEvent
from bt.exec.logging.schemas import FillArtifactRecord, OrderArtifactRecord
from bt.exec.services.portfolio_runner import PortfolioRunner
from bt.exec.state import ExecutionStateStore, OrderLifecycleRecord, ProcessedEventRecord


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
        adapter: SimulatedBrokerAdapter,
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

    def submit_order(self, *, order_seq: int, intent: OrderIntent, ts: pd.Timestamp) -> SubmitResult:
        request = BrokerOrderRequest(
            client_order_id=f"co-{order_seq}",
            symbol=intent.symbol,
            side=intent.side.value,
            qty=abs(float(intent.qty)),
            order_type=intent.order_type.value,
            limit_price=intent.limit_price,
            metadata=dict(intent.metadata),
        )
        request_id = self._adapter.submit_order(request)
        order_id = ""
        for evt in self._adapter.iter_events():
            if isinstance(evt, BrokerOrderAcknowledgedEvent):
                order_id = evt.order.id
                if self._store is not None:
                    self._store.persist_order_lifecycle_event(
                        OrderLifecycleRecord(ts=evt.ts, run_id=self._run_id, order_id=evt.order.id, event_type="acknowledged", payload={"order": _order_dict(evt.order), "request_id": request_id})
                    )
                break
        if not order_id:
            order_id = request_id
        return SubmitResult(request_id=request_id, order_id=order_id)

    def process_bar(self, *, ts: pd.Timestamp, bars_by_symbol: dict[str, object]) -> list[FillArtifactRecord]:
        if self._mode != "paper_simulated":
            return []
        self._adapter.process_bar(ts=ts, bars_by_symbol=bars_by_symbol)
        fills: list[Fill] = []
        artifacts: list[FillArtifactRecord] = []
        for evt in self._adapter.iter_events():
            if not isinstance(evt, BrokerOrderFilledEvent):
                continue
            if self._save_processed_event_ids and self._store is not None:
                if self._store.has_processed_event(run_id=self._run_id, dedupe_key=evt.broker_event_id):
                    continue
                self._store.persist_processed_event(
                    ProcessedEventRecord(ts=evt.ts, run_id=self._run_id, dedupe_key=evt.broker_event_id, source="broker")
                )
            fills.append(evt.fill)
            artifacts.append(FillArtifactRecord.from_fill(evt.fill))
            if self._store is not None:
                self._store.persist_order_lifecycle_event(
                    OrderLifecycleRecord(ts=evt.ts, run_id=self._run_id, order_id=evt.fill.order_id, event_type="filled", payload={"fill": _fill_dict(evt.fill)})
                )
        if fills:
            self._portfolio_runner.apply_fills(fills)
        return artifacts


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
        "state": order.state.value,
        "metadata": order.metadata,
    }
