"""Durable state-store contract for Bulletproof_exec runtime."""
from __future__ import annotations

from typing import Protocol, runtime_checkable

import pandas as pd

from bt.core.types import Order, Position
from bt.exec.adapters.base import BalanceSnapshot
from bt.exec.state.models import BrokerEventRecord, OrderLifecycleRecord, ProcessedEventRecord, RuntimeCheckpoint, RuntimeSessionState


@runtime_checkable
class ExecutionStateStore(Protocol):
    """Durable storage contract for execution runtime state and dedupe."""

    def persist_order_lifecycle_event(self, record: OrderLifecycleRecord) -> None:
        """Persist local order lifecycle event."""

    def persist_broker_event(self, record: BrokerEventRecord) -> None:
        """Persist normalized broker event envelope."""

    def persist_processed_event(self, record: ProcessedEventRecord) -> None:
        """Record that an event id/dedupe key has been processed."""

    def mark_broker_event_processed(self, dedupe_key: str, ts: pd.Timestamp) -> None:
        """Backward-compatible alias for marking processed broker event ids."""

    def has_processed_event(self, *, run_id: str, dedupe_key: str) -> bool:
        """Check if event id/dedupe key has already been processed."""

    def has_processed_broker_event(self, dedupe_key: str) -> bool:
        """Backward-compatible alias for processed-event checks."""

    def persist_checkpoint(self, checkpoint: RuntimeCheckpoint) -> None:
        """Persist runtime checkpoint snapshot."""

    def load_latest_checkpoint(self, run_id: str) -> RuntimeCheckpoint | None:
        """Load latest checkpoint for a run id."""

    def persist_positions_snapshot(self, *, run_id: str, ts: pd.Timestamp, positions: list[Position]) -> None:
        """Persist a latest-known positions snapshot."""

    def persist_balance_snapshot(self, *, run_id: str, snapshot: BalanceSnapshot) -> None:
        """Persist latest-known balance snapshot."""

    def query_open_local_orders(self, *, run_id: str) -> list[Order]:
        """Query latest view of open local orders."""

    def query_latest_local_positions_snapshot(self, *, run_id: str) -> list[Position]:
        """Query latest local positions snapshot."""

    def query_latest_balance_snapshot(self, *, run_id: str) -> BalanceSnapshot | None:
        """Query latest balance snapshot."""

    def record_session_liveness(self, session: RuntimeSessionState) -> None:
        """Record session start/heartbeat/last seen update."""

    def mark_session_final_status(self, *, run_id: str, status: str, ts: pd.Timestamp, error: str | None = None) -> None:
        """Mark run/session final status."""

    def load_latest_session(self, *, mode: str) -> RuntimeSessionState | None:
        """Load latest session metadata for mode."""
