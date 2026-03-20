"""Durable state-store contract for Bulletproof_exec runtime."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

import pandas as pd

from bt.core.types import Order, Position
from bt.exec.adapters.base import BalanceSnapshot


def _ensure_utc(ts: pd.Timestamp, field_name: str) -> None:
    if ts.tz is None or str(ts.tz) != "UTC":
        raise ValueError(f"{field_name} must be timezone-aware UTC")


@dataclass(frozen=True)
class OrderLifecycleRecord:
    ts: pd.Timestamp
    order_id: str
    event_type: str
    payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _ensure_utc(self.ts, "ts")


@dataclass(frozen=True)
class BrokerEventRecord:
    ts: pd.Timestamp
    broker_event_id: str
    event_type: str
    payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _ensure_utc(self.ts, "ts")


@dataclass(frozen=True)
class ExecutionCheckpoint:
    ts: pd.Timestamp
    run_id: str
    sequence: int
    payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _ensure_utc(self.ts, "ts")


@runtime_checkable
class ExecutionStateStore(Protocol):
    """Durable storage contract for execution runtime state and dedupe."""

    def persist_order_lifecycle_event(self, record: OrderLifecycleRecord) -> None:
        """Persist local order lifecycle event."""

    def persist_broker_event(self, record: BrokerEventRecord) -> None:
        """Persist normalized broker event."""

    def persist_checkpoint(self, checkpoint: ExecutionCheckpoint) -> None:
        """Persist latest runtime checkpoint."""

    def load_latest_checkpoint(self, run_id: str) -> ExecutionCheckpoint | None:
        """Load latest checkpoint for a run id."""

    def mark_broker_event_processed(self, dedupe_key: str, ts: pd.Timestamp) -> None:
        """Record that a broker event id/dedupe key has been processed."""

    def has_processed_broker_event(self, dedupe_key: str) -> bool:
        """Check if broker event id/dedupe key has already been processed."""

    def query_open_local_orders(self) -> list[Order]:
        """Query latest view of open local orders."""

    def query_latest_local_positions_snapshot(self) -> list[Position]:
        """Query latest local positions snapshot."""

    def query_latest_balance_snapshot(self) -> BalanceSnapshot | None:
        """Query latest balance snapshot."""
