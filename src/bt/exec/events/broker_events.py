"""Canonical broker-event envelopes for Bulletproof_exec."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import pandas as pd

from bt.core.types import Fill, Order, Position
from bt.exec.adapters.base import BalanceSnapshot


class BrokerConnectionStatus(str, Enum):
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    DEGRADED = "degraded"


def _ensure_utc(ts: pd.Timestamp, field_name: str) -> None:
    if ts.tz is None or str(ts.tz) != "UTC":
        raise ValueError(f"{field_name} must be timezone-aware UTC")


@dataclass(frozen=True)
class BrokerOrderAcknowledgedEvent:
    ts: pd.Timestamp
    broker_event_id: str
    order: Order
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _ensure_utc(self.ts, "ts")


@dataclass(frozen=True)
class BrokerOrderRejectedEvent:
    ts: pd.Timestamp
    broker_event_id: str
    client_order_id: str
    reason: str
    retryable: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _ensure_utc(self.ts, "ts")


@dataclass(frozen=True)
class BrokerOrderPartiallyFilledEvent:
    ts: pd.Timestamp
    broker_event_id: str
    fill: Fill
    leaves_qty: float
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _ensure_utc(self.ts, "ts")


@dataclass(frozen=True)
class BrokerOrderFilledEvent:
    ts: pd.Timestamp
    broker_event_id: str
    fill: Fill
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _ensure_utc(self.ts, "ts")


@dataclass(frozen=True)
class BrokerOrderCancelledEvent:
    ts: pd.Timestamp
    broker_event_id: str
    order_id: str
    reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _ensure_utc(self.ts, "ts")


@dataclass(frozen=True)
class BrokerPositionSnapshotEvent:
    ts: pd.Timestamp
    broker_event_id: str
    positions: list[Position]
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _ensure_utc(self.ts, "ts")


@dataclass(frozen=True)
class BrokerBalanceSnapshotEvent:
    ts: pd.Timestamp
    broker_event_id: str
    balance_snapshot: BalanceSnapshot
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _ensure_utc(self.ts, "ts")


@dataclass(frozen=True)
class BrokerConnectionStatusEvent:
    ts: pd.Timestamp
    broker_event_id: str
    status: BrokerConnectionStatus
    message: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _ensure_utc(self.ts, "ts")


BrokerEvent = (
    BrokerOrderAcknowledgedEvent
    | BrokerOrderRejectedEvent
    | BrokerOrderPartiallyFilledEvent
    | BrokerOrderFilledEvent
    | BrokerOrderCancelledEvent
    | BrokerPositionSnapshotEvent
    | BrokerBalanceSnapshotEvent
    | BrokerConnectionStatusEvent
)
