"""Execution adapter contracts for Bulletproof_exec."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol, runtime_checkable

import pandas as pd

from bt.core.types import Fill, Order, Position


class AdapterHealthStatus(str, Enum):
    """High-level adapter health state used by runtime supervision."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass(frozen=True)
class AdapterHealth:
    """Snapshot of adapter health for runtime checks."""

    source: str
    ts: pd.Timestamp
    status: AdapterHealthStatus
    message: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.ts.tz is None or str(self.ts.tz) != "UTC":
            raise ValueError("ts must be timezone-aware UTC")


@dataclass(frozen=True)
class BalanceSnapshot:
    """Canonical balance snapshot envelope for adapter responses."""

    ts: pd.Timestamp
    balances: dict[str, float]
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.ts.tz is None or str(self.ts.tz) != "UTC":
            raise ValueError("ts must be timezone-aware UTC")


@dataclass(frozen=True)
class BrokerOrderRequest:
    """Broker-facing order request mapped from internal decisions."""

    client_order_id: str
    symbol: str
    side: str
    qty: float
    order_type: str
    limit_price: float | None
    time_in_force: str | None = None
    reduce_only: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BrokerOrderAmendRequest:
    """Broker-facing order amendment request."""

    order_id: str | None
    client_order_id: str | None
    new_qty: float | None = None
    new_limit_price: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BrokerOrderCancelRequest:
    """Broker-facing order cancel request."""

    order_id: str | None
    client_order_id: str | None
    symbol: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class MarketDataAdapter(Protocol):
    """Contract for market data adapters consumed by the runtime."""

    def start(self) -> None:
        """Start adapter resources and subscriptions."""

    def stop(self) -> None:
        """Stop adapter resources and close connections."""

    def subscribe_closed_bars(self, symbols: list[str], timeframe: str) -> None:
        """Subscribe to closed-bar stream for symbols/timeframe."""

    def iter_events(self) -> Any:
        """Yield adapter events for the runtime loop."""

    def get_health(self) -> AdapterHealth:
        """Return current adapter health snapshot."""


@runtime_checkable
class BrokerAdapter(Protocol):
    """Contract for broker adapters consumed by the runtime."""

    def start(self) -> None:
        """Start broker connectivity resources."""

    def stop(self) -> None:
        """Stop broker connectivity resources."""

    def iter_events(self) -> list[object]:
        """Yield broker-side events for runtime processing."""

    def submit_order(self, request: BrokerOrderRequest) -> str:
        """Submit an order and return adapter-local request identifier."""

    def cancel_order(self, request: BrokerOrderCancelRequest) -> None:
        """Cancel an order by broker id or client id."""

    def amend_order(self, request: BrokerOrderAmendRequest) -> None:
        """Amend an existing order by broker id or client id."""

    def fetch_open_orders(self) -> list[Order]:
        """Fetch currently open orders."""

    def fetch_completed_orders(self, limit: int = 200) -> list[Order]:
        """Fetch recently completed orders for reconciliation."""

    def fetch_positions(self) -> list[Position]:
        """Fetch current positions snapshot."""

    def fetch_balances(self) -> BalanceSnapshot:
        """Fetch current balances snapshot."""

    def fetch_recent_fills_or_executions(self, limit: int = 200) -> list[Fill]:
        """Fetch recent fills/executions for reconciliation."""

    def get_health(self) -> AdapterHealth:
        """Return current adapter health snapshot."""
