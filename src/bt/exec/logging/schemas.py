from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from bt.core.types import Fill, Signal


@dataclass(frozen=True)
class DecisionArtifactRecord:
    ts: pd.Timestamp
    symbol: str
    signal: Signal
    approved: bool
    reason: str


@dataclass(frozen=True)
class OrderArtifactRecord:
    ts: pd.Timestamp
    event: str
    order_id: str
    symbol: str
    side: str
    qty: float
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FillArtifactRecord:
    ts: pd.Timestamp
    symbol: str
    order_id: str
    side: str
    qty: float
    price: float
    fee: float
    slippage: float
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_fill(cls, fill: Fill) -> "FillArtifactRecord":
        return cls(
            ts=fill.ts,
            symbol=fill.symbol,
            order_id=fill.order_id,
            side=fill.side.value,
            qty=fill.qty,
            price=fill.price,
            fee=fill.fee,
            slippage=fill.slippage,
            metadata=dict(fill.metadata),
        )


@dataclass(frozen=True)
class HeartbeatArtifactRecord:
    ts: pd.Timestamp
    sequence: int
    healthy: bool
    stale_seconds: float
    metadata: dict[str, Any] = field(default_factory=dict)
