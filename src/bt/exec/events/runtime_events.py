"""Runtime-level event envelopes for Bulletproof_exec."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import pandas as pd


class RuntimeLifecycleKind(str, Enum):
    STARTUP = "startup"
    SHUTDOWN = "shutdown"


class RuntimeHealthEventKind(str, Enum):
    HEALTHY = "healthy"
    STALE_DATA = "stale_data"
    DEGRADED = "degraded"


def _ensure_utc(ts: pd.Timestamp, field_name: str) -> None:
    if ts.tz is None or str(ts.tz) != "UTC":
        raise ValueError(f"{field_name} must be timezone-aware UTC")


@dataclass(frozen=True)
class RuntimeHeartbeatEvent:
    ts: pd.Timestamp
    sequence: int
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _ensure_utc(self.ts, "ts")


@dataclass(frozen=True)
class ClosedBarEvent:
    ts: pd.Timestamp
    symbol: str
    timeframe: str
    close_ts: pd.Timestamp
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _ensure_utc(self.ts, "ts")
        _ensure_utc(self.close_ts, "close_ts")


@dataclass(frozen=True)
class RuntimeHealthEvent:
    ts: pd.Timestamp
    source: str
    kind: RuntimeHealthEventKind
    message: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _ensure_utc(self.ts, "ts")


@dataclass(frozen=True)
class ReconciliationTickEvent:
    ts: pd.Timestamp
    reason: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _ensure_utc(self.ts, "ts")


@dataclass(frozen=True)
class RuntimeLifecycleEvent:
    ts: pd.Timestamp
    kind: RuntimeLifecycleKind
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _ensure_utc(self.ts, "ts")


RuntimeEvent = RuntimeHeartbeatEvent | ClosedBarEvent | RuntimeHealthEvent | ReconciliationTickEvent | RuntimeLifecycleEvent
