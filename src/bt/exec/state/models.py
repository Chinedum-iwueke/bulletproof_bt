from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import pandas as pd

from bt.core.types import Order, Position
from bt.exec.adapters.base import BalanceSnapshot


def ensure_utc(ts: pd.Timestamp, field_name: str) -> None:
    if ts.tz is None or str(ts.tz) != "UTC":
        raise ValueError(f"{field_name} must be timezone-aware UTC")


@dataclass(frozen=True)
class RuntimeSessionState:
    run_id: str
    mode: str
    restart_policy: str
    status: str
    started_at: pd.Timestamp
    updated_at: pd.Timestamp
    ended_at: pd.Timestamp | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        ensure_utc(self.started_at, "started_at")
        ensure_utc(self.updated_at, "updated_at")
        if self.ended_at is not None:
            ensure_utc(self.ended_at, "ended_at")


@dataclass(frozen=True)
class OrderLifecycleRecord:
    ts: pd.Timestamp
    run_id: str
    order_id: str
    event_type: str
    payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        ensure_utc(self.ts, "ts")


@dataclass(frozen=True)
class BrokerEventRecord:
    ts: pd.Timestamp
    run_id: str
    broker_event_id: str
    event_type: str
    payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        ensure_utc(self.ts, "ts")


@dataclass(frozen=True)
class ProcessedEventRecord:
    ts: pd.Timestamp
    run_id: str
    dedupe_key: str
    source: str

    def __post_init__(self) -> None:
        ensure_utc(self.ts, "ts")


@dataclass(frozen=True)
class RuntimeCheckpoint:
    ts: pd.Timestamp
    run_id: str
    sequence: int
    last_bar_ts: pd.Timestamp | None
    next_client_order_seq: int
    open_orders: list[Order] = field(default_factory=list)
    positions: list[Position] = field(default_factory=list)
    balances: BalanceSnapshot | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        ensure_utc(self.ts, "ts")
        if self.last_bar_ts is not None:
            ensure_utc(self.last_bar_ts, "last_bar_ts")


class RecoveryDisposition(str, Enum):
    NO_PRIOR_STATE = "no_prior_state"
    RESUME = "resume"
    START_FRESH = "start_fresh"
    INCOMPLETE_PRIOR_STATE = "incomplete_prior_state"
    CORRUPT_PRIOR_STATE = "corrupt_prior_state"


@dataclass(frozen=True)
class RecoveryPlan:
    disposition: RecoveryDisposition
    restart_policy: str
    message: str
    checkpoint: RuntimeCheckpoint | None = None


@dataclass(frozen=True)
class RecoveryResult:
    session_state: RuntimeSessionState
    plan: RecoveryPlan

ExecutionCheckpoint = RuntimeCheckpoint
