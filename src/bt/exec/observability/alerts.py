from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Protocol

import pandas as pd


class AlertEventType(str, Enum):
    STARTUP_SUCCEEDED = "startup_succeeded"
    STARTUP_BLOCKED = "startup_blocked"
    STARTUP_FAILED = "startup_failed"
    RUNTIME_FROZEN = "runtime_frozen"
    RECONCILIATION_MISMATCH_MATERIAL = "reconciliation_mismatch_material"
    ORDER_REJECTED = "order_rejected"
    PRIVATE_STREAM_STALE = "private_stream_stale"
    REPEATED_TRANSPORT_FAILURES = "repeated_transport_failures"
    CANARY_GUARD_BLOCK = "canary_guard_block"
    LIVE_MODE_ENABLED = "live_mode_enabled"
    LIVE_MODE_DISABLED = "live_mode_disabled"
    SHUTDOWN_CLEAN = "shutdown_clean"
    SHUTDOWN_UNCLEAN = "shutdown_unclean"
    RECOVERY_RESUME_STARTED = "recovery_resume_started"
    RECOVERY_RESUME_FAILED = "recovery_resume_failed"


class AlertSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass(frozen=True)
class Alert:
    ts: pd.Timestamp
    run_id: str
    event_type: AlertEventType
    severity: AlertSeverity
    message: str
    context: dict[str, Any] = field(default_factory=dict)

    def to_jsonable(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["ts"] = self.ts.isoformat()
        payload["event_type"] = self.event_type.value
        payload["severity"] = self.severity.value
        return payload


class AlertChannel(Protocol):
    def send(self, alert: Alert) -> None: ...


@dataclass
class AlertEmitter:
    channels: list[AlertChannel]

    def emit(self, alert: Alert) -> None:
        for channel in self.channels:
            channel.send(alert)
