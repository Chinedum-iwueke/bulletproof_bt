from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from bt.exec.events.runtime_events import RuntimeHeartbeatEvent


@dataclass
class HeartbeatScheduler:
    heartbeat_seconds: int

    def __post_init__(self) -> None:
        self._sequence = 0
        self._last_heartbeat_ts: pd.Timestamp | None = None

    def on_timestamp(self, ts: pd.Timestamp) -> list[RuntimeHeartbeatEvent]:
        if self._last_heartbeat_ts is None:
            return [self._emit(ts)]
        if (ts - self._last_heartbeat_ts).total_seconds() >= float(self.heartbeat_seconds):
            return [self._emit(ts)]
        return []

    def _emit(self, ts: pd.Timestamp) -> RuntimeHeartbeatEvent:
        self._sequence += 1
        self._last_heartbeat_ts = ts
        return RuntimeHeartbeatEvent(ts=ts, sequence=self._sequence)
