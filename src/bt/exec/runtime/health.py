from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class RuntimeHealthSnapshot:
    ts: pd.Timestamp
    healthy: bool
    last_bar_ts: pd.Timestamp | None
    stale_seconds: float


class RuntimeHealthMonitor:
    def __init__(self, *, stale_after_seconds: int) -> None:
        self._stale_after_seconds = int(stale_after_seconds)
        self._last_bar_ts: pd.Timestamp | None = None

    def observe_bar(self, ts: pd.Timestamp) -> None:
        self._last_bar_ts = ts

    def snapshot(self, ts: pd.Timestamp) -> RuntimeHealthSnapshot:
        stale_seconds = 0.0
        healthy = True
        if self._last_bar_ts is not None:
            stale_seconds = max((ts - self._last_bar_ts).total_seconds(), 0.0)
            healthy = stale_seconds <= self._stale_after_seconds
        return RuntimeHealthSnapshot(ts=ts, healthy=healthy, last_bar_ts=self._last_bar_ts, stale_seconds=stale_seconds)
