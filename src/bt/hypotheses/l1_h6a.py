"""L1-H6A hypothesis primitives (vol-of-vol gate over L1-H2)."""
from __future__ import annotations

from collections import deque
from statistics import pstdev
from typing import Deque

from bt.hypotheses.l1_h2 import RollingQuantileGate, bars_for_30_calendar_days


def wvov_hours_to_bars(*, timeframe: str, wvov_hours: int) -> int:
    """Map Wvov hours to signal-timeframe bars using 24/7 calendar assumptions."""
    timeframe_to_minutes = {
        "5m": 5,
        "15m": 15,
        "1h": 60,
    }
    if timeframe not in timeframe_to_minutes:
        raise ValueError(f"Unsupported timeframe '{timeframe}'.")
    if wvov_hours <= 0:
        raise ValueError("wvov_hours must be > 0")
    total_minutes = int(wvov_hours) * 60
    bar_minutes = timeframe_to_minutes[timeframe]
    if total_minutes % bar_minutes != 0:
        raise ValueError(f"Wvov={wvov_hours}h is not divisible by timeframe={timeframe}")
    return total_minutes // bar_minutes


class RollingStd:
    """Deterministic rolling population standard deviation over fixed window."""

    def __init__(self, window_bars: int) -> None:
        if window_bars <= 0:
            raise ValueError("window_bars must be > 0")
        self._history: Deque[float] = deque(maxlen=window_bars)

    def update(self, value: float | None) -> float | None:
        if value is None:
            return None
        self._history.append(float(value))
        if len(self._history) < self._history.maxlen:
            return None
        return float(pstdev(self._history))


__all__ = [
    "RollingQuantileGate",
    "RollingStd",
    "bars_for_30_calendar_days",
    "wvov_hours_to_bars",
]
