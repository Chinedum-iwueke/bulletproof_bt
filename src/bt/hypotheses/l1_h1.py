"""L1-H1 hypothesis primitives (causal vol-floor trend gate)."""
from __future__ import annotations

from collections import deque
from typing import Deque

_TIMEFRAME_TO_30D_BARS: dict[str, int] = {
    "5m": 30 * 24 * 12,
    "15m": 30 * 24 * 4,
    "1h": 30 * 24,
}


def bars_for_30_calendar_days(timeframe: str) -> int:
    try:
        return _TIMEFRAME_TO_30D_BARS[timeframe]
    except KeyError as exc:
        raise ValueError(f"Unsupported timeframe '{timeframe}'. Expected one of {sorted(_TIMEFRAME_TO_30D_BARS)}") from exc


def percentile_rank(reference: tuple[float, ...], value: float, *, include_current: bool = True) -> float:
    if not reference:
        return 0.0
    ranked = reference + ((value,) if include_current else ())
    count = sum(1 for item in ranked if item <= value)
    return float(count / len(ranked))


class RollingPercentileGate:
    """Past-only rolling percentile rank over a fixed-length window."""

    def __init__(self, lookback_bars: int, *, include_current: bool = True) -> None:
        if lookback_bars <= 0:
            raise ValueError("lookback_bars must be > 0")
        self._history: Deque[float] = deque(maxlen=lookback_bars)
        self._include_current = include_current

    def update(self, value: float | None) -> float | None:
        if value is None:
            return None
        reference = tuple(self._history)
        pct = percentile_rank(reference, value, include_current=self._include_current) if reference else None
        self._history.append(float(value))
        return pct
