"""L1-H5A hypothesis primitives (volatility-managed sizing overlay on L1-H1)."""
from __future__ import annotations

from collections import deque
from statistics import median
from typing import Deque

_TIMEFRAME_TO_HOURS_BARS: dict[str, dict[int, int]] = {
    "15m": {
        24: 96,
        72: 288,
    }
}


class RollingRmsVolatility:
    """Past-only rolling realized volatility using sqrt(mean(r^2))."""

    def __init__(self, lookback_bars: int) -> None:
        if lookback_bars <= 0:
            raise ValueError("lookback_bars must be > 0")
        self._history: Deque[float] = deque(maxlen=lookback_bars)

    def update(self, value: float | None) -> float | None:
        if value is None:
            return None
        self._history.append(float(value))
        if len(self._history) < self._history.maxlen:
            return None
        mean_sq = sum(item * item for item in self._history) / float(len(self._history))
        return float(mean_sq ** 0.5)


class RollingMedianReference:
    """Past-only rolling median reference over a fixed-length window."""

    def __init__(self, lookback_bars: int) -> None:
        if lookback_bars <= 0:
            raise ValueError("lookback_bars must be > 0")
        self._history: Deque[float] = deque(maxlen=lookback_bars)

    def update(self, value: float | None) -> float | None:
        if value is None:
            return None
        reference = tuple(self._history)
        if len(reference) < self._history.maxlen:
            ref = None
        else:
            ref = float(median(reference))
        self._history.append(float(value))
        return ref


def vol_window_bars(*, timeframe: str, hours: int) -> int:
    normalized_tf = str(timeframe).lower()
    mapping = _TIMEFRAME_TO_HOURS_BARS.get(normalized_tf)
    if mapping is None or int(hours) not in mapping:
        supported = sorted(mapping) if mapping is not None else []
        raise ValueError(
            f"Unsupported vol window for timeframe='{timeframe}', hours={hours}. Supported hours={supported}."
        )
    return mapping[int(hours)]


def clipped_inverse_vol_scale(
    *, sigma_t: float | None, sigma_star: float | None, s_min: float, s_max: float
) -> tuple[float | None, bool, bool]:
    if sigma_t is None or sigma_star is None:
        return None, False, False
    if sigma_t <= 0:
        return float(s_max), False, True
    ratio = float(sigma_star) / float(sigma_t)
    clipped = float(min(float(s_max), max(float(s_min), ratio)))
    return clipped, clipped <= float(s_min), clipped >= float(s_max)


__all__ = [
    "RollingMedianReference",
    "RollingRmsVolatility",
    "clipped_inverse_vol_scale",
    "vol_window_bars",
]
