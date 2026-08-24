"""L1-H2 hypothesis primitives (compression-gated VWAP mean reversion)."""
from __future__ import annotations

from collections import deque
import math
from typing import Deque

from bt.hypotheses.l1_h1 import bars_for_30_calendar_days


class RollingQuantileGate:
    """Past-only rolling quantile threshold over a fixed-length window."""

    def __init__(self, lookback_bars: int, *, q: float) -> None:
        if lookback_bars <= 0:
            raise ValueError("lookback_bars must be > 0")
        if not 0.0 <= q <= 1.0:
            raise ValueError("q must be in [0, 1]")
        self._history: Deque[float] = deque(maxlen=lookback_bars)
        self._q = float(q)

    @staticmethod
    def _linear_quantile(values: tuple[float, ...], q: float) -> float:
        if not values:
            raise ValueError("values must be non-empty")
        ordered = sorted(float(v) for v in values)
        if len(ordered) == 1:
            return ordered[0]
        position = q * (len(ordered) - 1)
        lo = int(position)
        hi = min(lo + 1, len(ordered) - 1)
        weight = position - lo
        return ordered[lo] * (1.0 - weight) + ordered[hi] * weight

    def update(self, value: float | None) -> tuple[float | None, bool | None]:
        if value is None:
            return None, None
        reference = tuple(self._history)
        if len(reference) < self._history.maxlen:
            threshold = None
            gate = None
        else:
            threshold = self._linear_quantile(reference, self._q)
            candidate = float(value)
            gate = candidate <= threshold or math.isclose(
                candidate,
                threshold,
                rel_tol=1e-12,
                abs_tol=1e-15,
            )
        self._history.append(float(value))
        return threshold, gate


__all__ = ["bars_for_30_calendar_days", "RollingQuantileGate"]
