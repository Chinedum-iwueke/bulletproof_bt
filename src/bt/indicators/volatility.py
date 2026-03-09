"""Volatility indicators."""
from __future__ import annotations

from bt.core.types import Bar
from bt.indicators.base import BaseIndicator, RollingMean, RollingStd, safe_div
from bt.indicators.registry import register


@register("bb_width")
class BollingerBandWidth(BaseIndicator):
    """Normalized Bollinger Band width: (upper - lower) / middle."""

    def __init__(self, lookback: int = 20, std_mult: float = 2.0) -> None:
        if lookback <= 1:
            raise ValueError("lookback must be > 1")
        super().__init__(name=f"bb_width_{lookback}", warmup_bars=lookback)
        self._std_mult = float(std_mult)
        self._mean = RollingMean(lookback)
        self._std = RollingStd(lookback)
        self._value: float | None = None

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._mean.update(bar.close)
        std = self._std.update(bar.close)
        mid = self._mean.mean
        if mid is None or std is None:
            self._value = None
            return
        upper = mid + self._std_mult * std
        lower = mid - self._std_mult * std
        self._value = safe_div(upper - lower, mid, default=0.0)

    def reset(self) -> None:
        self._bars_seen = 0
        self._mean.reset()
        self._std.reset()
        self._value = None

    @property
    def value(self) -> float | None:
        return self._value
