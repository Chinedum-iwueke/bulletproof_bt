"""Trend indicators."""
from __future__ import annotations

from collections import deque

from bt.core.types import Bar
from bt.indicators._helpers import WilderRMA
from bt.indicators.base import BaseIndicator, safe_div
from bt.indicators.registry import register


@register("adx_wilder")
class ADX(BaseIndicator):
    """Wilder ADX with causal smoothing."""

    def __init__(self, period: int = 14) -> None:
        if period <= 0:
            raise ValueError("period must be positive")
        super().__init__(name=f"adx_wilder_{period}", warmup_bars=period * 2)
        self._prev_h: float | None = None
        self._prev_l: float | None = None
        self._prev_c: float | None = None
        self._tr = WilderRMA(period)
        self._plus_dm = WilderRMA(period)
        self._minus_dm = WilderRMA(period)
        self._adx = WilderRMA(period)
        self._value: float | None = None

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        if self._prev_c is None:
            self._prev_h, self._prev_l, self._prev_c = bar.high, bar.low, bar.close
            self._value = None
            return

        up_move = bar.high - self._prev_h
        down_move = self._prev_l - bar.low
        plus_dm = up_move if up_move > down_move and up_move > 0 else 0.0
        minus_dm = down_move if down_move > up_move and down_move > 0 else 0.0
        tr = max(bar.high - bar.low, abs(bar.high - self._prev_c), abs(bar.low - self._prev_c))

        trv = self._tr.update(tr)
        pdm = self._plus_dm.update(plus_dm)
        mdm = self._minus_dm.update(minus_dm)
        self._prev_h, self._prev_l, self._prev_c = bar.high, bar.low, bar.close

        if None in (trv, pdm, mdm):
            self._value = None
            return

        plus_di = 100.0 * safe_div(pdm, trv)
        minus_di = 100.0 * safe_div(mdm, trv)
        dx = 100.0 * safe_div(abs(plus_di - minus_di), plus_di + minus_di)
        self._value = self._adx.update(dx)

    def reset(self) -> None:
        self._bars_seen = 0
        self._prev_h = self._prev_l = self._prev_c = None
        self._tr.reset()
        self._plus_dm.reset()
        self._minus_dm.reset()
        self._adx.reset()
        self._value = None

    @property
    def value(self) -> float | None:
        return self._value


@register("efficiency_ratio")
class EfficiencyRatio(BaseIndicator):
    """Kaufman efficiency ratio in [0,1] for non-flat windows."""

    def __init__(self, lookback: int = 10) -> None:
        if lookback <= 0:
            raise ValueError("lookback must be positive")
        super().__init__(name=f"efficiency_ratio_{lookback}", warmup_bars=lookback + 1)
        self._lookback = lookback
        self._closes: deque[float] = deque(maxlen=lookback + 1)
        self._value: float | None = None

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._closes.append(float(bar.close))
        if len(self._closes) < self._lookback + 1:
            self._value = None
            return

        change = abs(self._closes[-1] - self._closes[0])
        volatility = sum(abs(self._closes[i] - self._closes[i - 1]) for i in range(1, len(self._closes)))
        self._value = safe_div(change, volatility, default=0.0)

    def reset(self) -> None:
        self._bars_seen = 0
        self._closes.clear()
        self._value = None

    @property
    def value(self) -> float | None:
        return self._value
