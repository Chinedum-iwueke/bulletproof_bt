"""Reusable feature calculations for fast-path prototypes."""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np

from bt.engine.fast_path.data_session import SymbolArrays


def ema(values: np.ndarray, span: int) -> np.ndarray:
    out = np.empty_like(values, dtype="float64")
    if len(values) == 0:
        return out
    alpha = 2.0 / (float(span) + 1.0)
    out[0] = values[0]
    for idx in range(1, len(values)):
        out[idx] = alpha * values[idx] + (1.0 - alpha) * out[idx - 1]
    return out


def atr(arrays: SymbolArrays, period: int = 14) -> np.ndarray:
    n = len(arrays.close)
    tr = np.empty(n, dtype="float64")
    if n == 0:
        return tr
    tr[0] = arrays.high[0] - arrays.low[0]
    for idx in range(1, n):
        tr[idx] = max(
            arrays.high[idx] - arrays.low[idx],
            abs(arrays.high[idx] - arrays.close[idx - 1]),
            abs(arrays.low[idx] - arrays.close[idx - 1]),
        )
    out = np.full(n, np.nan, dtype="float64")
    csum = np.cumsum(tr)
    for idx in range(period - 1, n):
        total = csum[idx] - (csum[idx - period] if idx >= period else 0.0)
        out[idx] = total / period
    return out


@dataclass
class FeatureBank:
    arrays: SymbolArrays
    _cache: dict[tuple[str, int], np.ndarray] = field(default_factory=dict)

    def ema(self, span: int) -> np.ndarray:
        key = ("ema", int(span))
        if key not in self._cache:
            self._cache[key] = ema(self.arrays.close, int(span))
        return self._cache[key]

    def atr(self, period: int = 14) -> np.ndarray:
        key = ("atr", int(period))
        if key not in self._cache:
            self._cache[key] = atr(self.arrays, int(period))
        return self._cache[key]

