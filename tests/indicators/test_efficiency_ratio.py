from __future__ import annotations

import pandas as pd

from bt.core.types import Bar
from bt.indicators.trend import EfficiencyRatio


def _bar(i: int, close: float) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="A", open=close, high=close, low=close, close=close, volume=1)


def test_efficiency_ratio_trending_near_one_and_flat_safe() -> None:
    ind = EfficiencyRatio(lookback=5)
    for i in range(6):
        ind.update(_bar(i, float(i)))
    assert ind.value is not None and ind.value > 0.99

    ind = EfficiencyRatio(lookback=5)
    for i in range(6):
        ind.update(_bar(i, 100.0))
    assert ind.value == 0.0
