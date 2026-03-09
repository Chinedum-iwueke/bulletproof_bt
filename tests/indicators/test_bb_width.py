from __future__ import annotations

import pandas as pd
import pytest

from bt.core.types import Bar
from bt.indicators.volatility import BollingerBandWidth


def _bar(i: int, close: float) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="A", open=close, high=close, low=close, close=close, volume=1)


def test_bb_width_matches_reference() -> None:
    closes = [10, 11, 12, 13, 14]
    ind = BollingerBandWidth(lookback=5, std_mult=2)
    for i, c in enumerate(closes):
        ind.update(_bar(i, c))
    s = pd.Series(closes)
    mid = s.rolling(5).mean().iloc[-1]
    std = s.rolling(5).std(ddof=0).iloc[-1]
    expected = ((mid + 2 * std) - (mid - 2 * std)) / mid
    assert ind.value == pytest.approx(float(expected))
