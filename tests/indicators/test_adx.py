from __future__ import annotations

import pandas as pd

from bt.core.types import Bar
from bt.indicators.dmi_adx import DMIADX
from bt.indicators.trend import ADX


def _bar(i: int, close: float) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="A", open=close - 0.3, high=close + 0.8, low=close - 0.8, close=close, volume=1)


def test_adx_tracks_existing_reference_indicator() -> None:
    ref = DMIADX(14)
    new = ADX(14)
    for i in range(80):
        b = _bar(i, 100 + (i * 0.2))
        ref.update(b)
        new.update(b)
    assert new.value is not None
    assert ref.get("adx") is not None
    assert new.value >= 0
    assert abs(new.value - float(ref.get("adx"))) < 1e-9
