from __future__ import annotations

import pandas as pd
import pytest

from bt.core.types import Bar
from bt.indicators.vwap import AnchoredVWAP


def _bar(minute: int, close: float, volume: float = 1) -> Bar:
    ts = pd.Timestamp("2024-01-01 00:00:00", tz="UTC") + pd.Timedelta(minutes=minute)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=close, low=close, close=close, volume=volume)


def test_anchored_vwap_index_anchor() -> None:
    ind = AnchoredVWAP(anchor_index=2, price_source="close")
    vals = []
    for i, close in enumerate([10, 11, 12, 14]):
        ind.update(_bar(i, close))
        vals.append(ind.value)
    assert vals[0] is None and vals[1] is None
    assert vals[2] == 12
    assert vals[3] == 13


def test_anchored_vwap_invalid_anchor() -> None:
    with pytest.raises(ValueError):
        AnchoredVWAP(anchor_index=-1)
