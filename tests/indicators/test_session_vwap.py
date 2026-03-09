from __future__ import annotations

import pandas as pd

from bt.core.types import Bar
from bt.indicators.vwap import SessionVWAP


def _bar(ts: str, close: float, volume: float) -> Bar:
    t = pd.Timestamp(ts, tz="UTC")
    return Bar(ts=t, symbol="BTCUSDT", open=close, high=close, low=close, close=close, volume=volume)


def test_session_vwap_resets_on_utc_day_boundary() -> None:
    ind = SessionVWAP(price_source="close")
    ind.update(_bar("2024-01-01 23:59:00", 100, 2))
    assert ind.value == 100
    ind.update(_bar("2024-01-02 00:00:00", 200, 1))
    assert ind.value == 200


def test_session_vwap_handles_zero_volume() -> None:
    ind = SessionVWAP(price_source="close")
    ind.update(_bar("2024-01-01 00:00:00", 100, 0))
    assert ind.value is None
