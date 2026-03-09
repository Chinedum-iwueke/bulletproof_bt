import pandas as pd

from bt.core.types import Bar
from bt.data.resample import TimeframeResampler
from bt.strategy.htf_context import HTFContextStrategyAdapter
from bt.strategy.l1_h1_vol_floor_trend import L1H1VolFloorTrendStrategy


def _bar(i: int, close: float) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=close + 1, low=close - 1, close=close, volume=1000)


def test_no_signal_before_indicator_ready() -> None:
    s = HTFContextStrategyAdapter(
        inner=L1H1VolFloorTrendStrategy(timeframe="15m", theta_vol=0.0),
        resampler=TimeframeResampler(timeframes=["15m"], strict=True),
    )
    out = []
    for i in range(100):
        b = _bar(i, 100 + i)
        out.extend(s.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, {}))
    assert out == []


def test_trend_and_gate_generate_entry() -> None:
    s = HTFContextStrategyAdapter(
        inner=L1H1VolFloorTrendStrategy(timeframe="15m", theta_vol=0.0),
        resampler=TimeframeResampler(timeframes=["15m"], strict=True),
    )
    seen = []
    for i in range(900):
        b = _bar(i, 100 + i)
        seen.extend(s.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, {}))
    assert seen
    assert seen[-1].metadata["gate_pass"] is True
    assert seen[-1].metadata["trend_dir_t"] in (1, -1)
