from __future__ import annotations

from collections import deque

import pandas as pd

from bt.core.types import Bar
from bt.data.resample import HTFBar
from bt.strategy.volfloor_ema_pullback import VolFloorEmaPullbackStrategy


def _bar(ts: str, *, close: float, high: float | None = None, low: float | None = None) -> Bar:
    stamp = pd.Timestamp(ts, tz="UTC")
    resolved_high = close if high is None else high
    resolved_low = close if low is None else low
    return Bar(ts=stamp, symbol="AAA", open=close, high=resolved_high, low=resolved_low, close=close, volume=1.0)


def _htf(ts: str, *, close: float, high: float, low: float) -> HTFBar:
    stamp = pd.Timestamp(ts, tz="UTC")
    return HTFBar(
        ts=stamp,
        symbol="AAA",
        open=close,
        high=high,
        low=low,
        close=close,
        volume=15.0,
        timeframe="15m",
        n_bars=15,
        expected_bars=15,
        is_complete=True,
        metadata={},
    )


def _pin_ready_state(strategy: VolFloorEmaPullbackStrategy) -> None:
    state = strategy._state_for("AAA")
    state.atr.update = lambda bar: None  # type: ignore[method-assign]
    state.adx.update = lambda bar: None  # type: ignore[method-assign]
    state.ema_fast.update = lambda bar: None  # type: ignore[method-assign]
    state.ema_slow.update = lambda bar: None  # type: ignore[method-assign]
    state.atr._rma.value = 2.0
    state.adx._values = {"adx": 30.0, "plus_di": 30.0, "minus_di": 10.0}
    state.ema_fast._ema.value = 100.0
    state.ema_slow._ema.value = 99.0


def test_l1_h1c_vol_rank_uses_percent_scale_and_live_entry_reference_price() -> None:
    strategy = VolFloorEmaPullbackStrategy(
        timeframe="15m",
        atr_period=1,
        vol_lookback_bars=3,
        adx_min=0.0,
        vol_floor_pct=60.0,
        er_min=None,
        ema_fast_period=2,
        ema_slow_period=3,
        stop_atr_mult=1.0,
    )
    _pin_ready_state(strategy)
    state = strategy._state_for("AAA")
    state.ema_fast._ema.value = 99.5
    state.ema_slow._ema.value = 98.5
    state.natr_history = deque([0.01, 0.02, 0.03], maxlen=3)

    htf = _htf("2024-01-01 01:00:00", close=100.0, high=101.0, low=99.0)
    live = _bar("2024-01-01 01:00:00", close=99.0, high=100.0, low=98.0)
    out = strategy.on_bars(htf.ts, {"AAA": live}, {"AAA"}, {"htf": {"15m": {"AAA": htf}}})

    assert len(out) == 1
    meta = out[0].metadata
    assert meta["entry_reference_price"] == 99.0
    assert meta["stop_price"] == 97.0
    assert 66.6 < meta["vol_pct_rank"] < 66.7


def test_l1_h1c_er_filter_and_last_htf_timestamp_discipline() -> None:
    strategy = VolFloorEmaPullbackStrategy(
        atr_period=1,
        vol_lookback_bars=1,
        adx_min=0.0,
        vol_floor_pct=0.0,
        er_lookback=3,
        er_min=0.8,
        ema_fast_period=2,
        ema_slow_period=3,
    )
    _pin_ready_state(strategy)
    state = strategy._state_for("AAA")
    state.natr_history = deque([0.01], maxlen=1)
    state.closes.extend([100.0, 101.0, 100.0])

    ts = "2024-01-01 01:15:00"
    htf = _htf(ts, close=101.2, high=102.0, low=99.5)
    out_blocked = strategy.on_bars(htf.ts, {"AAA": _bar(ts, close=101.2)}, {"AAA"}, {"htf": {"15m": {"AAA": htf}}})
    assert out_blocked == []

    state.closes.clear()
    state.closes.extend([100.0, 101.0, 102.0])
    htf_ok = _htf("2024-01-01 01:30:00", close=103.0, high=103.5, low=99.5)
    out_ok = strategy.on_bars(htf_ok.ts, {"AAA": _bar("2024-01-01 01:30:00", close=103.0)}, {"AAA"}, {"htf": {"15m": {"AAA": htf_ok}}})
    assert len(out_ok) == 1

    out_duplicate = strategy.on_bars(htf_ok.ts, {"AAA": _bar("2024-01-01 01:30:00", close=103.0)}, {"AAA"}, {"htf": {"15m": {"AAA": htf_ok}}})
    assert out_duplicate == []
