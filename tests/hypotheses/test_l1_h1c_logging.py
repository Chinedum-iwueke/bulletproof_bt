from __future__ import annotations

from collections import deque

import pandas as pd

from bt.core.types import Bar
from bt.data.resample import HTFBar
from bt.strategy.volfloor_ema_pullback import VolFloorEmaPullbackStrategy


def _bar(ts: str, close: float) -> Bar:
    stamp = pd.Timestamp(ts, tz="UTC")
    return Bar(ts=stamp, symbol="AAA", open=close, high=close + 1, low=close - 1, close=close, volume=1.0)


def _htf(ts: str, close: float) -> HTFBar:
    stamp = pd.Timestamp(ts, tz="UTC")
    return HTFBar(
        ts=stamp,
        symbol="AAA",
        open=close,
        high=close + 1,
        low=close - 1,
        close=close,
        volume=15.0,
        timeframe="15m",
        n_bars=15,
        expected_bars=15,
        is_complete=True,
        metadata={},
    )


def test_l1_h1c_entry_metadata_contains_contract_fields() -> None:
    strategy = VolFloorEmaPullbackStrategy(
        atr_period=1,
        vol_lookback_bars=1,
        adx_min=0.0,
        vol_floor_pct=0.0,
        er_min=None,
        ema_fast_period=2,
        ema_slow_period=3,
    )
    state = strategy._state_for("AAA")
    state.atr.update = lambda bar: None  # type: ignore[method-assign]
    state.adx.update = lambda bar: None  # type: ignore[method-assign]
    state.ema_fast.update = lambda bar: None  # type: ignore[method-assign]
    state.ema_slow.update = lambda bar: None  # type: ignore[method-assign]
    state.atr._rma.value = 2.0
    state.adx._values = {"adx": 30.0, "plus_di": 30.0, "minus_di": 10.0}
    state.ema_fast._ema.value = 101.0
    state.ema_slow._ema.value = 100.0
    state.natr_history = deque([0.01], maxlen=1)

    htf = _htf("2024-01-01 01:00:00", 101.2)
    signals = strategy.on_bars(htf.ts, {"AAA": _bar("2024-01-01 01:00:00", 101.2)}, {"AAA"}, {"htf": {"15m": {"AAA": htf}}})

    assert len(signals) == 1
    meta = signals[0].metadata
    for key in [
        "strategy",
        "tf",
        "signal_timeframe",
        "exit_monitoring_timeframe",
        "exit_type",
        "ema_fast",
        "ema_slow",
        "adx",
        "efficiency_ratio",
        "er_min",
        "vol_pct_rank",
        "vol_floor_pct",
        "long_bias",
        "short_bias",
        "long_pullback",
        "short_pullback",
        "entry_reference_price",
        "stop_price",
        "stop_distance",
        "stop_source",
        "stop_details",
        "chandelier",
    ]:
        assert key in meta
