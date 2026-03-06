from __future__ import annotations

from collections import deque

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar
from bt.data.resample import HTFBar
from bt.strategy.volfloor_donchian import VolFloorDonchianStrategy
from bt.strategy.volfloor_ema_pullback import VolFloorEmaPullbackStrategy


def _bar(ts: str, *, high: float, low: float, close: float, symbol: str = "AAA") -> Bar:
    stamp = pd.Timestamp(ts, tz="UTC")
    return Bar(ts=stamp, symbol=symbol, open=close, high=high, low=low, close=close, volume=1.0)


def _htf_bar(ts: str, *, high: float, low: float, close: float, symbol: str = "AAA") -> HTFBar:
    stamp = pd.Timestamp(ts, tz="UTC")
    return HTFBar(
        ts=stamp,
        symbol=symbol,
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


def _pin_trend_gate(strategy: VolFloorDonchianStrategy, symbol: str = "AAA") -> None:
    state = strategy._state_for(symbol)
    state.adx.update = lambda bar: None  # type: ignore[method-assign]
    state.adx._values = {"adx": 30.0, "plus_di": 30.0, "minus_di": 10.0}


def _seed_donchian_position(strategy: VolFloorDonchianStrategy) -> None:
    state = strategy._state_for("AAA")
    _pin_trend_gate(strategy)
    state.position = Side.BUY
    state.highs.extend([100.0, 101.0, 102.0, 103.0, 104.0])
    state.lows.extend([95.0, 96.0, 97.0, 98.0, 99.0])
    state.natr_history = deque([0.01], maxlen=1)
    state.atr.update = lambda bar: None  # type: ignore[method-assign]
    state.atr._rma.value = 1.0
    state.trade_state = strategy._state_for("AAA").trade_state = type(
        "T",
        (),
        {"side": Side.BUY, "entry_price": 100.0, "stop_distance": 2.0, "partial_taken": False},
    )()


def test_donchian_default_exit_type_is_reversal() -> None:
    strategy = VolFloorDonchianStrategy()
    assert strategy._exit_type == "donchian_reversal"


def test_donchian_exit_type_chandelier_emits_exit() -> None:
    strategy = VolFloorDonchianStrategy(exit_type="chandelier", chandelier_lookback=3, atr_period=1, vol_lookback_bars=1)
    _seed_donchian_position(strategy)
    emitted = strategy.on_bars(
        pd.Timestamp("2024-01-01 01:00:00", tz="UTC"),
        {"AAA": _bar("2024-01-01 01:00:00", high=101.0, low=95.0, close=100.0)},
        {"AAA"},
        {"htf": {"15m": {"AAA": _htf_bar("2024-01-01 01:00:00", high=101.0, low=95.0, close=100.0)}}},
    )
    assert len(emitted) == 1
    assert emitted[0].metadata["exit_type"] == "chandelier"
    assert emitted[0].signal_type == "h1_volfloor_donchian_exit"


def test_donchian_exit_type_partial_donchian_emits_reduce_only_once() -> None:
    strategy = VolFloorDonchianStrategy(exit_type="partial_donchian", atr_period=1, vol_lookback_bars=1)
    _seed_donchian_position(strategy)
    out1 = strategy.on_bars(
        pd.Timestamp("2024-01-01 01:00:00", tz="UTC"),
        {"AAA": _bar("2024-01-01 01:00:00", high=103.0, low=99.0, close=101.0)},
        {"AAA"},
        {"htf": {"15m": {"AAA": _htf_bar("2024-01-01 01:00:00", high=103.0, low=99.0, close=101.0)}}},
    )
    assert len(out1) == 1
    assert out1[0].metadata.get("reduce_only") is True
    out2 = strategy.on_bars(
        pd.Timestamp("2024-01-01 01:15:00", tz="UTC"),
        {"AAA": _bar("2024-01-01 01:15:00", high=103.0, low=99.0, close=101.0)},
        {"AAA"},
        {"htf": {"15m": {"AAA": _htf_bar("2024-01-01 01:15:00", high=103.0, low=99.0, close=101.0)}}},
    )
    assert out2 == []


def test_donchian_exit_type_partial_chandelier_uses_chandelier_for_remainder() -> None:
    strategy = VolFloorDonchianStrategy(exit_type="partial_chandelier", chandelier_lookback=3, atr_period=1, vol_lookback_bars=1)
    _seed_donchian_position(strategy)
    state = strategy._state_for("AAA")
    state.trade_state.partial_taken = True
    emitted = strategy.on_bars(
        pd.Timestamp("2024-01-01 01:00:00", tz="UTC"),
        {"AAA": _bar("2024-01-01 01:00:00", high=101.0, low=95.0, close=100.0)},
        {"AAA"},
        {"htf": {"15m": {"AAA": _htf_bar("2024-01-01 01:00:00", high=101.0, low=95.0, close=100.0)}}},
    )
    assert len(emitted) == 1
    assert emitted[0].metadata.get("close_only") is True


def test_volfloor_ema_pullback_exit_types() -> None:
    strategy = VolFloorEmaPullbackStrategy(exit_type="ema_trend_end", atr_period=1, vol_lookback_bars=1, ema_fast_period=2, ema_slow_period=3)
    state = strategy._state_for("AAA")
    state.position = Side.BUY
    state.ema_fast.update = lambda bar: None  # type: ignore[method-assign]
    state.ema_slow.update = lambda bar: None  # type: ignore[method-assign]
    state.ema_fast._ema.value = 99.0
    state.ema_slow._ema.value = 100.0
    state.adx._values = {"adx": 30.0, "plus_di": 30.0, "minus_di": 10.0}
    state.natr_history = deque([0.01], maxlen=1)
    out = strategy.on_bars(
        pd.Timestamp("2024-01-01 01:00:00", tz="UTC"),
        {"AAA": _bar("2024-01-01 01:00:00", high=101.0, low=99.0, close=100.0)},
        {"AAA"},
        {"htf": {"15m": {"AAA": _htf_bar("2024-01-01 01:00:00", high=101.0, low=99.0, close=100.0)}}},
    )
    assert len(out) == 1
    assert out[0].signal_type == "h1_volfloor_ema_pullback_exit"
    assert out[0].metadata["exit_type"] == "ema_trend_end"


def test_volfloor_ema_pullback_entry_requires_efficiency_ratio_threshold() -> None:
    strategy = VolFloorEmaPullbackStrategy(
        atr_period=1,
        vol_lookback_bars=1,
        ema_fast_period=2,
        ema_slow_period=3,
        adx_min=0.0,
        vol_floor_pct=0.0,
        er_lookback=3,
        er_min=0.8,
    )
    state = strategy._state_for("AAA")
    state.ema_fast.update = lambda bar: None  # type: ignore[method-assign]
    state.ema_slow.update = lambda bar: None  # type: ignore[method-assign]
    state.ema_fast._ema.value = 101.0
    state.ema_slow._ema.value = 100.0
    state.adx.update = lambda bar: None  # type: ignore[method-assign]
    state.adx._values = {"adx": 30.0, "plus_di": 30.0, "minus_di": 10.0}
    state.natr_history = deque([0.01], maxlen=1)
    state.closes.extend([100.0, 101.0, 100.0])
    state.atr._prev_close = 101.0

    out = strategy.on_bars(
        pd.Timestamp("2024-01-01 01:00:00", tz="UTC"),
        {"AAA": _bar("2024-01-01 01:00:00", high=102.0, low=100.5, close=101.2)},
        {"AAA"},
        {"htf": {"15m": {"AAA": _htf_bar("2024-01-01 01:00:00", high=102.0, low=100.5, close=101.2)}}},
    )
    assert out == []

    strategy_ok = VolFloorEmaPullbackStrategy(
        atr_period=1,
        vol_lookback_bars=1,
        ema_fast_period=2,
        ema_slow_period=3,
        adx_min=0.0,
        vol_floor_pct=0.0,
        er_lookback=3,
        er_min=0.8,
    )
    state_ok = strategy_ok._state_for("AAA")
    state_ok.ema_fast.update = lambda bar: None  # type: ignore[method-assign]
    state_ok.ema_slow.update = lambda bar: None  # type: ignore[method-assign]
    state_ok.ema_fast._ema.value = 101.0
    state_ok.ema_slow._ema.value = 100.0
    state_ok.adx.update = lambda bar: None  # type: ignore[method-assign]
    state_ok.adx._values = {"adx": 30.0, "plus_di": 30.0, "minus_di": 10.0}
    state_ok.natr_history = deque([0.01], maxlen=1)
    state_ok.closes.extend([100.0, 101.0, 102.0])
    state_ok.atr._prev_close = 101.0

    out_ok = strategy_ok.on_bars(
        pd.Timestamp("2024-01-01 01:15:00", tz="UTC"),
        {"AAA": _bar("2024-01-01 01:15:00", high=103.5, low=100.5, close=103.0)},
        {"AAA"},
        {"htf": {"15m": {"AAA": _htf_bar("2024-01-01 01:15:00", high=103.5, low=100.5, close=103.0)}}},
    )
    assert len(out_ok) == 1
    assert out_ok[0].signal_type == "h1_volfloor_ema_pullback_entry"
    assert out_ok[0].metadata["efficiency_ratio"] == 1.0
