from __future__ import annotations

from collections import deque

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar
from bt.data.resample import HTFBar, TimeframeResampler
from bt.strategy.htf_context import HTFContextStrategyAdapter
from bt.strategy.volfloor_donchian import VolFloorDonchianStrategy


def _bar(ts: str, *, high: float, low: float, close: float, symbol: str = "AAA") -> Bar:
    stamp = pd.Timestamp(ts, tz="UTC")
    return Bar(
        ts=stamp,
        symbol=symbol,
        open=close,
        high=high,
        low=low,
        close=close,
        volume=1.0,
    )


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


def test_vol_gate_is_past_only() -> None:
    strategy = VolFloorDonchianStrategy(
        timeframe="15m",
        donchian_entry_lookback=3,
        donchian_exit_lookback=2,
        adx_min=18.0,
        vol_floor_pct=70.0,
        atr_period=1,
        vol_lookback_bars=3,
    )
    state = strategy._state_for("AAA")
    _pin_trend_gate(strategy)

    state.highs.extend([100.0, 101.0, 102.0])
    state.lows.extend([99.0, 99.0, 99.0])
    state.natr_history = deque([0.01, 0.02], maxlen=3)
    state.atr._prev_close = 101.0

    bar_insufficient = _htf_bar("2024-01-01 01:00:00", high=103.0, low=100.98, close=101.0)
    signals = strategy.on_bars(
        bar_insufficient.ts,
        {"AAA": _bar("2024-01-01 01:00:00", high=101.0, low=100.0, close=101.0)},
        {"AAA"},
        {"htf": {"15m": {"AAA": bar_insufficient}}},
    )
    assert signals == []

    state.highs.extend([100.0, 101.0, 102.0])
    state.lows.extend([99.0, 99.0, 99.0])
    state.natr_history = deque([0.01, 0.02, 0.03], maxlen=3)
    state.atr._prev_close = 101.0

    bar_rank_edge = _htf_bar("2024-01-01 01:15:00", high=103.0, low=100.98, close=101.0)
    signals = strategy.on_bars(
        bar_rank_edge.ts,
        {"AAA": _bar("2024-01-01 01:15:00", high=101.0, low=100.0, close=101.0)},
        {"AAA"},
        {"htf": {"15m": {"AAA": bar_rank_edge}}},
    )

    # Past-only percentile rank is 66.67 (2/3), below 70 floor.
    # If current value were incorrectly included, rank would be 75 (3/4) and could trigger.
    assert signals == []


def test_donchian_breakout_uses_closed_htf_only() -> None:
    strategy = VolFloorDonchianStrategy(
        timeframe="15m",
        donchian_entry_lookback=1,
        donchian_exit_lookback=1,
        adx_min=18.0,
        vol_floor_pct=0.0,
        atr_period=1,
        vol_lookback_bars=1,
    )
    _pin_trend_gate(strategy)
    strategy._state_for("AAA").natr_history = deque([0.01], maxlen=1)
    resampler = TimeframeResampler(timeframes=["15m"], strict=True)
    wrapped = HTFContextStrategyAdapter(inner=strategy, resampler=resampler)

    emitted = []
    # First full 15m bucket: baseline highs around 100.
    for minute in range(15):
        ts = f"2024-01-01 00:{minute:02d}:00"
        emitted.extend(wrapped.on_bars(pd.Timestamp(ts, tz="UTC"), {"AAA": _bar(ts, high=100.0, low=99.0, close=100.0)}, {"AAA"}, {}))

    # Second 15m bucket has an intrabucket high spike; should not emit until bucket close.
    for minute in range(15, 30):
        high = 120.0 if minute == 20 else 106.0
        close = 105.0
        ts = f"2024-01-01 00:{minute:02d}:00"
        out = wrapped.on_bars(pd.Timestamp(ts, tz="UTC"), {"AAA": _bar(ts, high=high, low=99.0, close=close)}, {"AAA"}, {})
        if minute < 30:
            assert out == []
        emitted.extend(out)

    # Advancing to 00:30 emits the closed 00:15 HTF bar and only then allows breakout entry.
    final = wrapped.on_bars(
        pd.Timestamp("2024-01-01 00:30:00", tz="UTC"),
        {"AAA": _bar("2024-01-01 00:30:00", high=106.0, low=99.0, close=105.0)},
        {"AAA"},
        {},
    )
    emitted.extend(final)

    assert len(emitted) == 1
    assert emitted[0].signal_type == "h1_volfloor_donchian_entry"
    assert emitted[0].ts == pd.Timestamp("2024-01-01 00:30:00", tz="UTC")


def test_missing_1m_bars_do_not_create_htf_signal_bars() -> None:
    strategy = VolFloorDonchianStrategy(
        timeframe="15m",
        donchian_entry_lookback=1,
        donchian_exit_lookback=1,
        adx_min=18.0,
        vol_floor_pct=0.0,
        atr_period=1,
        vol_lookback_bars=1,
    )
    _pin_trend_gate(strategy)
    wrapped = HTFContextStrategyAdapter(inner=strategy, resampler=TimeframeResampler(timeframes=["15m"], strict=True))

    # First bucket complete so lookback can form.
    for minute in range(15):
        ts = f"2024-01-01 00:{minute:02d}:00"
        wrapped.on_bars(pd.Timestamp(ts, tz="UTC"), {"AAA": _bar(ts, high=100.0, low=99.0, close=100.0)}, {"AAA"}, {})

    # Missing minute 20 makes second bucket incomplete in strict mode.
    all_signals = []
    for minute in [15, 16, 17, 18, 19, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30]:
        ts = f"2024-01-01 00:{minute:02d}:00"
        all_signals.extend(
            wrapped.on_bars(
                pd.Timestamp(ts, tz="UTC"),
                {"AAA": _bar(ts, high=106.0, low=99.0, close=105.0)},
                {"AAA"},
                {},
            )
        )

    assert all_signals == []


def test_entry_exit_symmetry_sanity() -> None:
    strategy = VolFloorDonchianStrategy(
        timeframe="15m",
        donchian_entry_lookback=2,
        donchian_exit_lookback=2,
        adx_min=18.0,
        vol_floor_pct=0.0,
        atr_period=1,
        vol_lookback_bars=1,
    )
    _pin_trend_gate(strategy)

    series = [
        _htf_bar("2024-01-01 00:00:00", high=10.0, low=9.0, close=9.5),
        _htf_bar("2024-01-01 00:15:00", high=10.0, low=9.0, close=9.8),
        _htf_bar("2024-01-01 00:30:00", high=11.0, low=10.0, close=10.5),  # long entry
        _htf_bar("2024-01-01 00:45:00", high=10.0, low=8.0, close=8.5),   # long exit
    ]

    emitted = []
    for htf_bar in series:
        emitted.extend(
            strategy.on_bars(
                htf_bar.ts,
                {"AAA": _bar(str(htf_bar.ts), high=htf_bar.high, low=htf_bar.low, close=htf_bar.close)},
                {"AAA"},
                {"htf": {"15m": {"AAA": htf_bar}}},
            )
        )

    assert [signal.signal_type for signal in emitted] == [
        "h1_volfloor_donchian_entry",
        "h1_volfloor_donchian_exit",
    ]
    assert [signal.side for signal in emitted] == [Side.BUY, Side.SELL]
    assert [signal.ts for signal in emitted] == [
        pd.Timestamp("2024-01-01 00:30:00", tz="UTC"),
        pd.Timestamp("2024-01-01 00:45:00", tz="UTC"),
    ]


def test_exit_takes_precedence_over_same_bar_entry() -> None:
    strategy = VolFloorDonchianStrategy(
        timeframe="15m",
        donchian_entry_lookback=2,
        donchian_exit_lookback=2,
        adx_min=0.0,
        vol_floor_pct=0.0,
        atr_period=1,
        vol_lookback_bars=1,
    )

    state = strategy._state_for("AAA")
    _pin_trend_gate(strategy)

    # Pre-load prior bars so the current bar can satisfy both:
    # - long exit (close < exit_low)
    # - short entry (close < entry_low)
    state.highs.extend([10.0, 9.0])
    state.lows.extend([9.0, 8.0])
    state.natr_history = deque([0.01], maxlen=1)
    state.position = Side.BUY
    state.atr._prev_close = 8.2

    htf_bar = _htf_bar("2024-01-01 01:00:00", high=8.5, low=7.0, close=7.5)
    emitted = strategy.on_bars(
        htf_bar.ts,
        {"AAA": _bar("2024-01-01 01:00:00", high=8.5, low=7.0, close=7.5)},
        {"AAA"},
        {"htf": {"15m": {"AAA": htf_bar}}},
    )

    assert len(emitted) == 1
    assert emitted[0].signal_type == "h1_volfloor_donchian_exit"
    assert emitted[0].side == Side.SELL
    assert state.position is None


def test_entry_stop_uses_live_entry_reference_price() -> None:
    strategy = VolFloorDonchianStrategy(
        timeframe="15m",
        donchian_entry_lookback=1,
        donchian_exit_lookback=1,
        adx_min=0.0,
        vol_floor_pct=0.0,
        atr_period=1,
        vol_lookback_bars=1,
        stop_mode="atr",
        atr_stop_multiple=1.0,
    )
    _pin_trend_gate(strategy)
    state = strategy._state_for("AAA")
    state.highs.extend([120.0])
    state.lows.extend([118.0])
    state.natr_history = deque([0.01], maxlen=1)
    state.atr._prev_close = 130.0

    htf_bar = _htf_bar("2024-01-01 01:00:00", high=131.0, low=129.0, close=130.0)
    live_bar = _bar("2024-01-01 01:00:00", high=101.0, low=99.0, close=100.0)

    emitted = strategy.on_bars(
        htf_bar.ts,
        {"AAA": live_bar},
        {"AAA"},
        {"htf": {"15m": {"AAA": htf_bar}}},
    )

    assert len(emitted) == 1
    assert emitted[0].signal_type == "h1_volfloor_donchian_entry"
    assert emitted[0].metadata["entry_reference_price"] == 100.0
    assert emitted[0].metadata["stop_price"] == 98.0
