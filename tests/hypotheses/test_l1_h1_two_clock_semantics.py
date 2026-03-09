import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar
from bt.data.resample import TimeframeResampler
from bt.strategy.htf_context import HTFContextStrategyAdapter
from bt.strategy.l1_h1_vol_floor_trend import L1H1VolFloorTrendStrategy


def _bar(i: int, close: float, *, low: float | None = None, high: float | None = None) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(
        ts=ts,
        symbol="BTCUSDT",
        open=close,
        high=close + 0.2 if high is None else high,
        low=close - 0.2 if low is None else low,
        close=close,
        volume=1000,
    )


def _ctx(side: Side | None) -> dict:
    if side is None:
        return {"positions": {}}
    return {"positions": {"BTCUSDT": {"side": side.value.lower()}}}


def _build_wrapped_strategy(t_hold: int = 24) -> HTFContextStrategyAdapter:
    return HTFContextStrategyAdapter(
        inner=L1H1VolFloorTrendStrategy(timeframe="5m", theta_vol=0.0, k_atr=0.5, T_hold=t_hold),
        resampler=TimeframeResampler(timeframes=["5m"], strict=True),
    )


def test_stop_loss_monitored_on_1m_after_entry() -> None:
    strategy = _build_wrapped_strategy()

    entry_signal = None
    for i in range(300):
        signals = strategy.on_bars(_bar(i, 100 + i * 0.05).ts, {"BTCUSDT": _bar(i, 100 + i * 0.05)}, {"BTCUSDT"}, _ctx(None))
        if signals:
            entry_signal = signals[0]
            entry_minute = i
            break

    assert entry_signal is not None

    # Next minute (not a completed 5m signal bar): force a deep low to trigger ATR stop.
    crash_close = 100 + (entry_minute + 1) * 0.05
    crash_bar = _bar(entry_minute + 1, crash_close, low=crash_close - 10.0)
    exits = strategy.on_bars(crash_bar.ts, {"BTCUSDT": crash_bar}, {"BTCUSDT"}, _ctx(Side.BUY))
    assert exits
    assert exits[0].metadata.get("exit_reason") == "atr_stop"


def test_t_hold_is_counted_in_signal_bars() -> None:
    strategy = _build_wrapped_strategy(t_hold=2)

    entry_signal = None
    for i in range(300):
        bar = _bar(i, 100 + i * 0.05)
        signals = strategy.on_bars(bar.ts, {"BTCUSDT": bar}, {"BTCUSDT"}, _ctx(None))
        if signals:
            entry_signal = signals[0]
            entry_minute = i
            break

    assert entry_signal is not None

    # Hold for 9 minutes after entry (< 2 completed 5m bars), no time-stop yet.
    for minute in range(entry_minute + 1, entry_minute + 10):
        bar = _bar(minute, 115.0)
        exits = strategy.on_bars(bar.ts, {"BTCUSDT": bar}, {"BTCUSDT"}, _ctx(Side.BUY))
        assert not exits

    # At +10 minutes, second completed 5m signal bar closes -> time-stop.
    minute = entry_minute + 10
    bar = _bar(minute, 115.0)
    exits = strategy.on_bars(bar.ts, {"BTCUSDT": bar}, {"BTCUSDT"}, _ctx(Side.BUY))
    assert exits
    assert exits[0].metadata.get("exit_reason") == "time_stop"
