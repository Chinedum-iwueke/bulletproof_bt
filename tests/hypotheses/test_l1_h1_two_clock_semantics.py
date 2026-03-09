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


def _build_wrapped_strategy(t_hold: int = 24, *, tp_enabled: bool = False) -> HTFContextStrategyAdapter:
    return HTFContextStrategyAdapter(
        inner=L1H1VolFloorTrendStrategy(timeframe="5m", theta_vol=0.0, k_atr=0.5, T_hold=t_hold, tp_enabled=tp_enabled, m_atr=1.0),
        resampler=TimeframeResampler(timeframes=["5m"], strict=True),
    )


def _first_entry(strategy: HTFContextStrategyAdapter) -> tuple[object, int]:
    for i in range(300):
        bar = _bar(i, 100 + i * 0.05)
        signals = strategy.on_bars(bar.ts, {"BTCUSDT": bar}, {"BTCUSDT"}, _ctx(None))
        if signals:
            return signals[0], i
    raise AssertionError("expected entry signal")


def test_frozen_atr_stop_does_not_recompute() -> None:
    strategy = _build_wrapped_strategy()
    entry_signal, entry_minute = _first_entry(strategy)
    frozen_stop = float(entry_signal.metadata["stop_price"])

    # Advance enough bars to change indicator ATR; fixed stop price must still be used.
    for i in range(entry_minute + 1, entry_minute + 10):
        bar = _bar(i, 150 + i, low=149 + i)
        exits = strategy.on_bars(bar.ts, {"BTCUSDT": bar}, {"BTCUSDT"}, _ctx(Side.BUY))
        assert not exits

    crash = _bar(entry_minute + 10, 300.0, low=frozen_stop - 0.01)
    exits = strategy.on_bars(crash.ts, {"BTCUSDT": crash}, {"BTCUSDT"}, _ctx(Side.BUY))
    assert exits
    assert exits[0].metadata["exit_reason"] == "atr_stop"
    assert float(exits[0].metadata["stop_price"]) == frozen_stop


def test_frozen_tp_does_not_recompute() -> None:
    strategy = _build_wrapped_strategy(tp_enabled=True)
    entry_signal, entry_minute = _first_entry(strategy)
    frozen_tp = float(entry_signal.metadata["tp_price"])
    frozen_stop = float(entry_signal.metadata["stop_price"])

    stable_close = frozen_stop + 0.05
    for i in range(entry_minute + 1, entry_minute + 10):
        bar = _bar(i, stable_close, high=frozen_tp - 0.02, low=frozen_stop + 0.02)
        exits = strategy.on_bars(bar.ts, {"BTCUSDT": bar}, {"BTCUSDT"}, _ctx(Side.BUY))
        assert not exits

    tp_hit = _bar(entry_minute + 10, stable_close, high=frozen_tp + 0.01, low=frozen_stop + 0.02)
    exits = strategy.on_bars(tp_hit.ts, {"BTCUSDT": tp_hit}, {"BTCUSDT"}, _ctx(Side.BUY))
    assert exits
    assert exits[0].metadata["exit_reason"] == "take_profit"
    assert float(exits[0].metadata["tp_price"]) == frozen_tp


def test_signal_timeframe_atr_is_used_for_stop_sourcing() -> None:
    strategy = _build_wrapped_strategy()
    entry_signal, _ = _first_entry(strategy)
    assert entry_signal.metadata["atr_source_timeframe"] == "signal_timeframe"
    assert entry_signal.metadata["stop_model"] == "fixed_atr_multiple"


def test_stop_loss_monitored_on_1m_after_entry() -> None:
    strategy = _build_wrapped_strategy()

    entry_signal, entry_minute = _first_entry(strategy)
    assert entry_signal is not None

    crash_close = 100 + (entry_minute + 1) * 0.05
    crash_bar = _bar(entry_minute + 1, crash_close, low=float(entry_signal.metadata["stop_price"]) - 0.01)
    exits = strategy.on_bars(crash_bar.ts, {"BTCUSDT": crash_bar}, {"BTCUSDT"}, _ctx(Side.BUY))
    assert exits
    assert exits[0].metadata.get("exit_reason") == "atr_stop"
    assert exits[0].metadata.get("exit_monitoring_timeframe") == "1m"


def test_t_hold_is_counted_in_signal_bars() -> None:
    strategy = _build_wrapped_strategy(t_hold=2)

    entry_signal, entry_minute = _first_entry(strategy)
    assert entry_signal is not None

    for minute in range(entry_minute + 1, entry_minute + 10):
        bar = _bar(minute, 115.0)
        exits = strategy.on_bars(bar.ts, {"BTCUSDT": bar}, {"BTCUSDT"}, _ctx(Side.BUY))
        assert not exits

    minute = entry_minute + 10
    bar = _bar(minute, 115.0)
    exits = strategy.on_bars(bar.ts, {"BTCUSDT": bar}, {"BTCUSDT"}, _ctx(Side.BUY))
    assert exits
    assert exits[0].metadata.get("exit_reason") == "time_stop"
    assert exits[0].metadata.get("hold_time_unit") == "signal_bars"
