import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar
from bt.data.resample import TimeframeResampler
from bt.strategy.htf_context import HTFContextStrategyAdapter
from bt.strategy.l1_h3b_har_rv_gate_mean_reversion import L1H3BHarRVGateMeanReversionStrategy


def _bar(i: int, close: float, *, low: float | None = None, high: float | None = None) -> Bar:
    ts = pd.Timestamp("2023-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=close + 0.2 if high is None else high, low=close - 0.2 if low is None else low, close=close, volume=1000)


def _ctx(side: Side | None) -> dict:
    if side is None:
        return {"positions": {}}
    return {"positions": {"BTCUSDT": {"side": side.value.lower()}}}


def _build() -> HTFContextStrategyAdapter:
    return HTFContextStrategyAdapter(
        inner=L1H3BHarRVGateMeanReversionStrategy(timeframe="5m", gate_quantile_low=1.0, fit_window_days=30, z0=0.0, k=1.5, T_hold=2),
        resampler=TimeframeResampler(timeframes=["5m"], strict=True),
    )


def _first_entry(strategy: HTFContextStrategyAdapter) -> tuple[object, int]:
    for i in range(50000):
        bar = _bar(i, 100 + i * 0.0005)
        signals = strategy.on_bars(bar.ts, {"BTCUSDT": bar}, {"BTCUSDT"}, _ctx(None))
        if signals:
            return signals[0], i
    raise AssertionError("expected entry")


def test_l1_h3b_frozen_stop_does_not_recompute() -> None:
    strategy = _build()
    entry, minute = _first_entry(strategy)
    frozen_stop = float(entry.metadata["stop_price"])
    stop_distance = float(entry.metadata["stop_distance"])
    side = entry.side
    entry_close = frozen_stop + stop_distance if side == Side.BUY else frozen_stop - stop_distance

    for i in range(minute + 1, minute + 8):
        hold_bar = _bar(i, entry_close, low=frozen_stop + 0.001) if side == Side.BUY else _bar(i, entry_close, high=frozen_stop - 0.001)
        exits = strategy.on_bars(hold_bar.ts, {"BTCUSDT": hold_bar}, {"BTCUSDT"}, _ctx(side))
        assert not exits

    crash = _bar(minute + 8, entry_close, low=frozen_stop - 0.01) if side == Side.BUY else _bar(minute + 8, entry_close, high=frozen_stop + 0.01)
    exits = strategy.on_bars(crash.ts, {"BTCUSDT": crash}, {"BTCUSDT"}, _ctx(side))
    assert exits
    assert exits[0].metadata["exit_reason"] == "rvhat_stop"
    assert float(exits[0].metadata["stop_price"]) == frozen_stop


def test_l1_h3b_time_stop_counts_signal_bars_only() -> None:
    strategy = _build()
    entry, minute = _first_entry(strategy)
    frozen_stop = float(entry.metadata["stop_price"])
    stop_distance = float(entry.metadata["stop_distance"])
    side = entry.side
    entry_close = frozen_stop + stop_distance if side == Side.BUY else frozen_stop - stop_distance

    for i in range(minute + 1, minute + 7):
        bar = _bar(i, entry_close, low=frozen_stop + 0.001) if side == Side.BUY else _bar(i, entry_close, high=frozen_stop - 0.001)
        exits = strategy.on_bars(bar.ts, {"BTCUSDT": bar}, {"BTCUSDT"}, _ctx(side))
        assert not exits

    observed = []
    for i in range(minute + 7, minute + 30):
        bar = _bar(i, entry_close, low=frozen_stop + 0.001) if side == Side.BUY else _bar(i, entry_close, high=frozen_stop - 0.001)
        exits = strategy.on_bars(bar.ts, {"BTCUSDT": bar}, {"BTCUSDT"}, _ctx(side))
        if exits:
            observed = exits
            break
    assert observed
    assert observed[0].metadata["exit_reason"] == "time_stop"
