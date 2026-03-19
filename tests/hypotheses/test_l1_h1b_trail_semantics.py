import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar
from bt.data.resample import TimeframeResampler
from bt.strategy.htf_context import HTFContextStrategyAdapter
from bt.strategy.l1_h1b_salvage import L1H1BSalvageStrategy


def _bar(i: int, close: float, *, low: float | None = None, high: float | None = None) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=close + 0.2 if high is None else high, low=close - 0.2 if low is None else low, close=close, volume=1000)


def _ctx(side: Side | None) -> dict:
    if side is None:
        return {"positions": {}}
    return {"positions": {"BTCUSDT": {"side": side.value.lower()}}}


def _build_wrapped_strategy(**kwargs) -> HTFContextStrategyAdapter:
    return HTFContextStrategyAdapter(
        inner=L1H1BSalvageStrategy(timeframe="5m", theta_vol=0.0, **kwargs),
        resampler=TimeframeResampler(timeframes=["5m"], strict=True),
    )


def _first_entry(strategy: HTFContextStrategyAdapter) -> tuple[object, int]:
    for i in range(300):
        sig = strategy.on_bars(_bar(i, 100 + i * 0.05).ts, {"BTCUSDT": _bar(i, 100 + i * 0.05)}, {"BTCUSDT"}, _ctx(None))
        if sig:
            return sig[0], i
    raise AssertionError("expected entry")


def test_trail_activates_on_bar_survival_and_ratchets_for_long() -> None:
    strategy = _build_wrapped_strategy(k_atr_entry_stop=0.5, T_hold=100, chandelier_lookback=2, chandelier_atr_mult=1.0, trail_activation_mode="bars", trail_activate_after_bars=1)
    entry, minute = _first_entry(strategy)
    assert entry.metadata["entry_reason"] == "vol_floor_trend_long"
    stop = float(entry.metadata["stop_price"])

    # before first signal bar completes, no activation-triggered stop hit
    out = strategy.on_bars(_bar(minute + 1, 120.0, low=stop + 0.1, high=122.0).ts, {"BTCUSDT": _bar(minute + 1, 120.0, low=stop + 0.1, high=122.0)}, {"BTCUSDT"}, _ctx(Side.BUY))
    assert not out

    # at next signal boundary, trail active and should never loosen (high spike then pullback)
    for k, price in enumerate([123.0, 121.0, 124.0], start=2):
        out = strategy.on_bars(_bar(minute + k * 5, price, high=price + 1.0, low=price - 0.1).ts, {"BTCUSDT": _bar(minute + k * 5, price, high=price + 1.0, low=price - 0.1)}, {"BTCUSDT"}, _ctx(Side.BUY))
        assert not out

    # force stop at a high tightened level, should attribute to chandelier
    exit_bar = _bar(minute + 16, 120.0, high=121.0, low=90.0)
    exits = strategy.on_bars(exit_bar.ts, {"BTCUSDT": exit_bar}, {"BTCUSDT"}, _ctx(Side.BUY))
    assert exits
    assert exits[0].metadata["exit_reason"] in {"stop_chandelier", "stop_initial"}


def test_profit_r_activation_mode_blocks_early_activation() -> None:
    strategy = _build_wrapped_strategy(k_atr_entry_stop=1.0, T_hold=100, chandelier_lookback=2, chandelier_atr_mult=1.0, trail_activation_mode="profit_r", trail_activate_after_profit_r=1.0)
    entry, minute = _first_entry(strategy)
    stop = float(entry.metadata["stop_price"])
    stop_dist = float(entry.metadata["stop_distance"])
    weak_close = stop + (1.1 * stop_dist)
    weak_high = stop + (1.2 * stop_dist)

    # insufficient profit (<1R), should still be initial stop if hit
    weak = _bar(minute + 5, weak_close, high=weak_high, low=stop - 0.01)
    exits = strategy.on_bars(weak.ts, {"BTCUSDT": weak}, {"BTCUSDT"}, _ctx(Side.BUY))
    assert exits
    assert exits[0].metadata["exit_reason"] == "stop_initial"
