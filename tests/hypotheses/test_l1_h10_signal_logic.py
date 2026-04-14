from __future__ import annotations

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar
from bt.strategy.l1_h10a_mean_reversion_small_tp import L1H10AMeanReversionSmallTPStrategy
from bt.strategy.l1_h10b_breakout_scalping import L1H10BBreakoutScalpingStrategy


def _bar(i: int, close: float, *, high: float | None = None, low: float | None = None, symbol: str = "BTCUSDT") -> Bar:
    ts = pd.Timestamp("2024-01-01T00:00:00Z") + pd.Timedelta(minutes=i)
    h = close + 0.2 if high is None else high
    l = close - 0.2 if low is None else low
    return Bar(ts=ts, symbol=symbol, open=close, high=h, low=l, close=close, volume=1000.0)


def _ctx(signal_bar: Bar | None, timeframe: str, side: Side | None = None) -> dict:
    payload = {timeframe: {"BTCUSDT": signal_bar} if signal_bar is not None else {}}
    pos = {}
    if side is not None:
        pos = {"BTCUSDT": {"side": side.name.lower()}}
    return {"htf": payload, "positions": pos}


def test_h10a_requires_two_clock_context() -> None:
    strategy = L1H10AMeanReversionSmallTPStrategy(timeframe="5m")
    try:
        strategy.on_bars(_bar(0, 100).ts, {"BTCUSDT": _bar(0, 100)}, {"BTCUSDT"}, {})
    except RuntimeError as exc:
        assert "two-clock semantics" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


def test_h10a_entry_and_tp_exit_and_frozen_stop() -> None:
    strategy = L1H10AMeanReversionSmallTPStrategy(timeframe="5m", z0=0.8, tp_r=0.5, k_atr_stop=2.5)

    for i in range(30):
        b = _bar(i, 100.0 + (0.02 * i))
        strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b, "5m", None))

    entry_bar = _bar(31, 95.0)
    out = strategy.on_bars(entry_bar.ts, {"BTCUSDT": entry_bar}, {"BTCUSDT"}, _ctx(entry_bar, "5m", None))
    assert out and out[0].side == Side.BUY
    meta = out[0].metadata
    assert meta["setup_type"] == "mean_reversion_small_tp"
    assert meta["risk_accounting"] == "engine_canonical_R"
    frozen_stop = meta["stop_distance"]

    hold_bar = _bar(32, 95.1, high=95.2, low=95.05)
    strategy.on_bars(hold_bar.ts, {"BTCUSDT": hold_bar}, {"BTCUSDT"}, _ctx(_bar(32, 102.0), "5m", Side.BUY))
    assert strategy._state_for("BTCUSDT").stop_distance_frozen == frozen_stop

    tp_price = float(strategy._state_for("BTCUSDT").tp_price_frozen)
    tp_hit = _bar(33, 95.2, high=tp_price + 0.01, low=95.1)
    exits = strategy.on_bars(tp_hit.ts, {"BTCUSDT": tp_hit}, {"BTCUSDT"}, _ctx(None, "5m", Side.BUY))
    assert exits and exits[0].metadata["exit_reason"] == "take_profit"
    assert exits[0].metadata["tp_hit_flag"] is True


def test_h10b_entry_and_stop_exit_semantics() -> None:
    strategy = L1H10BBreakoutScalpingStrategy(timeframe="5m", breakout_atr=0.5, tp_r=0.5, k_atr_stop=2.5, adx_min_fixed=5)

    for i in range(35):
        b = _bar(i, 100.0 + (0.05 * i))
        strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b, "5m", None))

    prev = _bar(36, 100.0)
    strategy.on_bars(prev.ts, {"BTCUSDT": prev}, {"BTCUSDT"}, _ctx(prev, "5m", None))

    breakout = _bar(37, 106.0)
    out = strategy.on_bars(breakout.ts, {"BTCUSDT": breakout}, {"BTCUSDT"}, _ctx(breakout, "5m", None))
    assert out and out[0].side == Side.BUY
    assert out[0].metadata["breakout_reference_type"] == "prior_signal_close"

    st = strategy._state_for("BTCUSDT")
    stop = float(st.stop_price_frozen)
    stop_hit = _bar(38, 105.5, high=106.0, low=stop - 0.01)
    exits = strategy.on_bars(stop_hit.ts, {"BTCUSDT": stop_hit}, {"BTCUSDT"}, _ctx(None, "5m", Side.BUY))
    assert exits and exits[0].metadata["exit_reason"] == "atr_stop"
    assert exits[0].metadata["tp_hit_flag"] is False
