from __future__ import annotations

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar
from bt.strategy.l1_h11_quality_filtered_continuation import L1H11QualityFilteredContinuationStrategy


def _bar(i: int, close: float, *, high: float | None = None, low: float | None = None) -> Bar:
    ts = pd.Timestamp("2024-01-01T00:00:00Z") + pd.Timedelta(minutes=i)
    return Bar(
        ts=ts,
        symbol="BTCUSDT",
        open=close,
        high=close + 0.2 if high is None else high,
        low=close - 0.2 if low is None else low,
        close=close,
        volume=1000,
    )


def _ctx(tf: str, signal_bar: Bar | None, side: Side | None = None) -> dict:
    payload = {tf: {"BTCUSDT": signal_bar} if signal_bar is not None else {}}
    positions = {} if side is None else {"BTCUSDT": {"side": side.name.lower()}}
    return {"htf": payload, "positions": positions}


def test_h11_requires_two_clock_context() -> None:
    strategy = L1H11QualityFilteredContinuationStrategy(timeframe="15m")
    try:
        strategy.on_bars(_bar(0, 100).ts, {"BTCUSDT": _bar(0, 100)}, {"BTCUSDT"}, {})
    except RuntimeError as exc:
        assert "two-clock semantics" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


def test_h11a_entry_and_frozen_stop_logging_fields() -> None:
    strategy = L1H11QualityFilteredContinuationStrategy(timeframe="15m", family_variant="L1-H11A", adx_min=0.0, impulse_min_atr_fixed=0.1)

    for i in range(80):
        b = _bar(i, 100 + (0.06 * i), high=100 + (0.06 * i) + 0.4, low=100 + (0.06 * i) - 0.3)
        strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx("15m", b))

    pull = _bar(90, 102.0, high=102.2, low=100.8)
    strategy.on_bars(pull.ts, {"BTCUSDT": pull}, {"BTCUSDT"}, _ctx("15m", pull))
    st = strategy._state_for("BTCUSDT")
    st.trend_dir = Side.BUY
    st.trend_anchor_price = 100.0
    st.trend_extreme_price = 106.0
    st.pullback_active = True
    st.pullback_extreme_low = 103.5
    st.pullback_extreme_high = 106.0
    reclaim = _bar(91, 105.0, high=105.3, low=104.9)
    out = strategy.on_bars(reclaim.ts, {"BTCUSDT": reclaim}, {"BTCUSDT"}, _ctx("15m", reclaim))
    assert out and out[0].side == Side.BUY
    meta = out[0].metadata
    for key in [
        "setup_type", "signal_timeframe", "exit_monitoring_timeframe", "trend_dir", "ema_fast_entry", "ema_slow_entry",
        "adx_entry", "atr_entry", "impulse_strength_atr", "swing_distance_atr", "pullback_depth_atr", "pull_entry_atr_low",
        "pull_entry_atr_high", "entry_position_metric", "reclaim_position_metric", "entry_reference_price", "stop_distance",
        "stop_price", "continuation_trigger_state", "risk_accounting",
    ]:
        assert key in meta
    frozen = meta["stop_distance"]
    hold = _bar(92, 104.6, high=104.7, low=104.4)
    strategy.on_bars(hold.ts, {"BTCUSDT": hold}, {"BTCUSDT"}, _ctx("15m", hold, Side.BUY))
    assert strategy._state_for("BTCUSDT").stop_distance_frozen == frozen


def test_h11c_lock_and_vwap_giveback_exit() -> None:
    strategy = L1H11QualityFilteredContinuationStrategy(
        timeframe="15m",
        family_variant="L1-H11C",
        adx_min_fixed=0.0,
        impulse_min_atr_fixed=0.1,
        stop_padding_atr=0.25,
        lock_r=0.5,
        vwap_giveback="on",
        pull_entry_atr_low=0.1,
        pull_entry_atr_high=4.0,
    )
    for i in range(80):
        b = _bar(i, 100 + (0.08 * i), high=100 + (0.08 * i) + 0.4, low=100 + (0.08 * i) - 0.3)
        strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx("15m", b))
    pull = _bar(90, 102.0, high=102.3, low=100.8)
    strategy.on_bars(pull.ts, {"BTCUSDT": pull}, {"BTCUSDT"}, _ctx("15m", pull))
    st = strategy._state_for("BTCUSDT")
    st.trend_dir = Side.BUY
    st.trend_anchor_price = 100.0
    st.trend_extreme_price = 106.0
    st.pullback_active = True
    st.pullback_extreme_low = 103.5
    st.pullback_extreme_high = 106.0
    entry = _bar(91, 110.0, high=110.2, low=109.8)
    out = strategy.on_bars(entry.ts, {"BTCUSDT": entry}, {"BTCUSDT"}, _ctx("15m", entry))
    assert out

    st = strategy._state_for("BTCUSDT")
    up = _bar(92, 113.0, high=116.0, low=112.8)
    strategy.on_bars(up.ts, {"BTCUSDT": up}, {"BTCUSDT"}, _ctx("15m", up, Side.BUY))
    assert st.lock_armed is True

    drop = _bar(93, 98.0, high=98.2, low=97.8)
    exits = strategy.on_bars(drop.ts, {"BTCUSDT": drop}, {"BTCUSDT"}, _ctx("15m", drop, Side.BUY))
    assert exits
    assert exits[0].metadata["exit_reason"] in {"vwap_giveback", "stop_loss"}
