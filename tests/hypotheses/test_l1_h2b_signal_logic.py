import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar
from bt.strategy.l1_h2b_confirmed_fade import L1H2BConfirmedFadeStrategy


def _bar(i: int, close: float, *, high: float | None = None, low: float | None = None) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=close + 0.2 if high is None else high, low=close - 0.2 if low is None else low, close=close, volume=1000)


def _ctx(signal_bar: Bar, side: Side | None = None) -> dict:
    positions = {} if side is None else {"BTCUSDT": {"side": side.value.lower()}}
    return {"positions": positions, "htf": {"5m": {"BTCUSDT": signal_bar}}}


def test_entry_requires_extension_then_reentry_not_raw_extension() -> None:
    strategy = L1H2BConfirmedFadeStrategy(timeframe="5m", q_comp=1.0, z_ext=0.6, z_reentry=0.2, k_atr=1.5, T_hold=8)
    st = strategy._state_for("BTCUSDT")
    st.gate._history.extend([0.05] * st.gate._history.maxlen)

    # warmup ATR / VWAP gate
    for i in range(300):
        bar = _bar(i, 100.0 + i * 0.01)
        out = strategy.on_bars(bar.ts, {"BTCUSDT": bar}, {"BTCUSDT"}, _ctx(bar, None))
        assert not out

    # extension-only bar should arm but not enter
    ext = _bar(301, 120.0)
    out = strategy.on_bars(ext.ts, {"BTCUSDT": ext}, {"BTCUSDT"}, _ctx(ext, None))
    assert not out

    # inward re-entry confirmation should now trigger fade short
    reentry = _bar(302, 98.0)
    out = strategy.on_bars(reentry.ts, {"BTCUSDT": reentry}, {"BTCUSDT"}, _ctx(reentry, None))
    assert out
    assert out[0].signal_type == "l1_h2b_confirmed_fade"
    assert out[0].side == Side.SELL
    assert out[0].metadata["extension_armed"] is True
    assert out[0].metadata["reentry_confirmed"] is True


def test_vwap_touch_exit_reason_is_emitted() -> None:
    strategy = L1H2BConfirmedFadeStrategy(timeframe="5m", q_comp=1.0, z_ext=0.6, z_reentry=0.2, k_atr=1.5, T_hold=8)
    st = strategy._state_for("BTCUSDT")
    st.gate._history.extend([0.05] * st.gate._history.maxlen)

    for i in range(300):
        bar = _bar(i, 100.0 + i * 0.01)
        strategy.on_bars(bar.ts, {"BTCUSDT": bar}, {"BTCUSDT"}, _ctx(bar, None))

    ext = _bar(301, 120.0)
    strategy.on_bars(ext.ts, {"BTCUSDT": ext}, {"BTCUSDT"}, _ctx(ext, None))
    reentry = _bar(302, 98.0)
    entry = strategy.on_bars(reentry.ts, {"BTCUSDT": reentry}, {"BTCUSDT"}, _ctx(reentry, None))
    assert entry and entry[0].side == Side.SELL

    # move into VWAP quickly to force touch exit
    touch = _bar(303, 100.0)
    exits = strategy.on_bars(touch.ts, {"BTCUSDT": touch}, {"BTCUSDT"}, _ctx(touch, Side.SELL))
    assert exits
    assert exits[0].metadata["exit_reason"] in {"vwap_touch", "time_stop", "stop_initial"}
