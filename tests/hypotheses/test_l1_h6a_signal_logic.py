import pandas as pd

from bt.core.types import Bar
from bt.strategy.l1_h6a_vov_gate_mean_reversion import L1H6AVovGateMeanReversionStrategy


def _bar(i: int, close: float, *, low: float | None = None, high: float | None = None) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=close if high is None else high, low=close if low is None else low, close=close, volume=1000)


def _ctx(signal_bar: Bar, *, side: str | None = None) -> dict:
    positions = {} if side is None else {"BTCUSDT": {"side": side}}
    return {"htf": {"5m": {"BTCUSDT": signal_bar}}, "positions": positions}


def _warm_ready(strategy: L1H6AVovGateMeanReversionStrategy) -> None:
    for i in range(20):
        b = _bar(i, 100.0, high=100.2, low=99.8)
        strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b))


def test_no_entry_before_gate_and_indicators_ready() -> None:
    strategy = L1H6AVovGateMeanReversionStrategy(timeframe="5m")
    seen = []
    for i in range(20):
        b = _bar(i, 100.0, high=100.2, low=99.8)
        seen.extend(strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b)))
    assert seen == []


def test_long_and_short_signals_respect_vov_gate() -> None:
    strategy = L1H6AVovGateMeanReversionStrategy(timeframe="5m", q_vov=0.6, z0=0.8)
    _warm_ready(strategy)
    st = strategy._state_for("BTCUSDT")
    st.vov_std._history.extend([0.01] * st.vov_std._history.maxlen)
    st.gate._history.extend([0.02] * st.gate._history.maxlen)

    long_bar = _bar(21, 98.0, high=98.2, low=97.8)
    out_long = strategy.on_bars(long_bar.ts, {"BTCUSDT": long_bar}, {"BTCUSDT"}, _ctx(long_bar))
    assert out_long
    assert out_long[0].metadata["vov_gate_t"] is True
    assert out_long[0].metadata["entry_reason"] == "vov_gate_fade_long"

    strategy2 = L1H6AVovGateMeanReversionStrategy(timeframe="5m", q_vov=0.6, z0=0.8)
    _warm_ready(strategy2)
    st2 = strategy2._state_for("BTCUSDT")
    st2.vov_std._history.extend([0.01] * st2.vov_std._history.maxlen)
    st2.gate._history.extend([0.02] * st2.gate._history.maxlen)

    short_bar = _bar(21, 102.0, high=102.2, low=101.8)
    out_short = strategy2.on_bars(short_bar.ts, {"BTCUSDT": short_bar}, {"BTCUSDT"}, _ctx(short_bar))
    assert out_short
    assert out_short[0].metadata["entry_reason"] == "vov_gate_fade_short"


def test_no_signal_when_vov_gate_fails() -> None:
    strategy = L1H6AVovGateMeanReversionStrategy(timeframe="5m", q_vov=0.6, z0=0.8)
    _warm_ready(strategy)
    st = strategy._state_for("BTCUSDT")
    st.vov_std._history.extend([0.01] * st.vov_std._history.maxlen)
    st.gate._history.extend([0.00001] * st.gate._history.maxlen)

    blocked = _bar(21, 98.0, high=102.0, low=94.0)
    out = strategy.on_bars(blocked.ts, {"BTCUSDT": blocked}, {"BTCUSDT"}, _ctx(blocked))
    assert out == []
