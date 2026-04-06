import pandas as pd

from bt.core.types import Bar
from bt.strategy.l1_h6a_vov_gate_mean_reversion import L1H6AVovGateMeanReversionStrategy


def _bar(i: int, close: float) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=close, low=close, close=close, volume=1000)


def test_entry_logging_fields_present() -> None:
    strategy = L1H6AVovGateMeanReversionStrategy(timeframe="5m")
    for i in range(20):
        b = _bar(i, 100.0)
        strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, {"htf": {"5m": {"BTCUSDT": b}}, "positions": {}})

    st = strategy._state_for("BTCUSDT")
    st.vov_std._history.extend([0.01] * st.vov_std._history.maxlen)
    st.gate._history.extend([0.02] * st.gate._history.maxlen)
    b = _bar(21, 98.0)
    out = strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, {"htf": {"5m": {"BTCUSDT": b}}, "positions": {}})
    md = out[0].metadata
    for key in (
        "rv_t", "vov_t", "Wvov_hours", "q_vov", "vov_gate_t", "session_vwap_t", "z_vwap_t", "entry_reason", "atr_entry", "stop_distance", "stop_price", "signal_timeframe", "exit_monitoring_timeframe", "vwap_mode"
    ):
        assert key in md
