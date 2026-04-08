import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar
from bt.strategy.l1_h7_squeeze_expansion_pullback import L1H7SqueezeExpansionPullbackStrategy


def _bar(i: int, close: float, *, low: float | None = None) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=close + 0.2, low=(close - 0.2 if low is None else low), close=close, volume=1000)


def _ctx(signal_bar: Bar) -> dict:
    return {"htf": {"15m": {"BTCUSDT": signal_bar}}, "positions": {}}


def test_entry_metadata_contains_family_logging_fields() -> None:
    s = L1H7SqueezeExpansionPullbackStrategy(timeframe="15m", squeeze_min_bars=1, adx_min=0.0, pullback_ema_period=3, pullback_use_session_vwap=False)
    for i in range(30):
        b = _bar(i, 100.0)
        s.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b))

    st = s._state_for("BTCUSDT")
    st.squeeze_qualified = True
    b1 = _bar(31, 105.0)
    s.on_bars(b1.ts, {"BTCUSDT": b1}, {"BTCUSDT"}, _ctx(b1))
    st.expansion_direction = Side.BUY
    b2 = _bar(32, 104.9, low=101.0)
    out = s.on_bars(b2.ts, {"BTCUSDT": b2}, {"BTCUSDT"}, _ctx(b2))
    assert out
    meta = out[0].metadata
    for key in [
        "entry_reason",
        "squeeze_on",
        "squeeze_duration",
        "expansion_trigger_state",
        "breakout_direction",
        "directional_bias",
        "signal_timeframe",
        "exit_monitoring_timeframe",
        "family_variant",
        "risk_accounting",
        "stop_details",
    ]:
        assert key in meta
    assert meta["risk_accounting"] == "engine_canonical_R"
