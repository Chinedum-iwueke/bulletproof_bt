import pandas as pd

from bt.core.types import Bar
from bt.strategy.l1_h4a_liquidity_gate_mean_reversion import L1H4ALiquidityGateMeanReversionStrategy


def _bar(i: int, close: float, *, low: float, high: float) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=high, low=low, close=close, volume=1000)


def test_entry_logging_fields_present() -> None:
    strategy = L1H4ALiquidityGateMeanReversionStrategy(timeframe="5m")
    for i in range(20):
        b = _bar(i, 100.0, high=100.2, low=99.8)
        strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, {"htf": {"5m": {"BTCUSDT": b}}, "positions": {}})

    st = strategy._state_for("BTCUSDT")
    st.gate._history.extend([0.02] * st.gate._history.maxlen)
    b = _bar(21, 98.0, high=98.2, low=97.8)
    out = strategy.on_bars(
        b.ts,
        {"BTCUSDT": b},
        {"BTCUSDT"},
        {
            "htf": {"5m": {"BTCUSDT": b}},
            "positions": {},
            "execution": {"profile": "tier2", "spread_bps": 2.0, "slippage_bps": 3.0},
        },
    )
    md = out[0].metadata
    for key in (
        "spread_proxy_t",
        "liq_gate_t",
        "q_liq",
        "q_threshold_t",
        "vwap_t",
        "z_vwap_t",
        "entry_reason",
        "atr_entry",
        "stop_distance",
        "stop_price",
        "signal_timeframe",
        "exit_monitoring_timeframe",
        "effective_spread_assumptions",
    ):
        assert key in md
    assert md["effective_spread_assumptions"] == {"profile": "tier2", "spread_bps": 2.0, "slippage_bps": 3.0}
