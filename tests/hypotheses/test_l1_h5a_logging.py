import pandas as pd

from bt.core.types import Bar
from bt.strategy.l1_h5a_vol_managed_trend import L1H5AVolManagedTrendStrategy


def _bar(i: int, close: float, *, low: float, high: float) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=high, low=low, close=close, volume=1000)


def test_entry_logging_fields_present() -> None:
    strategy = L1H5AVolManagedTrendStrategy(timeframe="15m")
    for i in range(420):
        b = _bar(i * 15, 100.0 + i * 0.05, high=100.3 + i * 0.05, low=99.7 + i * 0.05)
        strategy.on_bars(
            b.ts,
            {"BTCUSDT": b},
            {"BTCUSDT"},
            {"htf": {"15m": {"BTCUSDT": b}}, "positions": {}, "equity": 100000.0, "risk": {"r_per_trade": 0.01}},
        )

    b = _bar(9000, 170.0, high=170.3, low=169.7)
    out = strategy.on_bars(
        b.ts,
        {"BTCUSDT": b},
        {"BTCUSDT"},
        {"htf": {"15m": {"BTCUSDT": b}}, "positions": {}, "equity": 100000.0, "risk": {"r_per_trade": 0.01}},
    )
    md = out[0].metadata
    for key in (
        "entry_reason",
        "sigma_t",
        "sigma_star",
        "size_factor_t",
        "vol_window_hours",
        "qty_R",
        "qty_final",
        "cap_hit_lower",
        "cap_hit_upper",
        "signal_timeframe",
        "exit_monitoring_timeframe",
    ):
        assert key in md
