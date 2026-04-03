import pandas as pd

from bt.core.types import Bar
from bt.strategy.l1_h5b_vol_managed_har_trend import L1H5BVolManagedHarTrendStrategy


def _bar(i: int, close: float, *, low: float, high: float) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=high, low=low, close=close, volume=1000)


def test_l1_h5b_entry_logging_fields_present() -> None:
    strategy = L1H5BVolManagedHarTrendStrategy(gate_quantile=0.0, fit_window_days=30)
    for i in range(7000):
        b = _bar(i * 15, 100.0 + i * 0.005, high=100.2 + i * 0.005, low=99.8 + i * 0.005)
        out = strategy.on_bars(
            b.ts,
            {"BTCUSDT": b},
            {"BTCUSDT"},
            {"htf": {"15m": {"BTCUSDT": b}}, "positions": {}, "equity": 100000.0, "risk": {"r_per_trade": 0.01}},
        )
        if out:
            md = out[0].metadata
            for key in (
                "entry_reason",
                "sigma_t",
                "sigma_star",
                "s_t",
                "qty_R",
                "qty_final",
                "cap_hit_lower",
                "cap_hit_upper",
                "RV_hat_t",
                "rvhat_pct_t",
                "fit_ts_used",
                "signal_timeframe",
                "exit_monitoring_timeframe",
            ):
                assert key in md
            return
    raise AssertionError("expected entry")
