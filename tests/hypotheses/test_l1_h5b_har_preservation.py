import pandas as pd

from bt.core.types import Bar
from bt.strategy.l1_h5b_vol_managed_har_trend import L1H5BVolManagedHarTrendStrategy


def _bar(i: int, close: float) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(ts=ts, symbol="BTCUSDT", open=close, high=close + 0.2, low=close - 0.2, close=close, volume=1000)


def test_l1_h5b_har_artifacts_preserve_refit_manifest() -> None:
    strategy = L1H5BVolManagedHarTrendStrategy(gate_quantile=0.0, fit_window_days=30)
    for i in range(7000):
        b = _bar(i * 15, 100 + i * 0.005)
        strategy.on_bars(
            b.ts,
            {"BTCUSDT": b},
            {"BTCUSDT"},
            {"htf": {"15m": {"BTCUSDT": b}}, "positions": {}},
        )
    artifacts = strategy.strategy_artifacts()
    assert artifacts["har_coefficients"]["refit_cadence"] == "daily_on_completed_signal_day"
    assert artifacts["har_split_manifest"]["walk_forward"] == "rolling_window_past_only"
    assert "BTCUSDT" in artifacts["har_coefficients"]["rows"]
