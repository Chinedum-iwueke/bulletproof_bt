import pandas as pd

from bt.indicators.har_rv import HarRVForecaster


def test_l1_h3b_refit_is_daily_and_past_only() -> None:
    f = HarRVForecaster(timeframe="5m", fit_window_days=180)
    start = pd.Timestamp("2023-01-01", tz="UTC")
    for i in range(288 * 240):
        f.update(start + pd.Timedelta(minutes=5 * i), 100 + 0.0005 * i)

    assert f.fit_history
    fit_days = [row.fit_ts.normalize() for row in f.fit_history]
    assert len(fit_days) == len(set(fit_days))
    assert all(row.train_end_ts == row.fit_ts for row in f.fit_history)
    assert all(row.train_start_ts < row.train_end_ts for row in f.fit_history)


def test_l1_h3b_forecast_uses_latest_prior_fit_timestamp() -> None:
    f = HarRVForecaster(timeframe="5m", fit_window_days=30)
    start = pd.Timestamp("2023-01-01", tz="UTC")
    seen_fit_used = None
    for i in range(288 * 80):
        payload = f.update(start + pd.Timedelta(minutes=5 * i), 100 + 0.0001 * i)
        if payload["fit_ts_used"] is not None:
            seen_fit_used = pd.Timestamp(str(payload["fit_ts_used"]))
            assert seen_fit_used < start + pd.Timedelta(minutes=5 * i)
            break
    assert seen_fit_used is not None
