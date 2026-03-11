import pandas as pd

from bt.indicators.har_rv import HarRVForecaster, bars_per_day, rv1_from_close


def test_l1_h3b_rv1_definition() -> None:
    rv = rv1_from_close(100.0, 101.0)
    assert rv is not None
    assert rv > 0.0


def test_l1_h3b_5m_window_definitions_and_warmup() -> None:
    assert bars_per_day("5m") == 288
    f = HarRVForecaster(timeframe="5m", fit_window_days=180)
    start = pd.Timestamp("2023-01-01", tz="UTC")
    payload = {}
    for i in range(8640):
        payload = f.update(start + pd.Timedelta(minutes=5 * i), 100 + 0.001 * i)
    assert payload["rv_m"] is None
    payload = f.update(start + pd.Timedelta(minutes=5 * 8640), 100 + 0.001 * 8640)
    assert payload["rv_m"] is not None
