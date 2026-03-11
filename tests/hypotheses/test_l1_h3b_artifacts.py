import pandas as pd

from bt.strategy.l1_h3b_har_rv_gate_mean_reversion import L1H3BHarRVGateMeanReversionStrategy


def test_l1_h3b_strategy_artifacts_include_coefficients_and_split_manifest() -> None:
    strategy = L1H3BHarRVGateMeanReversionStrategy(timeframe="5m", fit_window_days=180)
    st = strategy._state_for("BTCUSDT")
    start = pd.Timestamp("2023-01-01", tz="UTC")
    for i in range(288 * 220):
        st.rv_forecaster.update(start + pd.Timedelta(minutes=5 * i), 100 + 0.001 * i)
    payload = strategy.strategy_artifacts()
    assert "har_coefficients" in payload
    assert "har_split_manifest" in payload
    assert "BTCUSDT" in payload["har_coefficients"]["rows"]
