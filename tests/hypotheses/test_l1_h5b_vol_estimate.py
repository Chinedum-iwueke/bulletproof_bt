import pytest

from bt.hypotheses.l1_h5b import RollingMedianReference, RollingRmsVolatility, vol_window_bars


def test_l1_h5b_rms_volatility_warmup_and_value() -> None:
    vol = RollingRmsVolatility(lookback_bars=3)
    assert vol.update(0.1) is None
    assert vol.update(0.2) is None
    sigma = vol.update(0.3)
    assert sigma == pytest.approx(((0.1**2 + 0.2**2 + 0.3**2) / 3.0) ** 0.5)


def test_l1_h5b_sigma_star_rolling_median_is_past_only() -> None:
    ref = RollingMedianReference(lookback_bars=3)
    assert ref.update(0.01) is None
    assert ref.update(0.03) is None
    assert ref.update(0.02) is None
    assert ref.update(0.04) == pytest.approx(0.02)


def test_l1_h5b_vol_window_hours_15m_mapping() -> None:
    assert vol_window_bars(timeframe="15m", hours=24) == 96
    assert vol_window_bars(timeframe="15m", hours=72) == 288
