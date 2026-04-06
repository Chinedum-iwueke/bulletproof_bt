import pytest

from bt.hypotheses.l1_h6a import RollingQuantileGate, RollingStd, wvov_hours_to_bars


def test_wvov_hours_mapping_for_5m() -> None:
    assert wvov_hours_to_bars(timeframe="5m", wvov_hours=24) == 288
    assert wvov_hours_to_bars(timeframe="5m", wvov_hours=72) == 864


def test_rolling_std_and_quantile_gate_are_causal() -> None:
    std = RollingStd(window_bars=3)
    gate = RollingQuantileGate(lookback_bars=3, q=0.6)

    assert std.update(0.01) is None
    assert std.update(0.02) is None

    vov_2 = std.update(0.03)
    assert vov_2 == pytest.approx(0.0081649658)
    assert gate.update(vov_2) == (None, None)

    vov_3 = std.update(0.04)
    assert vov_3 == pytest.approx(0.0081649658)
    assert gate.update(vov_3) == (None, None)

    vov_4 = std.update(0.05)
    assert vov_4 == pytest.approx(0.0081649658)
    assert gate.update(vov_4) == (None, None)

    vov_5 = std.update(0.03)
    threshold, passed = gate.update(vov_5)
    assert threshold == pytest.approx(0.0081649658)
    assert passed is False


def test_rolling_std_handles_missing_values() -> None:
    std = RollingStd(window_bars=3)
    assert std.update(None) is None
