import pytest

from bt.hypotheses.l1_h1 import RollingPercentileGate, bars_for_30_calendar_days


def test_timeframe_mapping_for_30_days() -> None:
    assert bars_for_30_calendar_days("5m") == 8640
    assert bars_for_30_calendar_days("15m") == 2880
    assert bars_for_30_calendar_days("1h") == 720


def test_timeframe_mapping_rejects_unknown() -> None:
    with pytest.raises(ValueError):
        bars_for_30_calendar_days("4h")


def test_percentile_gate_is_past_only() -> None:
    gate = RollingPercentileGate(lookback_bars=3, include_current=True)
    assert gate.update(1.0) is None
    assert gate.update(2.0) == pytest.approx(1.0)
    assert gate.update(3.0) == pytest.approx(1.0)
