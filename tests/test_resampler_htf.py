from __future__ import annotations

import pandas as pd
import pytest

from bt.core.types import Bar
from bt.data.resample import TimeframeResampler, normalize_timeframe, timeframe_minutes
from bt.data.timeframe_utils import is_timeframe_boundary


@pytest.mark.parametrize("timeframe", ["2m", "7m", "10m", "12m", "2h", "25h", "2d"])
def test_arbitrary_duration_complete_bucket_and_boundary(timeframe):
    minutes = timeframe_minutes(timeframe)
    start = pd.Timestamp("2025-01-01T23:59:00Z").floor(f"{minutes}min")
    resampler = TimeframeResampler([timeframe])
    for offset in range(minutes):
        assert resampler.update(_bar(start + pd.Timedelta(minutes=offset), 100 + offset)) == []
    emitted = resampler.update(_bar(start + pd.Timedelta(minutes=minutes), 999))
    assert len(emitted) == 1
    assert emitted[0].n_bars == minutes
    assert emitted[0].close == 100 + minutes - 1
    assert emitted[0].ts == start
    assert emitted[0].is_complete
    assert is_timeframe_boundary(start, timeframe)
    assert not is_timeframe_boundary(start + pd.Timedelta(minutes=1), timeframe)
    assert emitted[0].metadata["availability_policy"] == "next_bucket_input"


@pytest.mark.parametrize("value", ["30s", "60s", "0m", "-2m", "1.5m", "1w", "7", "01m", "999999999999999h"])
def test_invalid_or_subminute_duration_rejected(value):
    with pytest.raises(ValueError):
        normalize_timeframe(value)


def test_duplicate_and_out_of_order_input_cannot_create_complete_bucket():
    resampler = TimeframeResampler(["7m"])
    start = pd.Timestamp("2025-01-01T00:00:00Z").floor("7min")
    resampler.update(_bar(start, 100))
    for ts in [start, start - pd.Timedelta(minutes=1)]:
        with pytest.raises(ValueError, match="strictly increasing"):
            resampler.update(_bar(ts, 100))
    for offset in [1, 2, 4, 5, 6, 7]:
        assert resampler.update(_bar(start + pd.Timedelta(minutes=offset), 100)) == []
    resampler.reset()
    assert resampler.update(_bar(start, 100)) == []


def test_seconds_aligned_input_is_not_a_minute_bar():
    with pytest.raises(ValueError, match="whole UTC minutes"):
        TimeframeResampler(["7m"]).update(_bar("2025-01-01T00:00:30Z", 100))


@pytest.mark.parametrize("duration", ["7m", "12m", "2h", "60s"])
def test_research_contract_uses_native_duration_validation(duration):
    from bt.contracts.research_specs import _valid_timeframe

    assert _valid_timeframe(duration) is (duration != "60s")


def _utc_ts(ts: str | pd.Timestamp) -> pd.Timestamp:
    stamp = pd.Timestamp(ts)
    if stamp.tz is None:
        stamp = stamp.tz_localize("UTC")
    else:
        stamp = stamp.tz_convert("UTC")
    return stamp


def _bar(ts: str | pd.Timestamp, close: float, symbol: str = "BTCUSDT") -> Bar:
    t = _utc_ts(ts)
    return Bar(
        ts=t,
        symbol=symbol,
        open=close - 0.1,
        high=close + 0.2,
        low=close - 0.3,
        close=close,
        volume=10.0,
    )


def test_htf_bar_only_emitted_on_close() -> None:
    r = TimeframeResampler(timeframes=["5m"], strict=True)

    input_closes = [100.0, 101.0, 102.0, 103.0, 104.0]
    for minute, close in zip(["00:00", "00:01", "00:02", "00:03", "00:04"], input_closes):
        bars = r.update(_bar(f"2025-01-01 {minute}:00", close=close))
        assert bars == []

    emitted = r.update(_bar("2025-01-01 00:05:00", close=106.0))
    assert len(emitted) == 1
    htf = emitted[0]

    assert htf.ts == pd.Timestamp("2025-01-01 00:00:00", tz="UTC")
    assert htf.timeframe == "5m"
    assert htf.open == 99.9
    assert htf.close == 104.0
    assert htf.high == 104.2
    assert htf.low == 99.7
    assert htf.volume == 50.0
    assert htf.is_complete is True
    assert htf.n_bars == 5


def test_missing_1m_bars_do_not_get_filled_in_strict_mode() -> None:
    r = TimeframeResampler(timeframes=["5m"], strict=True)

    for minute in ["00:00", "00:01", "00:03", "00:04"]:
        r.update(_bar(f"2025-01-01 {minute}:00", close=100.0))

    emitted = r.update(_bar("2025-01-01 00:05:00", close=101.0))
    assert emitted == []
    assert r.latest_closed("BTCUSDT", "5m") is None


def test_gap_across_buckets_marks_bucket_incomplete() -> None:
    r = TimeframeResampler(timeframes=["5m"], strict=True)

    for minute in ["00:00", "00:01", "00:02", "00:03", "00:04"]:
        r.update(_bar(f"2025-01-01 {minute}:00", close=100.0))

    first_emit = r.update(_bar("2025-01-01 00:05:00", close=101.0))
    assert len(first_emit) == 1
    assert first_emit[0].ts == pd.Timestamp("2025-01-01 00:00:00", tz="UTC")

    r.update(_bar("2025-01-01 00:07:00", close=102.0))
    r.update(_bar("2025-01-01 00:08:00", close=103.0))
    r.update(_bar("2025-01-01 00:09:00", close=104.0))

    second_emit = r.update(_bar("2025-01-01 00:10:00", close=105.0))
    assert second_emit == []
    latest = r.latest_closed("BTCUSDT", "5m")
    assert latest is not None
    assert latest.ts == pd.Timestamp("2025-01-01 00:00:00", tz="UTC")


def test_multitimeframe_emission_correctness() -> None:
    r = TimeframeResampler(timeframes=["5m", "15m"], strict=True)

    emitted_5m = []
    emitted_15m = []

    start = pd.Timestamp("2025-01-01 00:00:00", tz="UTC")
    for idx in range(16):
        ts = start + pd.Timedelta(minutes=idx)
        out = r.update(_bar(ts, close=100.0 + idx))
        for bar in out:
            if bar.timeframe == "5m":
                emitted_5m.append(bar)
            elif bar.timeframe == "15m":
                emitted_15m.append(bar)

    assert [b.ts for b in emitted_5m] == [
        pd.Timestamp("2025-01-01 00:00:00", tz="UTC"),
        pd.Timestamp("2025-01-01 00:05:00", tz="UTC"),
        pd.Timestamp("2025-01-01 00:10:00", tz="UTC"),
    ]
    assert [b.ts for b in emitted_15m] == [
        pd.Timestamp("2025-01-01 00:00:00", tz="UTC"),
    ]
    assert len(emitted_5m) == 3
    assert len(emitted_15m) == 1
