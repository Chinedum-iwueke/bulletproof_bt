"""HTF-specific no-lookahead tests."""

from __future__ import annotations

from typing import Any, Mapping

import pandas as pd

from bt.core.types import Bar, Signal
from bt.data.resample import TimeframeResampler
from bt.strategy.base import Strategy
from bt.strategy.htf_context import HTFContextStrategyAdapter


class _CaptureHTFStrategy(Strategy):
    """Test probe strategy that captures HTF context seen at each timestamp."""

    def __init__(self) -> None:
        self.seen: dict[pd.Timestamp, dict[str, dict[str, Any]]] = {}

    def on_bars(
        self,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Bar],
        tradeable: set[str],
        ctx: Mapping[str, Any],
    ) -> list[Signal]:
        _ = bars_by_symbol
        _ = tradeable
        htf = ctx.get("htf", {})
        self.seen[ts] = dict(htf) if isinstance(htf, Mapping) else {}
        return []


def _bar(minute: int, *, high: float, close: float) -> Bar:
    ts = pd.Timestamp(f"2024-01-01 00:{minute:02d}:00", tz="UTC")
    return Bar(
        ts=ts,
        symbol="AAA",
        open=100.0,
        high=high,
        low=min(99.0, close),
        close=close,
        volume=1.0,
    )


def test_htf_context_uses_only_closed_buckets_no_lookahead() -> None:
    probe = _CaptureHTFStrategy()
    wrapped = HTFContextStrategyAdapter(
        inner=probe,
        resampler=TimeframeResampler(timeframes=["15m"], strict=True),
    )

    # First 15m bucket (00:00..00:14): modest highs.
    for minute in range(0, 15):
        bar = _bar(minute, high=101.0, close=100.0)
        wrapped.on_bars(bar.ts, {"AAA": bar}, {"AAA"}, {})

    # Second bucket carries a large intrabucket spike at 00:20.
    # If lookahead were present, this second bucket could leak before 00:30.
    for minute in range(15, 30):
        high = 150.0 if minute == 20 else 102.0
        bar = _bar(minute, high=high, close=101.0)
        wrapped.on_bars(bar.ts, {"AAA": bar}, {"AAA"}, {})

    # Rollover into 00:30 closes the second bucket.
    rollover = _bar(30, high=103.0, close=102.0)
    wrapped.on_bars(rollover.ts, {"AAA": rollover}, {"AAA"}, {})

    # No HTF bars should be visible while first bucket is still in progress.
    for minute in range(0, 15):
        ts = pd.Timestamp(f"2024-01-01 00:{minute:02d}:00", tz="UTC")
        assert probe.seen[ts] == {}

    # At 00:15, strategy sees only the just-closed 00:00 bucket.
    first_roll_ts = pd.Timestamp("2024-01-01 00:15:00", tz="UTC")
    first_htf = probe.seen[first_roll_ts]["15m"]["AAA"]
    assert first_htf.ts == pd.Timestamp("2024-01-01 00:00:00", tz="UTC")
    assert first_htf.high == 101.0

    # During 00:16..00:29, second bucket is still open: no HTF emission yet.
    for minute in range(16, 30):
        ts = pd.Timestamp(f"2024-01-01 00:{minute:02d}:00", tz="UTC")
        assert probe.seen[ts] == {}

    # At 00:30, second bucket becomes visible and includes its intrabucket spike.
    second_roll_ts = pd.Timestamp("2024-01-01 00:30:00", tz="UTC")
    second_htf = probe.seen[second_roll_ts]["15m"]["AAA"]
    assert second_htf.ts == pd.Timestamp("2024-01-01 00:15:00", tz="UTC")
    assert second_htf.high == 150.0
