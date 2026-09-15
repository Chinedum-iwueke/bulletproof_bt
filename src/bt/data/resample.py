"""Streaming higher-timeframe (HTF) resampling from 1-minute bars.

This module is event-driven and bar-by-bar only:
- no lookahead
- no interpolation/filling of missing 1m bars
- strict mode emits only complete HTF bars

Timestamp convention:
- ``HTFBar.ts`` is the HTF bucket start timestamp (UTC).
"""
from __future__ import annotations

from dataclasses import dataclass, field
import re
import pandas as pd

from bt.core.types import Bar


_TIMEFRAME_TO_MINUTES: dict[str, int] = {
    "1m": 1,
    "3m": 3,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
}

# Retained as common examples for existing consumers, not an exhaustive allowlist.
SUPPORTED_TIMEFRAMES: tuple[str, ...] = tuple(_TIMEFRAME_TO_MINUTES.keys())


def normalize_timeframe(value: object, *, key_path: str = "timeframe") -> str:
    """Normalize and validate a timeframe string against supported values."""
    if not isinstance(value, str):
        raise ValueError(
            f"Invalid {key_path}: expected an integer duration with m, h or d units "
            f"(got: {value!r})"
        )

    timeframe = value.strip().lower()
    match = re.fullmatch(r"([1-9][0-9]*)([mhd])", timeframe)
    if match is None:
        raise ValueError(
            f"Invalid {key_path}: {value!r}. Supported examples: "
            "positive integer durations with m, h or d units; seconds are unsupported"
            "; examples: 1m, 7m, 12m, 2h, 1d"
        )
    minutes = int(match[1]) * {"m": 1, "h": 60, "d": 1440}[match[2]]
    if minutes > pd.Timedelta.max.value // pd.Timedelta(minutes=1).value:
        raise ValueError(f"Invalid {key_path}: duration exceeds timestamp range")
    return timeframe


def timeframe_minutes(timeframe: str) -> int:
    normalized = normalize_timeframe(timeframe)
    return int(normalized[:-1]) * {"m": 1, "h": 60, "d": 1440}[normalized[-1]]


@dataclass(frozen=True)
class HTFBar:
    """Closed higher-timeframe bar aggregated from 1m bars."""

    ts: pd.Timestamp
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    timeframe: str
    n_bars: int
    expected_bars: int
    is_complete: bool
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass
class _BucketState:
    bucket_start: pd.Timestamp
    open: float
    high: float
    low: float
    close: float
    volume: float
    n_bars: int
    expected_bars: int
    is_incomplete: bool
    last_seen_minute: pd.Timestamp


class TimeframeResampler:
    """Streaming per-symbol/per-timeframe resampler for HTF bars.

    Notes:
    - Input is assumed to be 1-minute bars in UTC.
    - In strict mode, missing minutes mark a bucket incomplete and it is never emitted.

    TODO:
    - support non-1m base feeds / partial fill modes.
    - support emitting incomplete bars when strict=False.
    """

    def __init__(self, timeframes: list[str], strict: bool = True, base_freq: str = "1min") -> None:
        if base_freq != "1min":
            raise ValueError("Only base_freq='1min' is supported in v1")
        if not timeframes:
            raise ValueError("At least one timeframe is required")

        normalized = [normalize_timeframe(tf, key_path="timeframe") for tf in timeframes]

        # Preserve declared order while deduplicating.
        self._timeframes = list(dict.fromkeys(normalized))
        self._strict = strict
        self._base_freq = base_freq

        self._states: dict[tuple[str, str], _BucketState] = {}
        self._latest_closed: dict[tuple[str, str], HTFBar] = {}
        self._last_input: dict[str, pd.Timestamp] = {}

    def reset(self) -> None:
        """Reset all in-flight and latest-closed state."""
        self._states.clear()
        self._latest_closed.clear()
        self._last_input.clear()

    def latest_closed(self, symbol: str, timeframe: str) -> HTFBar | None:
        """Return the latest closed HTF bar for a symbol/timeframe."""
        return self._latest_closed.get((symbol, timeframe))

    def update(self, bar: Bar) -> list[HTFBar]:
        """Update state with one 1m bar and return newly closed HTF bars."""
        self._assert_utc(bar.ts)
        if bar.ts != bar.ts.floor("1min"):
            raise ValueError("Base bars must be aligned to whole UTC minutes")
        previous = self._last_input.get(bar.symbol)
        if previous is not None and bar.ts <= previous:
            raise ValueError("Base bars must be strictly increasing per symbol")
        self._last_input[bar.symbol] = bar.ts
        emitted: list[HTFBar] = []

        for timeframe in self._timeframes:
            bucket_start = self._bucket_start(bar.ts, timeframe)
            key = (bar.symbol, timeframe)
            state = self._states.get(key)

            if state is None:
                self._states[key] = self._init_state(bucket_start, timeframe, bar)
                continue

            if bucket_start != state.bucket_start:
                closed = self._finalize(bar.symbol, timeframe, state)
                if closed is not None:
                    emitted.append(closed)
                    self._latest_closed[key] = closed
                self._states[key] = self._init_state(bucket_start, timeframe, bar)
                continue

            # Same bucket: detect minute gap and roll current candle.
            if bar.ts - state.last_seen_minute > pd.Timedelta(minutes=1):
                state.is_incomplete = True

            state.high = max(state.high, bar.high)
            state.low = min(state.low, bar.low)
            state.close = bar.close
            state.volume += bar.volume
            state.n_bars += 1
            state.last_seen_minute = bar.ts

        return emitted

    @staticmethod
    def _assert_utc(ts: pd.Timestamp) -> None:
        if ts.tz is None:
            raise AssertionError("bar.ts must be timezone-aware UTC")
        if str(ts.tz) != "UTC":
            raise AssertionError("bar.ts must be UTC")

    @staticmethod
    def _bucket_start(ts: pd.Timestamp, timeframe: str) -> pd.Timestamp:
        minutes = timeframe_minutes(timeframe)
        return ts.floor(f"{minutes}min")

    @staticmethod
    def _init_state(bucket_start: pd.Timestamp, timeframe: str, bar: Bar) -> _BucketState:
        expected = timeframe_minutes(timeframe)
        return _BucketState(
            bucket_start=bucket_start,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            n_bars=1,
            expected_bars=expected,
            is_incomplete=False,
            last_seen_minute=bar.ts,
        )

    def _finalize(self, symbol: str, timeframe: str, state: _BucketState) -> HTFBar | None:
        is_complete = (not state.is_incomplete) and (state.n_bars == state.expected_bars)
        if self._strict and not is_complete:
            return None

        return HTFBar(
            ts=state.bucket_start,
            symbol=symbol,
            open=state.open,
            high=state.high,
            low=state.low,
            close=state.close,
            volume=state.volume,
            timeframe=timeframe,
            n_bars=state.n_bars,
            expected_bars=state.expected_bars,
            is_complete=is_complete,
            metadata={
                "bucket_origin": "1970-01-01T00:00:00Z",
                "timestamp_convention": "bucket_start",
                "availability_policy": "next_bucket_input",
                "base_timeframe": "1m",
                "strict": self._strict,
            },
        )
