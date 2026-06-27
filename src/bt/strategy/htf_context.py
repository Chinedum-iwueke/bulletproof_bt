"""Strategy adapter to enrich engine context with strict HTF bars."""
from __future__ import annotations

from typing import Any, Mapping

import pandas as pd

from bt.core.types import Bar, Signal
from bt.data.resample import HTFBar, TimeframeResampler
from bt.strategy.base import Strategy
from bt.strategy.context_view import StrategyContextView
from bt.strategy.signal_conflicts import SignalConflictSummary, resolve_signal_conflicts


def _inner_strategy_artifacts(inner: Strategy) -> dict[str, Any]:
    if hasattr(inner, "strategy_artifacts") and callable(getattr(inner, "strategy_artifacts")):
        payload = inner.strategy_artifacts()
        return dict(payload) if isinstance(payload, dict) else {}
    return {}


class ReadOnlyContextStrategyAdapter(Strategy):
    """Wrap a strategy to expose context as a read-only view."""

    def __init__(self, *, inner: Strategy) -> None:
        self._inner = inner

    def on_bars(
        self,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Bar],
        tradeable: set[str],
        ctx: Mapping[str, Any],
    ) -> list[Signal]:
        return self._inner.on_bars(ts, bars_by_symbol, tradeable, StrategyContextView(ctx))


class HTFContextStrategyAdapter(Strategy):
    """Wrap a strategy and inject closed HTF bars into ctx['htf'][timeframe][symbol]."""

    def __init__(
        self,
        *,
        inner: Strategy,
        resampler: TimeframeResampler,
        skip_empty_no_position: bool = False,
    ) -> None:
        self._inner = inner
        self._resampler = resampler
        self._latest_closed: dict[str, dict[str, HTFBar]] = {}
        self._skip_empty_no_position = bool(skip_empty_no_position)
        self._noop_skipped_calls = 0

    @staticmethod
    def _has_open_position(ctx: Mapping[str, Any]) -> bool:
        positions = ctx.get("positions") if isinstance(ctx, Mapping) else None
        if not isinstance(positions, Mapping):
            return False
        for payload in positions.values():
            if isinstance(payload, Mapping) and payload.get("side") is not None:
                return True
        return False

    def on_bars(
        self,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Bar],
        tradeable: set[str],
        ctx: Mapping[str, Any],
    ) -> list[Signal]:
        emitted_index: dict[str, dict[str, HTFBar]] = {}
        for bar in bars_by_symbol.values():
            emitted = self._resampler.update(bar)
            for htf_bar in emitted:
                by_tf = self._latest_closed.setdefault(htf_bar.timeframe, {})
                by_tf[htf_bar.symbol] = htf_bar
                emitted_index.setdefault(htf_bar.timeframe, {})[htf_bar.symbol] = htf_bar

        new_ctx = dict(ctx)
        new_ctx["htf"] = emitted_index
        if self._skip_empty_no_position and not emitted_index and not self._has_open_position(ctx):
            # Safe compiled control-flow kernel: all current research hypotheses
            # use two-clock entries and 1m exits. With no newly closed HTF bar
            # and no open position, the wrapped strategy cannot emit a valid
            # entry or exit, so skipping preserves event semantics exactly.
            self._noop_skipped_calls += 1
            return []
        return self._inner.on_bars(ts, bars_by_symbol, tradeable, StrategyContextView(new_ctx))

    def strategy_artifacts(self) -> dict[str, Any]:
        return {
            **_inner_strategy_artifacts(self._inner),
            "htf_event_kernel": {
                "enabled": self._skip_empty_no_position,
                "noop_skipped_calls": self._noop_skipped_calls,
                "mode": "streaming_htf_event_schedule",
            }
        }


class PrecomputedHTFContextStrategyAdapter(Strategy):
    """Inject closed HTF bars from causal research-panel columns.

    The research-data stamper writes ``htf_<tf>_*`` columns only on the 1m bar
    where the streaming ``TimeframeResampler`` would have emitted a completed
    HTF candle. This adapter reconstructs that emitted-index shape and avoids
    recomputing HTF aggregation inside every stable backtest worker.
    """

    def __init__(
        self,
        *,
        inner: Strategy,
        timeframes: list[str],
        skip_empty_no_position: bool = False,
    ) -> None:
        self._inner = inner
        self._timeframes = list(dict.fromkeys(str(tf).lower() for tf in timeframes))
        self._first_seen: dict[tuple[str, str], pd.Timestamp] = {}
        self._skip_empty_no_position = bool(skip_empty_no_position)
        self._noop_skipped_calls = 0

    @staticmethod
    def _has_open_position(ctx: Mapping[str, Any]) -> bool:
        positions = ctx.get("positions") if isinstance(ctx, Mapping) else None
        if not isinstance(positions, Mapping):
            return False
        for payload in positions.values():
            if isinstance(payload, Mapping) and payload.get("side") is not None:
                return True
        return False

    def on_bars(
        self,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Bar],
        tradeable: set[str],
        ctx: Mapping[str, Any],
    ) -> list[Signal]:
        emitted_index: dict[str, dict[str, HTFBar]] = {}
        for bar in bars_by_symbol.values():
            extra = bar.extra if isinstance(bar.extra, Mapping) else {}
            for timeframe in self._timeframes:
                key = (bar.symbol, timeframe)
                first_seen = self._first_seen.setdefault(key, pd.Timestamp(bar.ts))
                prefix = f"htf_{timeframe}_"
                if not bool(extra.get(f"{prefix}ready", False)):
                    continue
                htf_ts = pd.to_datetime(extra.get(f"{prefix}ts"), utc=True, errors="coerce")
                if pd.isna(htf_ts):
                    continue
                htf_ts = pd.Timestamp(htf_ts)
                # Match the classic streaming resampler's cold-start behavior:
                # a backtest that begins at T has not seen any HTF bucket whose
                # open timestamp is before T, even if the full-history panel has
                # a precomputed emission row at T.
                if htf_ts < first_seen:
                    continue
                try:
                    htf_bar = HTFBar(
                        ts=htf_ts,
                        symbol=bar.symbol,
                        open=float(extra[f"{prefix}open"]),
                        high=float(extra[f"{prefix}high"]),
                        low=float(extra[f"{prefix}low"]),
                        close=float(extra[f"{prefix}close"]),
                        volume=float(extra[f"{prefix}volume"]),
                        timeframe=timeframe,
                        n_bars=int(extra.get(f"{prefix}n_bars", 0) or 0),
                        expected_bars=int(extra.get(f"{prefix}expected_bars", 0) or 0),
                        is_complete=bool(extra.get(f"{prefix}is_complete", True)),
                        metadata={},
                    )
                except (KeyError, TypeError, ValueError):
                    continue
                emitted_index.setdefault(timeframe, {})[bar.symbol] = htf_bar

        new_ctx = dict(ctx)
        new_ctx["htf"] = emitted_index
        if self._skip_empty_no_position and not emitted_index and not self._has_open_position(ctx):
            # Same safety argument as the streaming adapter, except the HTF
            # event schedule has already been stamped causally onto the panel.
            self._noop_skipped_calls += 1
            return []
        return self._inner.on_bars(ts, bars_by_symbol, tradeable, StrategyContextView(new_ctx))

    def strategy_artifacts(self) -> dict[str, Any]:
        return {
            **_inner_strategy_artifacts(self._inner),
            "htf_event_kernel": {
                "enabled": self._skip_empty_no_position,
                "noop_skipped_calls": self._noop_skipped_calls,
                "mode": "precomputed_htf_event_schedule",
                "timeframes": self._timeframes,
            }
        }


class SignalConflictPolicyStrategyAdapter(Strategy):
    """Wrap strategy output with deterministic same-(ts,symbol) conflict resolution."""

    def __init__(self, *, inner: Strategy, policy: str) -> None:
        self._inner = inner
        self._policy = str(policy)
        self._last_conflict_summaries: list[SignalConflictSummary] = []

    @property
    def last_conflict_summaries(self) -> list[SignalConflictSummary]:
        return list(self._last_conflict_summaries)

    def on_bars(
        self,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Bar],
        tradeable: set[str],
        ctx: Mapping[str, Any],
    ) -> list[Signal]:
        emitted = self._inner.on_bars(ts, bars_by_symbol, tradeable, ctx)
        resolved, summaries = resolve_signal_conflicts(emitted, policy=self._policy)
        self._last_conflict_summaries = summaries
        return resolved
