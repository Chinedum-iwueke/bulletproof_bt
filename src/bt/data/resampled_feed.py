"""Feed wrappers for deterministic timeframe resampling."""
from __future__ import annotations

from dataclasses import replace
from typing import Any

from bt.core.types import Bar
from bt.data.resample import TimeframeResampler, normalize_timeframe


class ResampledDataFeed:
    """Wrap a base 1m feed and emit only closed bars for a target timeframe."""

    def __init__(self, *, inner_feed: Any, timeframe: str, strict: bool = True, audit_manager: Any | None = None) -> None:
        self._inner_feed = inner_feed
        self._timeframe = normalize_timeframe(timeframe, key_path="data.engine_timeframe")
        self._resampler = TimeframeResampler(timeframes=[self._timeframe], strict=bool(strict))
        self._audit = audit_manager

    def symbols(self) -> list[str]:
        if hasattr(self._inner_feed, "symbols"):
            return list(self._inner_feed.symbols())
        return []

    def reset(self) -> None:
        if hasattr(self._inner_feed, "reset"):
            self._inner_feed.reset()
        self._resampler.reset()

    def next(self) -> dict[str, Bar] | None:
        while True:
            bars = self._inner_feed.next()
            if bars is None:
                return None

            if isinstance(bars, dict):
                bars_list = list(bars.values())
            else:
                bars_list = list(bars)

            emitted_by_symbol: dict[str, Bar] = {}
            for bar in bars_list:
                emitted = self._resampler.update(bar)
                for htf_bar in emitted:
                    if self._audit is not None and self._audit.enabled:
                        self._audit.record_event(
                            "resample_audit",
                            {
                                "symbol": htf_bar.symbol,
                                "timeframe": htf_bar.timeframe,
                                "ts": htf_bar.ts,
                                "n_bars": htf_bar.n_bars,
                                "expected_bars": htf_bar.expected_bars,
                                "is_complete": htf_bar.is_complete,
                            },
                            violation=not bool(htf_bar.is_complete),
                        )
                    emitted_by_symbol[htf_bar.symbol] = Bar(
                        ts=htf_bar.ts,
                        symbol=htf_bar.symbol,
                        open=htf_bar.open,
                        high=htf_bar.high,
                        low=htf_bar.low,
                        close=htf_bar.close,
                        volume=htf_bar.volume,
                    )

            if emitted_by_symbol:
                return emitted_by_symbol


class EntryTimeframeGate:
    """Strategy adapter that filters entry signals by timestamp boundary."""

    def __init__(self, *, inner: Any, entry_timeframe: str) -> None:
        self._inner = inner
        self._entry_timeframe = normalize_timeframe(entry_timeframe, key_path="data.entry_timeframe")
        self._allowed_entry_signals = 0
        self._blocked_entry_signals = 0
        self._exit_signals_preserved = 0

    def _annotate_signal(self, signal, *, allow_entries: bool, blocked: bool = False):
        metadata = dict(getattr(signal, "metadata", {}) or {})
        metadata.update(
            {
                "entry_timeframe_gate_applied": True,
                "entry_timeframe": self._entry_timeframe,
                "allow_entries": bool(allow_entries),
                "entry_timeframe_boundary": bool(allow_entries),
                "entry_timeframe_gate_blocked": bool(blocked),
            }
        )
        try:
            return replace(signal, metadata=metadata)
        except TypeError:
            signal.metadata = metadata
            return signal

    def on_bars(self, ts, bars_by_symbol, tradeable, ctx):
        from bt.risk.risk_engine import RiskEngine
        from bt.data.timeframe_utils import is_timeframe_boundary

        allow_entries = is_timeframe_boundary(ts, self._entry_timeframe)
        emitted = self._inner.on_bars(ts, bars_by_symbol, tradeable, ctx)
        if allow_entries:
            out = []
            for signal in emitted:
                if RiskEngine._is_exit_signal(signal):
                    self._exit_signals_preserved += 1
                else:
                    self._allowed_entry_signals += 1
                out.append(self._annotate_signal(signal, allow_entries=True))
            return out

        filtered = []
        for signal in emitted:
            if RiskEngine._is_exit_signal(signal):
                self._exit_signals_preserved += 1
                filtered.append(self._annotate_signal(signal, allow_entries=False))
            else:
                self._blocked_entry_signals += 1
                self._annotate_signal(signal, allow_entries=False, blocked=True)
        return filtered

    def strategy_artifacts(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if hasattr(self._inner, "strategy_artifacts") and callable(getattr(self._inner, "strategy_artifacts")):
            inner_payload = self._inner.strategy_artifacts()
            if isinstance(inner_payload, dict):
                payload.update(inner_payload)
        payload["entry_timeframe_gate"] = {
            "enabled": True,
            "entry_timeframe": self._entry_timeframe,
            "allowed_entry_signals": self._allowed_entry_signals,
            "blocked_entry_signals": self._blocked_entry_signals,
            "exit_signals_preserved": self._exit_signals_preserved,
            "contract": "entry signals are boundary-gated; exit signals remain evaluated every engine bar",
        }
        return payload
