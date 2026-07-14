"""Columnar candidate-event helpers for sparse research-panel execution.

The candidate-event layer is deliberately a scheduling layer, not an
accounting engine.  It may suppress flat/no-candidate timestamps before they
reach the Python strategy loop, but the classic engine still owns risk,
execution, fills, equity, forced liquidation checks, and artifact writing.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import fnmatch
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

from bt.engine.fast_path.feature_registry import GLOBAL_FEATURE_REGISTRY, FeatureSpec


@dataclass(frozen=True)
class ColumnarCandidateEventPlan:
    """Compact event schedule keyed by integer symbol ids and timestamp ids."""

    ts_ns: np.ndarray
    symbol_ids: np.ndarray
    candidate_mask: np.ndarray
    symbols: tuple[str, ...]
    symbol_to_id: dict[str, int]

    @classmethod
    def from_frame(
        cls,
        frame: pd.DataFrame,
        *,
        candidate_columns: Iterable[str] | None = None,
    ) -> "ColumnarCandidateEventPlan":
        if frame.empty:
            return cls(
                ts_ns=np.asarray([], dtype=np.int64),
                symbol_ids=np.asarray([], dtype=np.int32),
                candidate_mask=np.asarray([], dtype=bool),
                symbols=(),
                symbol_to_id={},
            )
        required = {"ts", "symbol"}
        missing = required - set(frame.columns)
        if missing:
            raise ValueError(f"candidate event frame missing columns: {sorted(missing)}")
        resolved_candidate_columns = tuple(candidate_columns or globals()["candidate_columns"](frame.columns))
        work = frame[["ts", "symbol", *[col for col in resolved_candidate_columns if col in frame.columns]]].copy()
        work["ts"] = pd.to_datetime(work["ts"], utc=True)
        symbols = tuple(sorted(work["symbol"].astype(str).drop_duplicates()))
        symbol_to_id = {symbol: idx for idx, symbol in enumerate(symbols)}
        mask = np.zeros(len(work), dtype=bool)
        for col in resolved_candidate_columns:
            if col not in work.columns:
                continue
            values = work[col]
            if values.dtype == bool:
                mask |= values.fillna(False).to_numpy(dtype=bool)
            else:
                mask |= values.notna().to_numpy(dtype=bool) & values.astype(bool).to_numpy(dtype=bool)
        return cls(
            ts_ns=work["ts"].astype("int64").to_numpy(copy=True),
            symbol_ids=work["symbol"].astype(str).map(symbol_to_id).to_numpy(dtype=np.int32, copy=True),
            candidate_mask=mask,
            symbols=symbols,
            symbol_to_id=symbol_to_id,
        )

    @classmethod
    def from_snapshot(cls, snapshot: Any) -> "ColumnarCandidateEventPlan":
        ts_parts: list[np.ndarray] = []
        symbol_parts: list[np.ndarray] = []
        mask_parts: list[np.ndarray] = []
        symbols = tuple(getattr(snapshot, "symbols"))
        symbol_to_id = dict(getattr(snapshot, "symbol_to_id"))
        for symbol in symbols:
            arrays = snapshot.arrays_for_symbol(symbol)
            ts_parts.append(arrays.ts)
            symbol_parts.append(np.full(len(arrays.ts), int(arrays.symbol_id), dtype=np.int32))
            mask_parts.append(np.ascontiguousarray(arrays.candidate_ready, dtype=bool))
        if not ts_parts:
            return cls(
                ts_ns=np.asarray([], dtype=np.int64),
                symbol_ids=np.asarray([], dtype=np.int32),
                candidate_mask=np.asarray([], dtype=bool),
                symbols=symbols,
                symbol_to_id=symbol_to_id,
            )
        ts_ns = np.concatenate(ts_parts).astype(np.int64, copy=False)
        symbol_ids = np.concatenate(symbol_parts).astype(np.int32, copy=False)
        candidate_mask = np.concatenate(mask_parts).astype(bool, copy=False)
        order = np.lexsort((symbol_ids, ts_ns))
        return cls(
            ts_ns=np.ascontiguousarray(ts_ns[order]),
            symbol_ids=np.ascontiguousarray(symbol_ids[order]),
            candidate_mask=np.ascontiguousarray(candidate_mask[order]),
            symbols=symbols,
            symbol_to_id=symbol_to_id,
        )


@dataclass
class CandidateEventStats:
    enabled: bool = False
    mode: str = "off"
    emitted_timestamps: int = 0
    skipped_timestamps: int = 0
    emitted_rows: int = 0
    skipped_rows: int = 0
    dense_timestamps: int = 0
    candidate_timestamps: int = 0
    by_reason: dict[str, int] = field(default_factory=dict)
    emitted_by_reason: dict[str, int] = field(default_factory=dict)
    skipped_by_reason: dict[str, int] = field(default_factory=dict)
    emitted_rows_by_reason: dict[str, int] = field(default_factory=dict)
    skipped_rows_by_reason: dict[str, int] = field(default_factory=dict)

    def record_emit(
        self,
        *,
        rows: int,
        dense: bool,
        candidate: bool,
        reasons: Iterable[str] | None = None,
    ) -> None:
        self.emitted_timestamps += 1
        self.emitted_rows += int(rows)
        if dense:
            self.dense_timestamps += 1
        if candidate:
            self.candidate_timestamps += 1
        reason_list = tuple(reasons or ())
        if not reason_list:
            reason_list = ("dense_execution_state" if dense else "candidate_event",)
        for reason in reason_list:
            self.emitted_by_reason[reason] = int(self.emitted_by_reason.get(reason, 0)) + 1
            self.emitted_rows_by_reason[reason] = int(self.emitted_rows_by_reason.get(reason, 0)) + int(rows)

    def record_skip(self, *, rows: int, reason: str) -> None:
        self.skipped_timestamps += 1
        self.skipped_rows += int(rows)
        self.by_reason[reason] = int(self.by_reason.get(reason, 0)) + 1
        self.skipped_by_reason[reason] = int(self.skipped_by_reason.get(reason, 0)) + 1
        self.skipped_rows_by_reason[reason] = int(self.skipped_rows_by_reason.get(reason, 0)) + int(rows)

    def to_json(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "enabled": self.enabled,
            "mode": self.mode,
            "emitted_timestamps": self.emitted_timestamps,
            "skipped_timestamps": self.skipped_timestamps,
            "emitted_rows": self.emitted_rows,
            "skipped_rows": self.skipped_rows,
            "dense_timestamps": self.dense_timestamps,
            "candidate_timestamps": self.candidate_timestamps,
            "by_reason": dict(sorted(self.by_reason.items())),
            "emitted_by_reason": dict(sorted(self.emitted_by_reason.items())),
            "skipped_by_reason": dict(sorted(self.skipped_by_reason.items())),
            "emitted_rows_by_reason": dict(sorted(self.emitted_rows_by_reason.items())),
            "skipped_rows_by_reason": dict(sorted(self.skipped_rows_by_reason.items())),
        }


@dataclass(frozen=True)
class CandidateMarker:
    """One causal column pattern that can wake the sparse event loop."""

    pattern: str
    reason: str
    source_feature: str | None = None

    def matches(self, column: str) -> bool:
        return fnmatch.fnmatch(str(column), self.pattern)


def _markers_from_feature_spec(spec: FeatureSpec) -> list[CandidateMarker]:
    markers: list[CandidateMarker] = []
    for pattern in spec.candidate_columns:
        reason = pattern.replace("*", "").strip("_") or spec.name
        markers.append(CandidateMarker(pattern, f"{spec.name}:{reason}", spec.name))
    return markers


def registered_candidate_markers(extra_markers: Iterable[CandidateMarker] | None = None) -> tuple[CandidateMarker, ...]:
    markers: list[CandidateMarker] = []
    for spec in GLOBAL_FEATURE_REGISTRY.all():
        markers.extend(_markers_from_feature_spec(spec))
    markers.extend(
        [
            CandidateMarker("candidate_ready", "candidate_ready"),
            CandidateMarker("entry_candidate", "entry_candidate"),
            CandidateMarker("exit_candidate", "exit_candidate"),
            CandidateMarker("*_entry_candidate", "entry_candidate"),
            CandidateMarker("*_exit_candidate", "exit_candidate"),
            CandidateMarker("*_continuation_required", "continuation_required"),
            CandidateMarker("*_stop_check_required", "stop_check_required"),
            CandidateMarker("*_htf_boundary_ready", "htf_boundary_ready"),
        ]
    )
    if extra_markers:
        markers.extend(extra_markers)
    dedup: dict[tuple[str, str, str | None], CandidateMarker] = {}
    for marker in markers:
        dedup[(marker.pattern, marker.reason, marker.source_feature)] = marker
    return tuple(dedup.values())


def candidate_columns(columns: Iterable[str], *, markers: Iterable[CandidateMarker] | None = None) -> list[str]:
    marker_list = tuple(markers or registered_candidate_markers())
    out: list[str] = []
    for col in map(str, columns):
        if any(marker.matches(col) for marker in marker_list):
            out.append(col)
    return sorted(set(out))


def _truthy_candidate_value(value: Any) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def candidate_event_reasons(extra: Mapping[str, Any]) -> tuple[str, ...]:
    """Return causal event markers carried by a research-panel row.

    This function is observational only. It explains why a timestamp was
    emitted by the sparse scheduler; it does not alter strategy, risk,
    execution, or logging semantics.
    """

    reasons: list[str] = []
    markers = registered_candidate_markers()
    for key, value in extra.items():
        if not _truthy_candidate_value(value):
            continue
        for marker in markers:
            if marker.matches(str(key)):
                reasons.append(f"{marker.reason}:{key}")
                reasons.append(str(key))
    return tuple(sorted(set(reasons)))


def bar_has_candidate_event(extra: Mapping[str, Any]) -> bool:
    """Return true when a research-panel row carries a causal event marker."""

    return bool(candidate_event_reasons(extra))


def bars_have_candidate_event(bars: Iterable[Any]) -> bool:
    for bar in bars:
        extra = getattr(bar, "extra", None)
        if isinstance(extra, Mapping) and bar_has_candidate_event(extra):
            return True
    return False


def candidate_event_reasons_for_bars(bars: Iterable[Any]) -> tuple[str, ...]:
    reasons: list[str] = []
    for bar in bars:
        extra = getattr(bar, "extra", None)
        if isinstance(extra, Mapping):
            reasons.extend(candidate_event_reasons(extra))
    return tuple(sorted(set(reasons)))
