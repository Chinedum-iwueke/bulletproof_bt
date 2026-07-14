"""Generic causal feature registry for shared fast-path research data.

The registry is metadata and orchestration only. Feature builders may reduce
duplicate computation, but they must not change strategy decisions by using
future rows. Classic execution remains authoritative for fills, PnL, margin,
liquidation checks, and research artifacts.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import fnmatch
import json
from typing import Any, Callable, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

FeatureBuilder = Callable[[pd.DataFrame, Mapping[str, Any]], pd.DataFrame]


@dataclass(frozen=True)
class CausalityContract:
    """Declares how a feature remains safe for bar-by-bar backtests."""

    mode: str
    description: str
    source_timestamp_columns: tuple[str, ...] = ()
    asof_direction: str | None = None
    past_only: bool = True

    def to_json(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "description": self.description,
            "source_timestamp_columns": list(self.source_timestamp_columns),
            "asof_direction": self.asof_direction,
            "past_only": self.past_only,
        }


@dataclass(frozen=True)
class FeatureSpec:
    """A registered causal feature family."""

    name: str
    required_inputs: tuple[str, ...]
    output_columns: tuple[str, ...]
    output_dtype: str = "float64"
    warmup_bars: int = 0
    version: str = "1"
    causality: CausalityContract = field(
        default_factory=lambda: CausalityContract(
            mode="closed_bar_past_only",
            description="Uses current or prior closed-bar inputs only.",
        )
    )
    readiness_columns: tuple[str, ...] = ()
    candidate_columns: tuple[str, ...] = ()
    builder: FeatureBuilder | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def feature_hash(self) -> str:
        payload = {
            "name": self.name,
            "required_inputs": list(self.required_inputs),
            "output_columns": list(self.output_columns),
            "output_dtype": self.output_dtype,
            "warmup_bars": self.warmup_bars,
            "version": self.version,
            "causality": self.causality.to_json(),
            "readiness_columns": list(self.readiness_columns),
            "candidate_columns": list(self.candidate_columns),
            "metadata": dict(self.metadata),
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def validate_inputs(self, frame: pd.DataFrame) -> None:
        missing = [col for col in self.required_inputs if col not in frame.columns]
        if missing:
            raise ValueError(f"feature {self.name!r} missing required inputs: {missing}")

    def build(self, frame: pd.DataFrame, *, params: Mapping[str, Any] | None = None) -> pd.DataFrame:
        self.validate_inputs(frame)
        if self.builder is None:
            raise ValueError(f"feature {self.name!r} has no registered builder")
        return self.builder(frame, params or {})

    def to_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "required_inputs": list(self.required_inputs),
            "output_columns": list(self.output_columns),
            "output_dtype": self.output_dtype,
            "warmup_bars": self.warmup_bars,
            "version": self.version,
            "feature_hash": self.feature_hash,
            "causality": self.causality.to_json(),
            "readiness_columns": list(self.readiness_columns),
            "candidate_columns": list(self.candidate_columns),
            "metadata": dict(self.metadata),
        }


class FeatureRegistry:
    """Mutable process-local registry of causal feature specs."""

    def __init__(self) -> None:
        self._specs: dict[str, FeatureSpec] = {}

    def register(self, spec: FeatureSpec, *, replace: bool = False) -> FeatureSpec:
        if spec.name in self._specs and not replace:
            existing = self._specs[spec.name]
            if existing.feature_hash != spec.feature_hash:
                raise ValueError(f"feature spec already registered with different hash: {spec.name}")
            return existing
        self._specs[spec.name] = spec
        return spec

    def get(self, name: str) -> FeatureSpec:
        try:
            return self._specs[str(name)]
        except KeyError as exc:
            raise KeyError(f"unknown feature spec: {name}") from exc

    def maybe_get(self, name: str) -> FeatureSpec | None:
        return self._specs.get(str(name))

    def all(self) -> tuple[FeatureSpec, ...]:
        return tuple(self._specs[name] for name in sorted(self._specs))

    def build(
        self,
        frame: pd.DataFrame,
        feature_names: Iterable[str],
        *,
        params_by_feature: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> pd.DataFrame:
        merged = frame.copy()
        params_by_feature = params_by_feature or {}
        for name in feature_names:
            spec = self.get(name)
            features = spec.build(merged, params=params_by_feature.get(name, {}))
            if features.empty:
                continue
            key_cols = [col for col in ("ts", "symbol") if col in features.columns]
            if set(key_cols) != {"ts", "symbol"}:
                raise ValueError(f"feature {name!r} builder must return ts and symbol columns")
            drop_cols = [col for col in features.columns if col not in key_cols and col in merged.columns]
            if drop_cols:
                merged = merged.drop(columns=drop_cols)
            merged = merged.merge(features, on=["ts", "symbol"], how="left", validate="one_to_one")
        return merged


@dataclass(frozen=True)
class FeatureBank:
    """Columnar read-only feature view attached to a market snapshot."""

    specs: Mapping[str, FeatureSpec]
    arrays_by_symbol: Mapping[str, Mapping[str, np.ndarray]]

    def feature_names(self) -> tuple[str, ...]:
        return tuple(sorted(self.specs))

    def columns_for_feature(self, feature_name: str) -> tuple[str, ...]:
        return self.specs[str(feature_name)].output_columns

    def arrays_for_symbol(self, symbol: str) -> Mapping[str, np.ndarray]:
        return self.arrays_by_symbol[str(symbol)]

    def readiness_mask(self, symbol: str, feature_name: str) -> np.ndarray:
        spec = self.specs[str(feature_name)]
        arrays = self.arrays_for_symbol(symbol)
        if not spec.readiness_columns:
            first_col = next((col for col in spec.output_columns if col in arrays), None)
            if first_col is None:
                return np.zeros(0, dtype=bool)
            values = arrays[first_col]
            if values.dtype == bool:
                return np.ascontiguousarray(values, dtype=bool)
            if np.issubdtype(values.dtype, np.number):
                return np.ascontiguousarray(~np.isnan(values), dtype=bool)
            return np.ascontiguousarray(pd.notna(values), dtype=bool)
        mask: np.ndarray | None = None
        for col in spec.readiness_columns:
            matching = [name for name in arrays if fnmatch.fnmatch(name, col)]
            if not matching:
                sample = next(iter(arrays.values()), np.asarray([], dtype=bool))
                current = np.zeros(len(sample), dtype=bool)
            else:
                current = np.zeros(len(arrays[matching[0]]), dtype=bool)
                for name in matching:
                    values = arrays[name]
                    if values.dtype == bool:
                        current |= values.astype(bool, copy=False)
                    else:
                        current |= pd.Series(values).fillna(False).astype(bool).to_numpy(dtype=bool)
            mask = current if mask is None else (mask & current)
        return np.ascontiguousarray(mask if mask is not None else np.asarray([], dtype=bool))

    def to_json(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "features": [self.specs[name].to_json() for name in sorted(self.specs)],
        }


GLOBAL_FEATURE_REGISTRY = FeatureRegistry()


def infer_registered_specs_for_columns(columns: Sequence[str]) -> dict[str, FeatureSpec]:
    available = set(map(str, columns))
    out: dict[str, FeatureSpec] = {}
    for spec in GLOBAL_FEATURE_REGISTRY.all():
        patterns = spec.output_columns + spec.readiness_columns + spec.candidate_columns
        if any(any(fnmatch.fnmatch(col, pattern) for col in available) for pattern in patterns):
            out[spec.name] = spec
    return out


def register_builtin_feature_specs() -> FeatureRegistry:
    """Register built-in metadata-only specs.

    Builders that live in research-data jobs can replace these metadata shells
    with callable specs. The metadata remains useful at data-session time even
    when features were stamped earlier by a maintenance command.
    """

    past_only = CausalityContract(
        mode="closed_bar_past_only",
        description="Computed per symbol from current/prior closed bars with rolling or expanding past-only state.",
    )
    GLOBAL_FEATURE_REGISTRY.register(
        FeatureSpec(
            name="engine_state",
            required_inputs=("ts", "symbol", "open", "high", "low", "close", "volume"),
            output_columns=("entry_state_*",),
            output_dtype="mixed",
            warmup_bars=1,
            version="1",
            causality=past_only,
            metadata={"column_prefixes": ["entry_state_"]},
        ),
        replace=True,
    )
    GLOBAL_FEATURE_REGISTRY.register(
        FeatureSpec(
            name="htf_context",
            required_inputs=("ts", "symbol", "open", "high", "low", "close", "volume"),
            output_columns=("htf_*",),
            output_dtype="mixed",
            warmup_bars=1,
            version="1",
            causality=CausalityContract(
                mode="strict_closed_htf",
                description="Higher-timeframe context appears only after a complete HTF candle is closed and aligned to a base bar.",
            ),
            readiness_columns=("htf_*_ready",),
            candidate_columns=("htf_*_ready",),
            metadata={"column_prefixes": ["htf_"]},
        ),
        replace=True,
    )
    GLOBAL_FEATURE_REGISTRY.register(
        FeatureSpec(
            name="l7h1_csi_displacement",
            required_inputs=("ts", "symbol", "open", "high", "low", "close", "volume"),
            output_columns=("l7h1_*",),
            output_dtype="mixed",
            warmup_bars=15,
            version="1",
            causality=CausalityContract(
                mode="strict_closed_signal_timeframe",
                description="L7-H1 family features are emitted at decision time from completed signal-timeframe bars only.",
            ),
            readiness_columns=("l7h1_*_compiled_feature_ready",),
            candidate_columns=("l7h1_*_compiled_feature_ready",),
            metadata={"column_prefixes": ["l7h1_"]},
        ),
        replace=True,
    )
    return GLOBAL_FEATURE_REGISTRY


register_builtin_feature_specs()


__all__ = [
    "CausalityContract",
    "FeatureBank",
    "FeatureRegistry",
    "FeatureSpec",
    "GLOBAL_FEATURE_REGISTRY",
    "infer_registered_specs_for_columns",
    "register_builtin_feature_specs",
]
