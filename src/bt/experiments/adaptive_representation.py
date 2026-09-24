"""Causal compilation of outcome-blind multi-asset representation plans."""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from bt.data.resample import timeframe_minutes


class AdaptiveRepresentationError(ValueError):
    """A representation plan cannot be replayed without weakening causality."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


@dataclass(frozen=True)
class MaterializedRepresentation:
    frame: pd.DataFrame
    receipt: dict[str, Any]


_OPERATIONS = {
    "identity",
    "simple_return",
    "log_return",
    "fractional_difference",
    "rolling_zscore",
    "realized_volatility",
    "spread",
    "ratio",
    "cross_sectional_rank",
}


def validate_plan(plan: dict[str, Any]) -> None:
    required = {
        "schema_version",
        "candidate_key",
        "instruments",
        "basket_members",
        "source_timeframe",
        "research_timeframe",
        "resampling_policy",
        "transformations",
        "transformation_rationale",
        "rejected_alternatives",
        "selection_data_boundary",
        "outcome_data_consulted",
    }
    if set(plan) != required:
        raise AdaptiveRepresentationError("representation plan fields are not exact")
    if plan["schema_version"] != "adaptive-representation-plan-v1.0.0":
        raise AdaptiveRepresentationError("unsupported representation plan version")
    if plan["source_timeframe"] != "1m":
        raise AdaptiveRepresentationError("only canonical 1m source panels are supported")
    if plan["resampling_policy"] != "left_closed_left_labeled_complete_bars":
        raise AdaptiveRepresentationError("unsafe resampling policy")
    if plan["selection_data_boundary"] != "metadata_predictors_only_no_targets":
        raise AdaptiveRepresentationError("representation selection consulted an unsafe boundary")
    if plan["outcome_data_consulted"] is not False:
        raise AdaptiveRepresentationError("representation selection must be outcome blind")
    timeframe_minutes(str(plan["research_timeframe"]))
    instruments = plan["instruments"]
    if not isinstance(instruments, list) or not 1 <= len(instruments) <= 20:
        raise AdaptiveRepresentationError("representation basket must contain 1-20 assets")
    if len(instruments) != len(set(instruments)):
        raise AdaptiveRepresentationError("representation basket contains duplicates")
    members = plan["basket_members"]
    if not isinstance(members, list) or [item.get("instrument") for item in members] != instruments:
        raise AdaptiveRepresentationError("basket member order must match instruments")
    roles = {"primary", "predictor", "control", "hedge"}
    for member in members:
        if set(member) != {"instrument", "role", "legacy_groups", "selection_rationale"}:
            raise AdaptiveRepresentationError("basket member fields are not exact")
        if member["role"] not in roles or not str(member["selection_rationale"]).strip():
            raise AdaptiveRepresentationError("basket member role or rationale is invalid")
        if not set(member["legacy_groups"]).issubset({"stable", "volatile"}):
            raise AdaptiveRepresentationError("unknown legacy universe group")
    if len([item for item in members if item["role"] == "primary"]) != 1:
        raise AdaptiveRepresentationError("the basket must declare exactly one primary")
    transforms = plan["transformations"]
    if not isinstance(transforms, list) or not 1 <= len(transforms) <= 30:
        raise AdaptiveRepresentationError("representation requires 1-30 transformations")
    outputs: set[str] = set()
    available = {
        f"{instrument}__{field}"
        for instrument in instruments
        for field in ("open", "high", "low", "close", "volume", "quote_volume")
    }
    for transform in transforms:
        if set(transform) != {
            "output_field",
            "operation",
            "input_fields",
            "parameters",
            "fit_policy",
            "rationale",
        }:
            raise AdaptiveRepresentationError("transformation fields are not exact")
        operation = transform["operation"]
        inputs = transform["input_fields"]
        output = transform["output_field"]
        if operation not in _OPERATIONS:
            raise AdaptiveRepresentationError(f"unsupported operation: {operation}")
        if not isinstance(inputs, list) or not inputs or any(item not in available for item in inputs):
            raise AdaptiveRepresentationError(f"transformation {output} has unavailable inputs")
        if output in available or output in outputs:
            raise AdaptiveRepresentationError("transformation outputs must be unique")
        _validate_operation(transform)
        outputs.add(output)
        available.add(output)
    if not plan["rejected_alternatives"]:
        raise AdaptiveRepresentationError("representation must retain rejected alternatives")


def _validate_operation(transform: dict[str, Any]) -> None:
    operation = transform["operation"]
    inputs = transform["input_fields"]
    parameters = transform["parameters"]
    if not isinstance(parameters, dict):
        raise AdaptiveRepresentationError("transformation parameters must be an object")
    if operation in {"identity", "simple_return", "log_return"}:
        if len(inputs) != 1:
            raise AdaptiveRepresentationError(f"{operation} requires one input")
        expected = set() if operation == "identity" else {"periods"}
        if set(parameters) != expected:
            raise AdaptiveRepresentationError(f"{operation} parameters are invalid")
        if operation != "identity" and not 1 <= int(parameters["periods"]) <= 10_000:
            raise AdaptiveRepresentationError("return periods are outside the bounded range")
    elif operation == "fractional_difference":
        if len(inputs) != 1 or set(parameters) != {"d", "weight_threshold"}:
            raise AdaptiveRepresentationError("fractional_difference contract is invalid")
        if not 0 < float(parameters["d"]) < 0.5:
            raise AdaptiveRepresentationError("fractional difference d must be between 0 and 0.5")
        if not 1e-8 <= float(parameters["weight_threshold"]) <= 0.1:
            raise AdaptiveRepresentationError("fractional difference threshold is invalid")
        if transform["fit_policy"] != "train_only":
            raise AdaptiveRepresentationError("fractional difference selection must be train-only")
    elif operation in {"rolling_zscore", "realized_volatility"}:
        if len(inputs) != 1 or set(parameters) != {"window"}:
            raise AdaptiveRepresentationError(f"{operation} contract is invalid")
        if not 2 <= int(parameters["window"]) <= 100_000:
            raise AdaptiveRepresentationError("rolling window is outside the bounded range")
    elif operation in {"spread", "ratio"}:
        if len(inputs) != 2 or parameters:
            raise AdaptiveRepresentationError(f"{operation} requires two inputs and no parameters")
    elif operation == "cross_sectional_rank":
        if len(inputs) < 2 or parameters:
            raise AdaptiveRepresentationError("cross-sectional rank requires at least two inputs")
    if transform["fit_policy"] not in {"stateless", "train_only"}:
        raise AdaptiveRepresentationError("unsupported transformation fit policy")
    if not str(transform["rationale"]).strip():
        raise AdaptiveRepresentationError("every transformation requires a rationale")


def _complete_bars(frame: pd.DataFrame, timeframe: str, instrument: str) -> pd.DataFrame:
    required = {"ts", "open", "high", "low", "close", "volume"}
    missing = required - set(frame)
    if missing:
        raise AdaptiveRepresentationError(
            f"{instrument} is missing required columns: {sorted(missing)}"
        )
    ordered = frame.copy()
    ordered["ts"] = pd.to_datetime(ordered["ts"], utc=True, errors="raise")
    ordered = ordered.sort_values("ts", kind="stable")
    if ordered["ts"].duplicated().any():
        raise AdaptiveRepresentationError(f"{instrument} contains duplicate timestamps")
    if "symbol" in ordered and set(ordered["symbol"].astype(str)) != {instrument}:
        raise AdaptiveRepresentationError(f"{instrument} panel identity is inconsistent")
    minutes = timeframe_minutes(timeframe)
    bucket = ordered["ts"].dt.floor(f"{minutes}min")
    ordered = ordered.assign(_bucket=bucket)
    grouped = ordered.groupby("_bucket", sort=True)
    counts = grouped["ts"].count()
    spans = grouped["ts"].agg(["min", "max"])
    complete = (
        counts.eq(minutes)
        & spans["min"].eq(spans.index)
        & spans["max"].eq(spans.index + pd.Timedelta(minutes=minutes - 1))
    )
    selected = ordered[ordered["_bucket"].isin(complete[complete].index)]
    if selected.empty:
        raise AdaptiveRepresentationError(f"{instrument} has no complete {timeframe} bars")
    aggregations: dict[str, str] = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }
    if "quote_volume" in selected:
        aggregations["quote_volume"] = "sum"
    bars = selected.groupby("_bucket", sort=True).agg(aggregations)
    bars.index.name = "bucket_start"
    bars["decision_at"] = bars.index + pd.Timedelta(minutes=minutes)
    return bars.reset_index(drop=False)


def _fractional_weights(d: float, threshold: float) -> np.ndarray:
    weights = [1.0]
    for index in range(1, 100_001):
        value = -weights[-1] * (d - index + 1) / index
        if abs(value) < threshold:
            break
        weights.append(value)
    if len(weights) == 100_001:
        raise AdaptiveRepresentationError("fractional-difference weights did not converge")
    return np.asarray(weights[::-1], dtype=float)


def _apply_transform(frame: pd.DataFrame, transform: dict[str, Any]) -> pd.Series:
    inputs = transform["input_fields"]
    parameters = transform["parameters"]
    operation = transform["operation"]
    source = pd.to_numeric(frame[inputs[0]], errors="coerce")
    if operation == "identity":
        return source
    if operation == "simple_return":
        return source.pct_change(int(parameters["periods"]), fill_method=None)
    if operation == "log_return":
        positive = source.where(source > 0)
        return np.log(positive).diff(int(parameters["periods"]))
    if operation == "fractional_difference":
        weights = _fractional_weights(
            float(parameters["d"]), float(parameters["weight_threshold"])
        )
        values = source.to_numpy(dtype=float)
        output = np.full(len(values), np.nan)
        width = len(weights)
        for index in range(width - 1, len(values)):
            window = values[index - width + 1 : index + 1]
            if np.isfinite(window).all():
                output[index] = float(np.dot(weights, window))
        return pd.Series(output, index=frame.index)
    if operation == "rolling_zscore":
        window = int(parameters["window"])
        mean = source.rolling(window, min_periods=window).mean()
        deviation = source.rolling(window, min_periods=window).std(ddof=0)
        return (source - mean) / deviation.replace(0, np.nan)
    if operation == "realized_volatility":
        window = int(parameters["window"])
        return source.rolling(window, min_periods=window).std(ddof=0) * math.sqrt(window)
    if operation == "spread":
        return source - pd.to_numeric(frame[inputs[1]], errors="coerce")
    if operation == "ratio":
        denominator = pd.to_numeric(frame[inputs[1]], errors="coerce").replace(0, np.nan)
        return source / denominator
    values = frame[inputs].apply(pd.to_numeric, errors="coerce")
    return values.rank(axis=1, method="average", pct=True).mean(axis=1)


def materialize_adaptive_representation(
    plan: dict[str, Any], panels: dict[str, pd.DataFrame]
) -> MaterializedRepresentation:
    """Compile a frozen plan without targets, interpolation, or future observations."""
    validate_plan(plan)
    instruments = plan["instruments"]
    if set(panels) != set(instruments):
        raise AdaptiveRepresentationError("panel identities differ from the frozen basket")
    aligned: pd.DataFrame | None = None
    source_rows: dict[str, int] = {}
    complete_rows: dict[str, int] = {}
    for instrument in instruments:
        source_rows[instrument] = len(panels[instrument])
        bars = _complete_bars(panels[instrument], plan["research_timeframe"], instrument)
        complete_rows[instrument] = len(bars)
        rename = {
            column: f"{instrument}__{column}"
            for column in bars.columns
            if column not in {"bucket_start", "decision_at"}
        }
        member = bars.rename(columns=rename)
        aligned = member if aligned is None else aligned.merge(
            member, on=["bucket_start", "decision_at"], how="inner", validate="one_to_one"
        )
    assert aligned is not None
    if aligned.empty:
        raise AdaptiveRepresentationError("basket has no causally aligned complete bars")
    for transform in plan["transformations"]:
        aligned[transform["output_field"]] = _apply_transform(aligned, transform)
    output_fields = [item["output_field"] for item in plan["transformations"]]
    plan_digest = _digest(plan)
    frame_digest = _digest(
        {
            "columns": list(aligned.columns),
            "rows": len(aligned),
            "first_decision_at": aligned["decision_at"].iloc[0].isoformat(),
            "last_decision_at": aligned["decision_at"].iloc[-1].isoformat(),
            "null_counts": {
                field: int(aligned[field].isna().sum()) for field in output_fields
            },
        }
    )
    receipt = {
        "schema_version": "adaptive-representation-receipt-v1.0.0",
        "plan_digest": plan_digest,
        "basket": instruments,
        "source_rows": source_rows,
        "complete_rows": complete_rows,
        "aligned_rows": len(aligned),
        "research_timeframe": plan["research_timeframe"],
        "output_fields": output_fields,
        "first_decision_at": aligned["decision_at"].iloc[0].isoformat(),
        "last_decision_at": aligned["decision_at"].iloc[-1].isoformat(),
        "selection_data_boundary": plan["selection_data_boundary"],
        "outcome_data_consulted": False,
        "future_fill_used": False,
        "frame_digest": frame_digest,
    }
    receipt["receipt_digest"] = _digest(receipt)
    return MaterializedRepresentation(frame=aligned, receipt=receipt)
