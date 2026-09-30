"""Point-in-time XRP signed-impact/future-volatility research evaluator.

This is an observation strategy, not a directional trading strategy.  It emits
only provenance-validation observations; the governed evaluator estimates the
frozen conditional volatility model on chronological partitions.
"""
from __future__ import annotations

from collections import deque
import json
import math
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.strategy import register_strategy
from bt.strategy.base import Strategy


QUESTION = "Do point-in-time positive and negative XRPUSDT 30m signed return-per-quote_volume shocks predict unequal realized volatility over the next contiguous 3h after controlling for shock magnitude, prior volatility, and completed-bar USD liquidity?"
OUTPUT_FIELDS = (
    "xrpusdt_quote_volume_30m", "xrpusdt_volume_30m",
    "xrpusdt_log_return_30m", "xrpusdt_signed_impact_30m",
    "xrpusdt_realized_volatility_6h",
)
# Updated together with the exact plan embedded in the admitted YAML.
PLAN_DIGEST = "2cf262ba12d12fd47e5647f48d57ce801d49f111abf041fcfec276381e2c56b9"


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _timestamp(value: Any) -> pd.Timestamp | None:
    try:
        result = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if result.tz is None:
        return None
    return result.tz_convert("UTC")


def _fields(value: Any) -> tuple[str, ...] | None:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
    return tuple(value) if isinstance(value, (list, tuple)) and all(isinstance(x, str) for x in value) else None


def _validated_representation(frame: pd.DataFrame) -> pd.DataFrame:
    missing = {"ts", *OUTPUT_FIELDS} - set(frame.columns)
    if missing:
        raise ValueError(f"adaptive representation missing fields: {sorted(missing)}")
    raw_ts = frame["ts"]
    if not all(_timestamp(value) is not None for value in raw_ts):
        raise ValueError("strict UTC timestamps required")
    data = frame.loc[:, ["ts", *OUTPUT_FIELDS]].copy()
    data["ts"] = pd.DatetimeIndex([_timestamp(value) for value in raw_ts])
    data = data.sort_values("ts", kind="mergesort").reset_index(drop=True)
    if data["ts"].duplicated().any() or (data["ts"].diff().dropna() != pd.Timedelta(minutes=30)).any():
        raise ValueError("representation timestamps must be unique contiguous 30m decisions")
    for field in OUTPUT_FIELDS:
        data[field] = pd.to_numeric(data[field], errors="coerce")
    q, v, r, impact, rv = (data[field] for field in OUTPUT_FIELDS)
    finite = np.isfinite(data.loc[:, OUTPUT_FIELDS].to_numpy(dtype=float)).all(axis=1)
    semantic = finite & (q >= 1_000_000.0) & (v >= 0) & (q > 0)
    semantic &= np.isclose(impact, r / q, rtol=1e-10, atol=1e-18)
    semantic &= rv >= 0
    data["semantic_valid"] = semantic
    return data


def _compile_rows(frame: pd.DataFrame, quantile: float, control_window: int) -> pd.DataFrame:
    if not 0.5 < quantile < 1.0 or control_window not in {12, 24}:
        raise ValueError("parameters differ from frozen grid")
    data = _validated_representation(frame)
    ret = data[OUTPUT_FIELDS[2]]
    # The 12-bar contract consumes the producer's exact RV field. The admitted
    # 24-bar variant consumes the exact return output to form its longer control.
    prior_vol = data[OUTPUT_FIELDS[4]] if control_window == 12 else ret.rolling(24, min_periods=24).std(ddof=0) * math.sqrt(24)
    future = pd.concat([ret.shift(-step) for step in range(1, 7)], axis=1)
    data["future_volatility_3h"] = future.std(axis=1, ddof=0) * math.sqrt(6)
    data["prior_volatility"] = prior_vol
    history: deque[float] = deque(maxlen=1440)
    categories: list[str | None] = []
    impact = data[OUTPUT_FIELDS[3]]
    for value in impact:
        category = None
        if len(history) == 1440 and math.isfinite(float(value)):
            lower = float(pd.Series(history).quantile(1.0 - quantile, interpolation="lower"))
            upper = float(pd.Series(history).quantile(quantile, interpolation="higher"))
            if value <= lower:
                category = "negative"
            elif value >= upper:
                category = "positive"
        categories.append(category)
        if math.isfinite(float(value)):
            history.append(float(value))
    data["shock_category"] = categories
    data["shock_magnitude"] = ret.abs()
    data["log_liquidity"] = np.log(data[OUTPUT_FIELDS[0]])
    data["target_complete"] = [i + 6 < len(data) for i in range(len(data))]
    return data


def _split(rows: pd.DataFrame) -> dict[str, pd.DataFrame]:
    n = len(rows); a, b = int(n * .6), int(n * .8)
    raw = {"train": rows.iloc[:a], "validation": rows.iloc[a:b], "test": rows.iloc[b:]}
    result = {}
    for name, part in raw.items():
        # Six decision bars purge/embargo prevents overlapping forward targets.
        left = 6 if name != "train" else 0
        right = -6 if name != "test" and len(part) > 6 else None
        result[name] = part.iloc[left:right].copy()
    return result


def _ols_hac(rows: pd.DataFrame) -> dict[str, Any]:
    usable = rows.loc[rows["semantic_valid"] & rows["target_complete"]].dropna(subset=["future_volatility_3h", "prior_volatility"])
    support = {name: int((usable["shock_category"] == name).sum()) for name in ("positive", "negative")}
    if len(usable) < 8:
        return {"valid": False, "support": support, "reason": "insufficient_rows"}
    controls = usable[["shock_magnitude", "prior_volatility", "log_liquidity"]].to_numpy(float)
    scale = controls.std(axis=0, ddof=0)
    if (scale <= 0).any():
        return {"valid": False, "support": support, "reason": "singular_conditional_model"}
    controls = (controls - controls.mean(axis=0)) / scale
    x = np.column_stack([
        np.ones(len(usable)),
        (usable["shock_category"] == "positive").astype(float),
        (usable["shock_category"] == "negative").astype(float),
        controls,
    ])
    y = usable["future_volatility_3h"].to_numpy(float)
    if np.linalg.matrix_rank(x) < x.shape[1]:
        return {"valid": False, "support": support, "reason": "singular_conditional_model"}
    inv = np.linalg.inv(x.T @ x); beta = inv @ x.T @ y; residual = y - x @ beta
    meat = np.zeros((x.shape[1], x.shape[1]))
    for lag in range(6):
        weight = 1.0 - lag / 6.0
        for t in range(lag, len(x)):
            term = np.outer(x[t] * residual[t], x[t-lag] * residual[t-lag])
            meat += weight * (term if lag == 0 else term + term.T)
    cov = inv @ meat @ inv
    contrast = np.array([0., 1., -1., 0., 0., 0.])
    asymmetry = float(contrast @ beta); se = math.sqrt(max(0., float(contrast @ cov @ contrast)))
    return {"valid": True, "support": support, "positive_effect": float(beta[1]), "negative_effect": float(beta[2]), "asymmetry": asymmetry, "confidence_interval_95": [asymmetry - 1.96*se, asymmetry + 1.96*se], "rows": len(usable), "covariance": "Newey-West-HAC-lag-5"}


def signed_impact_volatility_evaluation(frame: pd.DataFrame, *, params: Mapping[str, Any], minimum_direction_support: int = 40, cost_bps: float = 9.0, evaluate_test: bool = True) -> dict[str, Any]:
    try:
        rows = _compile_rows(frame, float(params["signed_impact_tail_quantile"]), int(params["volatility_control_window_bars"]))
        partitions = _split(rows)
        validation = _ols_hac(partitions["validation"])
        test = _ols_hac(partitions["test"]) if evaluate_test else None
    except (KeyError, TypeError, ValueError, np.linalg.LinAlgError) as exc:
        return {"question": QUESTION, "parameters": dict(params), "outcome": "invalid", "reason": str(exc), "held_out_evaluated": evaluate_test, "decision_records": []}
    chosen = test if evaluate_test else validation
    if not chosen or not chosen["valid"] or min(chosen["support"].values()) < minimum_direction_support:
        outcome = "failed" if chosen and chosen["valid"] else "invalid"
    else:
        lo, hi = chosen["confidence_interval_95"]
        unequal = lo > 0 or hi < 0
        elevated = chosen["positive_effect"] > 0 and chosen["negative_effect"] > 0
        # Costs are registered as a robustness hurdle, never as a trading-PnL proxy.
        stress = min(chosen["positive_effect"], chosen["negative_effect"]) - 2.0 * cost_bps / 10_000.0
        outcome = "positive" if unequal and elevated and stress > 0 else "negative"
        chosen["doubled_cost_minimum_conditional_effect"] = stress
    records = []
    for row in rows.itertuples():
        records.append({"decision_ts": row.ts.isoformat(), "shock_category": row.shock_category, "representation_plan_digest": PLAN_DIGEST, "representation_output_fields": list(OUTPUT_FIELDS), "representation_decision_ts": row.ts.isoformat(), "terminal_outcome": outcome})
    return {"schema_version": "xrp-signed-impact-volatility-evaluation-v1.0.0", "question": QUESTION, "parameters": dict(params), "outcome": outcome, "passed": outcome == "positive", "held_out_evaluated": evaluate_test, "validation": validation, "test": test, "decision_records": records, **(chosen or {})}


def signed_impact_volatility_grid_evaluation(frame: pd.DataFrame, *, parameter_grid: Mapping[str, Sequence[Any]], minimum_direction_support: int = 40, cost_bps: float = 9.0) -> dict[str, Any]:
    keys = ("signed_impact_tail_quantile", "volatility_control_window_bars")
    if tuple(parameter_grid) != keys:
        raise ValueError("parameter grid order or fields differ from frozen contract")
    candidates = []
    for q in parameter_grid[keys[0]]:
        for window in parameter_grid[keys[1]]:
            params = {keys[0]: q, keys[1]: window}
            item = signed_impact_volatility_evaluation(frame, params=params, minimum_direction_support=minimum_direction_support, cost_bps=cost_bps, evaluate_test=False)
            item["record_digest_pending"] = True
            candidates.append(item)
    eligible = [(i, x) for i, x in enumerate(candidates) if x["outcome"] in {"positive", "negative"} and x.get("asymmetry") is not None]
    if not eligible:
        return {"question": QUESTION, "outcome": "failed", "passed": False, "held_out_evaluated": False, "selection_candidates": candidates, "selected_parameters": None, "test_open_count": 0, "decision_records": [], "asymmetry": 0.0, "confidence_interval_95": [0.0, 0.0], "positive_effect": 0.0, "negative_effect": 0.0, "doubled_cost_minimum_conditional_effect": 0.0, "support": {"positive": 0, "negative": 0}}
    _, selected = max(eligible, key=lambda pair: (abs(pair[1]["asymmetry"]), -pair[0]))
    result = signed_impact_volatility_evaluation(frame, params=selected["parameters"], minimum_direction_support=minimum_direction_support, cost_bps=cost_bps, evaluate_test=True)
    result.update(selection_candidates=candidates, selected_parameters=selected["parameters"], test_open_count=1)
    return result


@register_strategy("xrp_signed_impact_future_volatility_30m")
class XrpSignedImpactFutureVolatility30mStrategy(Strategy):
    """Classic-engine adapter which validates and retains causal observations."""
    def __init__(self, *, signed_impact_tail_quantile: float = .975, volatility_control_window_bars: int = 12) -> None:
        self.quantile = float(signed_impact_tail_quantile)
        self.window = int(volatility_control_window_bars)
        self.records: list[dict[str, Any]] = []

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: Mapping[str, Any]) -> list[Signal]:
        bar = bars_by_symbol.get("XRPUSDT")
        if bar is None:
            return []
        extra = bar.extra if isinstance(bar.extra, Mapping) else {}
        decision_ts = _timestamp(extra.get("representation_decision_ts"))
        values = [_number(extra.get(field)) for field in OUTPUT_FIELDS]
        reason = "consumed"
        valid = extra.get("representation_plan_digest") == PLAN_DIGEST and _fields(extra.get("representation_output_fields")) == OUTPUT_FIELDS
        valid &= decision_ts == _timestamp(ts) and values[0] is not None and values[0] >= 1_000_000 and values[1] is not None and values[1] >= 0
        valid &= all(value is not None for value in values)
        if valid:
            valid = math.isclose(values[3], values[2] / values[0], rel_tol=1e-10, abs_tol=1e-18) and values[4] >= 0
        if not valid:
            reason = "invalid_representation_payload"
        record = {"outcome": "consumed" if valid else "invalid", "reason": reason, "representation_plan_digest": extra.get("representation_plan_digest"), "representation_output_fields": list(_fields(extra.get("representation_output_fields")) or ()), "representation_decision_ts": decision_ts.isoformat() if decision_ts is not None else None}
        self.records.append(record)
        return [Signal(ts=ts, symbol="XRPUSDT", side=Side.BUY, signal_type="xrp_signed_volatility_representation_validation", confidence=0.0, metadata={"strategy": "xrp_signed_impact_future_volatility_30m", "research_observation_only": True, "no_order_authority": True, "native_payload_outcome": record["outcome"], "native_payload_reason": reason, **{key: record[key] for key in ("representation_plan_digest", "representation_output_fields", "representation_decision_ts")}})]
