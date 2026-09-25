"""Exact point-in-time BTC open-interest expansion research implementation.

The event strategy consumes the frozen adaptive representation only when its
row carries matching provenance.  The evaluator retains invalid decisions and
implements the preregistered chronological, matched, placebo and cost gates.
"""
from __future__ import annotations

from collections import defaultdict, deque
import json
import math
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.logging.decision_trace import make_decision_trace
from bt.strategy import register_strategy
from bt.strategy.base import Strategy


QUESTION = (
    "Does point-in-time open-interest expansion accompanying a completed 15m "
    "BTCUSDT return predict direction-dependent BTCUSDT close-to-close return "
    "over the next 60m, after controlling for prior return, realized volatility, "
    "funding-cycle position, and liquidity?"
)
PLAN_DIGEST = "9a00152f403d82662588e93c8f8eba47fabd182a1bbd67bd50768f5218d17efa"
OUTPUT_FIELDS = (
    "btc_15m_log_return",
    "btc_past_60m_log_return",
    "btc_past_6h_realized_volatility",
    "btc_15m_quote_volume",
)
PROVENANCE_FIELDS = (
    "representation_plan_digest",
    "representation_output_fields",
    "representation_decision_ts",
)
REQUIRED_PANEL_FIELDS = (
    "ts", "open", "high", "low", "close", "quote_volume",
    "open_interest", "oi_source_ts",
)


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
    try:
        return result.tz_convert("UTC")
    except (TypeError, ValueError):
        return None


def _lower_quantile(values: Sequence[float], quantile: float) -> float | None:
    if not values:
        return None
    return float(pd.Series(values).quantile(quantile, interpolation="lower"))


def _ordered_fields(value: Any) -> tuple[str, ...] | None:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
    if not isinstance(value, (list, tuple)) or not all(
        isinstance(item, str) for item in value
    ):
        return None
    return tuple(value)


def _complete_decision_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Compile exact causal 15m decisions and contiguous 60m targets."""
    missing = set(REQUIRED_PANEL_FIELDS) - set(frame.columns)
    if missing:
        raise ValueError(f"panel missing immutable fields: {sorted(missing)}")
    data = frame.loc[:, REQUIRED_PANEL_FIELDS].copy()
    data["ts"] = pd.to_datetime(data["ts"], utc=True, errors="raise")
    data["oi_source_ts"] = pd.to_datetime(
        data["oi_source_ts"], utc=True, errors="coerce"
    )
    data = data.sort_values("ts", kind="mergesort").reset_index(drop=True)
    if data["ts"].duplicated().any():
        raise ValueError("panel contains duplicate timestamps")
    data["bucket"] = data["ts"].dt.floor("15min")
    grouped = {key: part for key, part in data.groupby("bucket", sort=True)}
    if data.empty:
        return pd.DataFrame()
    buckets = pd.date_range(
        data["bucket"].min(), data["bucket"].max(), freq="15min", tz="UTC"
    )
    rows: list[dict[str, Any]] = []
    empty = data.iloc[0:0]
    for bucket in buckets:
        part = grouped.get(bucket, empty)
        expected = pd.date_range(bucket, periods=15, freq="1min", tz="UTC")
        numeric = part[["open", "high", "low", "close", "quote_volume"]].apply(
            pd.to_numeric, errors="coerce"
        )
        complete = bool(
            len(part) == 15
            and list(part["ts"]) == list(expected)
            and numeric.notna().all().all()
        )
        decision_ts = bucket + pd.Timedelta(minutes=15)
        eligible_oi = data.loc[
            (data["ts"] <= decision_ts)
            & data["oi_source_ts"].notna()
            & (data["oi_source_ts"] <= decision_ts)
        ]
        oi = oi_source = None
        if not eligible_oi.empty:
            latest_source = eligible_oi["oi_source_ts"].max()
            latest = eligible_oi.loc[
                eligible_oi["oi_source_ts"] == latest_source
            ].iloc[-1]
            oi, oi_source = _number(latest["open_interest"]), latest_source
        last = part.iloc[-1] if len(part) else None
        rows.append({
            "bucket_ts": bucket,
            "decision_ts": decision_ts,
            "complete": complete,
            "close": _number(last["close"]) if last is not None else None,
            "quote_volume": (
                float(numeric["quote_volume"].sum())
                if complete else None
            ),
            "open_interest": oi,
            "oi_source_ts": oi_source,
        })
    result = pd.DataFrame(rows)
    valid_close = pd.to_numeric(result["close"], errors="coerce").where(
        result["complete"]
    )
    positive_close = valid_close.where(valid_close > 0)
    one_return = np.log(positive_close).diff()
    result["return_15m"] = one_return
    result["prior_return_60m"] = np.log(positive_close).diff(4)
    result["realized_volatility_6h"] = one_return.rolling(
        24, min_periods=24
    ).std(ddof=0) * math.sqrt(24)
    valid_oi = pd.to_numeric(result["open_interest"], errors="coerce").where(
        result["complete"]
    ).where(lambda values: values > 0)
    result["oi_change"] = np.log(valid_oi).diff()
    result["placebo_oi_change"] = result["oi_change"].shift(1)
    result["future_return_60m"] = np.nan
    if len(result) > 4:
        result.loc[result.index[:-4], "future_return_60m"] = (
            np.log(positive_close.iloc[4:].to_numpy())
            - np.log(positive_close.iloc[:-4].to_numpy())
        )
    result["predictor_history_complete"] = [
        bool(
            index >= 24
            and result["complete"].iloc[index - 24:index + 1].all()
            and (
                result["decision_ts"].iloc[index]
                - result["decision_ts"].iloc[index - 24]
                == pd.Timedelta(minutes=360)
            )
        )
        for index in range(len(result))
    ]
    result["target_complete"] = [
        bool(
            index + 4 < len(result)
            and result["complete"].iloc[index + 1:index + 5].all()
            and result["decision_ts"].iloc[index + 4]
            - result["decision_ts"].iloc[index]
            == pd.Timedelta(minutes=60)
        )
        for index in range(len(result))
    ]
    result.loc[~result["target_complete"], "future_return_60m"] = np.nan
    result["funding_cycle_position"] = (
        result["decision_ts"].dt.hour * 60
        + result["decision_ts"].dt.minute
    ) % 480
    return result


def _split_rows(rows: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Chronological 60/20/20 split with a 60m purge and embargo."""
    count = len(rows)
    train_end, validation_end = int(count * 0.60), int(count * 0.80)
    raw = {
        "train": rows.iloc[:train_end],
        "validation": rows.iloc[train_end:validation_end],
        "test": rows.iloc[validation_end:],
    }
    output: dict[str, pd.DataFrame] = {}
    for name, part in raw.items():
        if part.empty:
            output[name] = part.copy()
            continue
        lower = part["decision_ts"].min()
        upper = part["decision_ts"].max()
        if name != "train":
            lower += pd.Timedelta(minutes=60)
        if name != "test":
            upper -= pd.Timedelta(minutes=60)
        output[name] = part.loc[
            (part["decision_ts"] >= lower) & (part["decision_ts"] <= upper)
        ].copy()
    return output


def _scale(train: pd.DataFrame, field: str) -> float:
    value = float(pd.to_numeric(train[field], errors="coerce").std(ddof=0))
    return value if math.isfinite(value) and value > 0 else 1.0


def _cycle_distance(left: float, right: float) -> float:
    difference = abs(left - right)
    return min(difference, 480.0 - difference) / 480.0


def _match_pairs(
    part: pd.DataFrame, *, scales: Mapping[str, float], direction: str
) -> list[dict[str, Any]]:
    treated = part.loc[part["treated"]].sort_values("decision_ts")
    controls = part.loc[part["valid"] & ~part["treated"]].sort_values("decision_ts")
    used: set[int] = set()
    multiplier = 1.0 if direction == "continuation" else -1.0
    pairs: list[dict[str, Any]] = []
    for treated_index, item in treated.iterrows():
        choices: list[tuple[float, pd.Timestamp, int, pd.Series]] = []
        for control_index, control in controls.iterrows():
            if control_index in used:
                continue
            distance = (
                abs(item["prior_return_60m"] - control["prior_return_60m"])
                / scales["prior_return_60m"]
                + abs(item["realized_volatility_6h"] - control["realized_volatility_6h"])
                / scales["realized_volatility_6h"]
                + _cycle_distance(
                    item["funding_cycle_position"],
                    control["funding_cycle_position"],
                )
                + abs(math.log(item["quote_volume"]) - math.log(control["quote_volume"]))
                / scales["log_liquidity"]
            )
            choices.append((distance, control["decision_ts"], control_index, control))
        if not choices:
            continue
        _, _, control_index, control = min(choices, key=lambda value: value[:2])
        used.add(control_index)
        matched_outcome = multiplier * math.copysign(1.0, item["return_15m"]) * (
            item["future_return_60m"] - control["future_return_60m"]
        )
        # Compare actual and one-bar-lag predictor contributions on the exact
        # same treated/control timestamps.  Continuous, train-scaled contrasts
        # avoid manufacturing a zero placebo whenever binary flags coincide.
        oi_scale = scales["oi_change"]
        actual_predictor_contrast = (
            item["oi_change"] - control["oi_change"]
        ) / oi_scale
        placebo_predictor_contrast = (
            item["placebo_oi_change"] - control["placebo_oi_change"]
        ) / oi_scale
        actual_predictor_payoff = matched_outcome * actual_predictor_contrast
        placebo_predictor_payoff = matched_outcome * placebo_predictor_contrast
        pairs.append({
            "treated_decision_ts": item["decision_ts"].isoformat(),
            "control_decision_ts": control["decision_ts"].isoformat(),
            "treated_index": int(treated_index),
            "control_index": int(control_index),
            "direction": "positive" if item["return_15m"] > 0 else "negative",
            "actual_difference": float(matched_outcome),
            "actual_predictor_contrast": float(actual_predictor_contrast),
            "placebo_predictor_contrast": float(placebo_predictor_contrast),
            "actual_predictor_payoff": float(actual_predictor_payoff),
            "placebo_difference": float(placebo_predictor_payoff),
            "actual_minus_placebo": float(
                actual_predictor_payoff - placebo_predictor_payoff
            ),
        })
    return pairs


def _mean_ci(values: Sequence[float]) -> tuple[float, list[float]]:
    if not values:
        return 0.0, [float("-inf"), float("inf")]
    mean = float(np.mean(values))
    if len(values) < 2:
        return mean, [float("-inf"), float("inf")]
    standard_error = float(np.std(values, ddof=1) / math.sqrt(len(values)))
    return mean, [mean - 1.96 * standard_error, mean + 1.96 * standard_error]


def _maximum_drawdown(returns: Sequence[float]) -> float:
    equity = 1.0
    peak = 1.0
    drawdown = 0.0
    for value in returns:
        equity *= max(0.0, 1.0 + float(value))
        peak = max(peak, equity)
        drawdown = min(drawdown, equity / peak - 1.0)
    return float(drawdown)


def oi_expansion_evaluation(
    frame: pd.DataFrame,
    *,
    params: Mapping[str, Any],
    minimum_direction_support: int = 30,
    cost_bps: float = 9.0,
    evaluate_test: bool = True,
) -> dict[str, Any]:
    """Run the exact frozen test, retaining invalid and failed outcomes."""
    direction = str(params["direction"])
    if direction not in {"continuation", "reversal"}:
        raise ValueError("direction must be continuation or reversal")
    oi_percentile = float(params["open_interest_expansion_percentile"])
    return_percentile = float(params["absolute_return_percentile"])
    if not 0 < oi_percentile < 1 or not 0 < return_percentile < 1:
        raise ValueError("percentiles must be between zero and one")
    rows = _complete_decision_rows(frame)
    if rows.empty:
        return {
            "schema_version": "btc-oi-expansion-evaluation-v2.0.0",
            "question": QUESTION, "parameters": dict(params), "outcome": "invalid",
            "decision_records": [], "pairs": [], "maximum_drawdown": 0.0,
        }
    splits = _split_rows(rows)
    train = splits["train"]
    oi_values = pd.to_numeric(train["oi_change"], errors="coerce").dropna().tolist()
    return_values = pd.to_numeric(train["return_15m"], errors="coerce").abs().dropna().tolist()
    oi_threshold = _lower_quantile(oi_values, oi_percentile)
    return_threshold = _lower_quantile(return_values, return_percentile)
    rows["oi_threshold"] = oi_threshold
    rows["return_threshold"] = return_threshold
    finite_fields = [
        "return_15m", "prior_return_60m", "realized_volatility_6h",
        "quote_volume", "oi_change", "placebo_oi_change", "future_return_60m",
    ]
    rows["valid"] = (
        rows["complete"]
        & rows["predictor_history_complete"]
        & rows["target_complete"]
        & (rows["quote_volume"] >= 1_000_000.0)
        & rows[finite_fields].notna().all(axis=1)
        & rows["oi_source_ts"].notna()
        & (rows["oi_source_ts"] <= rows["decision_ts"])
        & (oi_threshold is not None)
        & (return_threshold is not None)
    )
    rows["treated"] = (
        rows["valid"]
        & (rows["oi_change"] > float(oi_threshold or 0.0))
        & (rows["oi_change"] > 0)
        & (rows["return_15m"].abs() >= float(return_threshold or math.inf))
    )
    split_labels = pd.Series("purged", index=rows.index, dtype=object)
    for name, part in splits.items():
        split_labels.loc[part.index] = name
    rows["split"] = split_labels
    rows["status"] = np.where(rows["valid"], "control", "invalid")
    rows.loc[rows["treated"], "status"] = "treated"
    train_for_scale = rows.loc[rows["split"] == "train"].copy()
    train_for_scale["log_liquidity"] = np.log(train_for_scale["quote_volume"])
    scales = {
        "prior_return_60m": _scale(train_for_scale, "prior_return_60m"),
        "realized_volatility_6h": _scale(train_for_scale, "realized_volatility_6h"),
        "log_liquidity": _scale(train_for_scale, "log_liquidity"),
        "oi_change": _scale(train_for_scale, "oi_change"),
    }
    pair_sets = {
        "validation": _match_pairs(
            rows.loc[rows["split"] == "validation"],
            scales=scales,
            direction=direction,
        ),
        "test": (
            _match_pairs(
                rows.loc[rows["split"] == "test"],
                scales=scales,
                direction=direction,
            )
            if evaluate_test else []
        ),
    }
    validation_mean, validation_ci = _mean_ci(
        [item["actual_difference"] for item in pair_sets["validation"]]
    )
    pairs = pair_sets["test"]
    actual = [item["actual_difference"] for item in pairs]
    placebo_contrast = [item["actual_minus_placebo"] for item in pairs]
    effect, confidence = _mean_ci(actual)
    placebo_effect, placebo_confidence = _mean_ci(placebo_contrast)
    supports = {
        sign: sum(item["direction"] == sign for item in pairs)
        for sign in ("positive", "negative")
    }
    doubled_cost = effect - 4.0 * cost_bps / 10_000.0
    supported = min(supports.values(), default=0) >= minimum_direction_support
    if not rows["valid"].any():
        outcome = "invalid"
    elif not supported or len(pairs) < 2:
        outcome = "failed"
    elif confidence[0] > 0 and placebo_confidence[0] > 0 and doubled_cost > 0:
        outcome = "positive"
    else:
        outcome = "negative"
    test_indices = {item["treated_index"] for item in pairs}
    for index in test_indices:
        rows.loc[index, "status"] = outcome
    records = []
    for row in rows.itertuples():
        records.append({
            "decision_ts": row.decision_ts.isoformat(),
            "status": row.status,
            "split": row.split,
            "valid": bool(row.valid),
            "treated": bool(row.treated),
            "oi_source_ts": (
                row.oi_source_ts.isoformat() if pd.notna(row.oi_source_ts) else None
            ),
            "return_15m": _number(row.return_15m),
            "oi_change": _number(row.oi_change),
            "placebo_oi_change": _number(row.placebo_oi_change),
            "future_return_60m": _number(row.future_return_60m),
            "predictor_history_complete": bool(row.predictor_history_complete),
            "target_complete": bool(row.target_complete),
        })
    return {
        "schema_version": "btc-oi-expansion-evaluation-v2.0.0",
        "question": QUESTION,
        "parameters": dict(params),
        "outcome": outcome,
        "thresholds_fit_split": "train",
        "validation_directional_effect": validation_mean,
        "validation_confidence_interval_95": validation_ci,
        "test_directional_effect": effect,
        "confidence_interval_95": confidence,
        "paired_actual_minus_placebo_effect": placebo_effect,
        "paired_actual_minus_placebo_confidence_interval_95": placebo_confidence,
        "doubled_cost_directional_effect": doubled_cost,
        "directional_support": supports,
        "matched_support": len(pairs),
        "maximum_drawdown": _maximum_drawdown(
            [value - 2.0 * cost_bps / 10_000.0 for value in actual]
        ),
        "split_counts": {
            name: int((rows["split"] == name).sum())
            for name in ("train", "validation", "test", "purged")
        },
        "decision_records": records,
        "pairs": pairs,
    }


def oi_expansion_grid_evaluation(
    frame: pd.DataFrame,
    *,
    parameter_grid: Mapping[str, Sequence[Any]],
    minimum_direction_support: int = 30,
    cost_bps: float = 9.0,
) -> dict[str, Any]:
    """Select on validation only, then open test once for the selected variant."""
    keys = (
        "open_interest_expansion_percentile",
        "absolute_return_percentile",
        "direction",
    )
    if tuple(parameter_grid) != keys:
        raise ValueError("parameter grid order or fields differ from the frozen contract")
    candidates: list[dict[str, Any]] = []
    for oi_value in parameter_grid[keys[0]]:
        for return_value in parameter_grid[keys[1]]:
            for direction in parameter_grid[keys[2]]:
                params = {
                    keys[0]: oi_value, keys[1]: return_value, keys[2]: direction
                }
                result = oi_expansion_evaluation(
                    frame, params=params,
                    minimum_direction_support=minimum_direction_support,
                    cost_bps=cost_bps,
                    evaluate_test=False,
                )
                candidates.append({
                    "parameters": params,
                    "validation_directional_effect": result[
                        "validation_directional_effect"
                    ],
                })
    eligible = [
        item for item in candidates
        if item["validation_directional_effect"] > 0
    ]
    if not eligible:
        return {
            "schema_version": "btc-oi-expansion-grid-evaluation-v2.0.0",
            "question": QUESTION,
            "outcome": "negative",
            "selection_candidates": candidates,
            "selected_parameters": None,
            "selection_reason": "no_positive_validation_directional_effect",
            "test_open_count": 0,
            "pairs": [],
            "decision_records": [],
        }
    selected = max(
        eligible,
        key=lambda item: (
            item["validation_directional_effect"],
            tuple(str(item["parameters"][key]) for key in keys),
        ),
    )
    result = oi_expansion_evaluation(
        frame,
        params=selected["parameters"],
        minimum_direction_support=minimum_direction_support,
        cost_bps=cost_bps,
        evaluate_test=True,
    )
    result["selection_candidates"] = candidates
    result["selected_parameters"] = selected["parameters"]
    result["test_open_count"] = 1
    return result


@register_strategy("btc_oi_expansion_return_asymmetry_60m")
class BtcOiExpansionReturnAsymmetry60mStrategy(Strategy):
    """Engine adapter for the exact 15m OI-expansion directional setup."""

    def __init__(
        self,
        *,
        open_interest_expansion_percentile: float = 0.8,
        absolute_return_percentile: float = 0.5,
        direction: str = "continuation",
        r_per_trade: float = 0.005,
        history_window: int = 96,
    ) -> None:
        if direction not in {"continuation", "reversal"}:
            raise ValueError("direction must be continuation or reversal")
        self._oi_q = float(open_interest_expansion_percentile)
        self._return_q = float(absolute_return_percentile)
        self._direction = direction
        self._r_per_trade = float(r_per_trade)
        self._oi_history: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=history_window)
        )
        self._return_history: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=history_window)
        )
        self._previous_oi: dict[str, tuple[pd.Timestamp, float]] = {}
        self._last_decision: dict[str, pd.Timestamp] = {}
        self._exit_submitted: set[str] = set()

    @staticmethod
    def _position(ctx: Mapping[str, Any], symbol: str) -> Side | None:
        positions = ctx.get("positions")
        raw = positions.get(symbol) if isinstance(positions, Mapping) else None
        side = raw.get("side") if isinstance(raw, Mapping) else None
        if isinstance(side, Side):
            return side
        if isinstance(side, str) and side.lower() in {"buy", "sell"}:
            return Side.BUY if side.lower() == "buy" else Side.SELL
        return None

    @staticmethod
    def _target_exit(ctx: Mapping[str, Any], symbol: str) -> pd.Timestamp | None:
        positions = ctx.get("positions")
        raw = positions.get(symbol) if isinstance(positions, Mapping) else None
        metadata = raw.get("metadata") if isinstance(raw, Mapping) else None
        return _timestamp(metadata.get("target_exit_ts")) if isinstance(metadata, Mapping) else None

    def on_bars(
        self,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Bar],
        tradeable: set[str],
        ctx: Mapping[str, Any],
    ) -> list[Signal]:
        signals: list[Signal] = []
        for symbol, bar in sorted(bars_by_symbol.items()):
            position = self._position(ctx, symbol)
            if position is None:
                self._exit_submitted.discard(symbol)
            target_exit = self._target_exit(ctx, symbol)
            if (
                position is not None and target_exit is not None
                and symbol not in self._exit_submitted
                and ts >= target_exit - pd.Timedelta(minutes=1)
            ):
                signals.append(Signal(
                    ts=ts, symbol=symbol,
                    side=Side.SELL if position == Side.BUY else Side.BUY,
                    signal_type="btc_oi_expansion_60m_time_exit", confidence=1.0,
                    metadata={"strategy": "btc_oi_expansion_return_asymmetry_60m", "close_only": True, "is_exit": True, "exit_reason": "fixed_60m_target_horizon"},
                ))
                self._exit_submitted.add(symbol)
            extra = bar.extra if isinstance(bar.extra, Mapping) else {}
            decision_ts = _timestamp(extra.get("representation_decision_ts"))
            oi_source_ts = _timestamp(extra.get("oi_source_ts"))
            fields = _ordered_fields(extra.get("representation_output_fields"))
            values = {field: _number(extra.get(field)) for field in OUTPUT_FIELDS}
            oi = _number(extra.get("open_interest"))
            provenance_valid = bool(
                extra.get("representation_plan_digest") == PLAN_DIGEST
                and fields == OUTPUT_FIELDS
                and decision_ts is not None and decision_ts == ts
                and oi_source_ts is not None and oi_source_ts <= ts
                and all(value is not None for value in values.values())
                and oi is not None and oi > 0
            )
            if self._last_decision.get(symbol) == ts:
                continue
            if not provenance_valid:
                # Adaptive rows exist only at completed 15m decision
                # boundaries. Ordinary intervening 1m bars are not gaps, but
                # a missing/invalid expected boundary breaks OI continuity.
                if ts.second == 0 and ts.microsecond == 0 and ts.minute % 15 == 0:
                    self._previous_oi.pop(symbol, None)
                continue
            self._last_decision[symbol] = ts
            prior = self._previous_oi.get(symbol)
            self._previous_oi[symbol] = (ts, float(oi))
            if prior is None:
                continue
            prior_ts, prior_oi = prior
            if prior_oi <= 0 or ts - prior_ts != pd.Timedelta(minutes=15):
                continue
            oi_change = math.log(float(oi) / prior_oi)
            return_15m = float(values["btc_15m_log_return"])
            oi_history = self._oi_history[symbol]
            return_history = self._return_history[symbol]
            oi_threshold = _lower_quantile(list(oi_history), self._oi_q)
            return_threshold = _lower_quantile(list(return_history), self._return_q)
            oi_history.append(oi_change)
            return_history.append(abs(return_15m))
            eligible = bool(
                symbol in tradeable and position is None
                and float(values["btc_15m_quote_volume"]) >= 1_000_000.0
                and oi_threshold is not None and return_threshold is not None
                and oi_change > 0 and oi_change > oi_threshold
                and abs(return_15m) >= return_threshold and return_15m != 0
            )
            if not eligible:
                continue
            continuation = Side.BUY if return_15m > 0 else Side.SELL
            side = continuation if self._direction == "continuation" else (
                Side.SELL if continuation == Side.BUY else Side.BUY
            )
            stop_distance = max(float(bar.close) * 0.03, 1e-12)
            stop_price = float(bar.close) - stop_distance if side == Side.BUY else float(bar.close) + stop_distance
            trace = make_decision_trace(
                reason_code="point_in_time_oi_expansion_return_interaction",
                setup_class="positioning_conditioned_return",
                conditions_bool_map={"adaptive_provenance_valid": True, "oi_expansion": True, "absolute_return_gate": True, "liquidity_gate": True},
                permission_layer_state={"tradeable": symbol in tradeable},
                parameter_combination={"open_interest_expansion_percentile": self._oi_q, "absolute_return_percentile": self._return_q, "direction": self._direction},
                gate_values={"return_15m": return_15m, "oi_change": oi_change, "quote_volume_15m": values["btc_15m_quote_volume"]},
                gate_thresholds={"oi_change": oi_threshold, "absolute_return": return_threshold, "liquidity_floor_usd": 1_000_000.0},
            )
            signals.append(Signal(
                ts=ts, symbol=symbol, side=side,
                signal_type="btc_oi_expansion_return_asymmetry_entry", confidence=1.0,
                metadata={
                    "strategy": "btc_oi_expansion_return_asymmetry_60m",
                    "strategy_id": "ALPHA-003-BTC-OI-EXPANSION-RETURN-ASYMMETRY-60M",
                    "family_variant": self._direction,
                    "family_pattern": "positioning_conditioned_return",
                    "entry_reason": "point_in_time_oi_expansion_return_interaction",
                    "entry_price": float(bar.close), "entry_reference_price": float(bar.close), "intended_entry_price": float(bar.close),
                    "signal_timeframe": "15m", "execution_timeframe": "1m",
                    "risk_accounting": "engine_canonical_R", "r_per_trade": self._r_per_trade,
                    "sizing_mode": "risk_at_stop", "cap_policy": "allow_clip_with_truth",
                    "stop_model": "fixed_3pct_from_decision_close", "stop_price": stop_price,
                    "entry_stop_price": stop_price, "stop_distance": stop_distance,
                    "target_exit_ts": (ts + pd.Timedelta(minutes=60)).isoformat(),
                    "target_horizon_minutes": 60,
                    "representation_plan_digest": PLAN_DIGEST,
                    "representation_output_fields": list(OUTPUT_FIELDS),
                    "representation_decision_ts": decision_ts.isoformat(),
                    "oi_source_ts": oi_source_ts.isoformat(), "entry_state_oi_level": oi,
                    "entry_state_oi_change": oi_change,
                    "requested_risk_amount": None, "risk_utilization_pct": None, "under_risked_trade": None,
                    "decision_trace": trace,
                },
            ))
        return signals
