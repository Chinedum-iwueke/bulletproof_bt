"""Exact causal ETH liquidity-displacement/BTC residual-return experiment."""

from __future__ import annotations

from collections import defaultdict, deque
import json
import math
from itertools import product
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.logging.decision_trace import make_decision_trace
from bt.strategy import register_strategy
from bt.strategy.base import Strategy


QUESTION = "Does an extreme point-in-time ETHUSDT 15m liquidity-adjusted displacement predict a nonzero BTCUSDT residual close-to-close log return over the next 60m, after controlling for the contemporaneously completed BTCUSDT 15m return and prior-only BTCUSDT realized volatility?"
PLAN_DIGEST = "a36257fac2a22196ba3770bef321c1972cbdf40167a5176cd53f53153cfee096"
OUTPUT_FIELDS = (
    "eth_15m_log_return",
    "btc_15m_log_return",
    "eth_15m_quote_volume",
    "btc_15m_quote_volume",
    "eth_15m_signed_liquidity_displacement",
    "btc_15m_realized_volatility_96",
)
MIN_HISTORY = 35_040
LIQUIDITY_FLOOR = 1_000_000.0


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
    return result.tz_convert("UTC") if result.tz is not None else None


def _ordered_fields(value: Any) -> tuple[str, ...] | None:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
    return (
        tuple(value)
        if isinstance(value, (list, tuple)) and all(isinstance(x, str) for x in value)
        else None
    )


def _bars(frame: pd.DataFrame, prefix: str) -> pd.DataFrame:
    required = {"ts", "close", "quote_volume"}
    if required - set(frame):
        raise ValueError(
            f"{prefix} panel missing immutable fields: {sorted(required - set(frame))}"
        )
    data = frame.loc[:, sorted(required)].copy()
    data["ts"] = pd.to_datetime(data["ts"], utc=True, errors="raise")
    data = data.sort_values("ts", kind="mergesort")
    if data["ts"].duplicated().any():
        raise ValueError(f"{prefix} panel contains duplicate timestamps")
    data["bucket"] = data["ts"].dt.floor("15min")
    rows = []
    grouped = {bucket: part for bucket, part in data.groupby("bucket", sort=True)}
    buckets = (
        pd.date_range(data["bucket"].min(), data["bucket"].max(), freq="15min")
        if not data.empty
        else []
    )
    for bucket in buckets:
        part = grouped.get(bucket, data.iloc[0:0])
        expected = pd.date_range(bucket, periods=15, freq="1min", tz="UTC")
        close = pd.to_numeric(part["close"], errors="coerce")
        volume = pd.to_numeric(part["quote_volume"], errors="coerce")
        complete = bool(
            len(part) == 15
            and list(part["ts"]) == list(expected)
            and close.notna().all()
            and (close > 0).all()
            and volume.notna().all()
            and (volume >= 0).all()
        )
        rows.append(
            {
                "decision_ts": bucket + pd.Timedelta(minutes=15),
                f"{prefix}_complete": complete,
                f"{prefix}_close": float(close.iloc[-1]) if complete else np.nan,
                f"{prefix}_volume": float(volume.sum()) if complete else np.nan,
            }
        )
    return pd.DataFrame(rows)


def compile_decision_rows(panels: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    """Build complete synchronized bars, strictly-prior controls, and future target."""
    if set(panels) != {"BTCUSDT", "ETHUSDT"}:
        raise ValueError("exact BTCUSDT and ETHUSDT panels are required")
    rows = (
        _bars(panels["BTCUSDT"], "btc")
        .merge(
            _bars(panels["ETHUSDT"], "eth"),
            on="decision_ts",
            how="outer",
            validate="one_to_one",
        )
        .sort_values("decision_ts")
        .reset_index(drop=True)
    )
    complete = rows[["btc_complete", "eth_complete"]].fillna(False).all(axis=1)
    btc_close = rows["btc_close"].where(complete)
    eth_close = rows["eth_close"].where(complete)
    rows["btc_return"] = np.log(btc_close).diff()
    rows["eth_return"] = np.log(eth_close).diff()
    rows["displacement"] = rows["eth_return"] / rows["eth_volume"]
    # Shift is mandatory: signal-bar BTC return is excluded from this control.
    rows["prior_btc_volatility"] = rows["btc_return"].shift(1).rolling(
        96, min_periods=96
    ).std(ddof=0) * math.sqrt(96)
    rows["prior_only_volatility_source_end_ts"] = rows["decision_ts"].shift(1)
    rows["future_btc_return"] = np.nan
    if len(rows) > 4:
        rows.loc[rows.index[:-4], "future_btc_return"] = np.log(
            btc_close.iloc[4:].to_numpy()
        ) - np.log(btc_close.iloc[:-4].to_numpy())
    rows["target_complete"] = [
        bool(
            i + 4 < len(rows)
            and complete.iloc[i + 1 : i + 5].all()
            and rows.loc[i + 4, "decision_ts"] - rows.loc[i, "decision_ts"]
            == pd.Timedelta(minutes=60)
        )
        for i in range(len(rows))
    ]
    rows.loc[~rows["target_complete"], "future_btc_return"] = np.nan
    rows["complete"] = complete
    return rows


def _split(rows: pd.DataFrame) -> dict[str, pd.DataFrame]:
    a, b = len(rows) * 6 // 10, len(rows) * 8 // 10
    raw = {"train": rows.iloc[:a], "validation": rows.iloc[a:b], "test": rows.iloc[b:]}
    result = {}
    for name, part in raw.items():
        part = part.copy()
        if not part.empty and name != "train":
            part = part.loc[
                part.decision_ts >= part.decision_ts.min() + pd.Timedelta(minutes=60)
            ]
        if not part.empty and name != "test":
            part = part.loc[
                part.decision_ts <= part.decision_ts.max() - pd.Timedelta(minutes=60)
            ]
        result[name] = part
    return result


def _ci(values: Sequence[float]) -> tuple[float, list[float]]:
    if not values:
        return 0.0, [0.0, 0.0]
    mean = float(np.mean(values))
    if len(values) < 2:
        return mean, [mean, mean]
    margin = 1.96 * float(np.std(values, ddof=1)) / math.sqrt(len(values))
    return mean, [mean - margin, mean + margin]


def liquidity_displacement_evaluation(
    panels: Mapping[str, pd.DataFrame],
    *,
    params: Mapping[str, Any],
    minimum_history: int = MIN_HISTORY,
    minimum_extreme_support: int = 50,
    minimum_matched_support: int = 30,
    evaluate_test: bool = True,
    evaluation_start: Any = None,
    evaluation_end: Any = None,
) -> dict[str, Any]:
    rows = compile_decision_rows(panels)
    threshold_field = (
        rows["displacement"]
        .abs()
        .shift(1)
        .rolling(minimum_history, min_periods=minimum_history)
        .quantile(
            float(params["eth_displacement_tail_percentile"]), interpolation="lower"
        )
    )
    rows["prior_displacement_threshold"] = threshold_field
    if evaluation_start is not None:
        rows = rows.loc[rows.decision_ts >= pd.Timestamp(evaluation_start)].copy()
    if evaluation_end is not None:
        rows = rows.loc[rows.decision_ts < pd.Timestamp(evaluation_end)].copy()
    rows = rows.reset_index(drop=True)
    valid = (
        rows["complete"]
        & rows["target_complete"]
        & rows["btc_return"].notna()
        & rows["prior_btc_volatility"].notna()
        & rows["displacement"].notna()
        & rows["prior_displacement_threshold"].notna()
        & (rows["btc_volume"] >= LIQUIDITY_FLOOR)
        & (rows["eth_volume"] >= LIQUIDITY_FLOOR)
    )
    decision_rows = rows.loc[valid].reset_index(drop=True)
    splits = _split(decision_rows)
    train = splits["train"]
    if len(train) < 3:
        return {
            "schema_version": "eth-liquidity-residual-evaluation-v1.0.0",
            "question": QUESTION,
            "parameters": dict(params),
            "outcome": "invalid",
            "reason": "insufficient_preregistered_history",
            "passed": False,
            "extreme_support": 0,
            "matched_support": 0,
            "validation_directional_effect": 0.0,
            "test_directional_effect": 0.0,
            "confidence_interval_95": [0.0, 0.0],
            "doubled_cost_directional_effect": 0.0,
            "maximum_drawdown": 0.0,
            "pairs": [],
        }
    x = np.column_stack(
        [np.ones(len(train)), train["btc_return"], train["prior_btc_volatility"]]
    )
    coefficients = np.linalg.lstsq(
        x, train["future_btc_return"].to_numpy(), rcond=None
    )[0]
    scales = {
        field: max(float(train[field].std(ddof=0)), 1e-12)
        for field in ("btc_return", "prior_btc_volatility")
    }
    direction = str(params["response_direction"])

    def evaluate(part: pd.DataFrame) -> dict[str, Any]:
        part = part.copy()
        controls = part.loc[part.displacement.abs() < part.prior_displacement_threshold]
        extremes = part.loc[
            part.displacement.abs() >= part.prior_displacement_threshold
        ]
        used: set[int] = set()
        pairs = []
        for index, item in extremes.sort_values("decision_ts").iterrows():
            candidates = [
                (
                    abs(item.btc_return - c.btc_return) / scales["btc_return"]
                    + abs(item.prior_btc_volatility - c.prior_btc_volatility)
                    / scales["prior_btc_volatility"],
                    c.decision_ts,
                    j,
                    c,
                )
                for j, c in controls.iterrows()
                if j not in used
            ]
            if not candidates:
                continue
            _, _, selected, control = min(candidates, key=lambda value: value[:2])
            used.add(selected)
            residual = item.future_btc_return - float(
                coefficients
                @ np.array([1.0, item.btc_return, item.prior_btc_volatility])
            )
            control_residual = control.future_btc_return - float(
                coefficients
                @ np.array([1.0, control.btc_return, control.prior_btc_volatility])
            )
            orientation = math.copysign(1.0, item.eth_return) * (
                1.0 if direction == "continuation" else -1.0
            )
            difference = orientation * (residual - control_residual)
            pairs.append(
                {
                    "treated_decision_ts": item.decision_ts.isoformat(),
                    "control_decision_ts": control.decision_ts.isoformat(),
                    "signed_residual_difference": float(difference),
                }
            )
        values = [p["signed_residual_difference"] for p in pairs]
        mean, ci = _ci(values)
        registered_round_trip_cost = 0.0018
        return {
            "extreme_support": int(len(extremes)),
            "matched_support": len(pairs),
            "effect": mean - registered_round_trip_cost,
            "confidence_interval_95": [
                ci[0] - registered_round_trip_cost,
                ci[1] - registered_round_trip_cost,
            ],
            "doubled_cost_effect": mean - 2 * registered_round_trip_cost,
            "registered_round_trip_cost": registered_round_trip_cost,
            "pairs": pairs,
        }

    validation = evaluate(splits["validation"])
    heldout = (
        evaluate(splits["test"])
        if evaluate_test
        else {
            "extreme_support": 0,
            "matched_support": 0,
            "effect": 0.0,
            "confidence_interval_95": [0.0, 0.0],
            "doubled_cost_effect": 0.0,
            "pairs": [],
        }
    )
    selected = heldout if evaluate_test else validation
    enough = (
        selected["extreme_support"] >= minimum_extreme_support
        and selected["matched_support"] >= minimum_matched_support
    )
    outcome = (
        "failed"
        if not enough
        else (
            "positive"
            if selected["confidence_interval_95"][0] > 0
            and selected["doubled_cost_effect"] > 0
            else "negative"
        )
    )
    cumulative = np.cumsum(
        [
            p["signed_residual_difference"] - selected["registered_round_trip_cost"]
            for p in selected["pairs"]
        ]
    )
    drawdown = (
        float(np.min(cumulative - np.maximum.accumulate(np.r_[0.0, cumulative])[:-1]))
        if len(cumulative)
        else 0.0
    )
    return {
        "schema_version": "eth-liquidity-residual-evaluation-v1.0.0",
        "question": QUESTION,
        "parameters": dict(params),
        "outcome": outcome,
        "passed": outcome == "positive",
        "threshold_fit_policy": "rolling_prior_only",
        "residual_coefficients_fit_split": "train",
        "residual_coefficients": coefficients.tolist(),
        "registered_round_trip_cost": selected["registered_round_trip_cost"],
        "extreme_support": selected["extreme_support"],
        "matched_support": selected["matched_support"],
        "validation_directional_effect": validation["effect"],
        "test_directional_effect": heldout["effect"],
        "confidence_interval_95": selected["confidence_interval_95"],
        "doubled_cost_directional_effect": selected["doubled_cost_effect"],
        "maximum_drawdown": drawdown,
        "pairs": selected["pairs"],
    }


def liquidity_displacement_grid_evaluation(
    panels: Mapping[str, pd.DataFrame],
    *,
    parameter_grid: Mapping[str, Sequence[Any]],
    **kwargs: Any,
) -> dict[str, Any]:
    names = tuple(parameter_grid)
    candidates = []
    for values in product(*(parameter_grid[name] for name in names)):
        params = dict(zip(names, values, strict=True))
        candidates.append(
            liquidity_displacement_evaluation(
                panels, params=params, evaluate_test=False, **kwargs
            )
        )
    eligible = [
        item
        for item in candidates
        if item["outcome"] in {"positive", "negative"}
        and item["validation_directional_effect"] > 0
    ]
    if not eligible:
        outcomes = {item["outcome"] for item in candidates}
        outcome = (
            "negative"
            if "negative" in outcomes
            else "failed"
            if "failed" in outcomes
            else "invalid"
        )
        return {
            "schema_version": "eth-liquidity-residual-grid-v1.0.0",
            "outcome": outcome,
            "selection_candidates": candidates,
            "selected_parameters": None,
            "test_open_count": 0,
            "passed": False,
        }
    winner = max(
        eligible,
        key=lambda item: (
            item["validation_directional_effect"],
            tuple(str(item["parameters"][name]) for name in names),
        ),
    )
    result = liquidity_displacement_evaluation(
        panels, params=winner["parameters"], evaluate_test=True, **kwargs
    )
    return {
        **result,
        "schema_version": "eth-liquidity-residual-grid-v1.0.0",
        "selection_candidates": candidates,
        "selected_parameters": winner["parameters"],
        "test_open_count": 1,
    }


@register_strategy("eth_liquidity_displacement_btc_residual_60m")
class EthLiquidityDisplacementBtcResidual60mStrategy(Strategy):
    def __init__(
        self,
        *,
        eth_displacement_tail_percentile: float = 0.975,
        response_direction: str = "continuation",
        r_per_trade: float = 0.005,
        **_: Any,
    ) -> None:
        self.quantile = float(eth_displacement_tail_percentile)
        self.direction = response_direction
        self.r_per_trade = float(r_per_trade)
        self.history: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=MIN_HISTORY)
        )
        self.btc_returns: deque[float] = deque(maxlen=96)
        self.last: dict[str, pd.Timestamp] = {}
        self.exit_submitted: set[str] = set()

    def on_bars(
        self,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Bar],
        tradeable: set[str],
        ctx: Mapping[str, Any],
    ) -> list[Signal]:
        bar = bars_by_symbol.get("BTCUSDT")
        if bar is None:
            return []
        position = (
            (ctx.get("positions") or {}).get("BTCUSDT")
            if isinstance(ctx.get("positions"), Mapping)
            else None
        )
        raw_side = position.get("side") if isinstance(position, Mapping) else None
        if raw_side is not None:
            metadata = position.get("metadata", {})
            target = (
                _timestamp(metadata.get("target_exit_ts"))
                if isinstance(metadata, Mapping)
                else None
            )
            if (
                target is not None
                and ts >= target - pd.Timedelta(minutes=1)
                and "BTCUSDT" not in self.exit_submitted
            ):
                self.exit_submitted.add("BTCUSDT")
                return [
                    Signal(
                        ts=ts,
                        symbol="BTCUSDT",
                        side=Side.SELL if raw_side in (Side.BUY, "buy") else Side.BUY,
                        signal_type="eth_liquidity_displacement_60m_exit",
                        confidence=1.0,
                        metadata={
                            "strategy": "eth_liquidity_displacement_btc_residual_60m",
                            "close_only": True,
                            "is_exit": True,
                            "exit_reason": "fixed_60m_target_horizon",
                        },
                    )
                ]
            return []
        self.exit_submitted.discard("BTCUSDT")
        extra = bar.extra if isinstance(bar.extra, Mapping) else {}
        decision_ts = _timestamp(extra.get("representation_decision_ts"))
        source_end = _timestamp(extra.get("prior_only_volatility_source_end_ts"))
        fields = _ordered_fields(extra.get("representation_output_fields"))
        values = {field: _number(extra.get(field)) for field in OUTPUT_FIELDS}
        represented = bool(
            extra.get("representation_plan_digest") == PLAN_DIGEST
            and fields == OUTPUT_FIELDS
            and decision_ts == ts
        )
        if not represented:
            if ts.second == 0 and ts.microsecond == 0 and ts.minute % 15 == 0:
                self.btc_returns.clear()
                self.history["BTCUSDT"].clear()
            return []
        if self.last.get("BTCUSDT") == ts:
            return []
        prior_decision = self.last.get("BTCUSDT")
        self.last["BTCUSDT"] = ts
        if prior_decision is not None and ts - prior_decision != pd.Timedelta(
            minutes=15
        ):
            self.btc_returns.clear()
            self.history["BTCUSDT"].clear()
        btc_return = values["btc_15m_log_return"]
        supplied_volatility = values["btc_15m_realized_volatility_96"]
        expected_volatility = (
            float(np.std(self.btc_returns, ddof=0) * math.sqrt(96))
            if len(self.btc_returns) == 96
            else None
        )
        volatility_valid = bool(
            source_end == ts - pd.Timedelta(minutes=15)
            and supplied_volatility is not None
            and expected_volatility is not None
            and math.isclose(
                supplied_volatility,
                expected_volatility,
                rel_tol=1e-10,
                abs_tol=1e-12,
            )
        )
        if btc_return is not None:
            self.btc_returns.append(btc_return)
        displacement_value = values["eth_15m_signed_liquidity_displacement"]
        if displacement_value is None:
            return []
        history = self.history["BTCUSDT"]
        displacement = abs(float(displacement_value))
        threshold = (
            float(pd.Series(history).quantile(self.quantile, interpolation="lower"))
            if len(history) == MIN_HISTORY
            else None
        )
        history.append(displacement)
        if not volatility_valid or any(
            values[field] is None
            for field in OUTPUT_FIELDS
            if field not in {
                "btc_15m_realized_volatility_96",
                "eth_15m_signed_liquidity_displacement",
            }
        ):
            return []
        eligible = bool(
            threshold is not None
            and "BTCUSDT" in tradeable
            and values["eth_15m_quote_volume"] >= LIQUIDITY_FLOOR
            and values["btc_15m_quote_volume"] >= LIQUIDITY_FLOOR
            and displacement >= threshold
            and values["eth_15m_log_return"] != 0
        )
        if not eligible:
            return []
        continuation = Side.BUY if values["eth_15m_log_return"] > 0 else Side.SELL
        side = (
            continuation
            if self.direction == "continuation"
            else (Side.SELL if continuation == Side.BUY else Side.BUY)
        )
        distance = max(float(bar.close) * 0.03, 1e-12)
        stop = (
            float(bar.close) - distance
            if side == Side.BUY
            else float(bar.close) + distance
        )
        trace = make_decision_trace(
            reason_code="extreme_eth_liquidity_adjusted_displacement",
            setup_class="cross_asset_liquidity_transmission",
            conditions_bool_map={
                "adaptive_provenance_valid": True,
                "prior_only_volatility_verified": True,
                "minimum_history_complete": True,
                "liquidity_gates": True,
            },
            permission_layer_state={"tradeable": True},
            parameter_combination={
                "eth_displacement_tail_percentile": self.quantile,
                "response_direction": self.direction,
            },
            gate_values={"displacement": displacement},
            gate_thresholds={"displacement": threshold},
        )
        return [
            Signal(
                ts=ts,
                symbol="BTCUSDT",
                side=side,
                signal_type="eth_liquidity_displacement_btc_residual_entry",
                confidence=1.0,
                metadata={
                    "strategy": "eth_liquidity_displacement_btc_residual_60m",
                    "strategy_id": "ALPHA-003-ETH-LIQUIDITY-DISPLACEMENT-BTC-RESIDUAL-60M",
                    "family_variant": self.direction,
                    "family_pattern": "cross_asset_liquidity_transmission",
                    "entry_reason": "extreme_eth_liquidity_adjusted_displacement",
                    "entry_price": float(bar.close),
                    "entry_reference_price": float(bar.close),
                    "intended_entry_price": float(bar.close),
                    "signal_timeframe": "15m",
                    "execution_timeframe": "1m",
                    "risk_accounting": "engine_canonical_R",
                    "r_per_trade": self.r_per_trade,
                    "sizing_mode": "risk_at_stop",
                    "cap_policy": "allow_clip_with_truth",
                    "stop_model": "fixed_3pct_from_decision_close",
                    "stop_price": stop,
                    "entry_stop_price": stop,
                    "stop_distance": distance,
                    "target_exit_ts": (ts + pd.Timedelta(minutes=60)).isoformat(),
                    "target_horizon_minutes": 60,
                    "eth_15m_signed_liquidity_displacement": values[
                        "eth_15m_signed_liquidity_displacement"
                    ],
                    "btc_15m_log_return": values["btc_15m_log_return"],
                    "prior_only_btc_realized_volatility_96": values[
                        "btc_15m_realized_volatility_96"
                    ],
                    "prior_only_volatility_source_end_ts": source_end.isoformat(),
                    "representation_plan_digest": PLAN_DIGEST,
                    "representation_output_fields": list(OUTPUT_FIELDS),
                    "representation_decision_ts": decision_ts.isoformat(),
                    "requested_risk_amount": None,
                    "risk_utilization_pct": None,
                    "under_risked_trade": None,
                    "decision_trace": trace,
                },
            )
        ]
