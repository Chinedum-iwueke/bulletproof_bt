"""Causal native evaluator for the frozen ETH relative-liquidity question."""

from __future__ import annotations

import hashlib
import json
import math
import random
from itertools import product
from typing import Any, Mapping

import pandas as pd

from bt.core.types import Bar, Signal
from bt.strategy import register_strategy
from bt.strategy.base import Strategy

QUESTION = (
    "Among the preregistered Bybit basket ETHUSDT, BTCUSDT, and SOLUSDT, "
    "does persistent point-in-time ETHUSDT relative underperformance accompanied "
    "by declining ETHUSDT quote_volume predict a positive ETHUSDT residual log "
    "return over the next 6h?"
)
INSTRUMENTS = ("ETHUSDT", "BTCUSDT", "SOLUSDT")
OUTPUT_FIELDS = (
    "eth_return_6h",
    "btc_return_6h",
    "sol_return_6h",
    "eth_quote_volume_change_6h",
    "eth_quote_volume_usd",
    "btc_quote_volume_usd",
    "sol_quote_volume_usd",
    "eth_base_volume",
    "btc_base_volume",
    "sol_base_volume",
)
FROZEN_GRID = {
    "relative_return_lookback_hours": (6,),
    "underperformance_threshold": (-0.01, -0.02),
    "quote_volume_change_threshold": (0.0,),
    "residualization_control_set": ("btc_only", "btc_and_sol"),
}
ROUND_TRIP_COST = 0.0009


def _hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _terminal(outcome: str, reason: str, plan_digest: str = "") -> dict[str, Any]:
    ts = "1970-01-01T00:00:00+00:00"
    return {
        "record_kind": "terminal_no_observation",
        "decision_ts": ts,
        "target_exit_ts": ts,
        "decision_trace": {
            "scientific_observation": False,
            "terminal_outcome": True,
            "reason": reason,
        },
        "outcome": outcome,
        "reason": reason,
        "stop_price": 0.0,
        "representation_plan_digest": plan_digest,
        "representation_output_fields": json.dumps(
            list(OUTPUT_FIELDS), separators=(",", ":")
        ),
        "representation_decision_ts": ts,
    }


def _result(
    outcome: str,
    reason: str,
    params: Mapping[str, Any],
    *,
    records: list[dict[str, Any]] | None = None,
    **metrics: Any,
) -> dict[str, Any]:
    observations = records or []
    plan_digest = str(metrics.pop("representation_plan_digest", ""))
    value = {
        "schema_version": "eth-relative-liquidity-reversal-v1.0.0",
        "question": QUESTION,
        "parameters": dict(params),
        "outcome": outcome,
        "reason": reason,
        "passed": outcome == "positive",
        "observation_records": observations,
        "terminal_evidence_records": []
        if observations
        else [_terminal(outcome, reason, plan_digest)],
        **metrics,
    }
    value["record_digest"] = _hash(value)
    return value


def _normalize_grid(grid: Mapping[str, Any]) -> dict[str, tuple[Any, ...]]:
    return {str(k): tuple(v) for k, v in grid.items()}


def compile_decision_rows(panels: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    """Aggregate exact contiguous 1m data into completed left-labelled 1h decisions."""
    hourly: dict[str, pd.DataFrame] = {}
    for symbol in INSTRUMENTS:
        if symbol not in panels:
            return pd.DataFrame()
        frame = panels[symbol].copy()
        required = {"ts", "close", "quote_volume", "volume"}
        if not required.issubset(frame):
            return pd.DataFrame()
        frame["ts"] = pd.to_datetime(frame["ts"], utc=True, errors="coerce")
        if frame["ts"].isna().any() or frame["ts"].duplicated().any():
            return pd.DataFrame()
        frame = frame.sort_values("ts")
        frame["bucket"] = frame.ts.dt.floor("1h")
        rows = []
        for bucket, part in frame.groupby("bucket", sort=True):
            expected = pd.date_range(bucket, periods=60, freq="1min", tz="UTC")
            if len(part) != 60 or list(part.ts) != list(expected):
                continue
            if part[["close", "quote_volume", "volume"]].isna().any().any():
                continue
            rows.append(
                {
                    "decision_ts": bucket + pd.Timedelta(hours=1),
                    f"{symbol}_close": float(part.iloc[-1].close),
                    f"{symbol}_quote_volume": float(part.quote_volume.sum()),
                    f"{symbol}_volume": float(part.volume.sum()),
                }
            )
        hourly[symbol] = (
            pd.DataFrame(rows).set_index("decision_ts") if rows else pd.DataFrame()
        )
    if any(value.empty for value in hourly.values()):
        return pd.DataFrame()
    result = pd.concat(hourly.values(), axis=1, join="outer").sort_index()
    full_index = pd.date_range(
        result.index.min(), result.index.max(), freq="1h", tz="UTC"
    )
    result = result.reindex(full_index)
    result.index.name = "decision_ts"
    result["complete"] = result.notna().all(axis=1)
    past_complete = result["complete"].astype(int).rolling(7, min_periods=7).sum().eq(7)
    future_complete = (
        result["complete"]
        .astype(int)
        .iloc[::-1]
        .rolling(7, min_periods=7)
        .sum()
        .eq(7)
        .iloc[::-1]
    )
    for symbol in INSTRUMENTS:
        historical = result[f"{symbol}_close"].apply(math.log).diff(6)
        result[f"{symbol}_return_6h"] = historical.where(past_complete)
    result["eth_quote_volume_change_6h"] = (
        result["ETHUSDT_quote_volume"]
        .pct_change(6, fill_method=None)
        .where(past_complete)
    )
    result["target_exit_ts"] = result.index + pd.Timedelta(hours=6)
    for symbol in INSTRUMENTS:
        target = result[f"{symbol}_close"].shift(periods=-6).apply(math.log) - result[
            f"{symbol}_close"
        ].apply(math.log)
        result[f"{symbol}_target_6h"] = target.where(future_complete)
    result["target_complete"] = future_complete & result[
        [f"{s}_target_6h" for s in INSTRUMENTS]
    ].notna().all(axis=1)
    return result.reset_index()


def _fit(rows: pd.DataFrame, controls: tuple[str, ...]) -> tuple[float, list[float]]:
    # Stable normal equations with an intercept; no validation/test targets enter here.
    import numpy as np

    y = rows["ETHUSDT_target_6h"].to_numpy(float)
    x = np.column_stack(
        [np.ones(len(rows)), *[rows[c].to_numpy(float) for c in controls]]
    )
    beta = np.linalg.lstsq(x, y, rcond=None)[0]
    return float(beta[0]), [float(v) for v in beta[1:]]


def _ci(values: list[float]) -> tuple[float, float]:
    """Deterministic circular block bootstrap, valid for overlapping six-hour targets."""
    if not values:
        return (0.0, 0.0)
    rng, means = random.Random(20260927), []
    width = min(6, len(values))
    for _ in range(2000):
        sample = []
        while len(sample) < len(values):
            start = rng.randrange(len(values))
            sample.extend(values[(start + j) % len(values)] for j in range(width))
        means.append(sum(sample[: len(values)]) / len(values))
    means.sort()
    return means[int(1999 * 0.025)], means[int(1999 * 0.975)]


def _score(
    rows: pd.DataFrame,
    train: pd.DataFrame,
    params: Mapping[str, Any],
    partition: str,
    plan_digest: str = "",
) -> dict[str, Any]:
    chosen = str(params["residualization_control_set"])
    controls = (
        ("BTCUSDT_target_6h",)
        if chosen == "btc_only"
        else ("BTCUSDT_target_6h", "SOLUSDT_target_6h")
    )
    rival_controls = (
        ("BTCUSDT_target_6h", "SOLUSDT_target_6h")
        if chosen == "btc_only"
        else ("BTCUSDT_target_6h",)
    )
    intercept, beta = _fit(train, controls)
    rival_intercept, rival_beta = _fit(train, rival_controls)
    records, state_values, nondeclining, rival_values = [], [], [], []
    for _, row in rows.iterrows():
        if not bool(row.complete and row.target_complete):
            continue
        if any(float(row[f"{s}_quote_volume"]) < 1_000_000 for s in INSTRUMENTS):
            continue
        historical = float(row.ETHUSDT_return_6h) - float(row.BTCUSDT_return_6h)
        if chosen == "btc_and_sol":
            historical -= float(row.SOLUSDT_return_6h)
        state = historical <= float(params["underperformance_threshold"])
        residual = (
            float(row.ETHUSDT_target_6h)
            - intercept
            - sum(
                coefficient * float(row[field])
                for coefficient, field in zip(beta, controls)
            )
        )
        rival = (
            float(row.ETHUSDT_target_6h)
            - rival_intercept
            - sum(
                coefficient * float(row[field])
                for coefficient, field in zip(rival_beta, rival_controls)
            )
        )
        declining = float(row.eth_quote_volume_change_6h) < float(
            params["quote_volume_change_threshold"]
        )
        if state and declining:
            net = residual - ROUND_TRIP_COST
            state_values.append(net)
            rival_values.append(rival - ROUND_TRIP_COST)
            records.append(
                {
                    "record_kind": "scientific_observation",
                    "decision_ts": row.decision_ts.isoformat(),
                    "target_exit_ts": row.target_exit_ts.isoformat(),
                    "decision_trace": {
                        "state": True,
                        "partition": partition,
                        "control_set": chosen,
                    },
                    "outcome": "observed",
                    "reason": "qualified_state",
                    "stop_price": 0.0,
                    "representation_plan_digest": plan_digest,
                    "representation_output_fields": list(OUTPUT_FIELDS),
                    "representation_decision_ts": row.decision_ts.isoformat(),
                    "net_residual_return": net,
                }
            )
        elif state:
            nondeclining.append(residual - ROUND_TRIP_COST)
    ci = _ci(state_values)
    mean = sum(state_values) / len(state_values) if state_values else 0.0
    rival_mean = sum(rival_values) / len(rival_values) if rival_values else 0.0
    non_mean = sum(nondeclining) / len(nondeclining) if nondeclining else float("-inf")
    doubled = mean - ROUND_TRIP_COST
    gates = {
        "minimum_support": len(state_values) >= 30,
        "positive_mean": mean > 0,
        "confidence_excludes_zero": ci[0] > 0,
        "both_control_sets_positive": rival_mean > 0,
        "doubled_cost_positive": doubled > 0,
        "declining_exceeds_nondeclining": mean > non_mean,
    }
    outcome = "positive" if all(gates.values()) else "negative"
    return _result(
        outcome,
        "all_gates_passed" if outcome == "positive" else "falsification_gate_failed",
        params,
        records=records,
        support=len(state_values),
        mean_net_residual_return=mean,
        rival_control_mean_net_residual_return=rival_mean,
        nondeclining_mean_net_residual_return=non_mean,
        confidence_interval_95=ci,
        doubled_cost_mean_net_residual_return=doubled,
        gates=gates,
        partition=partition,
        held_out_evaluated=partition == "test",
        representation_plan_digest=plan_digest,
    )


def relative_liquidity_reversal_grid_evaluation(
    panels: Mapping[str, pd.DataFrame],
    *,
    parameter_grid: Mapping[str, Any],
    evaluation_start: str,
    evaluation_end: str,
    representation_plan_digest: str = "",
) -> dict[str, Any]:
    """Select on validation, then open the test once for the deterministic winner."""
    if _normalize_grid(parameter_grid) != FROZEN_GRID:
        return {
            "outcome": "invalid",
            "reason": "parameter_grid_differs_from_frozen_contract",
            "selection_candidates": [],
            "selected_parameters": None,
            "test_open_count": 0,
            "held_out_evaluation": _result(
                "invalid", "parameter_grid_differs_from_frozen_contract", {}
            ),
        }
    rows = compile_decision_rows(panels)
    if rows.empty:
        failed = _result("invalid", "market_schema_or_complete_hour_failure", {})
        return {
            "outcome": "invalid",
            "reason": failed["reason"],
            "selection_candidates": [],
            "selected_parameters": None,
            "test_open_count": 0,
            "held_out_evaluation": failed,
        }
    rows = rows[
        (rows.decision_ts >= pd.Timestamp(evaluation_start))
        & (rows.target_exit_ts <= pd.Timestamp(evaluation_end))
    ].reset_index(drop=True)
    if len(rows) < 60:
        failed = _result("invalid", "insufficient_chronological_rows", {})
        return {
            "outcome": "invalid",
            "reason": failed["reason"],
            "selection_candidates": [],
            "selected_parameters": None,
            "test_open_count": 0,
            "held_out_evaluation": failed,
        }
    train_end, val_end = int(len(rows) * 0.6), int(len(rows) * 0.8)
    purge = pd.Timedelta(hours=6)
    validation_boundary = pd.Timestamp(rows.iloc[train_end].decision_ts)
    test_boundary = pd.Timestamp(rows.iloc[val_end].decision_ts)
    train = rows.loc[
        rows.target_complete & (rows.target_exit_ts <= validation_boundary - purge)
    ]
    validation_rows = rows.loc[
        (rows.decision_ts >= validation_boundary + purge)
        & (rows.target_exit_ts <= test_boundary - purge)
    ]
    test_rows = rows.loc[rows.decision_ts >= test_boundary + purge]
    if len(train) < 12 or validation_rows.empty or test_rows.empty:
        failed = _result("invalid", "insufficient_purged_split", {})
        return {
            "outcome": "invalid",
            "reason": failed["reason"],
            "selection_candidates": [],
            "selected_parameters": None,
            "test_open_count": 0,
            "held_out_evaluation": failed,
        }
    names = tuple(FROZEN_GRID)
    candidates = []
    for values in product(*(FROZEN_GRID[name] for name in names)):
        params = dict(zip(names, values))
        try:
            candidates.append(
                _score(
                    validation_rows,
                    train,
                    params,
                    "validation",
                    representation_plan_digest,
                )
            )
        except (
            Exception
        ) as exc:  # retain computation failures; never relabel as falsification
            candidates.append(
                _result(
                    "failed",
                    f"validation_evaluation_failed:{type(exc).__name__}",
                    params,
                )
            )
    eligible = [(i, item) for i, item in enumerate(candidates) if item["passed"]]
    if not eligible:
        outcomes = {item["outcome"] for item in candidates}
        outcome = (
            "failed"
            if "failed" in outcomes
            else "invalid"
            if outcomes == {"invalid"}
            else "negative"
        )
        terminal = _result(
            outcome,
            "no_validation_variant_passed",
            {},
            representation_plan_digest=representation_plan_digest,
        )
        return {
            "outcome": outcome,
            "reason": terminal["reason"],
            "selection_candidates": candidates,
            "selected_parameters": None,
            "test_open_count": 0,
            "held_out_evaluation": terminal,
        }
    index, winner = max(
        eligible, key=lambda pair: (pair[1]["mean_net_residual_return"], -pair[0])
    )
    selected = winner["parameters"]
    try:
        heldout = _score(test_rows, train, selected, "test", representation_plan_digest)
    except Exception as exc:
        heldout = _result(
            "failed", f"heldout_evaluation_failed:{type(exc).__name__}", selected
        )
    result = {
        "outcome": heldout["outcome"],
        "reason": heldout["reason"],
        "selection_candidates": candidates,
        "selected_parameters": selected,
        "selected_index": index,
        "test_open_count": 1,
        "held_out_evaluation": heldout,
    }
    result["record_digest"] = _hash(result)
    return result


def relative_liquidity_reversal_evaluation(
    panels: Mapping[str, pd.DataFrame], **kwargs: Any
) -> dict[str, Any]:
    """Compatibility name for the governed frozen-grid evaluation."""
    return relative_liquidity_reversal_grid_evaluation(
        panels, parameter_grid=FROZEN_GRID, **kwargs
    )


@register_strategy("eth_relative_liquidity_reversal_6h")
class EthRelativeLiquidityReversal6hStrategy(Strategy):
    """Observation-only native consumer; it never emits an order-bearing signal.

    There is therefore no exit signal; any future executable adaptation must use
    explicit ``close_only`` engine exits rather than strategy-owned accounting.
    """

    def __init__(
        self, *, adaptive_representation_plan_digest: str = "", **_: Any
    ) -> None:
        self.expected_plan_digest = adaptive_representation_plan_digest

    def on_bars(
        self,
        ts: pd.Timestamp,
        bars_by_symbol: Mapping[str, Bar],
        tradeable: set[str],
        ctx: Mapping[str, Any],
    ) -> list[Signal]:
        bars = [bars_by_symbol.get(symbol) for symbol in INSTRUMENTS]
        if any(bar is None for bar in bars):
            return []
        extras = [bar.extra for bar in bars if bar is not None]
        representation_times = [
            extra.get("representation_decision_ts") for extra in extras
        ]
        if all(value is None for value in representation_times):
            return []
        digest_value = extras[0].get("representation_plan_digest")
        serialized_fields = json.dumps(list(OUTPUT_FIELDS), separators=(",", ":"))
        fields = extras[0].get("representation_output_fields")
        provenance_valid = (
            len(self.expected_plan_digest) == 64
            and digest_value == self.expected_plan_digest
            and fields == serialized_fields
            and all(
                extra.get("representation_plan_digest") == self.expected_plan_digest
                and extra.get("representation_output_fields") == serialized_fields
                and pd.Timestamp(extra.get("representation_decision_ts"))
                == pd.Timestamp(ts)
                for extra in extras
            )
        )
        outputs_complete = all(
            all(name in extra and extra[name] is not None for name in OUTPUT_FIELDS)
            for extra in extras
        )
        outcome = (
            "consumed"
            if provenance_valid and outputs_complete
            else "warmup"
            if provenance_valid
            else "invalid"
        )
        metadata = {
            "native_payload_outcome": outcome,
            "native_payload_reason": (
                "exact_ordered_fields_consumed"
                if outcome == "consumed"
                else "causal_history_incomplete"
                if outcome == "warmup"
                else "representation_mismatch"
            ),
            "representation_plan_digest": digest_value,
            "representation_output_fields": list(OUTPUT_FIELDS),
            "representation_decision_ts": str(
                extras[0].get("representation_decision_ts")
            ),
            "decision_trace": {
                "causal_decision_ts": str(ts),
                "all_basket_payloads_checked": True,
            },
        }
        return [
            Signal(
                ts=ts,
                symbol="ETHUSDT",
                side=None,
                signal_type="eth_relative_representation_validation",
                confidence=1.0,
                metadata=metadata,
            )
        ]
