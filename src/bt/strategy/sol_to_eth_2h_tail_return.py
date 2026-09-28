"""Point-in-time native evaluator for the frozen SOL-to-ETH diffusion question."""

from __future__ import annotations

import hashlib
import json
import math
import random
from itertools import product
from typing import Any, Mapping

import numpy as np
import pandas as pd

from bt.core.types import Bar, Signal
from bt.strategy import register_strategy
from bt.strategy.base import Strategy

QUESTION = (
    "Does a point-in-time completed SOLUSDT 2h tail return predict same-direction "
    "ETHUSDT close-to-close log return over the next contiguous 4h after controlling "
    "for ETHUSDT's contemporaneous 2h return, prior-only 24h volatility, and both "
    "instruments' completed-bar USD liquidity?"
)
INSTRUMENTS = ("ETHUSDT", "SOLUSDT")
OUTPUT_FIELDS = (
    "solusdt_log_return_2h",
    "ethusdt_log_return_2h",
    "ethusdt_realized_volatility_24h",
    "solusdt_quote_volume_2h",
    "ethusdt_quote_volume_2h",
)
FROZEN_GRID = {
    "sol_return_tail_percentile": (0.90, 0.95),
    "response_direction": ("same",),
    "volatility_lookback": (12,),
}
MINIMUM_HISTORY = 4000
MINIMUM_PARTITION_SUPPORT = 30
LIQUIDITY_FLOOR = 1_000_000.0
ROUND_TRIP_COST = 0.0009
EXPECTED_EVALUATION_START = "2023-01-01T00:00:00Z"
EXPECTED_EVALUATION_END = "2024-01-01T00:00:00Z"
EXPECTED_REPRESENTATION_PLAN_DIGEST = (
    "da66f342076d9d15600355066237cb17c8470ae133e7ebfce1319c597621dbdb"
)
EXPECTED_DATASET_IDENTITIES = {
    "ETHUSDT": {
        "source_venue": "bybit",
        "source_dataset_build_id": "e1abea55-f597-415b-af85-95e6a12b5a3d",
        "source_dataset_digest": (
            "17c0dc35d32f3e838f43ba116eb8917f01f08594f14ea2a6c104c3905e4a4473"
        ),
        "source_catalog_digest": (
            "debec183dbaead71f96286920a805998c61c5141fe93499efcfb41e72249e031"
        ),
        "source_manifest_digest": (
            "1add98708c8427d7d4daa5f94d05c54920df8ea2bbbd1582f9aa532cd30d639c"
        ),
        "source_producer_receipt_digest": (
            "f40db3e88a6ab082091269f99b55d2885ca52c938d25a919783f9b5675aefad6"
        ),
        "source_lake_governance_digest": (
            "3c831d64cad97c076a82700f59e8c2021c8316a711b6c29dfed9a47e0fe003c1"
        ),
        "source_partition_digest": (
            "17c0dc35d32f3e838f43ba116eb8917f01f08594f14ea2a6c104c3905e4a4473"
        ),
    },
    "SOLUSDT": {
        "source_venue": "bybit",
        "source_dataset_build_id": "233b1bf0-cde2-4eff-a58e-0ff57b7ade1f",
        "source_dataset_digest": (
            "3921896b9b2be24b0834766a2945e54e89d1223ce5480c83a2a6757b9eaf9a41"
        ),
        "source_catalog_digest": (
            "b69af601f8e2c8c76f2692f7186f99069c3ea821975483f4dd4eb0f60ac3c290"
        ),
        "source_manifest_digest": (
            "455beed3cf4e5c80e8f63bb33cf2e4b7beff9b0813063080aae9dfb6e11b4788"
        ),
        "source_producer_receipt_digest": (
            "29c440c17effbb3261262299a23f65a99afabf914b2048c92c64a557b9a1245b"
        ),
        "source_lake_governance_digest": (
            "d62fb5a4ed3f9acbdd76cc8896d68ad1ce36d0ca0e72a673042c40b754513b96"
        ),
        "source_partition_digest": (
            "3921896b9b2be24b0834766a2945e54e89d1223ce5480c83a2a6757b9eaf9a41"
        ),
    },
}


def _hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _terminal(outcome: str, reason: str, plan_digest: str) -> dict[str, Any]:
    ts = "1970-01-01T00:00:00+00:00"
    return {
        "record_kind": "terminal_no_observation",
        "decision_ts": ts,
        "target_exit_ts": ts,
        "decision_trace": {"scientific_observation": False, "reason": reason},
        "outcome": outcome,
        "reason": reason,
        "stop_price": 0.0,
        "representation_plan_digest": plan_digest,
        "representation_output_fields": json.dumps(list(OUTPUT_FIELDS), separators=(",", ":")),
        "representation_decision_ts": ts,
    }


def _result(outcome: str, reason: str, params: Mapping[str, Any], *,
            records: list[dict[str, Any]] | None = None,
            plan_digest: str, **metrics: Any) -> dict[str, Any]:
    observations = records or []
    value = {
        "schema_version": "sol-to-eth-2h-tail-return-v1.0.0",
        "question": QUESTION,
        "parameters": dict(params),
        "outcome": outcome,
        "reason": reason,
        "passed": outcome == "positive",
        "observation_records": observations,
        "terminal_evidence_records": [] if observations else [_terminal(outcome, reason, plan_digest)],
        "representation_plan_digest": plan_digest,
        **metrics,
    }
    value["record_digest"] = _hash(value)
    return value


def _ci(values: list[float]) -> tuple[float, float]:
    """Circular block bootstrap for two overlapping 2h observations per 4h target."""
    if not values:
        return (0.0, 0.0)
    rng, means = random.Random(20260928), []
    width = min(2, len(values))
    for _ in range(2000):
        sample: list[float] = []
        while len(sample) < len(values):
            start = rng.randrange(len(values))
            sample.extend(values[(start + offset) % len(values)] for offset in range(width))
        means.append(sum(sample[:len(values)]) / len(values))
    means.sort()
    return means[int(1999 * 0.025)], means[int(1999 * 0.975)]


def _selection_bias_audit(
    candidates: list[dict[str, Any]],
    *,
    selected_index: int | None,
    test_open_count: int,
) -> dict[str, Any]:
    """Record the preregistered validation-only selection without reopening test data."""
    candidate_audit = [
        {
            "index": index,
            "parameters": item.get("parameters", {}),
            "outcome": item.get("outcome"),
            "eligible_for_selection": bool(item.get("selection_eligible", False)),
            "validation_mean_net_signed_residual_return": item.get(
                "mean_net_signed_residual_return"
            ),
            "record_digest": item.get("record_digest"),
        }
        for index, item in enumerate(candidates)
    ]
    return {
        "schema_version": "selection-bias-audit-v1.0.0",
        "preregistered_variant_count": math.prod(len(values) for values in FROZEN_GRID.values()),
        "evaluated_variant_count": len(candidates),
        "selection_partition": "validation",
        "selection_rule": (
            "highest_validation_mean_net_signed_residual_return_among_"
            "support_eligible_preregistered_variants_then_lowest_grid_index"
        ),
        "held_out_used_for_selection": False,
        "selected_index": selected_index,
        "test_open_count": test_open_count,
        "candidates": candidate_audit,
        "audit_digest": _hash(candidate_audit),
    }


def _evaluation_result(
    *,
    outcome: str,
    reason: str,
    candidates: list[dict[str, Any]],
    selected_parameters: Mapping[str, Any] | None,
    selected_index: int | None,
    test_open_count: int,
    held_out_evaluation: dict[str, Any],
) -> dict[str, Any]:
    result = {
        "outcome": outcome,
        "reason": reason,
        "selection_candidates": candidates,
        "selected_parameters": (
            dict(selected_parameters) if selected_parameters is not None else None
        ),
        "selected_index": selected_index,
        "test_open_count": test_open_count,
        "selection_bias_audit": _selection_bias_audit(
            candidates,
            selected_index=selected_index,
            test_open_count=test_open_count,
        ),
        "held_out_evaluation": held_out_evaluation,
    }
    result["record_digest"] = _hash(result)
    return result


def compile_decision_rows(frame: pd.DataFrame, *, plan_digest: str) -> pd.DataFrame:
    """Bind exact materialized predictors to contiguous future ETH closes."""
    if plan_digest != EXPECTED_REPRESENTATION_PLAN_DIGEST:
        return pd.DataFrame()
    source_identity_fields = {
        field
        for identity in EXPECTED_DATASET_IDENTITIES.values()
        for field in identity
    }
    required = {"ts", "symbol", "open", "high", "low", "close", "volume",
                "quote_volume", *OUTPUT_FIELDS, *source_identity_fields,
                "prior_only_volatility_source_end_ts", "representation_plan_digest",
                "representation_output_fields", "representation_decision_ts"}
    if not required.issubset(frame):
        return pd.DataFrame()
    data = frame.copy()
    if not isinstance(data["ts"].dtype, pd.DatetimeTZDtype):
        return pd.DataFrame()
    if str(data["ts"].dt.tz) != "UTC":
        return pd.DataFrame()
    data["ts"] = pd.to_datetime(data["ts"], utc=True, errors="coerce")
    if data.ts.isna().any() or data.duplicated(["symbol", "ts"]).any():
        return pd.DataFrame()
    frozen_start = pd.Timestamp(EXPECTED_EVALUATION_START)
    frozen_end = pd.Timestamp(EXPECTED_EVALUATION_END)
    if not data.ts.ge(frozen_start).all() or not data.ts.lt(frozen_end).all():
        return pd.DataFrame()
    if set(data.symbol.astype(str)) != set(EXPECTED_DATASET_IDENTITIES):
        return pd.DataFrame()
    for symbol, expected_identity in EXPECTED_DATASET_IDENTITIES.items():
        member = data.loc[data.symbol.eq(symbol)]
        if member.empty or any(
            member[field].isna().any()
            or not member[field].astype(str).eq(expected).all()
            for field, expected in expected_identity.items()
        ):
            return pd.DataFrame()
        if (
            member.ts.min() != frozen_start
            or member.ts.max() != frozen_end - pd.Timedelta(minutes=1)
        ):
            return pd.DataFrame()
    encoded = json.dumps(list(OUTPUT_FIELDS), separators=(",", ":"))
    decisions = data.loc[data["representation_plan_digest"].notna()].copy()
    if decisions.empty:
        return pd.DataFrame()
    decision_times = decisions["ts"].drop_duplicates().sort_values()
    aligned = (
        decision_times.dt.minute.eq(0)
        & decision_times.dt.second.eq(0)
        & decision_times.dt.microsecond.eq(0)
        & decision_times.dt.hour.mod(2).eq(0)
    )
    if not aligned.all() or not decision_times.diff().dropna().eq(
        pd.Timedelta(hours=2)
    ).all():
        return pd.DataFrame()
    valid = (
        decisions["representation_plan_digest"].eq(plan_digest)
        & decisions["representation_output_fields"].eq(encoded)
        & pd.to_datetime(decisions["representation_decision_ts"], utc=True, errors="coerce").eq(decisions.ts)
    )
    if not valid.all():
        return pd.DataFrame()
    grouped = []
    for ts, part in decisions.groupby("ts", sort=True):
        if set(part.symbol) != set(INSTRUMENTS) or len(part) != 2:
            continue
        first = part.iloc[0]
        if any(part[field].nunique(dropna=False) != 1 for field in OUTPUT_FIELDS):
            return pd.DataFrame()
        provenance = pd.Timestamp(first.prior_only_volatility_source_end_ts)
        if provenance != ts - pd.Timedelta(hours=2):
            return pd.DataFrame()
        grouped.append({"decision_ts": ts, **{field: first[field] for field in OUTPUT_FIELDS}})
    rows = pd.DataFrame(grouped)
    if rows.empty:
        return rows
    expected = _recompute_representation(data)
    expected = expected.loc[expected.decision_ts.isin(set(rows.decision_ts))]
    if expected.empty or set(rows.decision_ts) != set(expected.decision_ts):
        return pd.DataFrame()
    expected = expected.set_index("decision_ts")
    for row in rows.itertuples(index=False):
        reference = expected.loc[row.decision_ts]
        for field in OUTPUT_FIELDS:
            observed, wanted = getattr(row, field), reference[field]
            if pd.isna(observed) and pd.isna(wanted):
                continue
            if (
                pd.isna(observed)
                or pd.isna(wanted)
                or not math.isclose(
                    float(observed), float(wanted), rel_tol=1e-12, abs_tol=1e-15
                )
            ):
                return pd.DataFrame()
    eth = data.loc[data.symbol.eq("ETHUSDT"), ["ts", "close"]].sort_values("ts")
    eth_start, eth_end = eth.ts.min(), eth.ts.max()
    eth = eth.set_index("ts").reindex(pd.date_range(eth_start, eth_end, freq="1min", tz="UTC"))
    close_at = eth.close.shift(1)
    future_complete_at = close_at.notna().rolling(240, min_periods=240).sum().eq(240)
    rows["target_exit_ts"] = rows.decision_ts + pd.Timedelta(hours=4)
    rows["eth_target_4h"] = [
        math.log(float(close_at.get(exit_ts))) - math.log(float(close_at.get(ts)))
        if pd.notna(close_at.get(exit_ts)) and pd.notna(close_at.get(ts))
        and bool(future_complete_at.get(exit_ts, False)) else np.nan
        for ts, exit_ts in zip(rows.decision_ts, rows.target_exit_ts)
    ]
    sol_by_ts = rows.set_index("decision_ts").solusdt_log_return_2h
    rows["sol_lag_return"] = [
        sol_by_ts.get(ts - pd.Timedelta(hours=2), np.nan)
        for ts in rows.decision_ts
    ]
    return rows


def _recompute_representation(data: pd.DataFrame) -> pd.DataFrame:
    """Replay the frozen representation directly from complete raw minute bars."""
    members: dict[str, pd.DataFrame] = {}
    for symbol in INSTRUMENTS:
        source = data.loc[
            data.symbol.eq(symbol), ["ts", "close", "quote_volume"]
        ].copy()
        if source.empty or source[["close", "quote_volume"]].isna().any().any():
            return pd.DataFrame()
        source = source.sort_values("ts", kind="stable")
        if not source.ts.dt.floor("1min").eq(source.ts).all():
            return pd.DataFrame()
        source["bucket_start"] = source.ts.dt.floor("120min")
        grouped = source.groupby("bucket_start", sort=True)
        counts = grouped.ts.count()
        spans = grouped.ts.agg(["min", "max"])
        complete = (
            counts.eq(120)
            & spans["min"].eq(spans.index)
            & spans["max"].eq(spans.index + pd.Timedelta(minutes=119))
        )
        source = source.loc[source.bucket_start.isin(complete[complete].index)]
        bars = source.groupby("bucket_start", sort=True).agg(
            close=("close", "last"), quote_volume=("quote_volume", "sum")
        )
        bars["decision_ts"] = bars.index + pd.Timedelta(hours=2)
        bars[f"{symbol.lower()}_log_return_2h"] = np.log(
            bars.close.where(bars.close > 0)
        ).diff()
        bars = bars.set_index("decision_ts")
        members[symbol] = bars
    joined = members["ETHUSDT"].join(
        members["SOLUSDT"], how="inner", lsuffix="_eth", rsuffix="_sol"
    )
    if joined.empty:
        return pd.DataFrame()
    eth_returns = joined["ethusdt_log_return_2h"]
    result = pd.DataFrame(index=joined.index)
    result["solusdt_log_return_2h"] = joined["solusdt_log_return_2h"]
    result["ethusdt_log_return_2h"] = eth_returns
    result["ethusdt_realized_volatility_24h"] = (
        eth_returns.rolling(12, min_periods=12).std(ddof=0).mul(math.sqrt(12)).shift(1)
    )
    result["solusdt_quote_volume_2h"] = joined["quote_volume_sol"]
    result["ethusdt_quote_volume_2h"] = joined["quote_volume_eth"]
    return result.reset_index()


def _fit_controls(train: pd.DataFrame) -> tuple[float, np.ndarray]:
    fields = ["ethusdt_log_return_2h", "ethusdt_realized_volatility_24h",
              "solusdt_quote_volume_2h", "ethusdt_quote_volume_2h"]
    x = np.column_stack([np.ones(len(train)), train[fields].to_numpy(float)])
    beta = np.linalg.lstsq(x, train.eth_target_4h.to_numpy(float), rcond=None)[0]
    return float(beta[0]), beta[1:]


def _score(rows: pd.DataFrame, train: pd.DataFrame, params: Mapping[str, Any],
           partition: str, plan_digest: str) -> dict[str, Any]:
    qualified_train = train.loc[
        train.solusdt_quote_volume_2h.ge(LIQUIDITY_FLOOR)
        & train.ethusdt_quote_volume_2h.ge(LIQUIDITY_FLOOR)
    ].dropna(subset=[*OUTPUT_FIELDS, "eth_target_4h"])
    if len(qualified_train) < MINIMUM_HISTORY:
        return _result("invalid", "fewer_than_4000_aligned_liquid_history_observations",
                       params, plan_digest=plan_digest)
    threshold = float(qualified_train.solusdt_log_return_2h.abs().quantile(
        float(params["sol_return_tail_percentile"]), interpolation="lower"))
    intercept, beta = _fit_controls(qualified_train)
    fields = ["ethusdt_log_return_2h", "ethusdt_realized_volatility_24h",
              "solusdt_quote_volume_2h", "ethusdt_quote_volume_2h"]
    records, values, lag_values = [], [], []
    for row in rows.itertuples(index=False):
        if any(pd.isna(getattr(row, name)) for name in [*OUTPUT_FIELDS, "eth_target_4h", "sol_lag_return"]):
            continue
        if row.solusdt_quote_volume_2h < LIQUIDITY_FLOOR or row.ethusdt_quote_volume_2h < LIQUIDITY_FLOOR:
            continue
        if abs(float(row.solusdt_log_return_2h)) < threshold:
            continue
        residual = float(row.eth_target_4h) - intercept - float(np.dot(beta, [getattr(row, f) for f in fields]))
        net = math.copysign(1.0, float(row.solusdt_log_return_2h)) * residual - ROUND_TRIP_COST
        lag_net = math.copysign(1.0, float(row.sol_lag_return)) * residual - ROUND_TRIP_COST
        values.append(net)
        lag_values.append(lag_net)
        records.append({
            "record_kind": "scientific_observation", "decision_ts": row.decision_ts.isoformat(),
            "target_exit_ts": row.target_exit_ts.isoformat(), "outcome": "observed",
            "reason": "qualified_current_sol_tail", "stop_price": 0.0,
            "decision_trace": {"scientific_observation": True, "partition": partition,
                               "tail_threshold": threshold, "same_timestamp_lag_rival": True},
            "representation_plan_digest": plan_digest,
            "representation_output_fields": json.dumps(list(OUTPUT_FIELDS), separators=(",", ":")),
            "representation_decision_ts": row.decision_ts.isoformat(),
            "net_signed_residual_return": net,
        })
    mean = float(np.mean(values)) if values else 0.0
    lag_mean = float(np.mean(lag_values)) if lag_values else 0.0
    ci = _ci(values)
    doubled = mean - ROUND_TRIP_COST
    gates = {"minimum_support": len(values) >= MINIMUM_PARTITION_SUPPORT,
             "minimum_support_required": MINIMUM_PARTITION_SUPPORT,
             "positive_mean": mean > 0,
             "dependence_aware_ci_positive": ci[0] > 0,
             "current_exceeds_same_timestamp_lag": mean > lag_mean,
             "doubled_cost_positive": doubled > 0}
    outcome = "positive" if all(gates.values()) else "negative"
    selection_eligible = partition == "validation" and gates["minimum_support"]
    return _result(outcome, "all_gates_passed" if outcome == "positive" else "falsification_gate_failed",
                   params, records=records, plan_digest=plan_digest, support=len(values),
                   mean_net_signed_residual_return=mean, lag_rival_mean_net_signed_residual_return=lag_mean,
                   confidence_interval_95=ci, doubled_cost_mean_net_signed_residual_return=doubled,
                   gates=gates, partition=partition, held_out_evaluated=partition == "test",
                   selection_eligible=selection_eligible)


def sol_to_eth_tail_grid_evaluation(frame: pd.DataFrame, *, parameter_grid: Mapping[str, Any],
                                    evaluation_start: str, evaluation_end: str,
                                    representation_plan_digest: str) -> dict[str, Any]:
    if (
        pd.Timestamp(evaluation_start) != pd.Timestamp(EXPECTED_EVALUATION_START)
        or pd.Timestamp(evaluation_end) != pd.Timestamp(EXPECTED_EVALUATION_END)
    ):
        terminal = _result(
            "invalid",
            "evaluation_window_differs_from_frozen_contract",
            {},
            plan_digest=representation_plan_digest,
        )
        return _evaluation_result(
            outcome="invalid", reason=terminal["reason"], candidates=[],
            selected_parameters=None, selected_index=None, test_open_count=0,
            held_out_evaluation=terminal,
        )
    if {str(k): tuple(v) for k, v in parameter_grid.items()} != FROZEN_GRID:
        terminal = _result("invalid", "parameter_grid_differs_from_frozen_contract", {},
                           plan_digest=representation_plan_digest)
        return _evaluation_result(
            outcome="invalid", reason=terminal["reason"], candidates=[],
            selected_parameters=None, selected_index=None, test_open_count=0,
            held_out_evaluation=terminal,
        )
    rows = compile_decision_rows(frame, plan_digest=representation_plan_digest)
    rows = rows.loc[(rows.decision_ts >= pd.Timestamp(evaluation_start))
                    & (rows.target_exit_ts <= pd.Timestamp(evaluation_end))].reset_index(drop=True) if not rows.empty else rows
    if len(rows) < MINIMUM_HISTORY + 30:
        terminal = _result("invalid", "insufficient_chronological_rows", {}, plan_digest=representation_plan_digest)
        return _evaluation_result(
            outcome="invalid", reason=terminal["reason"], candidates=[],
            selected_parameters=None, selected_index=None, test_open_count=0,
            held_out_evaluation=terminal,
        )
    history_eligible = (
        rows.solusdt_quote_volume_2h.ge(LIQUIDITY_FLOOR)
        & rows.ethusdt_quote_volume_2h.ge(LIQUIDITY_FLOOR)
        & rows[[*OUTPUT_FIELDS, "eth_target_4h"]].notna().all(axis=1)
    )
    eligible_rows = rows.loc[history_eligible].reset_index(drop=True)
    if len(eligible_rows) < MINIMUM_HISTORY:
        terminal = _result("invalid", "fewer_than_4000_aligned_liquid_history_observations", {},
                           plan_digest=representation_plan_digest)
        return _evaluation_result(
            outcome="invalid", reason=terminal["reason"], candidates=[],
            selected_parameters=None, selected_index=None, test_open_count=0,
            held_out_evaluation=terminal,
        )
    purge = pd.Timedelta(hours=4)
    train = eligible_rows.iloc[:MINIMUM_HISTORY].copy()
    history_end = train.iloc[-1].target_exit_ts
    validation_start = history_end + purge
    remaining = eligible_rows.loc[
        eligible_rows.decision_ts >= validation_start
    ].reset_index(drop=True)
    # After a 4h target closes, three intervening 2h decisions place the next
    # partition at target_exit + the frozen 4h purge/embargo boundary.
    partition_gap = 3
    if len(remaining) < 2 * MINIMUM_PARTITION_SUPPORT + partition_gap:
        terminal = _result(
            "invalid",
            "insufficient_post_history_chronological_rows",
            {},
            plan_digest=representation_plan_digest,
        )
        return _evaluation_result(
            outcome="invalid", reason=terminal["reason"], candidates=[],
            selected_parameters=None, selected_index=None, test_open_count=0,
            held_out_evaluation=terminal,
        )
    partition_size = (len(remaining) - partition_gap) // 2
    validation = remaining.iloc[:partition_size].copy()
    test = remaining.iloc[
        partition_size + partition_gap : partition_size * 2 + partition_gap
    ].copy()
    names, candidates = tuple(FROZEN_GRID), []
    for values in product(*(FROZEN_GRID[name] for name in names)):
        params = dict(zip(names, values))
        try:
            candidates.append(_score(validation, train, params, "validation", representation_plan_digest))
        except Exception as exc:
            candidates.append(_result("failed", f"validation_evaluation_failed:{type(exc).__name__}", params,
                                      plan_digest=representation_plan_digest))
    eligible = [
        (i, item)
        for i, item in enumerate(candidates)
        if item.get("selection_eligible") is True
    ]
    if not eligible:
        outcomes = {item["outcome"] for item in candidates}
        outcome = "failed" if "failed" in outcomes else "invalid" if outcomes == {"invalid"} else "negative"
        terminal = _result(outcome, "no_validation_variant_passed", {}, plan_digest=representation_plan_digest)
        return _evaluation_result(
            outcome=outcome, reason=terminal["reason"], candidates=candidates,
            selected_parameters=None, selected_index=None, test_open_count=0,
            held_out_evaluation=terminal,
        )
    index, winner = max(eligible, key=lambda pair: (pair[1]["mean_net_signed_residual_return"], -pair[0]))
    try:
        heldout = _score(test, train, winner["parameters"], "test", representation_plan_digest)
    except Exception as exc:
        heldout = _result("failed", f"heldout_evaluation_failed:{type(exc).__name__}", winner["parameters"],
                          plan_digest=representation_plan_digest)
    return _evaluation_result(
        outcome=heldout["outcome"], reason=heldout["reason"], candidates=candidates,
        selected_parameters=winner["parameters"], selected_index=index,
        test_open_count=1, held_out_evaluation=heldout,
    )


@register_strategy("sol_to_eth_2h_tail_return")
class SolToEth2hTailReturnStrategy(Strategy):
    """Observation-only consumer of the exact ordered adaptive representation.

    No entry or exit is emitted; an executable successor must use ``close_only``
    engine exits and is outside this card's authority.
    """

    def __init__(self, *, adaptive_representation_plan_digest: str = "", **_: Any) -> None:
        self.expected_plan_digest = (
            adaptive_representation_plan_digest
            if adaptive_representation_plan_digest
            == EXPECTED_REPRESENTATION_PLAN_DIGEST
            else ""
        )

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: Mapping[str, Bar],
                tradeable: set[str], ctx: Mapping[str, Any]) -> list[Signal]:
        bars = [bars_by_symbol.get(symbol) for symbol in INSTRUMENTS]
        if any(bar is None for bar in bars):
            return []
        extras = [bar.extra for bar in bars if bar is not None]
        if all(extra.get("representation_decision_ts") is None for extra in extras):
            return []
        encoded = json.dumps(list(OUTPUT_FIELDS), separators=(",", ":"))
        provenance_valid = all(
            extra.get("representation_plan_digest") == self.expected_plan_digest
            and extra.get("representation_output_fields") == encoded
            and pd.Timestamp(extra.get("representation_decision_ts")) == pd.Timestamp(ts)
            and pd.Timestamp(extra.get("prior_only_volatility_source_end_ts")) == pd.Timestamp(ts) - pd.Timedelta(hours=2)
            for extra in extras
        ) and len(self.expected_plan_digest) == 64
        payloads = [
            {name: extra.get(name) for name in OUTPUT_FIELDS}
            for extra in extras
        ]
        outputs = payloads[0]
        outputs_complete = all(
            value is not None and pd.notna(value)
            for payload in payloads
            for value in payload.values()
        )
        payloads_equal = all(payload == outputs for payload in payloads[1:])
        outcome = (
            "consumed"
            if provenance_valid and outputs_complete and payloads_equal
            else "warmup"
            if provenance_valid and payloads_equal
            else "invalid"
        )
        return [Signal(ts=ts, symbol="ETHUSDT", side=None,
                       signal_type="sol_to_eth_representation_validation", confidence=1.0,
                       metadata={"native_payload_outcome": outcome,
                                 "native_payload_reason": "exact_ordered_fields_consumed" if outcome == "consumed" else "causal_history_incomplete" if outcome == "warmup" else "representation_mismatch",
                                 "representation_plan_digest": extras[0].get("representation_plan_digest"),
                                 "representation_output_fields": list(OUTPUT_FIELDS),
                                 "representation_decision_ts": str(extras[0].get("representation_decision_ts")),
                                 "consumed_representation_values": outputs,
                                 "decision_trace": {"causal_decision_ts": str(ts), "prior_volatility_lag_hours": 2}})]
