"""Point-in-time held-out evaluation for governed alpha research."""

from __future__ import annotations

import json
import math
from pathlib import Path
import re
from typing import Any

import pandas as pd

from bt.data.resample import timeframe_minutes
from bt.governance.research_bridge import BridgeError
from bt.institutional.receipt import digest


def empirical_lower_quantile(values: Any, probability: float) -> float:
    """Return the deterministic lower empirical quantile used by strategy and audit."""
    ordered = sorted(float(value) for value in values)
    if not ordered:
        raise BridgeError("empirical quantile requires at least one observation")
    if not 0.0 <= float(probability) <= 1.0:
        raise BridgeError("empirical quantile probability must be in [0, 1]")
    return ordered[int((len(ordered) - 1) * float(probability))]


def trade_decision_timestamps(trades: pd.DataFrame) -> pd.Series:
    """Resolve the canonical decision timestamp and reject missing membership proof."""
    column = next(
        (candidate for candidate in ("identity_ts_signal", "signal_ts") if candidate in trades.columns),
        None,
    )
    if column is None:
        raise BridgeError(
            "trade evaluation requires identity_ts_signal or signal_ts; "
            "entry fill time cannot define decision-row membership"
        )
    timestamps = pd.to_datetime(trades[column], utc=True, errors="coerce")
    if timestamps.isna().any():
        raise BridgeError("trade evaluation rejects missing or invalid decision timestamps")
    return timestamps


def required_trade_logging_evaluation(
    run_dir: Path,
    required_fields: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """Prove every retained trade carries its exact preregistered logging contract."""
    trades_path = run_dir / "trades.csv"
    try:
        trades = pd.read_csv(trades_path)
    except pd.errors.EmptyDataError:
        trades = pd.DataFrame()
    engine_required = (
        "identity_ts_signal",
        "requested_risk_amount",
        "risk_amount",
        "risk_utilization_pct",
        "under_risked_trade",
    )
    declared = list(required_fields or ())
    if any(not isinstance(field, str) or not field for field in declared):
        raise BridgeError("required trade logging fields must be non-empty strings")
    if len(declared) != len(set(declared)):
        raise BridgeError("required trade logging fields must be unique")
    required = tuple(dict.fromkeys([*declared, *engine_required]))
    missing_columns = sorted(set(required) - set(trades.columns))
    null_fields: dict[str, int] = {}
    invalid_fields: dict[str, int] = {}
    if not trades.empty:
        for field in required:
            if field in trades:
                count = int(trades[field].isna().sum())
                if count:
                    null_fields[field] = count
        if "decision_trace" in trades:
            import json

            invalid = 0
            for value in trades["decision_trace"].dropna():
                try:
                    parsed = json.loads(str(value))
                except (TypeError, ValueError):
                    invalid += 1
                    continue
                if not isinstance(parsed, dict) or not parsed:
                    invalid += 1
            if invalid:
                invalid_fields["decision_trace"] = invalid
    passed = not missing_columns and not null_fields and not invalid_fields
    report = {
        "schema_version": "alpha-required-trade-logging-v1.1.0",
        "trade_count": int(len(trades)),
        "declared_fields": declared,
        "engine_required_fields": list(engine_required),
        "required_fields": list(required),
        "missing_columns": missing_columns,
        "null_fields": null_fields,
        "invalid_fields": invalid_fields,
        "passed": passed,
    }
    report["record_digest"] = digest(report)
    return report


def required_observation_logging_evaluation(
    evaluation: dict[str, Any],
    required_fields: list[str] | tuple[str, ...],
) -> dict[str, Any]:
    """Validate scientific observation records that intentionally emit no trades."""
    declared = list(required_fields)
    if any(not isinstance(field, str) or not field for field in declared):
        raise BridgeError("required observation logging fields must be non-empty strings")
    if len(declared) != len(set(declared)):
        raise BridgeError("required observation logging fields must be unique")
    records = evaluation.get("observation_records", [])
    if not isinstance(records, list) or any(not isinstance(item, dict) for item in records):
        raise BridgeError("observation_records must be a list of objects")
    missing: dict[str, list[str]] = {}
    nulls: dict[str, list[str]] = {}
    invalid: dict[str, list[str]] = {}
    for index, record in enumerate(records):
        key = str(index)
        absent = sorted(set(declared) - set(record))
        if absent:
            missing[key] = absent
        empty = sorted(field for field in declared if field in record and record[field] is None)
        if empty:
            nulls[key] = empty
        trace = record.get("decision_trace")
        if "decision_trace" in declared and (
            not isinstance(trace, dict) or not trace
        ):
            invalid[key] = ["decision_trace"]
    terminal = evaluation.get("terminal_evidence_records", [])
    if not isinstance(terminal, list) or any(
        not isinstance(item, dict) for item in terminal
    ):
        raise BridgeError("terminal_evidence_records must be a list of objects")
    terminal_errors: list[str] = []
    if terminal:
        if records:
            terminal_errors.append("terminal_evidence_with_scientific_observations")
        if len(terminal) != 1:
            terminal_errors.append("terminal_evidence_count_must_equal_one")
        record = terminal[0]
        if record.get("record_kind") != "terminal_no_observation":
            terminal_errors.append("invalid_terminal_record_kind")
        if record.get("outcome") != evaluation.get("outcome") or record.get(
            "outcome"
        ) not in {"negative", "invalid", "failed"}:
            terminal_errors.append("terminal_outcome_mismatch")
        if not isinstance(record.get("reason"), str) or not record["reason"]:
            terminal_errors.append("terminal_reason_missing")
        trace = record.get("decision_trace")
        if not isinstance(trace, dict) or trace.get("scientific_observation") is not False:
            terminal_errors.append("terminal_decision_trace_invalid")
        plan_digest = record.get("representation_plan_digest")
        if not isinstance(plan_digest, str) or re.fullmatch(
            r"[0-9a-f]{64}", plan_digest
        ) is None:
            terminal_errors.append("terminal_representation_digest_invalid")
        try:
            fields = json.loads(record["representation_output_fields"])
        except (KeyError, TypeError, ValueError):
            fields = None
        if not isinstance(fields, list) or not fields or any(
            not isinstance(field, str) or not field for field in fields
        ):
            terminal_errors.append("terminal_representation_fields_invalid")
        for name in ("decision_ts", "representation_decision_ts"):
            try:
                timestamp = pd.Timestamp(record[name])
            except (KeyError, TypeError, ValueError):
                timestamp = pd.NaT
            if pd.isna(timestamp) or timestamp.tzinfo is None:
                terminal_errors.append(f"terminal_{name}_invalid")
    terminal_mode = not records and bool(terminal) and not terminal_errors
    observation_mode = bool(records) and not missing and not nulls and not invalid
    report = {
        "schema_version": "alpha-required-observation-logging-v1.1.0",
        "observation_count": len(records),
        "terminal_evidence_count": len(terminal),
        "evidence_mode": (
            "scientific_observations"
            if observation_mode
            else "terminal_no_observation"
            if terminal_mode
            else "incomplete"
        ),
        "declared_fields": declared,
        "missing_fields": missing,
        "null_fields": nulls,
        "invalid_fields": invalid,
        "terminal_errors": terminal_errors,
        "scientific_observation_logging_complete": observation_mode,
        "terminal_retention_complete": terminal_mode,
        "passed": observation_mode or terminal_mode,
    }
    report["record_digest"] = digest(report)
    return report


def held_out_trade_evaluation(run_dir: Path, test_start: str) -> dict[str, Any]:
    """Score only held-out trades and apply a second copy of observed costs."""
    trades_path = run_dir / "trades.csv"
    try:
        trades = pd.read_csv(trades_path)
    except pd.errors.EmptyDataError:
        trades = pd.DataFrame()
    if trades.empty:
        return {
            "test_start": test_start,
            "trade_count": 0,
            "mean_net_r": 0.0,
            "double_cost_mean_net_r": 0.0,
            "adequate_support": False,
            "positive_net_edge": False,
            "cost_stress_passed": False,
        }
    decisions = trade_decision_timestamps(trades)
    sample = trades.loc[decisions >= pd.Timestamp(test_start)]
    net_column = "r_net" if "r_net" in sample else "r_multiple_net"
    cost_column = "cost_drag_r" if "cost_drag_r" in sample else None
    net = pd.to_numeric(sample[net_column], errors="coerce").dropna()
    costs = (
        pd.to_numeric(sample.loc[net.index, cost_column], errors="coerce").fillna(0.0)
        if cost_column
        else pd.Series(0.0, index=net.index)
    )
    stressed = net - costs.abs()
    return {
        "test_start": test_start,
        "trade_count": int(len(net)),
        "mean_net_r": float(net.mean()) if len(net) else 0.0,
        "double_cost_mean_net_r": float(stressed.mean()) if len(stressed) else 0.0,
        "adequate_support": len(net) >= 50,
        "positive_net_edge": bool(len(net) and net.mean() > 0),
        "cost_stress_passed": bool(len(stressed) and stressed.mean() > 0),
    }


def complete_timeframe_bars(frame: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """Build strict left-labeled bars from complete, unique 1m observations."""
    required = {"ts", "symbol", "close"}
    missing = required - set(frame.columns)
    if missing:
        raise BridgeError(f"timeframe reconstruction is missing source fields: {sorted(missing)}")
    minutes = timeframe_minutes(timeframe)
    if minutes < 1:
        raise BridgeError("decision timeframe must be at least one minute")
    columns = sorted(required | ({"quote_volume"} & set(frame.columns)))
    ordered = frame.loc[:, columns].copy()
    ordered["ts"] = pd.to_datetime(ordered["ts"], utc=True, errors="raise")
    ordered = ordered.sort_values(["symbol", "ts"])
    if ordered.duplicated(["symbol", "ts"]).any():
        raise BridgeError("timeframe reconstruction rejects duplicate minute bars")
    if (ordered["ts"].dt.second != 0).any() or (
        ordered["ts"].dt.microsecond != 0
    ).any():
        raise BridgeError("timeframe reconstruction requires minute-aligned source bars")
    ordered["bucket"] = ordered["ts"].dt.floor(f"{minutes}min")
    grouped = ordered.groupby(["symbol", "bucket"], sort=True)
    complete = grouped.filter(
        lambda sample: len(sample) == minutes
        and sample["ts"].nunique() == minutes
        and sample["ts"].max() - sample["ts"].min() == pd.Timedelta(minutes=minutes - 1)
        and bool(sample[columns].notna().all().all())
        and (
            "quote_volume" not in sample
            or bool((sample["quote_volume"] >= 0.0).all())
        )
    )
    if complete.empty:
        optional = [column for column in columns if column not in required]
        return pd.DataFrame(columns=["symbol", "ts", "close", *optional])
    aggregation: dict[str, tuple[str, str]] = {"close": ("close", "last")}
    if "quote_volume" in complete:
        aggregation["quote_volume"] = ("quote_volume", "sum")
    return complete.groupby(["symbol", "bucket"], sort=True).agg(
        **aggregation
    ).reset_index().rename(columns={"bucket": "ts"})


def complete_five_minute_bars(frame: pd.DataFrame) -> pd.DataFrame:
    """Build strict left-labeled 5m bars with complete quote volume."""
    if "quote_volume" not in frame:
        raise BridgeError("impact-proxy evaluation is missing source fields: ['quote_volume']")
    return complete_timeframe_bars(frame, "5m")


def impact_proxy_evaluation(
    frame: pd.DataFrame,
    *,
    test_start: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    """Evaluate held-out impact extremes against preregistered matched shocks."""
    bars = complete_five_minute_bars(frame)
    if bars.empty:
        raise BridgeError("impact-proxy evaluation has no complete 5m bars")
    threshold = float(params["impact_proxy_threshold"])
    window = int(params["normalization_window"])
    band = float(params["return_shock_control_band"])
    parts: list[pd.DataFrame] = []
    for _, sample in bars.groupby("symbol", sort=False):
        sample = sample.sort_values("ts").copy()
        sample["segment"] = sample["ts"].diff().ne(
            pd.Timedelta(minutes=5)
        ).cumsum()
        segments = sample.groupby("segment", sort=False)
        sample["signal_return"] = segments["close"].pct_change()
        sample["impact_proxy"] = sample["signal_return"].abs() / sample["quote_volume"]
        sample["threshold_value"] = segments["impact_proxy"].transform(
            lambda values: values.shift(1)
            .rolling(window=window, min_periods=window)
            .apply(
                lambda prior: empirical_lower_quantile(prior, threshold), raw=True
            )
        )
        sample["decision_at"] = sample["ts"] + pd.Timedelta(minutes=5)
        # Six subsequent complete 5m buckets prove both the exact 30-minute
        # decision horizon and an uninterrupted underlying 1m monitoring path.
        future_close = segments["close"].shift(-6)
        sample["next_30m_return"] = future_close / sample["close"] - 1.0
        sample["signed_reversal"] = (
            -sample["signal_return"].apply(lambda value: 1.0 if value > 0 else -1.0)
            * sample["next_30m_return"]
        )
        parts.append(sample)
    evaluated = pd.concat(parts, ignore_index=True)
    evaluated = evaluated.loc[
        (evaluated["decision_at"] >= pd.Timestamp(test_start))
        & evaluated["signal_return"].notna()
        & evaluated["next_30m_return"].notna()
        & evaluated["threshold_value"].notna()
        & (evaluated["quote_volume"] >= 1_000_000.0)
    ].copy()
    extreme = evaluated.loc[evaluated["impact_proxy"] >= evaluated["threshold_value"]]
    controls: list[float] = []
    paired_differences: list[float] = []
    used_control_indices: set[int] = set()
    for row in extreme.itertuples(index=False):
        magnitude = abs(float(row.signal_return))
        low, high = magnitude * (1.0 - band), magnitude * (1.0 + band)
        candidates = evaluated.loc[
            (evaluated["symbol"] == row.symbol)
            & (evaluated["ts"] != row.ts)
            & (~evaluated.index.isin(used_control_indices))
            & (evaluated["impact_proxy"] < evaluated["threshold_value"])
            & (evaluated["signal_return"].abs().between(low, high))
            & ((evaluated["signal_return"] > 0) == (row.signal_return > 0))
        ]
        if not candidates.empty:
            distance = (candidates["signal_return"].abs() - magnitude).abs()
            control_index = int(distance.idxmin())
            used_control_indices.add(control_index)
            control_return = float(candidates.loc[control_index, "signed_reversal"])
            controls.append(control_return)
            paired_differences.append(float(row.signed_reversal) - control_return)
    extreme_reversal = pd.to_numeric(extreme["signed_reversal"], errors="coerce").dropna()
    control_mean = float(pd.Series(controls, dtype=float).mean()) if controls else 0.0
    extreme_mean = float(extreme_reversal.mean()) if len(extreme_reversal) else 0.0
    paired = pd.Series(paired_differences, dtype=float)
    paired_mean = float(paired.mean()) if len(paired) else 0.0
    paired_standard_error = (
        float(paired.std(ddof=1) / math.sqrt(len(paired))) if len(paired) > 1 else 0.0
    )
    paired_lower_95 = paired_mean - 1.96 * paired_standard_error
    minimum_matched_pairs = 30
    statistically_outperformed = bool(
        len(paired) >= minimum_matched_pairs and paired_lower_95 > 0.0
    )
    matched = {
        "extreme_observations": int(len(extreme_reversal)),
        "matched_control_observations": len(controls),
        "extreme_mean_signed_30m_return": extreme_mean,
        "control_mean_signed_30m_return": control_mean,
        "extreme_minus_control": extreme_mean - control_mean,
        "outperformed_control": bool(controls and extreme_mean > control_mean),
        "control_reuse": False,
        "paired_difference_mean": paired_mean,
        "paired_difference_standard_error": paired_standard_error,
        "paired_difference_lower_95": paired_lower_95,
        "minimum_matched_pairs": minimum_matched_pairs,
        "statistically_outperformed_control": statistically_outperformed,
    }
    direction_observations: dict[str, dict[str, Any]] = {}
    for direction, mask in (
        ("long", extreme["signal_return"] < 0),
        ("short", extreme["signal_return"] > 0),
    ):
        returns = pd.to_numeric(
            extreme.loc[mask, "signed_reversal"], errors="coerce"
        ).dropna()
        direction_observations[direction] = {
            "observations": int(len(returns)),
            "mean_signed_30m_return": float(returns.mean()) if len(returns) else 0.0,
        }
    minimum_per_direction = 10
    direction_gate_passed = all(
        item["observations"] >= minimum_per_direction
        and item["mean_signed_30m_return"] > 0.0
        for item in direction_observations.values()
    )
    report = {
        "schema_version": "alpha-impact-proxy-evaluation-v1.2.0",
        "measurement": "held-out causal predictive association; not executable PnL",
        "test_start": pd.Timestamp(test_start).isoformat(),
        "resampling": "strict complete left-labeled 5m bars from unique 1m rows",
        "decision_time": "bucket_start_plus_5m",
        "gap_policy": "reset_return_normalization_and_atr_state",
        "target_path_policy": "six_contiguous_complete_5m_buckets_after_decision",
        "evaluated_observations": int(len(evaluated)),
        "parameters": {
            "impact_proxy_threshold": threshold,
            "normalization_window": window,
            "return_shock_control_band": band,
        },
        "direction_balance": {
            "long": direction_observations["long"]["observations"],
            "short": direction_observations["short"]["observations"],
            "minimum_per_direction": minimum_per_direction,
            "per_direction": direction_observations,
            "balanced_positive_reversal": direction_gate_passed,
        },
        "matched_return_shock_control": matched,
    }
    report["record_digest"] = digest(report)
    return report
