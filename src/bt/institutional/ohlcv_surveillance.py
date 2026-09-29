"""DISC-010 native, bounded OHLCV signal-family surveillance."""

from __future__ import annotations

import math
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import numpy as np
import pandas as pd

from bt.experiments.adaptive_representation import (
    AdaptiveRepresentationError,
    complete_bars,
)

from .receipt import ProducerReceipt, build_receipt, digest


class OhlcvSurveillanceError(ValueError):
    """The signal screen violates its frozen causal contract."""


_SPEC_KEYS = {
    "schema_version",
    "family_id",
    "instruments",
    "source_timeframe",
    "research_timeframe",
    "selection_data_boundary",
    "outcome_data_consulted_during_selection",
    "exploration_start",
    "exploration_end",
    "validation_end",
    "sealed_oos_start",
    "alpha",
    "permutation_count",
    "seed",
    "trials",
}
_TRIAL_KEYS = {
    "trial_id",
    "predictor_instrument",
    "target_instrument",
    "predictor",
    "lookback_bars",
    "target_horizon_bars",
    "tail_quantile",
    "relation",
    "minimum_support",
    "minimum_effect",
    "parameters",
}
_PREDICTORS = {
    "log_return",
    "fractional_difference",
    "volume_zscore",
    "realized_volatility",
}
_RELATIONS = {"same_direction", "opposite_direction", "positive", "negative"}


def _timestamp(value: object, name: str) -> pd.Timestamp:
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise OhlcvSurveillanceError(f"{name} must be a timestamp") from exc
    if parsed.tzinfo is None:
        raise OhlcvSurveillanceError(f"{name} must include a timezone")
    return parsed.tz_convert("UTC")


def _validate_specification(specification: dict[str, Any]) -> tuple[pd.Timestamp, ...]:
    if set(specification) != _SPEC_KEYS:
        raise OhlcvSurveillanceError("signal-screen specification fields are not exact")
    if specification["schema_version"] != "disc010-signal-screen-spec-v1.0.0":
        raise OhlcvSurveillanceError("unsupported signal-screen specification")
    instruments = specification["instruments"]
    if (
        not isinstance(instruments, list)
        or not 1 <= len(instruments) <= 100
        or len(instruments) != len(set(instruments))
        or any(not isinstance(item, str) or not item for item in instruments)
    ):
        raise OhlcvSurveillanceError("instruments must be a bounded unique frozen basket")
    if specification["source_timeframe"] != "1m":
        raise OhlcvSurveillanceError("DISC-010 currently requires native one-minute inputs")
    if specification["selection_data_boundary"] != "metadata_predictors_only_no_targets":
        raise OhlcvSurveillanceError("selection must not consult outcomes")
    if specification["outcome_data_consulted_during_selection"] is not False:
        raise OhlcvSurveillanceError("outcomes cannot select the representation or family")
    start = _timestamp(specification["exploration_start"], "exploration_start")
    exploration_end = _timestamp(specification["exploration_end"], "exploration_end")
    validation_end = _timestamp(specification["validation_end"], "validation_end")
    sealed_oos_start = _timestamp(specification["sealed_oos_start"], "sealed_oos_start")
    if not start < exploration_end < validation_end <= sealed_oos_start:
        raise OhlcvSurveillanceError("research intervals must be ordered and non-overlapping")
    alpha = float(specification["alpha"])
    permutations = int(specification["permutation_count"])
    if not 0 < alpha <= 0.05 or not 99 <= permutations <= 9_999:
        raise OhlcvSurveillanceError("alpha or permutation budget is outside policy")
    if type(specification["seed"]) is not int:
        raise OhlcvSurveillanceError("seed must be an integer")
    trials = specification["trials"]
    if not isinstance(trials, list) or not 1 <= len(trials) <= 256:
        raise OhlcvSurveillanceError("trial family must contain between 1 and 256 trials")
    identifiers: set[str] = set()
    frozen_trials: set[str] = set()
    for trial in trials:
        if not isinstance(trial, dict) or set(trial) != _TRIAL_KEYS:
            raise OhlcvSurveillanceError("trial fields are not exact")
        trial_id = trial["trial_id"]
        if not isinstance(trial_id, str) or not trial_id or trial_id in identifiers:
            raise OhlcvSurveillanceError("trial identifiers must be nonempty and unique")
        identifiers.add(trial_id)
        frozen_digest = digest({key: value for key, value in trial.items() if key != "trial_id"})
        if frozen_digest in frozen_trials:
            raise OhlcvSurveillanceError("semantic duplicate in preregistered trial family")
        frozen_trials.add(frozen_digest)
        if (
            trial["predictor_instrument"] not in instruments
            or trial["target_instrument"] not in instruments
            or trial["predictor"] not in _PREDICTORS
            or trial["relation"] not in _RELATIONS
        ):
            raise OhlcvSurveillanceError("trial references an unfrozen instrument or method")
        if not 1 <= int(trial["lookback_bars"]) <= 10_000:
            raise OhlcvSurveillanceError("lookback is outside policy")
        if not 1 <= int(trial["target_horizon_bars"]) <= 10_000:
            raise OhlcvSurveillanceError("target horizon is outside policy")
        if not 0.5 < float(trial["tail_quantile"]) < 1:
            raise OhlcvSurveillanceError("tail quantile must be strictly between 0.5 and 1")
        if not 10 <= int(trial["minimum_support"]) <= 1_000_000:
            raise OhlcvSurveillanceError("minimum support is outside policy")
        if not math.isfinite(float(trial["minimum_effect"])) or float(
            trial["minimum_effect"]
        ) < 0:
            raise OhlcvSurveillanceError("minimum effect must be finite and nonnegative")
        parameters = trial["parameters"]
        if not isinstance(parameters, dict):
            raise OhlcvSurveillanceError("trial parameters must be an object")
        if trial["predictor"] == "fractional_difference":
            if set(parameters) != {"d", "weight_threshold"}:
                raise OhlcvSurveillanceError("fractional difference parameters are incomplete")
            if not 0 < float(parameters["d"]) < 0.5 or not 1e-8 <= float(
                parameters["weight_threshold"]
            ) <= 0.1:
                raise OhlcvSurveillanceError("fractional difference parameters violate policy")
        elif parameters:
            raise OhlcvSurveillanceError("predictor has unexpected parameters")
    return start, exploration_end, validation_end, sealed_oos_start


def _fractional_weights(d: float, threshold: float) -> np.ndarray:
    weights = [1.0]
    for index in range(1, 100_001):
        value = -weights[-1] * (d - index + 1) / index
        if abs(value) < threshold:
            break
        weights.append(value)
    if len(weights) == 100_001:
        raise OhlcvSurveillanceError("fractional-difference weights did not converge")
    return np.asarray(weights[::-1], dtype=np.float64)


def _predictor(frame: pd.DataFrame, trial: dict[str, Any]) -> pd.Series:
    close = frame[f"{trial['predictor_instrument']}__close"].astype(float)
    lookback = int(trial["lookback_bars"])
    kind = trial["predictor"]
    log_close = np.log(close.where(close > 0))
    if kind == "log_return":
        return log_close.diff(lookback)
    if kind == "realized_volatility":
        returns = log_close.diff()
        return returns.rolling(lookback, min_periods=lookback).std(ddof=0)
    if kind == "volume_zscore":
        volume = frame[f"{trial['predictor_instrument']}__volume"].astype(float)
        mean = volume.rolling(lookback, min_periods=lookback).mean()
        deviation = volume.rolling(lookback, min_periods=lookback).std(ddof=0)
        return (volume - mean) / deviation.replace(0, np.nan)
    weights = _fractional_weights(
        float(trial["parameters"]["d"]),
        float(trial["parameters"]["weight_threshold"]),
    )
    values = log_close.to_numpy(dtype=np.float64)
    output = np.full(len(values), np.nan)
    width = len(weights)
    for index in range(width - 1, len(values)):
        window = values[index - width + 1 : index + 1]
        if np.isfinite(window).all():
            output[index] = float(np.dot(weights, window))
    return pd.Series(output, index=frame.index)


def _signed_outcome(
    predictor: pd.Series, future_return: pd.Series, relation: str
) -> pd.Series:
    if relation == "same_direction":
        return np.sign(predictor) * future_return
    if relation == "opposite_direction":
        return -np.sign(predictor) * future_return
    if relation == "positive":
        return future_return
    return -future_return


def _segment(
    *,
    frame: pd.DataFrame,
    predictor: pd.Series,
    outcome: pd.Series,
    target_available_at: pd.Series,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    decision_at = frame["decision_at"]
    selected = pd.DataFrame(
        {"predictor": predictor, "outcome": outcome, "target_available_at": target_available_at}
    )
    mask = (decision_at >= start) & (decision_at < end) & (target_available_at < end)
    return selected.loc[mask].replace([np.inf, -np.inf], np.nan).dropna()


def _empirical_p_value(
    outcome: np.ndarray,
    tail: np.ndarray,
    observed: float,
    *,
    permutations: int,
    seed: int,
) -> float:
    if len(outcome) < 3 or not tail.any():
        return 1.0
    rng = np.random.default_rng(seed)
    exceedances = 0
    for _ in range(permutations):
        shift = int(rng.integers(1, len(outcome)))
        permuted = float(np.mean(np.roll(outcome, shift)[tail]))
        if permuted >= observed:
            exceedances += 1
    return (exceedances + 1.0) / (permutations + 1.0)


def _trial_result(
    *,
    frame: pd.DataFrame,
    trial: dict[str, Any],
    exploration_start: pd.Timestamp,
    exploration_end: pd.Timestamp,
    validation_end: pd.Timestamp,
    permutations: int,
    seed: int,
) -> dict[str, Any]:
    predictor = _predictor(frame, trial)
    target = frame[f"{trial['target_instrument']}__close"].astype(float)
    horizon = int(trial["target_horizon_bars"])
    future_return = np.log(target.shift(-horizon) / target)
    target_available_at = frame["decision_at"].shift(-horizon)
    signed = _signed_outcome(predictor, future_return, trial["relation"])
    exploration = _segment(
        frame=frame,
        predictor=predictor,
        outcome=signed,
        target_available_at=target_available_at,
        start=exploration_start,
        end=exploration_end,
    )
    validation = _segment(
        frame=frame,
        predictor=predictor,
        outcome=signed,
        target_available_at=target_available_at,
        start=exploration_end,
        end=validation_end,
    )
    minimum_support = int(trial["minimum_support"])
    if len(exploration) < minimum_support or len(validation) < minimum_support:
        return {
            "trial_id": trial["trial_id"],
            "trial_digest": digest(trial),
            "status": "invalid",
            "reason": "insufficient_aligned_observations",
            "exploration_observations": len(exploration),
            "validation_observations": len(validation),
        }
    cutoff = float(exploration["predictor"].abs().quantile(float(trial["tail_quantile"])))
    exploration_tail = exploration["predictor"].abs().to_numpy() >= cutoff
    validation_tail = validation["predictor"].abs().to_numpy() >= cutoff
    exploration_support = int(exploration_tail.sum())
    validation_support = int(validation_tail.sum())
    if exploration_support < minimum_support or validation_support < minimum_support:
        return {
            "trial_id": trial["trial_id"],
            "trial_digest": digest(trial),
            "status": "invalid",
            "reason": "insufficient_tail_support",
            "exploration_observations": len(exploration),
            "validation_observations": len(validation),
            "exploration_support": exploration_support,
            "validation_support": validation_support,
            "exploration_fitted_cutoff": cutoff,
        }
    exploration_effect = float(exploration["outcome"].to_numpy()[exploration_tail].mean())
    validation_values = validation["outcome"].to_numpy()
    validation_effect = float(validation_values[validation_tail].mean())
    p_value = _empirical_p_value(
        validation_values,
        validation_tail,
        validation_effect,
        permutations=permutations,
        seed=seed,
    )
    return {
        "trial_id": trial["trial_id"],
        "trial_digest": digest(trial),
        "status": "evaluated",
        "reason": None,
        "exploration_observations": len(exploration),
        "validation_observations": len(validation),
        "exploration_support": exploration_support,
        "validation_support": validation_support,
        "exploration_fitted_cutoff": cutoff,
        "exploration_effect": exploration_effect,
        "validation_effect": validation_effect,
        "validation_empirical_p_value": p_value,
        "direction_stable": exploration_effect > 0 and validation_effect > 0,
        "minimum_effect_met": validation_effect >= float(trial["minimum_effect"]),
    }


def _benjamini_hochberg(results: list[dict[str, Any]], alpha: float) -> set[str]:
    evaluated = [item for item in results if item["status"] == "evaluated"]
    ordered = sorted(
        evaluated,
        key=lambda item: (item["validation_empirical_p_value"], item["trial_digest"]),
    )
    accepted_through = 0
    for rank, item in enumerate(ordered, start=1):
        if item["validation_empirical_p_value"] <= alpha * rank / len(ordered):
            accepted_through = rank
    return {item["trial_digest"] for item in ordered[:accepted_through]}


def ohlcv_signal_surveillance_receipt(
    *,
    specification: dict[str, Any],
    panels: dict[str, pd.DataFrame],
    dataset_digest: str,
    source_commit: str,
    max_workers: int = 1,
) -> ProducerReceipt:
    """Screen a frozen signal family without consulting or opening final OOS data."""
    start, exploration_end, validation_end, sealed_oos_start = _validate_specification(
        specification
    )
    if type(max_workers) is not int or not 1 <= max_workers <= 8:
        raise OhlcvSurveillanceError("execution workers must be an integer from 1 to 8")
    instruments = specification["instruments"]
    if set(panels) != set(instruments):
        raise OhlcvSurveillanceError("panel identities differ from the frozen basket")
    aligned: pd.DataFrame | None = None
    source_rows: dict[str, int] = {}
    admitted_rows: dict[str, int] = {}
    complete_rows: dict[str, int] = {}
    for instrument in instruments:
        raw = panels[instrument].copy()
        source_rows[instrument] = len(raw)
        timestamps = pd.to_datetime(raw["ts"], utc=True, errors="raise")
        admitted = raw.loc[timestamps < sealed_oos_start].copy()
        admitted_rows[instrument] = len(admitted)
        try:
            bars = complete_bars(admitted, specification["research_timeframe"], instrument)
        except AdaptiveRepresentationError as exc:
            raise OhlcvSurveillanceError(str(exc)) from exc
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
        raise OhlcvSurveillanceError("basket has no aligned complete research bars")
    trial_arguments = [
        (ordinal, trial)
        for ordinal, trial in enumerate(specification["trials"], start=1)
    ]

    def evaluate(item: tuple[int, dict[str, Any]]) -> dict[str, Any]:
        ordinal, trial = item
        return _trial_result(
            frame=aligned,
            trial=trial,
            exploration_start=start,
            exploration_end=exploration_end,
            validation_end=validation_end,
            permutations=int(specification["permutation_count"]),
            seed=int(specification["seed"]) + ordinal,
        )

    if max_workers == 1 or len(trial_arguments) == 1:
        results = [evaluate(item) for item in trial_arguments]
    else:
        with ThreadPoolExecutor(max_workers=min(max_workers, len(trial_arguments))) as pool:
            results = list(pool.map(evaluate, trial_arguments))
    discoveries = _benjamini_hochberg(results, float(specification["alpha"]))
    question_candidates = []
    for result in results:
        result["family_adjusted_discovery"] = result["trial_digest"] in discoveries
        result["question_candidate"] = bool(
            result["status"] == "evaluated"
            and result["family_adjusted_discovery"]
            and result["direction_stable"]
            and result["minimum_effect_met"]
        )
        if result["question_candidate"]:
            question_candidates.append(result["trial_digest"])
    result = {
        "schema_version": "disc010-signal-screen-receipt-v1.0.0",
        "family_id": specification["family_id"],
        "family_digest": digest(specification["trials"]),
        "basket": instruments,
        "research_timeframe": specification["research_timeframe"],
        "source_rows": source_rows,
        "pre_oos_admitted_rows": admitted_rows,
        "complete_rows": complete_rows,
        "aligned_complete_rows": len(aligned),
        "trial_count": len(results),
        "evaluated_count": sum(item["status"] == "evaluated" for item in results),
        "invalid_count": sum(item["status"] == "invalid" for item in results),
        "trials": results,
        "question_candidate_digests": sorted(question_candidates),
        "final_oos_opened": False,
        "final_oos_metrics": {},
        "selection_data_boundary": specification["selection_data_boundary"],
        "execution_authority": False,
        "strategy_authority": False,
        "promotion_authority": False,
        "claim_boundary": (
            "DISC-010 emits validation-screened research questions only. Final OOS, "
            "strategy, promotion, order, and capital authority remain closed."
        ),
    }
    return build_receipt(
        milestone="DISC-010",
        producer=(
            "bt.institutional.ohlcv_surveillance."
            "ohlcv_signal_surveillance_receipt"
        ),
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={
            "specification": specification,
            "pre_oos_admitted_rows": admitted_rows,
            "complete_rows": complete_rows,
        },
        dataset_digest=dataset_digest,
        configuration={
            "alpha": specification["alpha"],
            "permutation_count": specification["permutation_count"],
            "seed": specification["seed"],
        },
        artifacts={"trials": results, "question_candidates": question_candidates},
        result=result,
    )
