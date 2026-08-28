"""ML-002..004 causal materialization, evaluation and calibration producers."""

from __future__ import annotations

from collections.abc import Iterable
from itertools import pairwise
from typing import Any

import numpy as np

from .receipt import ProducerReceipt, build_receipt, digest


class MLProducerError(ValueError):
    """An ML computation violates its causal or evaluation contract."""


def causal_materialization_receipt(
    *,
    timestamps: Iterable[str],
    features: np.ndarray,
    labels: np.ndarray,
    feature_lags: Iterable[int],
    folds: Iterable[dict[str, int]],
    purge: int,
    embargo: int,
    dataset_digest: str,
    factor_receipt: ProducerReceipt,
    source_commit: str,
) -> ProducerReceipt:
    times, lags, split_specs = list(timestamps), list(feature_lags), list(folds)
    if factor_receipt.milestone != "DISC-003":
        raise MLProducerError("ML-002 requires a DISC-003 producer receipt")
    if (
        features.ndim != 2
        or labels.ndim != 1
        or len(features) != len(labels)
        or len(times) != len(labels)
    ):
        raise MLProducerError("feature, label and timestamp shapes differ")
    if len(lags) != features.shape[1] or any(lag < 1 for lag in lags):
        raise MLProducerError("every feature must be causally lagged")
    if (
        purge < 1
        or embargo < 1
        or not np.isfinite(features).all()
        or not np.isfinite(labels).all()
    ):
        raise MLProducerError("invalid purge, embargo or non-finite materialization")
    normalized_folds: list[dict[str, Any]] = []
    prior_train_end = -1
    for fold in split_specs:
        train_start, train_end = int(fold["train_start"]), int(fold["train_end"])
        test_start, test_end = int(fold["test_start"]), int(fold["test_end"])
        if (
            train_start != 0
            or train_end <= prior_train_end
            or test_start - train_end <= purge
            or test_end <= test_start
        ):
            raise MLProducerError("folds must be expanding, purged and ordered")
        if test_end + embargo > len(labels):
            raise MLProducerError("embargo extends beyond materialization")
        prior_train_end = train_end
        normalized_folds.append(
            {
                **fold,
                "train_rows": train_end - train_start,
                "test_rows": test_end - test_start,
            }
        )
    result = {
        "schema_version": "ml002-causal-materialization-v1.0.0",
        "factor_receipt_digest": factor_receipt.receipt_digest,
        "rows": len(labels),
        "columns": features.shape[1],
        "feature_lags": lags,
        "purge": purge,
        "embargo": embargo,
        "folds": normalized_folds,
        "features_digest": digest(features.tolist()),
        "labels_digest": digest(labels.tolist()),
        "timestamps_digest": digest(times),
    }
    return build_receipt(
        milestone="ML-002",
        producer="bt.institutional.ml.causal_materialization_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={
            "timestamps": times,
            "features": features.tolist(),
            "labels": labels.tolist(),
        },
        dataset_digest=dataset_digest,
        configuration={
            "lags": lags,
            "folds": split_specs,
            "purge": purge,
            "embargo": embargo,
        },
        artifacts=result,
        result=result,
    )


def _binary_metrics(labels: np.ndarray, probabilities: np.ndarray) -> dict[str, float]:
    clipped = np.clip(probabilities, 1e-12, 1 - 1e-12)
    predicted = probabilities >= 0.5
    return {
        "accuracy": float(np.mean(predicted == labels)),
        "log_loss": float(
            -np.mean(labels * np.log(clipped) + (1 - labels) * np.log(1 - clipped))
        ),
        "brier": float(np.mean(np.square(probabilities - labels))),
    }


def model_family_evaluation_receipt(
    *,
    materialization_receipt: ProducerReceipt,
    labels: np.ndarray,
    predictions: dict[str, np.ndarray],
    regimes: Iterable[str],
    baseline_families: set[str],
    minimum_increment: float,
    dataset_digest: str,
    source_commit: str,
) -> ProducerReceipt:
    if materialization_receipt.milestone != "ML-002":
        raise MLProducerError("ML-003 requires an ML-002 receipt")
    regime_values = np.asarray(list(regimes), dtype=object)
    if (
        labels.ndim != 1
        or len(regime_values) != len(labels)
        or len(np.unique(labels)) != 2
    ):
        raise MLProducerError("binary labels and aligned regimes are required")
    if not baseline_families or not baseline_families.issubset(predictions):
        raise MLProducerError("declared baseline predictions are required")
    scorecards: dict[str, Any] = {}
    for family, probability in sorted(predictions.items()):
        values = np.asarray(probability, dtype=np.float64)
        if values.shape != labels.shape or np.any((values < 0) | (values > 1)):
            raise MLProducerError("predictions must be aligned probabilities")
        by_regime = {
            str(regime): _binary_metrics(
                labels[regime_values == regime], values[regime_values == regime]
            )
            for regime in sorted(set(regime_values))
            if np.sum(regime_values == regime) >= 2
        }
        scorecards[family] = {
            "overall": _binary_metrics(labels, values),
            "regimes": by_regime,
            "prediction_digest": digest(values.tolist()),
        }
    strongest_baseline = max(
        scorecards[name]["overall"]["accuracy"] for name in baseline_families
    )
    qualified = sorted(
        name
        for name, card in scorecards.items()
        if name not in baseline_families
        and card["overall"]["accuracy"] >= strongest_baseline + minimum_increment
        and card["regimes"]
        and min(item["accuracy"] for item in card["regimes"].values()) >= 0.5
    )
    result = {
        "schema_version": "ml003-model-family-evaluation-v1.0.0",
        "materialization_receipt_digest": materialization_receipt.receipt_digest,
        "scorecards": scorecards,
        "strongest_baseline_accuracy": strongest_baseline,
        "minimum_increment": minimum_increment,
        "qualified_families": qualified,
    }
    return build_receipt(
        milestone="ML-003",
        producer="bt.institutional.ml.model_family_evaluation_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={
            "labels": labels.tolist(),
            "predictions": {key: value.tolist() for key, value in predictions.items()},
            "regimes": regime_values.tolist(),
        },
        dataset_digest=dataset_digest,
        configuration={
            "baselines": sorted(baseline_families),
            "minimum_increment": minimum_increment,
        },
        artifacts=scorecards,
        result=result,
    )


def calibration_receipt(
    *,
    evaluation_receipt: ProducerReceipt,
    family: str,
    labels: np.ndarray,
    probabilities: np.ndarray,
    bins: int,
    minimum_confidence: float,
    maximum_ece: float,
    dataset_digest: str,
    source_commit: str,
) -> ProducerReceipt:
    if (
        evaluation_receipt.milestone != "ML-003"
        or family not in evaluation_receipt.result["qualified_families"]
    ):
        raise MLProducerError("ML-004 requires a qualified ML-003 family")
    if (
        labels.shape != probabilities.shape
        or bins < 2
        or not 0.5 <= minimum_confidence < 1
    ):
        raise MLProducerError("invalid calibration inputs")
    edges = np.linspace(0, 1, bins + 1)
    reliability, ece = [], 0.0
    for lower, upper in pairwise(edges):
        mask = (probabilities >= lower) & (
            probabilities <= upper if upper == 1 else probabilities < upper
        )
        if not np.any(mask):
            continue
        confidence, observed = (
            float(np.mean(probabilities[mask])),
            float(np.mean(labels[mask])),
        )
        weight = float(np.mean(mask))
        ece += weight * abs(confidence - observed)
        reliability.append(
            {
                "lower": float(lower),
                "upper": float(upper),
                "count": int(np.sum(mask)),
                "confidence": confidence,
                "observed": observed,
            }
        )
    abstain = (probabilities < minimum_confidence) & (
        probabilities > 1 - minimum_confidence
    )
    result = {
        "schema_version": "ml004-calibration-abstention-v1.0.0",
        "evaluation_receipt_digest": evaluation_receipt.receipt_digest,
        "family": family,
        "ece": round(ece, 12),
        "maximum_ece": maximum_ece,
        "reliability": reliability,
        "abstention_threshold": minimum_confidence,
        "abstention_fraction": float(np.mean(abstain)),
        "qualified": ece <= maximum_ece,
    }
    return build_receipt(
        milestone="ML-004",
        producer="bt.institutional.ml.calibration_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={"labels": labels.tolist(), "probabilities": probabilities.tolist()},
        dataset_digest=dataset_digest,
        configuration={
            "bins": bins,
            "minimum_confidence": minimum_confidence,
            "maximum_ece": maximum_ece,
        },
        artifacts=result,
        result=result,
    )
