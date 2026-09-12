"""Point-in-time execution-quality calibration with conservative fallbacks."""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import UTC, datetime
from statistics import fmean, stdev
from typing import Any, Iterable

from bt.execution.model_registry import (
    CalibrationProvenance,
    MarketModelBundle,
    MarketModelCard,
    assert_pessimistic_cost_order,
    validate_model_bundle_document,
)

from .receipt import ProducerReceipt, build_receipt, digest, verify_receipt

CALIBRATION_SCHEMA_VERSION = "exec005-execution-calibration-v1.0.0"
CALIBRATION_SPECIFICATION = {
    "schema_version": CALIBRATION_SCHEMA_VERSION,
    "observation_identity": "observation_id with immutable observation_digest",
    "timestamp_semantics": "only records available_at <= known_at are admissible",
    "sample_roles": ["calibration", "holdout"],
    "strata": ["venue_id", "order_type", "size_bucket", "regime"],
    "metrics": [
        "fill_rate",
        "partial_fill_rate",
        "censoring_rate",
        "ack_latency_ms",
        "fill_latency_ms",
        "implementation_shortfall_bps",
        "model_error_bps",
        "adverse_selection_bps",
    ],
    "unsupported_action": "pessimistic_fallback",
    "authority": {"allocation": False, "capital": False, "orders": False, "promotion": False},
}


class ExecutionCalibrationError(ValueError):
    """Execution observations cannot support a truthful calibration."""


def _time(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ExecutionCalibrationError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExecutionCalibrationError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _number(value: Any, field: str, *, positive: bool = False) -> float:
    if isinstance(value, bool):
        raise ExecutionCalibrationError(f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ExecutionCalibrationError(f"{field} must be numeric") from exc
    if not math.isfinite(result) or result < 0 or (positive and result <= 0):
        qualifier = "positive" if positive else "non-negative"
        raise ExecutionCalibrationError(f"{field} must be {qualifier} and finite")
    return result


def _quantile(values: list[float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def _mean_interval(values: list[float]) -> dict[str, float] | None:
    if not values:
        return None
    mean = fmean(values)
    half_width = 0.0 if len(values) == 1 else 1.96 * stdev(values) / math.sqrt(len(values))
    return {"estimate": mean, "lower": mean - half_width, "upper": mean + half_width}


def _wilson(successes: int, total: int) -> dict[str, float] | None:
    if total == 0:
        return None
    z = 1.96
    p = successes / total
    denominator = 1 + z * z / total
    centre = (p + z * z / (2 * total)) / denominator
    radius = z * math.sqrt((p * (1 - p) + z * z / (4 * total)) / total) / denominator
    return {"estimate": p, "lower": max(0.0, centre - radius), "upper": min(1.0, centre + radius)}


def _observation(raw: dict[str, Any], *, known_at: datetime) -> dict[str, Any] | None:
    required = {
        "observation_id",
        "venue_id",
        "listing_id",
        "order_type",
        "side",
        "size_bucket",
        "regime",
        "sample_role",
        "submitted_at",
        "available_at",
        "arrival_mid",
        "modeled_fill_price",
        "quantity",
        "filled_quantity",
        "source_event_digest",
    }
    missing = sorted(field for field in required if raw.get(field) in (None, ""))
    if missing:
        raise ExecutionCalibrationError(f"observation is missing {', '.join(missing)}")
    if raw["side"] not in {"buy", "sell"}:
        raise ExecutionCalibrationError("side must be buy or sell")
    if raw["sample_role"] not in {"calibration", "holdout"}:
        raise ExecutionCalibrationError("sample_role must be calibration or holdout")
    source_digest = str(raw["source_event_digest"])
    if len(source_digest) != 64 or any(char not in "0123456789abcdef" for char in source_digest):
        raise ExecutionCalibrationError("source_event_digest must be lowercase sha256")
    submitted = _time(raw["submitted_at"], "submitted_at")
    available = _time(raw["available_at"], "available_at")
    if available > known_at:
        return None
    acknowledged = _time(raw["acknowledged_at"], "acknowledged_at") if raw.get("acknowledged_at") else None
    first_fill = _time(raw["first_fill_at"], "first_fill_at") if raw.get("first_fill_at") else None
    terminal = _time(raw["terminal_at"], "terminal_at") if raw.get("terminal_at") else None
    times = [value for value in (acknowledged, first_fill, terminal, available) if value is not None]
    if any(value < submitted for value in times):
        raise ExecutionCalibrationError("observation timestamps precede submission")
    arrival = _number(raw["arrival_mid"], "arrival_mid", positive=True)
    modeled = _number(raw["modeled_fill_price"], "modeled_fill_price", positive=True)
    quantity = _number(raw["quantity"], "quantity", positive=True)
    filled = _number(raw["filled_quantity"], "filled_quantity")
    if filled > quantity:
        raise ExecutionCalibrationError("filled quantity exceeds requested quantity")
    observed = _number(raw["observed_fill_price"], "observed_fill_price", positive=True) if raw.get("observed_fill_price") is not None else None
    future_mid = _number(raw["future_mid"], "future_mid", positive=True) if raw.get("future_mid") is not None else None
    if filled > 0 and (observed is None or first_fill is None):
        raise ExecutionCalibrationError("a fill requires observed price and first_fill_at")
    censored = bool(raw.get("censored", False))
    if censored and terminal is not None:
        raise ExecutionCalibrationError("censored observation cannot declare terminal_at")
    normalized = {
        "observation_id": str(raw["observation_id"]),
        "venue_id": str(raw["venue_id"]),
        "listing_id": str(raw["listing_id"]),
        "order_type": str(raw["order_type"]),
        "side": str(raw["side"]),
        "size_bucket": str(raw["size_bucket"]),
        "regime": str(raw["regime"]),
        "sample_role": str(raw["sample_role"]),
        "submitted_at": submitted.isoformat(),
        "acknowledged_at": acknowledged.isoformat() if acknowledged else None,
        "first_fill_at": first_fill.isoformat() if first_fill else None,
        "terminal_at": terminal.isoformat() if terminal else None,
        "available_at": available.isoformat(),
        "arrival_mid": arrival,
        "modeled_fill_price": modeled,
        "observed_fill_price": observed,
        "future_mid": future_mid,
        "quantity": quantity,
        "filled_quantity": filled,
        "censored": censored,
        "source_event_digest": source_digest,
    }
    normalized["observation_digest"] = digest(normalized)
    return normalized


def _measure(item: dict[str, Any]) -> dict[str, float | None]:
    submitted = _time(item["submitted_at"], "submitted_at")
    acknowledged = _time(item["acknowledged_at"], "acknowledged_at") if item["acknowledged_at"] else None
    first_fill = _time(item["first_fill_at"], "first_fill_at") if item["first_fill_at"] else None
    sign = 1.0 if item["side"] == "buy" else -1.0
    arrival = item["arrival_mid"]
    modeled_shortfall = sign * (item["modeled_fill_price"] - arrival) / arrival * 10_000
    observed = item["observed_fill_price"]
    shortfall = sign * (observed - arrival) / arrival * 10_000 if observed is not None else None
    adverse = (
        sign * (observed - item["future_mid"]) / arrival * 10_000
        if observed is not None and item["future_mid"] is not None
        else None
    )
    return {
        "fill_fraction": item["filled_quantity"] / item["quantity"],
        "ack_latency_ms": (acknowledged - submitted).total_seconds() * 1000 if acknowledged else None,
        "fill_latency_ms": (first_fill - submitted).total_seconds() * 1000 if first_fill else None,
        "implementation_shortfall_bps": shortfall,
        "model_error_bps": shortfall - modeled_shortfall if shortfall is not None else None,
        "adverse_selection_bps": adverse,
    }


def _summary(items: list[dict[str, Any]]) -> dict[str, Any]:
    measures = [_measure(item) for item in items]
    filled = sum(measure["fill_fraction"] > 0 for measure in measures)
    partial = sum(0 < measure["fill_fraction"] < 1 for measure in measures)
    censored = sum(item["censored"] for item in items)
    result: dict[str, Any] = {
        "observations": len(items),
        "fill_rate": _wilson(filled, len(items)),
        "partial_fill_rate": _wilson(partial, len(items)),
        "censoring_rate": _wilson(censored, len(items)),
    }
    for name in (
        "ack_latency_ms",
        "fill_latency_ms",
        "implementation_shortfall_bps",
        "model_error_bps",
        "adverse_selection_bps",
    ):
        values = [float(measure[name]) for measure in measures if measure[name] is not None]
        result[name] = {
            "mean_interval_95": _mean_interval(values),
            "p50": _quantile(values, 0.5),
            "p95": _quantile(values, 0.95),
            "observations": len(values),
        }
    return result


def calibrate_execution_quality(
    *,
    observations: Iterable[dict[str, Any]],
    known_at: datetime,
    configuration: dict[str, Any],
) -> dict[str, Any]:
    """Calibrate execution observations without using future-available records."""

    known = _time(known_at, "known_at")
    normalized: list[dict[str, Any]] = []
    identities: dict[str, str] = {}
    future_excluded = 0
    for raw in observations:
        item = _observation(raw, known_at=known)
        if item is None:
            future_excluded += 1
            continue
        prior = identities.get(item["observation_id"])
        if prior:
            if prior != item["observation_digest"]:
                raise ExecutionCalibrationError("observation identity was reused with different content")
            continue
        identities[item["observation_id"]] = item["observation_digest"]
        normalized.append(item)
    if not normalized:
        raise ExecutionCalibrationError("no point-in-time observations are available")
    roles = {item["sample_role"] for item in normalized}
    if roles != {"calibration", "holdout"}:
        raise ExecutionCalibrationError("disjoint calibration and holdout samples are required")
    calibration_ids = {item["observation_id"] for item in normalized if item["sample_role"] == "calibration"}
    holdout_ids = {item["observation_id"] for item in normalized if item["sample_role"] == "holdout"}
    if calibration_ids & holdout_ids:
        raise ExecutionCalibrationError("calibration and holdout identities overlap")

    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for item in normalized:
        grouped[(item["venue_id"], item["order_type"], item["size_bucket"], item["regime"])].append(item)
    minimum = int(configuration.get("minimum_observations_per_role", 20))
    maximum_censoring = float(configuration.get("maximum_censoring_rate", 0.25))
    maximum_shift = float(configuration.get("maximum_holdout_cost_shift_bps", 5.0))
    declared_fallback = _number(configuration.get("declared_pessimistic_cost_bps", 10.0), "declared_pessimistic_cost_bps")
    strata = []
    for key, items in sorted(grouped.items()):
        calibration = [item for item in items if item["sample_role"] == "calibration"]
        holdout = [item for item in items if item["sample_role"] == "holdout"]
        calibration_summary = _summary(calibration)
        holdout_summary = _summary(holdout)
        calibration_cost = calibration_summary["implementation_shortfall_bps"]["mean_interval_95"]
        holdout_cost = holdout_summary["implementation_shortfall_bps"]["mean_interval_95"]
        cost_shift = (
            abs(holdout_cost["estimate"] - calibration_cost["estimate"])
            if calibration_cost and holdout_cost
            else None
        )
        censoring = holdout_summary["censoring_rate"]["estimate"] if holdout_summary["censoring_rate"] else 1.0
        reasons = []
        if len(calibration) < minimum or len(holdout) < minimum:
            reasons.append("sparse_sample")
        if censoring > maximum_censoring:
            reasons.append("excessive_censoring")
        if cost_shift is None:
            reasons.append("cost_evidence_unavailable")
        elif cost_shift > maximum_shift:
            reasons.append("holdout_regime_shift")
        empirical_upper = max(
            [
                value
                for value in (
                    calibration_summary["implementation_shortfall_bps"]["p95"],
                    holdout_summary["implementation_shortfall_bps"]["p95"],
                    holdout_cost["upper"] if holdout_cost else None,
                )
                if value is not None
            ],
            default=declared_fallback,
        )
        fallback = max(declared_fallback, empirical_upper, 0.0)
        assert_pessimistic_cost_order(baseline_cost=max(empirical_upper, 0.0), stressed_cost=fallback)
        strata.append(
            {
                "venue_id": key[0],
                "order_type": key[1],
                "size_bucket": key[2],
                "regime": key[3],
                "calibration": calibration_summary,
                "holdout": holdout_summary,
                "holdout_cost_shift_bps": cost_shift,
                "qualified": not reasons,
                "reasons": reasons,
                "pessimistic_cost_bps": fallback,
                "observation_digests": sorted(item["observation_digest"] for item in items),
            }
        )
    qualified = bool(strata) and all(stratum["qualified"] for stratum in strata)
    result = {
        "schema_version": CALIBRATION_SCHEMA_VERSION,
        "known_at": known.isoformat(),
        "observations": len(normalized),
        "future_observations_excluded": future_excluded,
        "strata": strata,
        "qualified": qualified,
        "decision": "use_empirical_calibration" if qualified else "use_pessimistic_fallback",
        "authority": CALIBRATION_SPECIFICATION["authority"],
    }
    result["calibration_digest"] = digest(result)
    return result


def _empirical_bundle(
    calibration: dict[str, Any], *, dataset_digest: str, sample_start: str, sample_end: str
) -> dict[str, Any]:
    strata = calibration["strata"]
    supported = calibration["qualified"]
    fallback_cost = max(stratum["pessimistic_cost_bps"] for stratum in strata)
    holdout_count = sum(stratum["holdout"]["observations"] for stratum in strata)
    fill_values = [stratum["holdout"]["fill_rate"]["estimate"] for stratum in strata]
    error_values = [
        stratum["holdout"]["model_error_bps"]["mean_interval_95"]["estimate"]
        for stratum in strata
        if stratum["holdout"]["model_error_bps"]["mean_interval_95"]
    ]
    provenance = CalibrationProvenance(
        source="empirical",
        dataset_digest=dataset_digest,
        sample_start=sample_start,
        sample_end=sample_end,
        method="point-in-time stratified calibration with disjoint holdout",
        fit_diagnostics={"strata": float(len(strata))},
        holdout_diagnostics={
            "observations": float(holdout_count),
            "mean_fill_rate": fmean(fill_values),
            "mean_absolute_model_error_bps": fmean(abs(value) for value in error_values) if error_values else fallback_cost,
        },
    )

    def card(kind: str, model_id: str, parameter: str, value: float) -> MarketModelCard:
        return MarketModelCard(
            model_id=model_id,
            version="1.0.0",
            kind=kind,  # type: ignore[arg-type]
            support_status="supported" if supported else "unavailable",
            implementation="bt.institutional.execution_calibration.calibrate_execution_quality" if supported else None,
            applicability={
                "venues": tuple(sorted({stratum["venue_id"] for stratum in strata})),
                "order_types": tuple(sorted({stratum["order_type"] for stratum in strata})),
                "size_buckets": tuple(sorted({stratum["size_bucket"] for stratum in strata})),
                "regimes": tuple(sorted({stratum["regime"] for stratum in strata})),
            },
            timestamp_semantics="observations available by known_at; disjoint holdout",
            parameters={parameter: value},
            uncertainty={"pessimistic_cost_bps": fallback_cost},
            stress_ranges={parameter: (value, max(value, fallback_cost))},
            calibration=provenance,
            fallback=f"use {fallback_cost:.12g} bps pessimistic cost and abstain from unsupported execution claims",
            unsupported_reason=None if supported else "calibration qualification failed; pessimistic fallback required",
        )

    fill_rate = min(fill_values)
    latency = max(
        (
            stratum["holdout"]["fill_latency_ms"]["p95"] or 0.0
            for stratum in strata
        ),
        default=0.0,
    )
    bundle = MarketModelBundle(
        name="empirical-execution-quality",
        version="1.0.0",
        models=(
            card("fill", "empirical-fill-rate", "minimum_fill_rate", fill_rate),
            card("queue_latency", "empirical-fill-latency", "p95_latency_ms", latency),
            card("slippage_impact", "empirical-shortfall", "pessimistic_cost_bps", fallback_cost),
        ),
    )
    return bundle.document()


def execution_calibration_receipt(
    *,
    exec001_receipt: dict[str, Any],
    exec002_receipt: dict[str, Any],
    exec004_receipt: dict[str, Any],
    shadow001_receipt: dict[str, Any],
    market_model_bundle: dict[str, Any],
    observations: Iterable[dict[str, Any]],
    known_at: datetime,
    source_commit: str,
    dataset_digest: str,
    configuration: dict[str, Any],
) -> ProducerReceipt:
    dependencies = (
        (exec001_receipt, "EXEC-001"),
        (exec002_receipt, "EXEC-002"),
        (exec004_receipt, "EXEC-004"),
        (shadow001_receipt, "SHADOW-001"),
    )
    for receipt, milestone in dependencies:
        if not verify_receipt(receipt) or receipt["milestone"] != milestone:
            raise ExecutionCalibrationError(f"{milestone} receipt is not admissible")
    validate_model_bundle_document(market_model_bundle)
    observation_list = list(observations)
    calibration = calibrate_execution_quality(
        observations=observation_list, known_at=known_at, configuration=configuration
    )
    admitted = [
        _observation(item, known_at=_time(known_at, "known_at"))
        for item in observation_list
    ]
    admitted = [item for item in admitted if item is not None]
    sample_start = min(item["submitted_at"] for item in admitted)
    sample_end = max(item["available_at"] for item in admitted)
    empirical_bundle = _empirical_bundle(
        calibration,
        dataset_digest=dataset_digest,
        sample_start=sample_start,
        sample_end=sample_end,
    )
    result = {
        "schema_version": CALIBRATION_SCHEMA_VERSION,
        "calibration_schema_digest": digest(CALIBRATION_SPECIFICATION),
        "calibration": calibration,
        "empirical_market_model_bundle": empirical_bundle,
        "prior_market_model_bundle_digest": market_model_bundle["bundle_digest"],
        "dependency_receipts": {
            milestone.lower().replace("-", ""): receipt["receipt_digest"]
            for receipt, milestone in dependencies
        },
        "qualified": calibration["qualified"],
        "claim": "execution-quality calibration evidence only; no order submission, allocation, promotion, routing, or capital authority",
    }
    return build_receipt(
        milestone="EXEC-005",
        producer="bt.institutional.execution_calibration.execution_calibration_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={"observations": observation_list, "market_model_bundle": market_model_bundle},
        dataset_digest=dataset_digest,
        configuration=configuration,
        artifacts={
            "calibration_digest": calibration["calibration_digest"],
            "empirical_bundle_digest": empirical_bundle["bundle_digest"],
        },
        result=result,
    )
