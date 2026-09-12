"""EXEC-009 venue and strategy execution-degradation feedback producer."""

from __future__ import annotations

import math
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

from .receipt import ProducerReceipt, build_receipt, digest, is_sha256, verify_receipt

EXECUTION_DEGRADATION_SCHEMA_VERSION = "exec009-execution-degradation-v1.0.0"
METRICS = {
    "fill_rate": ("minimum", "venue"),
    "ack_latency_ms": ("maximum", "infrastructure"),
    "fill_latency_ms": ("maximum", "infrastructure"),
    "implementation_shortfall_bps": ("maximum", "strategy"),
    "adverse_selection_bps": ("maximum", "strategy"),
    "queue_model_error_bps": ("maximum", "model"),
    "reject_rate": ("maximum", "venue"),
    "reconciliation_breaks": ("maximum", "infrastructure"),
}
EXECUTION_DEGRADATION_SPECIFICATION = {
    "schema_version": EXECUTION_DEGRADATION_SCHEMA_VERSION,
    "dependencies": ["EXEC-005", "EXEC-008", "SHADOW-002", "GOV-003", "PLAT-005"],
    "downstream_authority": ["CAD-001", "GOV-003"],
    "strata": [
        "venue",
        "environment",
        "strategy_id",
        "listing_id",
        "order_type",
        "size_bucket",
        "regime",
    ],
    "diagnoses": ["venue", "model", "strategy", "infrastructure"],
    "responses": [
        "continue_monitoring",
        "observe_and_recalibrate",
        "route_restriction_review",
        "shadow_fallback_review",
        "demotion_review",
        "freeze_and_kill_review",
        "independent_restore_review",
    ],
    "controls": [
        "point_in_time_evidence",
        "immutable_incident_digests",
        "consecutive_breach_confirmation",
        "critical_failure_immediate_review",
        "no_automatic_reactivation",
        "no_self_modification",
        "no_order_or_capital_authority",
    ],
    "authority": {
        "allocation": False,
        "capital": False,
        "orders": False,
        "promotion": False,
    },
}


class ExecutionDegradationError(ValueError):
    pass


def _utc(value: str | datetime, field: str) -> datetime:
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ExecutionDegradationError(f"{field} must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExecutionDegradationError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _number(value: Any, field: str, *, maximum: float | None = None) -> float:
    if isinstance(value, bool):
        raise ExecutionDegradationError(f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ExecutionDegradationError(f"{field} must be numeric") from exc
    if (
        not math.isfinite(result)
        or result < 0
        or (maximum is not None and result > maximum)
    ):
        raise ExecutionDegradationError(f"{field} is outside its declared bounds")
    return result


def _dependency(
    value: ProducerReceipt | dict[str, Any], milestone: str
) -> dict[str, Any]:
    item = value.as_dict() if isinstance(value, ProducerReceipt) else dict(value)
    if not verify_receipt(item) or item.get("milestone") != milestone:
        raise ExecutionDegradationError(
            f"EXEC-009 requires an exact {milestone} receipt"
        )
    return item


def _configuration(raw: dict[str, Any]) -> dict[str, Any]:
    thresholds = raw.get("thresholds")
    if not isinstance(thresholds, dict) or set(thresholds) != set(METRICS):
        raise ExecutionDegradationError(
            "thresholds must declare every EXEC-009 metric exactly"
        )
    normalized = {
        name: _number(
            value,
            f"thresholds.{name}",
            maximum=1 if name in {"fill_rate", "reject_rate"} else None,
        )
        for name, value in thresholds.items()
    }
    consecutive = raw.get("consecutive_breaches")
    recovery = raw.get("recovery_observations")
    expiry = raw.get("observation_expiry_seconds")
    if (
        isinstance(consecutive, bool)
        or not isinstance(consecutive, int)
        or consecutive < 2
    ):
        raise ExecutionDegradationError(
            "consecutive_breaches must be an integer of at least two"
        )
    if isinstance(recovery, bool) or not isinstance(recovery, int) or recovery < 2:
        raise ExecutionDegradationError(
            "recovery_observations must be an integer of at least two"
        )
    return {
        "thresholds": normalized,
        "consecutive_breaches": consecutive,
        "recovery_observations": recovery,
        "observation_expiry_seconds": _number(expiry, "observation_expiry_seconds"),
    }


def _observation(raw: dict[str, Any], known_at: datetime) -> dict[str, Any] | None:
    required = {
        "observation_id",
        "venue",
        "environment",
        "strategy_id",
        "listing_id",
        "order_type",
        "size_bucket",
        "regime",
        "observed_at",
        "available_at",
        "source_digest",
        "service_available",
        "venue_rule_current",
        *METRICS,
    }
    missing = sorted(name for name in required if raw.get(name) in (None, ""))
    if missing:
        raise ExecutionDegradationError(f"observation is missing {', '.join(missing)}")
    observed_at = _utc(raw["observed_at"], "observed_at")
    available_at = _utc(raw["available_at"], "available_at")
    if available_at < observed_at:
        raise ExecutionDegradationError("available_at precedes observed_at")
    if available_at > known_at:
        return None
    if raw["environment"] not in {"shadow", "demo", "live"}:
        raise ExecutionDegradationError("environment must be shadow, demo, or live")
    if not isinstance(raw["service_available"], bool) or not isinstance(
        raw["venue_rule_current"], bool
    ):
        raise ExecutionDegradationError(
            "availability and venue-rule flags must be boolean"
        )
    if not is_sha256(raw["source_digest"]):
        raise ExecutionDegradationError("source_digest must be lowercase sha256")
    metrics = {
        name: _number(
            raw[name], name, maximum=1 if name in {"fill_rate", "reject_rate"} else None
        )
        for name in METRICS
    }
    item = {
        "observation_id": str(raw["observation_id"]),
        "venue": str(raw["venue"]).lower(),
        "environment": raw["environment"],
        "strategy_id": str(raw["strategy_id"]),
        "listing_id": str(raw["listing_id"]),
        "order_type": str(raw["order_type"]),
        "size_bucket": str(raw["size_bucket"]),
        "regime": str(raw["regime"]),
        "observed_at": observed_at.isoformat(),
        "available_at": available_at.isoformat(),
        "source_digest": raw["source_digest"],
        "service_available": raw["service_available"],
        "venue_rule_current": raw["venue_rule_current"],
        "metrics": metrics,
    }
    item["observation_digest"] = digest(item)
    return item


def evaluate_execution_degradation(
    *,
    observations: Iterable[dict[str, Any]],
    known_at: datetime,
    prior_status: str,
    candidate_lifecycle: dict[str, Any],
    configuration: dict[str, Any],
) -> dict[str, Any]:
    """Diagnose execution degradation and propose bounded, review-only responses."""

    if prior_status not in {
        "monitoring",
        "restricted",
        "shadow_fallback",
        "demotion_pending",
        "killed",
    }:
        raise ExecutionDegradationError("prior_status is invalid")
    if candidate_lifecycle.get("status") not in {
        "shadow",
        "demo",
        "live",
        "demoted",
        "retired",
    }:
        raise ExecutionDegradationError("candidate lifecycle status is invalid")
    if not is_sha256(candidate_lifecycle.get("record_digest")):
        raise ExecutionDegradationError(
            "candidate lifecycle record_digest must be sha256"
        )
    known = _utc(known_at, "known_at")
    config = _configuration(configuration)
    accepted: list[dict[str, Any]] = []
    identities: dict[str, str] = {}
    future_excluded = 0
    for raw in observations:
        item = _observation(raw, known)
        if item is None:
            future_excluded += 1
            continue
        prior = identities.get(item["observation_id"])
        if prior and prior != item["observation_digest"]:
            raise ExecutionDegradationError(
                "observation identity was reused with different content"
            )
        if prior is None:
            identities[item["observation_id"]] = item["observation_digest"]
            accepted.append(item)
    accepted.sort(key=lambda item: (item["available_at"], item["observation_id"]))

    assessed: list[dict[str, Any]] = []
    breach_streak = healthy_streak = maximum_breach_streak = 0
    all_diagnoses: set[str] = set()
    incidents: list[dict[str, Any]] = []
    for item in accepted:
        breaches: list[str] = []
        diagnoses: set[str] = set()
        for name, value in item["metrics"].items():
            direction, diagnosis = METRICS[name]
            threshold = config["thresholds"][name]
            breached = (
                value < threshold if direction == "minimum" else value > threshold
            )
            if breached:
                breaches.append(name)
                diagnoses.add(diagnosis)
        critical: list[str] = []
        if not item["service_available"]:
            critical.append("service_unavailable")
            diagnoses.add("infrastructure")
        if not item["venue_rule_current"]:
            critical.append("venue_rule_stale")
            diagnoses.add("venue")
        breach_streak = breach_streak + 1 if breaches or critical else 0
        healthy_streak = healthy_streak + 1 if not breaches and not critical else 0
        maximum_breach_streak = max(maximum_breach_streak, breach_streak)
        all_diagnoses.update(diagnoses)
        assessed_item = {
            **item,
            "breaches": sorted(breaches),
            "critical_failures": sorted(critical),
            "diagnoses": sorted(diagnoses),
        }
        assessed.append(assessed_item)
        if breaches or critical:
            incident = {
                "observation_digest": item["observation_digest"],
                "observed_at": item["observed_at"],
                "stratum": {
                    name: item[name]
                    for name in EXECUTION_DEGRADATION_SPECIFICATION["strata"]
                },
                "breaches": sorted(breaches),
                "critical_failures": sorted(critical),
                "diagnoses": sorted(diagnoses),
            }
            incident["incident_digest"] = digest(incident)
            incidents.append(incident)

    latest_age = None
    stale = not assessed
    if assessed:
        latest_age = (
            known - _utc(assessed[-1]["observed_at"], "observed_at")
        ).total_seconds()
        stale = latest_age < 0 or latest_age > config["observation_expiry_seconds"]
    critical = any(item["critical_failures"] for item in assessed)
    confirmed = maximum_breach_streak >= config["consecutive_breaches"]
    current_breaches = bool(
        assessed and (assessed[-1]["breaches"] or assessed[-1]["critical_failures"])
    )
    recovery_evidence_complete = healthy_streak >= config["recovery_observations"]

    if prior_status != "monitoring":
        status, action = prior_status, "independent_restore_review"
    elif stale or critical:
        status, action = (
            "killed" if critical else "restricted",
            "freeze_and_kill_review",
        )
    elif confirmed:
        if "venue" in all_diagnoses or "infrastructure" in all_diagnoses:
            status, action = "restricted", "route_restriction_review"
        elif "strategy" in all_diagnoses:
            status, action = "shadow_fallback", "shadow_fallback_review"
        else:
            status, action = "demotion_pending", "demotion_review"
    elif current_breaches:
        status, action = "monitoring", "observe_and_recalibrate"
    else:
        status, action = "monitoring", "continue_monitoring"

    result = {
        "schema_version": EXECUTION_DEGRADATION_SCHEMA_VERSION,
        "known_at": known.isoformat(),
        "prior_status": prior_status,
        "status": status,
        "recommended_action": action,
        "candidate_lifecycle_digest": candidate_lifecycle["record_digest"],
        "observation_count": len(assessed),
        "future_observations_excluded": future_excluded,
        "latest_observation_age_seconds": latest_age,
        "maximum_breach_streak": maximum_breach_streak,
        "healthy_streak": healthy_streak,
        "recovery_evidence_complete": recovery_evidence_complete,
        "diagnoses": sorted(all_diagnoses),
        "incidents": incidents,
        "observations": assessed,
        "continued_execution_eligible": status == "monitoring"
        and action == "continue_monitoring",
        "automatic_execution_change": False,
        "automatic_reactivation": False,
        "strategy_self_modification": False,
        "authority": EXECUTION_DEGRADATION_SPECIFICATION["authority"],
    }
    result["degradation_digest"] = digest(result)
    return result


def execution_degradation_receipt(
    *,
    exec005_receipt: ProducerReceipt | dict[str, Any],
    exec008_receipt: ProducerReceipt | dict[str, Any],
    shadow002_receipt: ProducerReceipt | dict[str, Any],
    governance_policy_digest: str,
    platform_observability_digest: str,
    candidate_lifecycle: dict[str, Any],
    observations: Iterable[dict[str, Any]],
    known_at: datetime,
    prior_status: str,
    dataset_digest: str,
    source_commit: str,
    configuration: dict[str, Any],
) -> ProducerReceipt:
    dependencies = {
        "EXEC-005": _dependency(exec005_receipt, "EXEC-005"),
        "EXEC-008": _dependency(exec008_receipt, "EXEC-008"),
        "SHADOW-002": _dependency(shadow002_receipt, "SHADOW-002"),
    }
    if any(item["dataset_digest"] != dataset_digest for item in dependencies.values()):
        raise ExecutionDegradationError("dependency dataset digests do not match")
    if not is_sha256(governance_policy_digest):
        raise ExecutionDegradationError("GOV-003 policy digest must be sha256")
    if not is_sha256(platform_observability_digest):
        raise ExecutionDegradationError("PLAT-005 observability digest must be sha256")
    candidate_digest = candidate_lifecycle.get("candidate_digest")
    if not is_sha256(candidate_digest):
        raise ExecutionDegradationError(
            "candidate lifecycle candidate_digest must be sha256"
        )
    if dependencies["SHADOW-002"]["result"].get("candidate_digest") != candidate_digest:
        raise ExecutionDegradationError("SHADOW-002 candidate binding does not match")
    values = list(observations)
    result = evaluate_execution_degradation(
        observations=values,
        known_at=known_at,
        prior_status=prior_status,
        candidate_lifecycle=candidate_lifecycle,
        configuration=configuration,
    )
    result.update(
        {
            "degradation_schema_digest": digest(EXECUTION_DEGRADATION_SPECIFICATION),
            "dependency_receipts": {
                key.lower().replace("-", ""): value["receipt_digest"]
                for key, value in sorted(dependencies.items())
            },
            "governance_policy_digest": governance_policy_digest,
            "platform_observability_digest": platform_observability_digest,
            "claim": "execution degradation evidence and bounded review proposal only; no self-modification, restoration, order, capital, allocation, or promotion authority",
        }
    )
    return build_receipt(
        milestone="EXEC-009",
        producer="bt.institutional.execution_degradation.execution_degradation_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={
            "dependencies": dependencies,
            "candidate_lifecycle": candidate_lifecycle,
            "observations": values,
        },
        dataset_digest=dataset_digest,
        configuration=configuration,
        artifacts={
            "incidents": result["incidents"],
            "degradation_digest": result["degradation_digest"],
        },
        result=result,
    )
