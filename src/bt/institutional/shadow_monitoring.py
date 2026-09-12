"""SHADOW-002 prospective candidate monitoring and re-evaluation producer."""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from typing import Any, Iterable

from .receipt import ProducerReceipt, build_receipt, digest

SHADOW_MONITORING_SCHEMA_VERSION = "shadow002-prospective-monitoring-v1.0.0"
SHADOW_MONITORING_SPECIFICATION = {
    "schema_version": SHADOW_MONITORING_SCHEMA_VERSION,
    "dependencies": ["SHADOW-001", "PORT-001", "GOV-003", "PLAT-005"],
    "clock": "only observations available_at <= known_at may affect a decision",
    "failure_policy": "freeze and require independent review; never silently reactivate",
    "controls": [
        "sealed_prospective_journal",
        "data_and_service_freshness",
        "performance_and_cost_drift",
        "consecutive_breach_demotion",
        "false_recovery_resistance",
        "scheduled_re_evaluation",
    ],
    "authority": {
        "allocation": False,
        "capital": False,
        "orders": False,
        "promotion": False,
    },
}


class ShadowMonitoringError(ValueError):
    """Monitoring evidence is malformed or cannot support a truthful assessment."""


def _time(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ShadowMonitoringError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ShadowMonitoringError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _number(
    value: Any, field: str, *, minimum: float = 0, maximum: float | None = None
) -> float:
    if isinstance(value, bool):
        raise ShadowMonitoringError(f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ShadowMonitoringError(f"{field} must be numeric") from exc
    if (
        not math.isfinite(result)
        or result < minimum
        or (maximum is not None and result > maximum)
    ):
        raise ShadowMonitoringError(f"{field} is outside its declared bounds")
    return result


def _positive_integer(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ShadowMonitoringError(f"{field} must be a positive integer")
    return value


def _configuration(raw: dict[str, Any]) -> dict[str, Any]:
    result = {
        "observation_expiry_seconds": _number(
            raw.get("observation_expiry_seconds"),
            "observation_expiry_seconds",
            minimum=1,
        ),
        "journal_expiry_seconds": _number(
            raw.get("journal_expiry_seconds"), "journal_expiry_seconds", minimum=1
        ),
        "review_interval_seconds": _number(
            raw.get("review_interval_seconds"), "review_interval_seconds", minimum=1
        ),
        "maximum_data_lag_seconds": _number(
            raw.get("maximum_data_lag_seconds"), "maximum_data_lag_seconds"
        ),
        "maximum_prediction_drift": _number(
            raw.get("maximum_prediction_drift"), "maximum_prediction_drift"
        ),
        "maximum_cost_drift_bps": _number(
            raw.get("maximum_cost_drift_bps"), "maximum_cost_drift_bps"
        ),
        "maximum_drawdown_fraction": _number(
            raw.get("maximum_drawdown_fraction"), "maximum_drawdown_fraction", maximum=1
        ),
        "demotion_after_consecutive_breaches": _positive_integer(
            raw.get("demotion_after_consecutive_breaches"),
            "demotion_after_consecutive_breaches",
        ),
        "recovery_observations": _positive_integer(
            raw.get("recovery_observations"), "recovery_observations"
        ),
    }
    return result


def _validate_journal(
    replay: dict[str, Any],
    candidate_digest: str,
    known_at: datetime,
    config: dict[str, Any],
) -> None:
    if not replay.get("success") or not replay.get("sealed"):
        raise ShadowMonitoringError(
            "SHADOW-001 journal must be sealed and replay-valid"
        )
    if replay.get("capital_or_order_authority") is not False:
        raise ShadowMonitoringError("SHADOW-001 journal authority boundary is invalid")
    bindings = replay.get("bindings") or {}
    if bindings.get("candidate_digest") != candidate_digest:
        raise ShadowMonitoringError("SHADOW-001 candidate binding does not match")
    sealed_at = _time(replay.get("sealed_at"), "journal.sealed_at")
    if sealed_at > known_at:
        raise ShadowMonitoringError("SHADOW-001 journal is future evidence")
    if (known_at - sealed_at).total_seconds() > config["journal_expiry_seconds"]:
        raise ShadowMonitoringError("SHADOW-001 journal is expired")


def _observation(raw: dict[str, Any], known_at: datetime) -> dict[str, Any] | None:
    required = {
        "observation_id",
        "observed_at",
        "available_at",
        "source_digest",
        "data_lag_seconds",
        "service_available",
        "prediction_drift",
        "cost_drift_bps",
        "drawdown_fraction",
    }
    missing = sorted(key for key in required if raw.get(key) in (None, ""))
    if missing:
        raise ShadowMonitoringError(f"observation is missing {', '.join(missing)}")
    observed = _time(raw["observed_at"], "observed_at")
    available = _time(raw["available_at"], "available_at")
    if available < observed:
        raise ShadowMonitoringError("available_at precedes observed_at")
    if available > known_at:
        return None
    source_digest = str(raw["source_digest"])
    if len(source_digest) != 64 or any(
        char not in "0123456789abcdef" for char in source_digest
    ):
        raise ShadowMonitoringError("source_digest must be lowercase sha256")
    if not isinstance(raw["service_available"], bool):
        raise ShadowMonitoringError("service_available must be boolean")
    item = {
        "observation_id": str(raw["observation_id"]),
        "observed_at": observed.isoformat(),
        "available_at": available.isoformat(),
        "source_digest": source_digest,
        "data_lag_seconds": _number(raw["data_lag_seconds"], "data_lag_seconds"),
        "service_available": raw["service_available"],
        "prediction_drift": _number(raw["prediction_drift"], "prediction_drift"),
        "cost_drift_bps": _number(raw["cost_drift_bps"], "cost_drift_bps"),
        "drawdown_fraction": _number(
            raw["drawdown_fraction"], "drawdown_fraction", maximum=1
        ),
    }
    item["observation_digest"] = digest(item)
    return item


def monitor_shadow_candidate(
    *,
    candidate_digest: str,
    shadow001_replay: dict[str, Any],
    observations: Iterable[dict[str, Any]],
    known_at: datetime,
    prior_status: str,
    last_reviewed_at: datetime,
    configuration: dict[str, Any],
) -> dict[str, Any]:
    """Evaluate prospective evidence without promotion, capital or order authority."""

    if len(candidate_digest) != 64 or any(
        char not in "0123456789abcdef" for char in candidate_digest
    ):
        raise ShadowMonitoringError("candidate_digest must be lowercase sha256")
    if prior_status not in {"monitoring", "frozen", "demotion_pending"}:
        raise ShadowMonitoringError("prior_status is invalid")
    known = _time(known_at, "known_at")
    reviewed = _time(last_reviewed_at, "last_reviewed_at")
    if reviewed > known:
        raise ShadowMonitoringError("last_reviewed_at is future evidence")
    config = _configuration(configuration)
    _validate_journal(shadow001_replay, candidate_digest, known, config)

    accepted: list[dict[str, Any]] = []
    identities: dict[str, str] = {}
    future_excluded = 0
    for raw in observations:
        item = _observation(raw, known)
        if item is None:
            future_excluded += 1
            continue
        prior_digest = identities.get(item["observation_id"])
        if prior_digest and prior_digest != item["observation_digest"]:
            raise ShadowMonitoringError(
                "observation identity was reused with different content"
            )
        identities[item["observation_id"]] = item["observation_digest"]
        accepted.append(item)
    accepted.sort(key=lambda item: (item["available_at"], item["observation_id"]))

    consecutive_breaches = 0
    maximum_breach_streak = 0
    healthy_streak = 0
    observed_breach = False
    assessed: list[dict[str, Any]] = []
    for item in accepted:
        breaches = []
        if not item["service_available"]:
            breaches.append("service_outage")
        if item["data_lag_seconds"] > config["maximum_data_lag_seconds"]:
            breaches.append("data_lag")
        if item["prediction_drift"] > config["maximum_prediction_drift"]:
            breaches.append("prediction_drift")
        if item["cost_drift_bps"] > config["maximum_cost_drift_bps"]:
            breaches.append("cost_drift")
        if item["drawdown_fraction"] > config["maximum_drawdown_fraction"]:
            breaches.append("drawdown")
        consecutive_breaches = consecutive_breaches + 1 if breaches else 0
        maximum_breach_streak = max(maximum_breach_streak, consecutive_breaches)
        healthy_streak = healthy_streak + 1 if not breaches else 0
        observed_breach = observed_breach or bool(breaches)
        assessed.append({**item, "breaches": breaches})

    failures: list[str] = []
    if not assessed:
        failures.append("observations_unavailable")
    else:
        age = (
            known - _time(assessed[-1]["observed_at"], "observed_at")
        ).total_seconds()
        if age < 0 or age > config["observation_expiry_seconds"]:
            failures.append("observation_expired")
        failures.extend(assessed[-1]["breaches"])
    review_due_at = reviewed + timedelta(seconds=config["review_interval_seconds"])
    review_due = known >= review_due_at

    if maximum_breach_streak >= config["demotion_after_consecutive_breaches"]:
        status, action = "demotion_pending", "demotion_review_required"
    elif failures:
        status, action = "frozen", "freeze_and_review"
    elif prior_status != "monitoring" or observed_breach:
        status, action = "frozen", "independent_reactivation_review_required"
        if healthy_streak < config["recovery_observations"]:
            failures.append("recovery_streak_incomplete")
    elif review_due:
        status, action = "monitoring", "scheduled_re_evaluation_required"
    else:
        status, action = "monitoring", "continue_monitoring"

    result = {
        "schema_version": SHADOW_MONITORING_SCHEMA_VERSION,
        "candidate_digest": candidate_digest,
        "known_at": known.isoformat(),
        "prior_status": prior_status,
        "status": status,
        "recommended_action": action,
        "review_due": review_due,
        "review_due_at": review_due_at.isoformat(),
        "next_review_at": (
            known + timedelta(seconds=config["review_interval_seconds"])
        ).isoformat(),
        "future_observations_excluded": future_excluded,
        "observation_count": len(assessed),
        "consecutive_breaches": consecutive_breaches,
        "maximum_breach_streak": maximum_breach_streak,
        "healthy_streak": healthy_streak,
        "failures": sorted(set(failures)),
        "observations": assessed,
        "qualified_for_continued_shadow": status == "monitoring" and not failures,
        "automatic_reactivation": False,
        "authority": SHADOW_MONITORING_SPECIFICATION["authority"],
    }
    result["monitoring_digest"] = digest(result)
    return result


def shadow_monitoring_receipt(
    *,
    candidate_digest: str,
    shadow001_replay: dict[str, Any],
    observations: Iterable[dict[str, Any]],
    known_at: datetime,
    prior_status: str,
    last_reviewed_at: datetime,
    configuration: dict[str, Any],
    dataset_digest: str,
    source_commit: str,
) -> ProducerReceipt:
    values = list(observations)
    result = monitor_shadow_candidate(
        candidate_digest=candidate_digest,
        shadow001_replay=shadow001_replay,
        observations=values,
        known_at=known_at,
        prior_status=prior_status,
        last_reviewed_at=last_reviewed_at,
        configuration=configuration,
    )
    result["shadow_monitoring_schema_digest"] = digest(SHADOW_MONITORING_SPECIFICATION)
    result["shadow001_journal_digest"] = shadow001_replay["journal_digest"]
    result["claim"] = (
        "prospective shadow monitoring evidence only; lifecycle changes require independent authority"
    )
    return build_receipt(
        milestone="SHADOW-002",
        producer="bt.institutional.shadow_monitoring.shadow_monitoring_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={"shadow001_replay": shadow001_replay, "observations": values},
        dataset_digest=dataset_digest,
        configuration={
            "known_at": _time(known_at, "known_at").isoformat(),
            "prior_status": prior_status,
            "last_reviewed_at": _time(last_reviewed_at, "last_reviewed_at").isoformat(),
            **configuration,
        },
        artifacts={
            "monitoring_digest": result["monitoring_digest"],
            "observations": result["observations"],
        },
        result=result,
    )
