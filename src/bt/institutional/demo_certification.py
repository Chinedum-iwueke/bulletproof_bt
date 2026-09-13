"""DEMO-001 production-like venue demo certification."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from .adapter_certification import require_adapter_certification
from .receipt import ProducerReceipt, build_receipt, digest, is_sha256, verify_receipt

DEMO_CERTIFICATION_SCHEMA_VERSION = "demo001-production-like-venue-v1.1.0"
VENUE_OBSERVED_DRILLS = (
    "authentication",
    "server_clock",
    "dynamic_instrument_rules",
    "order_submission",
    "fill_observation",
    "cancel",
    "amend",
    "venue_rejection",
    "private_stream_reconnect",
    "restart_reconciliation",
    "position_flattened",
)
IN_ENVIRONMENT_DRILLS = (
    "partial_fill_handling",
    "duplicate_suppression",
    "stale_data_rejection",
    "runtime_kill",
    "incident_retention",
)
REQUIRED_DRILLS = VENUE_OBSERVED_DRILLS + IN_ENVIRONMENT_DRILLS
DEPENDENCIES = ("EXEC-004", "EXEC-005", "EXEC-006", "EXEC-007", "EXEC-008", "RISK-005")

DEMO_CERTIFICATION_SPECIFICATION = {
    "schema_version": DEMO_CERTIFICATION_SCHEMA_VERSION,
    "dependencies": [*DEPENDENCIES, "PLAT-005"],
    "venue": "bybit",
    "environment": "demo",
    "endpoint": "https://api-demo.bybit.com",
    "required_drills": list(REQUIRED_DRILLS),
    "evidence_rules": {
        "venue_observed": list(VENUE_OBSERVED_DRILLS),
        "authenticated_demo_or_venue_observed": list(IN_ENVIRONMENT_DRILLS),
    },
    "credential_boundary": {
        "dedicated_demo_identity": True,
        "secret_values_retained": False,
        "withdrawal_authority": False,
    },
    "terminal_invariant": "no open orders and zero position after every drill",
    "qualification": "all drills pass with permitted provenance and current dependencies",
    "dependency_binding": (
        "exact independently versioned producer receipts; each native dataset digest is retained"
    ),
    "authority": {"allocation": False, "capital": False, "orders": False, "promotion": False},
}


class DemoCertificationError(ValueError):
    pass


def _utc(value: str | datetime, field: str) -> datetime:
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise DemoCertificationError(f"{field} must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise DemoCertificationError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _dependency(value: ProducerReceipt | dict[str, Any], milestone: str) -> dict[str, Any]:
    item = value.as_dict() if isinstance(value, ProducerReceipt) else dict(value)
    if not verify_receipt(item) or item.get("milestone") != milestone:
        raise DemoCertificationError(f"DEMO-001 requires an exact {milestone} receipt")
    return item


def demo_certification_receipt(
    *,
    observed_at: datetime,
    valid_until: datetime,
    endpoint_identity: str,
    credential_fingerprint: str,
    drill_evidence: dict[str, dict[str, Any]],
    dependency_receipts: dict[str, ProducerReceipt | dict[str, Any]],
    platform_observability_digest: str,
    dataset_digest: str,
    source_commit: str,
    configuration: dict[str, Any],
) -> ProducerReceipt:
    if set(dependency_receipts) != set(DEPENDENCIES):
        raise DemoCertificationError("dependency receipt set is incomplete or unexpected")
    dependencies = {name: _dependency(value, name) for name, value in dependency_receipts.items()}
    observed = _utc(observed_at, "observed_at")
    expiry = _utc(valid_until, "valid_until")
    if expiry <= observed:
        raise DemoCertificationError("valid_until must follow observed_at")
    if endpoint_identity != "api-demo.bybit.com":
        raise DemoCertificationError("DEMO-001 is restricted to the Bybit demo endpoint")
    if not is_sha256(credential_fingerprint) or not is_sha256(platform_observability_digest):
        raise DemoCertificationError("credential and observability digests must be sha256")
    require_adapter_certification(
        dependencies["EXEC-008"], venue="bybit", environment="demo", now=observed
    )

    blockers: list[str] = []
    normalized: dict[str, dict[str, Any]] = {}
    for name in REQUIRED_DRILLS:
        evidence = dict(drill_evidence.get(name, {}))
        origin = evidence.get("origin")
        allowed = {"venue_observed"}
        if name in IN_ENVIRONMENT_DRILLS:
            allowed.add("authenticated_demo_replay")
        passed = evidence.get("passed") is True
        evidence_digest = evidence.get("evidence_digest")
        if origin not in allowed:
            blockers.append(f"provenance:{name}")
        if not passed:
            blockers.append(f"drill:{name}")
        if not is_sha256(evidence_digest):
            blockers.append(f"evidence:{name}")
        normalized[name] = {
            "passed": passed,
            "origin": origin,
            "evidence_digest": evidence_digest,
        }

    terminal = configuration.get("terminal_state", {})
    flat = terminal.get("open_orders") == 0 and terminal.get("position_qty") == 0
    if not flat:
        blockers.append("terminal_state_not_flat")
    if configuration.get("withdrawal_authority") is not False:
        blockers.append("credential_withdrawal_boundary_unproven")
    if configuration.get("capital_environment") is not False:
        blockers.append("capital_environment_forbidden")

    qualified = not blockers
    result = {
        "schema_version": DEMO_CERTIFICATION_SCHEMA_VERSION,
        "demo_certification_schema_digest": digest(DEMO_CERTIFICATION_SPECIFICATION),
        "venue": "bybit",
        "environment": "demo",
        "endpoint_identity": endpoint_identity,
        "credential_fingerprint": credential_fingerprint,
        "platform_observability_digest": platform_observability_digest,
        "observed_at": observed.isoformat(),
        "valid_until": expiry.isoformat(),
        "drill_evidence": normalized,
        "terminal_state": terminal,
        "blockers": sorted(set(blockers)),
        "status": "certified" if qualified else "blocked",
        "qualified": qualified,
        "demo_execution_eligible": qualified,
        "micro_live_eligible": False,
        "dependency_receipts": {
            key.lower().replace("-", ""): value["receipt_digest"]
            for key, value in sorted(dependencies.items())
        },
        "dependency_dataset_digests": {
            key.lower().replace("-", ""): value["dataset_digest"]
            for key, value in sorted(dependencies.items())
        },
        "claim": "production-like Bybit demo operations only; no profitability, capital, live-order, allocation, promotion, or scaling authority",
    }
    return build_receipt(
        milestone="DEMO-001",
        producer="bt.institutional.demo_certification.demo_certification_receipt",
        producer_version="1.1.0",
        source_commit=source_commit,
        inputs={"dependencies": dependencies, "drill_evidence": normalized},
        dataset_digest=dataset_digest,
        configuration=configuration,
        artifacts={"platform_observability_digest": platform_observability_digest},
        result=result,
    )


def require_demo_certification(
    receipt: ProducerReceipt | dict[str, Any], *, now: datetime
) -> dict[str, Any]:
    item = _dependency(receipt, "DEMO-001")
    result = item["result"]
    if result.get("qualified") is not True or result.get("demo_execution_eligible") is not True:
        raise DemoCertificationError("current qualified demo certification is required")
    if result.get("venue") != "bybit" or result.get("environment") != "demo":
        raise DemoCertificationError("demo certification targets another venue or environment")
    if _utc(now, "now") >= _utc(result.get("valid_until"), "valid_until"):
        raise DemoCertificationError("demo certification has expired")
    return result
