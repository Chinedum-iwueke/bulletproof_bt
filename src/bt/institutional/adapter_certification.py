"""EXEC-008 venue adapter capability and certification evidence."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from .receipt import ProducerReceipt, build_receipt, digest, verify_receipt

ADAPTER_CERTIFICATION_SCHEMA_VERSION = "exec008-adapter-certification-v1.1.0"
REQUIRED_DRILLS = (
    "authentication",
    "server_clock",
    "instrument_precision",
    "order_submission",
    "partial_fill",
    "cancel",
    "amend",
    "venue_rejection",
    "private_stream_reconnect",
    "restart_reconciliation",
    "duplicate_suppression",
    "emergency_kill",
)

CAPABILITY_MATRICES: dict[str, dict[str, Any]] = {
    "binance:perpetual": {
        "venue": "binance",
        "product_type": "perpetual",
        "order_types": ["market", "limit"],
        "time_in_force": ["GTC", "IOC"],
        "cancel": True,
        "amend": True,
        "reduce_only": True,
        "partial_fills": True,
        "precision_source": "GET /fapi/v1/exchangeInfo",
        "clock_source": "server-time-adjusted signed timestamp",
        "private_events": ["order", "fill", "position", "balance"],
        "reconciliation_source": "REST open/all orders, trades, positions, balances",
        "error_contract": "typed transport, authentication, rate-limit, rejection",
    },
    "bybit:perpetual": {
        "venue": "bybit",
        "product_type": "perpetual",
        "order_types": ["market", "limit"],
        "time_in_force": ["GTC", "IOC"],
        "cancel": True,
        "amend": True,
        "reduce_only": True,
        "partial_fills": True,
        "precision_source": "GET /v5/market/instruments-info",
        "clock_source": "venue-time-checked signed timestamp",
        "private_events": ["order", "execution", "position", "wallet"],
        "reconciliation_source": "REST order history, executions, positions, wallet",
        "error_contract": "typed transport, authentication, rate-limit, rejection",
    },
}

ADAPTER_CERTIFICATION_SPECIFICATION = {
    "schema_version": ADAPTER_CERTIFICATION_SCHEMA_VERSION,
    "dependencies": ["EXEC-003", "EXEC-004", "EXEC-007"],
    "capability_matrices": CAPABILITY_MATRICES,
    "required_drills": list(REQUIRED_DRILLS),
    "evidence_classes": {
        "deterministic_conformance": "may prove implementation conformance only",
        "venue_observed": "may certify the named environment until its explicit expiry",
    },
    "dependency_binding": (
        "exact independently versioned producer receipts; each native dataset digest is retained"
    ),
    "admission": {
        "demo": "current venue-observed demo evidence is required",
        "micro_live": "current venue-observed live evidence and a later capital gate are required",
    },
    "revocation": "blocked, expired, mismatched, or revoked evidence fails closed",
    "authority": {
        "allocation": False,
        "capital": False,
        "orders": False,
        "promotion": False,
    },
}


class AdapterCertificationError(ValueError):
    pass


def _utc(value: str | datetime, field: str) -> datetime:
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise AdapterCertificationError(f"{field} must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise AdapterCertificationError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _dependency(
    value: ProducerReceipt | dict[str, Any], milestone: str
) -> dict[str, Any]:
    item = value.as_dict() if isinstance(value, ProducerReceipt) else dict(value)
    if not verify_receipt(item) or item.get("milestone") != milestone:
        raise AdapterCertificationError(
            f"EXEC-008 requires an exact {milestone} receipt"
        )
    return item


def capability_matrix(venue: str, product_type: str) -> dict[str, Any]:
    key = f"{venue.strip().lower()}:{product_type.strip().lower()}"
    try:
        return dict(CAPABILITY_MATRICES[key])
    except KeyError as exc:
        raise AdapterCertificationError(
            f"unsupported adapter capability matrix: {key}"
        ) from exc


def adapter_certification_receipt(
    *,
    venue: str,
    environment: str,
    product_type: str,
    evidence_class: str,
    observed_at: datetime,
    valid_until: datetime,
    endpoint_identity: str,
    drill_results: dict[str, bool],
    dependency_receipts: dict[str, ProducerReceipt | dict[str, Any]],
    dataset_digest: str,
    source_commit: str,
    configuration: dict[str, Any],
) -> ProducerReceipt:
    expected = {"EXEC-003", "EXEC-004", "EXEC-007"}
    if set(dependency_receipts) != expected:
        raise AdapterCertificationError(
            "dependency receipt set is incomplete or unexpected"
        )
    dependencies = {
        key: _dependency(value, key) for key, value in dependency_receipts.items()
    }
    if evidence_class not in {"deterministic_conformance", "venue_observed"}:
        raise AdapterCertificationError("unsupported evidence class")
    environment = environment.strip().lower()
    if environment not in {"demo", "live"}:
        raise AdapterCertificationError("environment must be demo or live")
    observed = _utc(observed_at, "observed_at")
    expiry = _utc(valid_until, "valid_until")
    if expiry <= observed:
        raise AdapterCertificationError("valid_until must follow observed_at")
    matrix = capability_matrix(venue, product_type)
    normalized_drills = {
        name: drill_results.get(name) is True for name in REQUIRED_DRILLS
    }
    drill_blockers = [
        f"drill:{name}" for name, passed in normalized_drills.items() if not passed
    ]
    blockers = list(drill_blockers)
    if evidence_class != "venue_observed":
        blockers.append("venue_observation_required")
    venue_observed = evidence_class == "venue_observed"
    qualified = venue_observed and not blockers
    status = (
        "certified"
        if qualified
        else ("conformance_only" if not drill_blockers else "blocked")
    )
    result = {
        "schema_version": ADAPTER_CERTIFICATION_SCHEMA_VERSION,
        "adapter_certification_schema_digest": digest(
            ADAPTER_CERTIFICATION_SPECIFICATION
        ),
        "venue": matrix["venue"],
        "environment": environment,
        "product_type": matrix["product_type"],
        "capability_matrix": matrix,
        "capability_matrix_digest": digest(matrix),
        "evidence_class": evidence_class,
        "venue_observed": venue_observed,
        "endpoint_identity": endpoint_identity,
        "observed_at": observed.isoformat(),
        "valid_until": expiry.isoformat(),
        "drill_results": normalized_drills,
        "blockers": blockers,
        "status": status,
        "qualified": qualified,
        "demo_execution_eligible": qualified and environment == "demo",
        "micro_live_eligible": False,
        "dependency_receipts": {
            key.lower().replace("-", ""): item["receipt_digest"]
            for key, item in sorted(dependencies.items())
        },
        "dependency_dataset_digests": {
            key.lower().replace("-", ""): item["dataset_digest"]
            for key, item in sorted(dependencies.items())
        },
        "claim": "adapter certification evidence only; no order, capital, allocation, promotion, or profitability authority",
    }
    return build_receipt(
        milestone="EXEC-008",
        producer="bt.institutional.adapter_certification.adapter_certification_receipt",
        producer_version="1.1.0",
        source_commit=source_commit,
        inputs={"dependencies": dependencies, "drills": normalized_drills},
        dataset_digest=dataset_digest,
        configuration=configuration,
        artifacts={"capability_matrix_digest": result["capability_matrix_digest"]},
        result=result,
    )


def require_adapter_certification(
    receipt: ProducerReceipt | dict[str, Any],
    *,
    venue: str,
    environment: str,
    now: datetime,
) -> dict[str, Any]:
    item = _dependency(receipt, "EXEC-008")
    result = item["result"]
    if result.get("venue") != venue or result.get("environment") != environment:
        raise AdapterCertificationError(
            "adapter certification targets another venue or environment"
        )
    if result.get("qualified") is not True or result.get("venue_observed") is not True:
        raise AdapterCertificationError(
            "current venue-observed certification is required"
        )
    if _utc(now, "now") >= _utc(result.get("valid_until"), "valid_until"):
        raise AdapterCertificationError("adapter certification has expired")
    return result
