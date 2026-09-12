"""RISK-004 candidate admission and lifecycle recommendation producer."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from bt.portfolio_engine.candidate_service import validate_portfolio_candidate_dossier

from .receipt import ProducerReceipt, build_receipt, digest, verify_receipt

ADMISSION_SCHEMA_VERSION = "risk004-candidate-admission-v1.0.0"
ADMISSION_SPECIFICATION = {
    "schema_version": ADMISSION_SCHEMA_VERSION,
    "dependencies": ["PORT-001", "PORT-002", "PORT-003", "PORT-004", "RISK-001", "RISK-002", "RISK-003", "SHADOW-002", "GOV-003"],
    "decisions": ["admit", "allocate", "scale", "demote", "retire"],
    "failure_policy": "positive decisions fail closed; defensive decisions remain available",
    "authority": {"allocation": False, "capital": False, "orders": False, "promotion": False},
}


class CandidateAdmissionError(ValueError):
    """Candidate evidence cannot support a governed lifecycle recommendation."""


_QUALIFIERS = {
    "PORT-002": "qualified",
    "PORT-003": "valid",
    "PORT-004": "qualified",
    "RISK-001": "admissible",
    "RISK-002": "allowed",
    "RISK-003": "qualified",
}


def _time(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise CandidateAdmissionError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CandidateAdmissionError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _receipt(value: ProducerReceipt | dict[str, Any], milestone: str) -> dict[str, Any]:
    item = value.as_dict() if isinstance(value, ProducerReceipt) else dict(value)
    if not verify_receipt(item) or item.get("milestone") != milestone:
        raise CandidateAdmissionError(f"RISK-004 requires an exact {milestone} receipt")
    qualifier = _QUALIFIERS.get(milestone)
    if qualifier and item["result"].get(qualifier) is not True:
        raise CandidateAdmissionError(f"{milestone} is not qualified")
    return item


def candidate_admission_receipt(
    *,
    candidate_digest: str,
    port001_dossier: dict[str, Any],
    dependency_receipts: dict[str, ProducerReceipt | dict[str, Any]],
    requested_action: str,
    prior_operations_state: str,
    prior_capital_state: str,
    requested_capital: float,
    requested_risk_fraction: float,
    known_at: datetime,
    evidence_epoch: datetime,
    expires_at: datetime,
    dataset_digest: str,
    source_commit: str,
) -> ProducerReceipt:
    """Assess a lifecycle request; never execute or authorize the recommendation."""
    if requested_action not in {"admit", "allocate", "scale", "demote", "retire"}:
        raise CandidateAdmissionError("requested_action is invalid")
    if len(candidate_digest) != 64 or any(c not in "0123456789abcdef" for c in candidate_digest):
        raise CandidateAdmissionError("candidate_digest must be lowercase sha256")
    validate_portfolio_candidate_dossier(port001_dossier)
    if port001_dossier.get("decision") != "candidate" or candidate_digest not in port001_dossier.get("candidate_digests", []):
        raise CandidateAdmissionError("PORT-001 does not nominate the candidate")
    expected = set(_QUALIFIERS) | {"SHADOW-002"}
    if set(dependency_receipts) != expected:
        raise CandidateAdmissionError("dependency receipt set is incomplete or unexpected")
    receipts = {key: _receipt(value, key) for key, value in dependency_receipts.items()}
    if any(item["dataset_digest"] != dataset_digest for item in receipts.values()):
        raise CandidateAdmissionError("dependency dataset digests do not match")
    shadow = receipts["SHADOW-002"]["result"]
    if shadow.get("candidate_digest") != candidate_digest:
        raise CandidateAdmissionError("SHADOW-002 candidate binding does not match")
    known, epoch, expiry = _time(known_at, "known_at"), _time(evidence_epoch, "evidence_epoch"), _time(expires_at, "expires_at")
    if epoch > known or expiry <= known:
        raise CandidateAdmissionError("evidence epoch or decision expiry is invalid")
    capital = float(requested_capital)
    risk_fraction = float(requested_risk_fraction)
    if capital < 0 or not 0 <= risk_fraction <= 1:
        raise CandidateAdmissionError("requested capital or risk fraction is invalid")

    failures: list[str] = []
    positive = requested_action in {"admit", "allocate", "scale"}
    if positive and not shadow.get("qualified_for_continued_shadow", False):
        failures.append("shadow_not_qualified")
    budget = receipts["RISK-003"]["result"]
    if positive and risk_fraction > float(budget.get("effective_risk_fraction", 0)):
        failures.append("risk_fraction_exceeds_budget")
    if positive and capital > float(budget.get("budget_capital", 0)):
        failures.append("capital_exceeds_budget")
    allowed_prior = {
        "admit": prior_operations_state == "candidate" and prior_capital_state == "no-authority",
        "allocate": prior_operations_state in {"approved", "shadow", "demo"} and prior_capital_state == "eligible",
        "scale": prior_operations_state in {"shadow", "demo", "live"} and prior_capital_state in {"allocated", "reduced"},
        "demote": prior_operations_state in {"approved", "shadow", "demo", "live"},
        "retire": prior_operations_state in {"candidate", "approved", "shadow", "demo", "live", "suspended"},
    }[requested_action]
    if not allowed_prior:
        failures.append("invalid_prior_lifecycle_state")
    if requested_action == "demote" and shadow.get("recommended_action") not in {"freeze_and_review", "demotion_review_required"}:
        failures.append("demotion_not_supported_by_monitoring")
    if requested_action == "retire" and shadow.get("status") not in {"frozen", "demotion_pending"}:
        failures.append("retirement_not_supported_by_monitoring")

    eligible = not failures
    result = {
        "schema_version": ADMISSION_SCHEMA_VERSION,
        "candidate_digest": candidate_digest,
        "requested_action": requested_action,
        "prior_operations_state": prior_operations_state,
        "prior_capital_state": prior_capital_state,
        "requested_capital": capital,
        "requested_risk_fraction": risk_fraction,
        "known_at": known.isoformat(),
        "evidence_epoch": epoch.isoformat(),
        "expires_at": expiry.isoformat(),
        "eligible_for_authority_review": eligible,
        "recommended_action": requested_action if eligible else "reject",
        "failures": sorted(set(failures)),
        "dependency_receipts": {key: item["receipt_digest"] for key, item in sorted(receipts.items())},
        "port001_dossier_digest": port001_dossier["dossier_digest"],
        "admission_schema_digest": digest(ADMISSION_SPECIFICATION),
        "automatic_transition": False,
        "authority": ADMISSION_SPECIFICATION["authority"],
        "claim": "candidate lifecycle recommendation only; independent Hermes authority is required",
    }
    result["admission_digest"] = digest(result)
    return build_receipt(
        milestone="RISK-004",
        producer="bt.institutional.candidate_admission.candidate_admission_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={"port001_dossier": port001_dossier, "dependency_receipts": receipts},
        dataset_digest=dataset_digest,
        configuration={"requested_action": requested_action, "prior_operations_state": prior_operations_state, "prior_capital_state": prior_capital_state, "requested_capital": capital, "requested_risk_fraction": risk_fraction, "known_at": known.isoformat(), "evidence_epoch": epoch.isoformat(), "expires_at": expiry.isoformat()},
        artifacts={"admission_digest": result["admission_digest"]},
        result=result,
    )
