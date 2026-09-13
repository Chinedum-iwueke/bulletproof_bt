"""RISK-005 deterministic pre-trade and portfolio risk producer."""

from __future__ import annotations

import math
from datetime import UTC, datetime
from typing import Any

from .receipt import ProducerReceipt, build_receipt, digest, verify_receipt

REALTIME_RISK_SCHEMA_VERSION = "risk005-realtime-risk-v1.2.0"
REALTIME_RISK_SPECIFICATION = {
    "schema_version": REALTIME_RISK_SCHEMA_VERSION,
    "required_dependencies": ["RISK-002", "RISK-003", "EXEC-001", "EXEC-004"],
    "conditional_dependencies": {
        "RISK-004": "required for allow; absence produces a deterministic deny"
    },
    "decision": "deterministic allow, deny, or reduce-only exit against one state version",
    "degraded_mode": "deny exposure increases; permit only verified exposure-reducing exits",
    "dependency_binding": (
        "exact independently versioned producer receipts; each native dataset digest is retained"
    ),
    "authority": {
        "allocation": False,
        "capital": False,
        "orders": False,
        "promotion": False,
    },
}

_EXTERNAL_ORDER_MODES = {"demo_broker", "live_broker"}


class RealtimeRiskError(ValueError):
    pass


def _time(value: Any, field: str) -> datetime:
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise RealtimeRiskError(f"{field} must be ISO-8601") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise RealtimeRiskError(f"{field} must be timezone-aware")
    return result.astimezone(UTC)


def _number(value: Any, field: str, *, positive: bool = False) -> float:
    if isinstance(value, bool):
        raise RealtimeRiskError(f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise RealtimeRiskError(f"{field} must be numeric") from exc
    if not math.isfinite(result) or (positive and result <= 0):
        raise RealtimeRiskError(f"{field} is invalid")
    return result


def _receipt(value: ProducerReceipt | dict[str, Any], milestone: str) -> dict[str, Any]:
    item = value.as_dict() if isinstance(value, ProducerReceipt) else dict(value)
    if not verify_receipt(item) or item.get("milestone") != milestone:
        raise RealtimeRiskError(f"RISK-005 requires an exact {milestone} receipt")
    return item


def order_binding(
    *,
    symbol: str,
    side: str,
    quantity: float,
    reference_price: float,
    reduce_only: bool,
) -> dict[str, Any]:
    """Return the canonical fields bound by a real-time risk decision."""
    return {
        "symbol": str(symbol).upper(),
        "side": str(side).lower(),
        "quantity": _number(quantity, "quantity", positive=True),
        "reference_price": _number(reference_price, "reference_price", positive=True),
        "reduce_only": bool(reduce_only),
    }


def require_realtime_risk_authorization(
    *,
    receipt: ProducerReceipt | dict[str, Any],
    order: dict[str, Any],
    expected_state_version: int,
    now: datetime,
    maximum_age_seconds: float,
) -> dict[str, Any]:
    """Fail closed unless an external order exactly matches a current RISK-005 receipt."""
    document = _receipt(receipt, "RISK-005")
    result = document["result"]
    if result.get("allowed") is not True:
        raise RealtimeRiskError("RISK-005 denied this order")
    if result.get("state_version") != expected_state_version:
        raise RealtimeRiskError("RISK-005 state version changed before submission")
    decided_at = _time(result.get("known_at"), "risk_decision.known_at")
    current = _time(now, "now")
    age = (current - decided_at).total_seconds()
    if age < 0 or age > _number(
        maximum_age_seconds, "maximum_age_seconds", positive=True
    ):
        raise RealtimeRiskError("RISK-005 decision is stale")
    binding = order_binding(
        symbol=str(order.get("symbol", "")),
        side=str(order.get("side", "")),
        quantity=order.get("quantity"),
        reference_price=order.get("reference_price"),
        reduce_only=order.get("reduce_only") is True,
    )
    if digest(binding) != result.get("order_binding_digest"):
        raise RealtimeRiskError("order does not match the RISK-005 decision")
    if result.get("decision") == "reduce_only_exit" and not binding["reduce_only"]:
        raise RealtimeRiskError("degraded authorization is reduce-only")
    return result


def realtime_risk_decision_receipt(
    *,
    intent: dict[str, Any],
    state: dict[str, Any],
    dependency_receipts: dict[str, ProducerReceipt | dict[str, Any]],
    policy: dict[str, Any],
    known_at: datetime,
    dataset_digest: str,
    source_commit: str,
) -> ProducerReceipt:
    required = {"RISK-002", "RISK-003", "EXEC-001", "EXEC-004"}
    allowed = required | {"RISK-004"}
    if not required.issubset(dependency_receipts) or not set(
        dependency_receipts
    ).issubset(allowed):
        raise RealtimeRiskError("dependency receipt set is incomplete or unexpected")
    receipts = {key: _receipt(value, key) for key, value in dependency_receipts.items()}
    known = _time(known_at, "known_at")
    observed, available = (
        _time(state.get("observed_at"), "state.observed_at"),
        _time(state.get("available_at"), "state.available_at"),
    )
    received = _time(intent.get("received_at"), "intent.received_at")
    if observed > available or available > known or received > known:
        raise RealtimeRiskError("state or intent violates point-in-time availability")
    version = state.get("version")
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise RealtimeRiskError("state.version must be a positive integer")
    if intent.get("expected_state_version") != version:
        raise RealtimeRiskError("state version race detected")
    candidate_digest = str(intent.get("candidate_digest", ""))
    admission_receipt = receipts.get("RISK-004")
    admission = admission_receipt["result"] if admission_receipt else None
    if admission is not None and admission.get("candidate_digest") != candidate_digest:
        raise RealtimeRiskError("RISK-004 candidate binding does not match")

    quantity = _number(intent.get("quantity"), "quantity", positive=True)
    price = _number(intent.get("price"), "price", positive=True)
    notional = quantity * price
    symbol = str(intent.get("symbol", "")).upper()
    side = str(intent.get("side", "")).lower()
    if side not in {"buy", "sell"}:
        raise RealtimeRiskError("side is invalid")
    reduce_only = intent.get("reduce_only")
    if not isinstance(reduce_only, bool):
        raise RealtimeRiskError("reduce_only must be boolean")
    current_position = _number(
        (state.get("positions") or {}).get(symbol, 0), "position"
    )
    delta = quantity if side == "buy" else -quantity
    exposure_reducing = reduce_only and abs(current_position + delta) < abs(
        current_position
    )

    reasons: list[str] = []
    if (known - available).total_seconds() > _number(
        policy.get("snapshot_expiry_seconds"), "snapshot_expiry_seconds", positive=True
    ):
        reasons.append("stale_state")
    latency_ms = (known - received).total_seconds() * 1000
    if latency_ms > _number(
        policy.get("decision_deadline_ms"), "decision_deadline_ms", positive=True
    ):
        reasons.append("decision_deadline_exceeded")
    if receipts["RISK-002"]["result"].get("allowed") is not True:
        reasons.append("venue_rule_not_qualified")
    budget = receipts["RISK-003"]["result"]
    if budget.get("qualified") is not True:
        reasons.append("risk_budget_not_qualified")
    if admission is None:
        reasons.append("candidate_admission_missing")
    elif (
        admission.get("eligible_for_authority_review") is not True
        or admission.get("requested_action") not in {"allocate", "scale"}
        or _time(admission.get("expires_at"), "admission.expires_at") <= known
    ):
        reasons.append("candidate_not_admitted")
    if receipts["EXEC-001"]["result"].get("reconstructable") is not True:
        reasons.append("event_state_not_reconstructable")
    oms = receipts["EXEC-004"]["result"]
    if (
        oms.get("qualified") is not True
        or (oms.get("reconciliation") or {}).get("submission_allowed") is not True
    ):
        reasons.append("oms_not_reconciled")
    if not state.get("connector_healthy", False):
        reasons.append("connector_unhealthy")
    if not state.get("reconciliation_healthy", False):
        reasons.append("reconciliation_unhealthy")
    if state.get("kill_active", False):
        reasons.append("kill_active")
    if int(state.get("critical_incidents", 0)) > 0:
        reasons.append("critical_incident")
    if symbol not in {str(item).upper() for item in policy.get("allowed_symbols", [])}:
        reasons.append("symbol_not_allowed")
    if quantity > _number(
        policy.get("maximum_order_quantity"), "maximum_order_quantity", positive=True
    ):
        reasons.append("order_quantity_limit")
    if notional > _number(
        policy.get("maximum_order_notional"), "maximum_order_notional", positive=True
    ):
        reasons.append("order_notional_limit")
    if (
        int(state.get("open_orders", 0)) >= int(policy.get("maximum_open_orders", 0))
        and not reduce_only
    ):
        reasons.append("open_order_limit")
    projected_gross = _number(state.get("gross_notional", 0), "gross_notional") + (
        0 if exposure_reducing else notional
    )
    maximum_gross = min(
        _number(
            policy.get("maximum_gross_notional"),
            "maximum_gross_notional",
            positive=True,
        ),
        float(budget.get("budget_capital", 0)),
    )
    if projected_gross > maximum_gross:
        reasons.append("gross_notional_limit")
    if _number(state.get("daily_pnl", 0), "daily_pnl") <= -abs(
        _number(policy.get("maximum_daily_loss"), "maximum_daily_loss", positive=True)
    ):
        reasons.append("daily_loss_limit")
    if reduce_only and not exposure_reducing:
        reasons.append("reduce_only_does_not_reduce")

    structural = {
        "stale_state",
        "decision_deadline_exceeded",
        "venue_rule_not_qualified",
        "candidate_not_admitted",
        "event_state_not_reconstructable",
        "oms_not_reconciled",
        "connector_unhealthy",
        "reconciliation_unhealthy",
        "symbol_not_allowed",
        "order_quantity_limit",
        "order_notional_limit",
        "reduce_only_does_not_reduce",
    }
    reduction_allowed = exposure_reducing and not (set(reasons) & structural)
    decision = (
        "allow"
        if not reasons
        else ("reduce_only_exit" if reduction_allowed else "deny")
    )
    binding = order_binding(
        symbol=symbol,
        side=side,
        quantity=quantity,
        reference_price=price,
        reduce_only=reduce_only,
    )
    result = {
        "schema_version": REALTIME_RISK_SCHEMA_VERSION,
        "intent_id": str(intent.get("intent_id", "")),
        "candidate_digest": candidate_digest,
        "state_id": str(state.get("state_id", "")),
        "state_version": version,
        "known_at": known.isoformat(),
        "decision_latency_ms": round(latency_ms, 6),
        "decision": decision,
        "allowed": decision in {"allow", "reduce_only_exit"},
        "effective_quantity": quantity
        if decision in {"allow", "reduce_only_exit"}
        else 0.0,
        "reduce_only": reduce_only,
        "order_binding_digest": digest(binding),
        "reasons": sorted(set(reasons)),
        "kill_path_independent": True,
        "dependency_receipts": {
            key: value["receipt_digest"] for key, value in sorted(receipts.items())
        },
        "dependency_dataset_digests": {
            key: value["dataset_digest"] for key, value in sorted(receipts.items())
        },
        "realtime_risk_schema_digest": digest(REALTIME_RISK_SPECIFICATION),
        "authority": REALTIME_RISK_SPECIFICATION["authority"],
        "claim": "deterministic risk decision evidence only; the execution adapter separately enforces it",
    }
    result["decision_digest"] = digest(result)
    return build_receipt(
        milestone="RISK-005",
        producer="bt.institutional.realtime_risk.realtime_risk_decision_receipt",
        producer_version="1.2.0",
        source_commit=source_commit,
        inputs={"intent": intent, "state": state, "dependencies": receipts},
        dataset_digest=dataset_digest,
        configuration=policy,
        artifacts={"decision_digest": result["decision_digest"]},
        result=result,
    )
