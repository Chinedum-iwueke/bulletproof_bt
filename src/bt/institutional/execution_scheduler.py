"""EXEC-006 deterministic baseline execution schedules and release gate."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from .realtime_risk import RealtimeRiskError, require_realtime_risk_authorization
from .receipt import ProducerReceipt, build_receipt, digest, verify_receipt

EXECUTION_SCHEDULE_SCHEMA_VERSION = "exec006-baseline-schedule-v1.0.0"
EXECUTION_SCHEDULE_SPECIFICATION = {
    "schema_version": EXECUTION_SCHEDULE_SCHEMA_VERSION,
    "algorithms": ["market", "limit", "twap", "vwap"],
    "dependencies": ["EXEC-004", "EXEC-005", "RISK-005"],
    "causality": "VWAP profiles must be available before schedule creation",
    "release_gate": "every child order requires a fresh exact RISK-005 receipt",
    "partial_fills": "unfilled target carries forward within the next slice participation cap",
    "clock_drift": "fail closed beyond the configured tolerance",
    "rollback": "single market child with unchanged hard constraints",
    "authority": {
        "allocation": False,
        "capital": False,
        "orders": False,
        "promotion": False,
    },
}


class ExecutionScheduleError(ValueError):
    pass


def _time(value: Any, field: str) -> datetime:
    try:
        result = datetime.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise ExecutionScheduleError(f"{field} must be ISO-8601") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise ExecutionScheduleError(f"{field} must be timezone-aware")
    return result.astimezone(UTC)


def _decimal(value: Any, field: str, *, positive: bool = False) -> Decimal:
    if isinstance(value, bool):
        raise ExecutionScheduleError(f"{field} must be numeric")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ExecutionScheduleError(f"{field} must be numeric") from exc
    if not result.is_finite() or (positive and result <= 0):
        raise ExecutionScheduleError(f"{field} is invalid")
    return result


def _receipt(value: ProducerReceipt | dict[str, Any], milestone: str) -> dict[str, Any]:
    item = value.as_dict() if isinstance(value, ProducerReceipt) else dict(value)
    if not verify_receipt(item) or item.get("milestone") != milestone:
        raise ExecutionScheduleError(f"EXEC-006 requires an exact {milestone} receipt")
    return item


def _quantities(total: Decimal, weights: list[Decimal]) -> list[Decimal]:
    quantities: list[Decimal] = []
    allocated = Decimal(0)
    for position, weight in enumerate(weights):
        quantity = total - allocated if position == len(weights) - 1 else total * weight
        quantities.append(quantity)
        allocated += quantity
    return quantities


def build_execution_schedule(
    *,
    parent_intent: dict[str, Any],
    algorithm: str,
    policy: dict[str, Any],
    known_at: datetime,
    volume_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create an immutable, non-authoritative child-order schedule."""
    known = _time(known_at, "known_at")
    name = str(algorithm).lower()
    allowed = {str(item).lower() for item in policy.get("allowed_algorithms", [])}
    if (
        name not in EXECUTION_SCHEDULE_SPECIFICATION["algorithms"]
        or name not in allowed
    ):
        raise ExecutionScheduleError("algorithm is not allowed")
    total = _decimal(
        parent_intent.get("total_quantity"), "total_quantity", positive=True
    )
    price = _decimal(
        parent_intent.get("reference_price"), "reference_price", positive=True
    )
    start = _time(parent_intent.get("start_at"), "start_at")
    end = _time(parent_intent.get("end_at"), "end_at")
    if start < known or end < start:
        raise ExecutionScheduleError("schedule window is invalid or already known")
    horizon = (end - start).total_seconds()
    maximum_horizon = float(
        _decimal(
            policy.get("maximum_horizon_seconds"),
            "maximum_horizon_seconds",
            positive=True,
        )
    )
    if horizon > maximum_horizon:
        raise ExecutionScheduleError("schedule horizon exceeds policy")
    slice_count = (
        1 if name in {"market", "limit"} else int(parent_intent.get("slice_count", 0))
    )
    if slice_count < 1 or slice_count > int(policy.get("maximum_slices", 0)):
        raise ExecutionScheduleError("slice count exceeds policy")
    if name == "limit" and parent_intent.get("limit_price") is None:
        raise ExecutionScheduleError("limit schedules require a limit price")
    urgency = str(parent_intent.get("urgency", "normal")).lower()
    if urgency not in {"patient", "normal", "high", "immediate"}:
        raise ExecutionScheduleError("urgency is invalid")
    if name == "limit":
        limit_price = _decimal(
            parent_intent["limit_price"], "limit_price", positive=True
        )
        side = str(parent_intent.get("side", "")).lower()
        if (side == "buy" and limit_price > price) or (
            side == "sell" and limit_price < price
        ):
            raise ExecutionScheduleError(
                "limit price is more aggressive than reference"
            )

    if name == "vwap":
        if not volume_profile:
            raise ExecutionScheduleError("VWAP requires a causal volume profile")
        if (
            _time(volume_profile.get("available_at"), "volume_profile.available_at")
            > known
        ):
            raise ExecutionScheduleError(
                "VWAP volume profile contains future information"
            )
        raw_weights = volume_profile.get("bucket_weights") or []
        if len(raw_weights) != slice_count:
            raise ExecutionScheduleError("VWAP profile does not match slice count")
        weights = [
            _decimal(value, "bucket_weight", positive=True) for value in raw_weights
        ]
        denominator = sum(weights)
        weights = [value / denominator for value in weights]
    else:
        weights = [Decimal(1) / Decimal(slice_count)] * slice_count

    minimum = _decimal(
        policy.get("minimum_child_quantity"), "minimum_child_quantity", positive=True
    )
    participation = _decimal(
        policy.get("maximum_participation_rate"),
        "maximum_participation_rate",
        positive=True,
    )
    if participation > 1:
        raise ExecutionScheduleError("maximum participation rate cannot exceed one")
    interval_volumes = parent_intent.get("projected_interval_volumes") or []
    if len(interval_volumes) != slice_count:
        raise ExecutionScheduleError(
            "one causal projected volume is required per slice"
        )
    quantities = _quantities(total, weights)
    if any(value < minimum for value in quantities):
        raise ExecutionScheduleError("child quantity falls below policy minimum")

    spacing = horizon / max(slice_count - 1, 1)
    children: list[dict[str, Any]] = []
    cumulative = Decimal(0)
    for index, quantity in enumerate(quantities):
        interval_volume = _decimal(
            interval_volumes[index], "projected_interval_volume", positive=True
        )
        cap = interval_volume * participation
        if quantity > cap:
            raise ExecutionScheduleError(
                "participation constraint conflicts with parent quantity"
            )
        cumulative += quantity
        scheduled = start + timedelta(seconds=spacing * index)
        order_type = "market" if name in {"market", "twap", "vwap"} else "limit"
        child = {
            "sequence": index + 1,
            "scheduled_at": scheduled.isoformat(),
            "target_quantity": float(quantity),
            "target_cumulative_quantity": float(cumulative),
            "maximum_quantity": float(cap),
            "order_type": order_type,
            "limit_price": float(
                _decimal(parent_intent["limit_price"], "limit_price", positive=True)
            )
            if order_type == "limit"
            else None,
            "cancel_at": (
                scheduled
                + timedelta(seconds=int(policy.get("child_lifetime_seconds", 0)))
            ).isoformat(),
        }
        child["child_id"] = digest(
            {"parent": parent_intent.get("intent_id"), "child": child}
        )[:24]
        children.append(child)

    schedule = {
        "schema_version": EXECUTION_SCHEDULE_SCHEMA_VERSION,
        "parent_intent_id": str(parent_intent.get("intent_id", "")),
        "candidate_digest": str(parent_intent.get("candidate_digest", "")),
        "symbol": str(parent_intent.get("symbol", "")).upper(),
        "side": str(parent_intent.get("side", "")).lower(),
        "reference_price": float(price),
        "reduce_only": parent_intent.get("reduce_only") is True,
        "total_quantity": float(total),
        "algorithm": name,
        "urgency": urgency,
        "known_at": known.isoformat(),
        "end_at": end.isoformat(),
        "clock_tolerance_seconds": float(
            _decimal(
                policy.get("clock_tolerance_seconds"),
                "clock_tolerance_seconds",
                positive=True,
            )
        ),
        "children": children,
        "volume_profile_digest": digest(volume_profile) if volume_profile else None,
        "risk_release_required": True,
        "order_authority": False,
    }
    schedule["schedule_digest"] = digest(schedule)
    return schedule


def _verify_schedule(schedule: dict[str, Any]) -> None:
    document = dict(schedule)
    supplied = document.pop("schedule_digest", None)
    if document.get("schema_version") != EXECUTION_SCHEDULE_SCHEMA_VERSION:
        raise ExecutionScheduleError("execution schedule schema is invalid")
    if supplied != digest(document):
        raise ExecutionScheduleError("execution schedule digest does not match content")


def next_schedule_action(
    *,
    schedule: dict[str, Any],
    now: datetime,
    cumulative_filled_quantity: float,
    child_states: dict[str, str],
) -> dict[str, Any]:
    """Return one deterministic action; this does not authorize submission."""
    _verify_schedule(schedule)
    current = _time(now, "now")
    filled = _decimal(cumulative_filled_quantity, "cumulative_filled_quantity")
    total = _decimal(schedule.get("total_quantity"), "total_quantity", positive=True)
    valid_states = {
        "pending",
        "open",
        "partially_filled",
        "filled",
        "cancelled",
        "rejected",
    }
    if any(value not in valid_states for value in child_states.values()):
        raise ExecutionScheduleError("child state is invalid")
    if filled >= total:
        return {"action": "complete", "schedule_digest": schedule["schedule_digest"]}
    for child in schedule.get("children", []):
        state = child_states.get(child["child_id"], "pending")
        if state in {"open", "partially_filled"}:
            if current >= _time(child["cancel_at"], "cancel_at"):
                return {
                    "action": "cancel",
                    "child_id": child["child_id"],
                    "reason": "child_deadline",
                }
            return {
                "action": "wait",
                "child_id": child["child_id"],
                "reason": "child_active",
            }
    for child in schedule.get("children", []):
        if child_states.get(child["child_id"], "pending") != "pending":
            continue
        scheduled = _time(child["scheduled_at"], "scheduled_at")
        if current < scheduled:
            return {
                "action": "wait",
                "until": child["scheduled_at"],
                "reason": "not_due",
            }
        if (current - scheduled).total_seconds() > float(
            schedule["clock_tolerance_seconds"]
        ):
            return {
                "action": "abort",
                "reason": "schedule_clock_drift",
                "child_id": child["child_id"],
            }
        target = _decimal(
            child["target_cumulative_quantity"], "target_cumulative_quantity"
        )
        quantity = min(
            max(target - filled, Decimal(0)),
            _decimal(child["maximum_quantity"], "maximum_quantity"),
            total - filled,
        )
        if quantity <= 0:
            continue
        order = {
            "intent_id": child["child_id"],
            "symbol": schedule["symbol"],
            "side": schedule["side"],
            "quantity": float(quantity),
            "reference_price": schedule["reference_price"],
            "reduce_only": schedule["reduce_only"],
            "order_type": child["order_type"],
            "limit_price": child["limit_price"],
        }
        return {
            "action": "propose_submit",
            "child_id": child["child_id"],
            "order": order,
            "risk_authorized": False,
        }
    return {
        "action": "incomplete",
        "reason": "schedule_exhausted",
        "remaining_quantity": float(total - filled),
    }


def authorize_schedule_action(
    *,
    action: dict[str, Any],
    risk_receipt: ProducerReceipt | dict[str, Any],
    expected_state_version: int,
    now: datetime,
    maximum_age_seconds: float,
) -> dict[str, Any]:
    if action.get("action") != "propose_submit":
        raise ExecutionScheduleError("only a proposed submission can be authorized")
    try:
        result = require_realtime_risk_authorization(
            receipt=risk_receipt,
            order=action["order"],
            expected_state_version=expected_state_version,
            now=now,
            maximum_age_seconds=maximum_age_seconds,
        )
    except RealtimeRiskError as exc:
        raise ExecutionScheduleError(str(exc)) from exc
    return action | {
        "action": "submit",
        "risk_authorized": True,
        "risk_decision_digest": result["decision_digest"],
    }


def execution_schedule_receipt(
    *,
    parent_intent: dict[str, Any],
    algorithm: str,
    policy: dict[str, Any],
    known_at: datetime,
    exec004_receipt: ProducerReceipt | dict[str, Any],
    exec005_receipt: ProducerReceipt | dict[str, Any],
    dataset_digest: str,
    source_commit: str,
    volume_profile: dict[str, Any] | None = None,
) -> ProducerReceipt:
    oms = _receipt(exec004_receipt, "EXEC-004")
    calibration = _receipt(exec005_receipt, "EXEC-005")
    if (
        oms["dataset_digest"] != dataset_digest
        or calibration["dataset_digest"] != dataset_digest
    ):
        raise ExecutionScheduleError("dependency dataset digests do not match")
    if (
        oms["result"].get("qualified") is not True
        or calibration["result"].get("qualified") is not True
    ):
        raise ExecutionScheduleError("OMS and execution calibration must be qualified")
    strata = (calibration["result"].get("calibration") or {}).get("strata") or []
    if not strata:
        raise ExecutionScheduleError("EXEC-005 calibration strata are required")
    base_cost = max(
        _decimal(
            item.get("pessimistic_cost_bps"),
            "pessimistic_cost_bps",
            positive=True,
        )
        for item in strata
    )
    multipliers = policy.get("algorithm_cost_multipliers") or {}
    if set(multipliers) != set(EXECUTION_SCHEDULE_SPECIFICATION["algorithms"]):
        raise ExecutionScheduleError("algorithm cost multipliers are incomplete")
    cost_comparison = {
        name: float(
            base_cost * _decimal(multiplier, f"{name}_cost_multiplier", positive=True)
        )
        for name, multiplier in sorted(multipliers.items())
    }
    schedule = build_execution_schedule(
        parent_intent=parent_intent,
        algorithm=algorithm,
        policy=policy,
        known_at=known_at,
        volume_profile=volume_profile,
    )
    result = {
        "schema_version": EXECUTION_SCHEDULE_SCHEMA_VERSION,
        "execution_schedule_schema_digest": digest(EXECUTION_SCHEDULE_SPECIFICATION),
        "schedule": schedule,
        "dependency_receipts": {
            "exec004": oms["receipt_digest"],
            "exec005": calibration["receipt_digest"],
        },
        "qualified": True,
        "risk005_required_per_child": True,
        "pessimistic_cost_comparison_bps": cost_comparison,
        "claim": "baseline execution schedule evidence only; every child needs a fresh exact RISK-005 receipt and adapter enforcement",
    }
    return build_receipt(
        milestone="EXEC-006",
        producer="bt.institutional.execution_scheduler.execution_schedule_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={"parent_intent": parent_intent, "volume_profile": volume_profile},
        dataset_digest=dataset_digest,
        configuration={"algorithm": algorithm, "policy": policy},
        artifacts={"schedule_digest": schedule["schedule_digest"]},
        result=result,
    )
