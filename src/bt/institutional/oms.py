"""Deterministic, restart-safe order management and venue reconciliation."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from .receipt import ProducerReceipt, build_receipt, digest, verify_receipt

OMS_SCHEMA_VERSION = "exec004-oms-reconciliation-v1.0.0"
COMMAND_ACTIONS = {"submit", "cancel"}
EVENT_TYPES = {
    "submission_unknown",
    "acknowledged",
    "partially_filled",
    "filled",
    "cancel_requested",
    "cancelled",
    "rejected",
    "expired",
}
TERMINAL_STATES = {"filled", "cancelled", "rejected", "expired"}
TRANSITIONS = {
    "pending_submit": {"submission_unknown", "acknowledged", "partially_filled", "filled", "rejected"},
    "submission_unknown": {"acknowledged", "partially_filled", "filled", "cancelled", "rejected", "expired"},
    "acknowledged": {"partially_filled", "filled", "cancel_requested", "cancelled", "rejected", "expired"},
    "partially_filled": {"partially_filled", "filled", "cancel_requested", "cancelled", "expired"},
    "cancel_requested": {"partially_filled", "filled", "cancelled", "expired"},
}
OMS_SPECIFICATION = {
    "schema_version": OMS_SCHEMA_VERSION,
    "command_identity": "sha256(venue_id,idempotency_key)",
    "command_actions": sorted(COMMAND_ACTIONS),
    "event_types": sorted(EVENT_TYPES),
    "terminal_states": sorted(TERMINAL_STATES),
    "reconciliation_domains": ["orders", "fills", "positions", "balances", "snapshot_freshness"],
    "material_action": "freeze_and_investigate",
    "authority": {"allocation": False, "capital": False, "orders": False, "promotion": False},
}


class OmsError(ValueError):
    """OMS evidence is ambiguous, inconsistent, or unsafe to replay."""


def _time(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise OmsError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise OmsError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _decimal(value: Any, field: str, *, allow_zero: bool = False) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise OmsError(f"{field} must be numeric") from exc
    if not number.is_finite() or number < 0 or (number == 0 and not allow_zero):
        qualifier = "non-negative" if allow_zero else "positive"
        raise OmsError(f"{field} must be {qualifier} and finite")
    return number


def client_order_id(idempotency_key: str, venue_id: str) -> str:
    """Produce a stable venue-safe identity without relying on process counters."""

    if not idempotency_key or not venue_id:
        raise OmsError("idempotency_key and venue_id are required")
    return f"ir-{venue_id[:6].lower()}-{digest({'key': idempotency_key, 'venue': venue_id})[:20]}"


def _command(raw: dict[str, Any]) -> dict[str, Any]:
    action = str(raw.get("action", ""))
    if action not in COMMAND_ACTIONS:
        raise OmsError("command action is unsupported")
    required = ["command_id", "idempotency_key", "venue_id", "issued_at", "approval_digest", "risk_decision_digest"]
    missing = [field for field in required if not raw.get(field)]
    if missing:
        raise OmsError(f"command is missing {', '.join(missing)}")
    issued_at = _time(raw["issued_at"], "issued_at")
    result = {
        "command_id": str(raw["command_id"]),
        "idempotency_key": str(raw["idempotency_key"]),
        "client_order_id": client_order_id(str(raw["idempotency_key"]), str(raw["venue_id"])),
        "action": action,
        "venue_id": str(raw["venue_id"]),
        "issued_at": issued_at.isoformat(),
        "approval_digest": str(raw["approval_digest"]),
        "risk_decision_digest": str(raw["risk_decision_digest"]),
        "target_client_order_id": raw.get("target_client_order_id"),
    }
    if action == "submit":
        for field in ("listing_id", "side", "quantity", "order_type"):
            if raw.get(field) in (None, ""):
                raise OmsError(f"submit command is missing {field}")
        quantity = _decimal(raw["quantity"], "quantity")
        result.update(
            {
                "listing_id": str(raw["listing_id"]),
                "side": str(raw["side"]),
                "quantity": str(quantity.normalize()),
                "order_type": str(raw["order_type"]),
                "limit_price": raw.get("limit_price"),
                "intent_digest": str(raw.get("intent_digest", "")),
            }
        )
    elif not raw.get("target_client_order_id"):
        raise OmsError("cancel command requires target_client_order_id")
    result["command_digest"] = digest(result)
    result["idempotency_digest"] = digest(
        {key: value for key, value in result.items() if key not in {"command_id", "command_digest"}}
    )
    return result


def _event(raw: dict[str, Any]) -> dict[str, Any]:
    event_type = str(raw.get("event_type", ""))
    if event_type not in EVENT_TYPES:
        raise OmsError("event type is unsupported")
    required = ["event_id", "command_id", "sequence", "occurred_at", "available_at"]
    missing = [field for field in required if raw.get(field) in (None, "")]
    if missing:
        raise OmsError(f"event is missing {', '.join(missing)}")
    occurred_at = _time(raw["occurred_at"], "occurred_at")
    available_at = _time(raw["available_at"], "available_at")
    if occurred_at > available_at:
        raise OmsError("event availability precedes occurrence")
    result = {
        "event_id": str(raw["event_id"]),
        "command_id": str(raw["command_id"]),
        "sequence": int(raw["sequence"]),
        "event_type": event_type,
        "occurred_at": occurred_at.isoformat(),
        "available_at": available_at.isoformat(),
        "venue_order_id": raw.get("venue_order_id"),
        "execution_id": raw.get("execution_id"),
        "fill_quantity": raw.get("fill_quantity"),
        "fill_price": raw.get("fill_price"),
    }
    result["event_digest"] = digest(result)
    return result


def replay_oms(*, commands: Iterable[dict[str, Any]], events: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Replay an append-only command/event journal into one authoritative local view."""

    command_by_id: dict[str, dict[str, Any]] = {}
    command_by_key: dict[str, dict[str, Any]] = {}
    duplicate_commands = 0
    for raw in commands:
        item = _command(raw)
        existing = command_by_key.get(item["idempotency_key"])
        if existing:
            if existing["idempotency_digest"] != item["idempotency_digest"]:
                raise OmsError("idempotency key was reused with different command content")
            duplicate_commands += 1
            continue
        if item["command_id"] in command_by_id:
            raise OmsError("command_id is duplicated")
        command_by_id[item["command_id"]] = item
        command_by_key[item["idempotency_key"]] = item

    states = {
        item["client_order_id"]: {
            "command_id": item["command_id"],
            "client_order_id": item["client_order_id"],
            "venue_id": item["venue_id"],
            "listing_id": item.get("listing_id"),
            "requested_quantity": item.get("quantity"),
            "cumulative_fill_quantity": "0",
            "venue_order_id": None,
            "state": "pending_submit",
            "approval_digest": item["approval_digest"],
            "risk_decision_digest": item["risk_decision_digest"],
        }
        for item in command_by_id.values()
        if item["action"] == "submit"
    }
    state_by_command = {value["command_id"]: value for value in states.values()}
    for item in command_by_id.values():
        if item["action"] == "cancel" and item["target_client_order_id"] not in states:
            raise OmsError("cancel command references an unknown client order")
    seen_events: dict[str, str] = {}
    seen_executions: dict[str, tuple[str, str, str | None]] = {}
    duplicate_events = 0
    duplicate_fills = 0
    expected_sequence = 1
    accepted_events = []
    for raw in sorted((_event(item) for item in events), key=lambda item: item["sequence"]):
        prior_digest = seen_events.get(raw["event_id"])
        if prior_digest:
            if prior_digest != raw["event_digest"]:
                raise OmsError("event_id was reused with different event content")
            duplicate_events += 1
            continue
        if raw["sequence"] != expected_sequence:
            raise OmsError("event sequence is not contiguous")
        expected_sequence += 1
        seen_events[raw["event_id"]] = raw["event_digest"]
        state = state_by_command.get(raw["command_id"])
        if state is None:
            raise OmsError("event references an unknown submit command")
        event_type = raw["event_type"]
        if state["state"] in TERMINAL_STATES:
            raise OmsError("terminal order received another lifecycle event")
        if event_type not in TRANSITIONS[state["state"]]:
            raise OmsError(f"invalid order transition {state['state']} -> {event_type}")
        if raw["venue_order_id"]:
            if state["venue_order_id"] and state["venue_order_id"] != raw["venue_order_id"]:
                raise OmsError("venue order identity changed during lifecycle")
            state["venue_order_id"] = str(raw["venue_order_id"])
        if event_type in {"partially_filled", "filled"}:
            execution_id = str(raw.get("execution_id") or "")
            if not execution_id:
                raise OmsError("fill event requires execution_id")
            execution_signature = (
                str(raw.get("fill_quantity")),
                str(raw.get("fill_price")),
                str(raw.get("venue_order_id")) if raw.get("venue_order_id") else None,
            )
            if execution_id in seen_executions:
                if seen_executions[execution_id] != execution_signature:
                    raise OmsError("execution_id was reused with different fill content")
                duplicate_fills += 1
                accepted_events.append(raw)
                continue
            seen_executions[execution_id] = execution_signature
            fill_qty = _decimal(raw.get("fill_quantity"), "fill_quantity")
            requested = _decimal(state["requested_quantity"], "requested_quantity")
            cumulative = _decimal(state["cumulative_fill_quantity"], "cumulative_fill_quantity", allow_zero=True) + fill_qty
            if cumulative > requested:
                raise OmsError("cumulative fill quantity exceeds requested quantity")
            if event_type == "filled" and cumulative != requested:
                raise OmsError("filled event does not complete requested quantity")
            if event_type == "partially_filled" and cumulative >= requested:
                raise OmsError("partial fill is terminal by quantity")
            state["cumulative_fill_quantity"] = str(cumulative.normalize())
        state["state"] = event_type
        accepted_events.append(raw)

    orders = sorted(states.values(), key=lambda item: item["client_order_id"])
    ambiguous = [item["client_order_id"] for item in orders if item["state"] == "submission_unknown"]
    result = {
        "schema_version": OMS_SCHEMA_VERSION,
        "commands": sorted(command_by_id.values(), key=lambda item: item["command_id"]),
        "events": accepted_events,
        "orders": orders,
        "duplicate_commands_suppressed": duplicate_commands,
        "duplicate_events_suppressed": duplicate_events,
        "duplicate_fills_suppressed": duplicate_fills,
        "ambiguous_submissions": ambiguous,
        "journal_digest": "",
    }
    result["journal_digest"] = digest({key: value for key, value in result.items() if key != "journal_digest"})
    return result


def reconcile_oms(
    *,
    journal: dict[str, Any],
    venue_snapshot: dict[str, Any],
    local_positions: dict[str, Any],
    local_balances: dict[str, Any],
    known_at: datetime,
    maximum_snapshot_age_seconds: int = 30,
    quantity_tolerance: str = "0",
    balance_tolerance: str = "0",
) -> dict[str, Any]:
    """Compare independent venue truth and fail closed on every material ambiguity."""

    if journal.get("schema_version") != OMS_SCHEMA_VERSION:
        raise OmsError("journal schema is unsupported")
    known = _time(known_at, "known_at")
    observed = _time(venue_snapshot.get("observed_at"), "observed_at")
    available = _time(venue_snapshot.get("available_at"), "available_at")
    if observed > available or available > known:
        raise OmsError("venue snapshot violates point-in-time availability")
    snapshot_age = (known - available).total_seconds()
    qty_tolerance = _decimal(quantity_tolerance, "quantity_tolerance", allow_zero=True)
    cash_tolerance = _decimal(balance_tolerance, "balance_tolerance", allow_zero=True)
    discrepancies: list[dict[str, Any]] = []
    if maximum_snapshot_age_seconds <= 0 or snapshot_age > maximum_snapshot_age_seconds:
        discrepancies.append({"category": "stale_snapshot", "key": "venue", "severity": "material"})

    local_orders = {item["client_order_id"]: item for item in journal["orders"]}
    remote_orders: dict[str, dict[str, Any]] = {}
    for raw in venue_snapshot.get("orders", []):
        key = str(raw.get("client_order_id", ""))
        if not key or key in remote_orders:
            raise OmsError("venue snapshot has empty or duplicate client order identity")
        remote_orders[key] = raw
    for key in sorted(set(remote_orders) - set(local_orders)):
        discrepancies.append({"category": "unknown_venue_order", "key": key, "severity": "material"})
    recovered = []
    for key, local in sorted(local_orders.items()):
        remote = remote_orders.get(key)
        if remote is None:
            if local["state"] not in TERMINAL_STATES:
                category = "ambiguous_submission" if local["state"] == "submission_unknown" else "missing_venue_order"
                discrepancies.append({"category": category, "key": key, "severity": "material"})
            continue
        if local["venue_order_id"] and str(remote.get("venue_order_id")) != local["venue_order_id"]:
            discrepancies.append({"category": "venue_order_identity", "key": key, "severity": "material"})
        if local["state"] == "submission_unknown":
            recovered.append(key)
        elif str(remote.get("state")) != local["state"]:
            discrepancies.append({"category": "order_state", "key": key, "severity": "material"})
        local_fill = _decimal(local["cumulative_fill_quantity"], "local_fill", allow_zero=True)
        remote_fill = _decimal(remote.get("cumulative_fill_quantity", "0"), "remote_fill", allow_zero=True)
        if abs(local_fill - remote_fill) > qty_tolerance:
            discrepancies.append({"category": "fill_quantity", "key": key, "severity": "material"})

    def compare_values(category: str, local: dict[str, Any], remote: dict[str, Any], tolerance: Decimal) -> None:
        for key in sorted(set(local) | set(remote)):
            if key not in local or key not in remote:
                discrepancies.append({"category": f"{category}_presence", "key": key, "severity": "material"})
                continue
            left = _decimal(local[key], f"local_{category}", allow_zero=True)
            right = _decimal(remote[key], f"remote_{category}", allow_zero=True)
            if abs(left - right) > tolerance:
                discrepancies.append({"category": f"{category}_quantity", "key": key, "severity": "material"})

    compare_values("position", local_positions, venue_snapshot.get("positions", {}), qty_tolerance)
    compare_values("balance", local_balances, venue_snapshot.get("balances", {}), cash_tolerance)
    material = sum(item["severity"] == "material" for item in discrepancies)
    result = {
        "schema_version": OMS_SCHEMA_VERSION,
        "known_at": known.isoformat(),
        "snapshot_digest": digest(venue_snapshot),
        "journal_digest": journal["journal_digest"],
        "discrepancies": discrepancies,
        "material_discrepancy_count": material,
        "recovered_ambiguous_submissions": recovered,
        "decision": "allow" if material == 0 else "freeze_and_investigate",
        "submission_allowed": material == 0,
        "qualified": material == 0,
    }
    result["reconciliation_digest"] = digest(result)
    return result


def oms_reconciliation_receipt(
    *,
    exec001_receipt: dict[str, Any],
    exec003_receipt: dict[str, Any],
    risk002_receipt: dict[str, Any],
    commands: Iterable[dict[str, Any]],
    events: Iterable[dict[str, Any]],
    venue_snapshot: dict[str, Any],
    local_positions: dict[str, Any],
    local_balances: dict[str, Any],
    known_at: datetime,
    source_commit: str,
    dataset_digest: str,
    configuration: dict[str, Any],
) -> ProducerReceipt:
    for receipt, milestone in ((exec001_receipt, "EXEC-001"), (exec003_receipt, "EXEC-003"), (risk002_receipt, "RISK-002")):
        if not verify_receipt(receipt) or receipt["milestone"] != milestone:
            raise OmsError(f"{milestone} receipt is not admissible")
    command_list = list(commands)
    event_list = list(events)
    journal = replay_oms(commands=command_list, events=event_list)
    reconciliation = reconcile_oms(
        journal=journal,
        venue_snapshot=venue_snapshot,
        local_positions=local_positions,
        local_balances=local_balances,
        known_at=known_at,
        maximum_snapshot_age_seconds=int(configuration.get("maximum_snapshot_age_seconds", 30)),
        quantity_tolerance=str(configuration.get("quantity_tolerance", "0")),
        balance_tolerance=str(configuration.get("balance_tolerance", "0")),
    )
    result = {
        "schema_version": OMS_SCHEMA_VERSION,
        "oms_schema_digest": digest(OMS_SPECIFICATION),
        "journal": journal,
        "reconciliation": reconciliation,
        "dependency_receipts": {
            "exec001": exec001_receipt["receipt_digest"],
            "exec003": exec003_receipt["receipt_digest"],
            "risk002": risk002_receipt["receipt_digest"],
        },
        "qualified": reconciliation["qualified"],
        "claim": "deterministic OMS and reconciliation evidence only; no order submission, allocation, promotion, or capital authority",
    }
    return build_receipt(
        milestone="EXEC-004",
        producer="bt.institutional.oms.oms_reconciliation_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={"commands": command_list, "events": event_list, "venue_snapshot": venue_snapshot},
        dataset_digest=dataset_digest,
        configuration=configuration,
        artifacts={"journal_digest": journal["journal_digest"], "reconciliation_digest": reconciliation["reconciliation_digest"]},
        result=result,
    )
