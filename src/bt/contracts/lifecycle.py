"""Canonical cross-stage lifecycle contracts shared with the SaaS product."""
from __future__ import annotations

from hashlib import sha256
import json
from typing import Any


LIFECYCLE_SCHEMA_VERSION = "research_lifecycle_event_v1"
STAGES = {"backtest", "demo", "live_canary", "live"}
EVENT_TYPES = {
    "deployment", "connector", "signal_decision", "order", "fill", "position",
    "portfolio", "trade_episode", "state_snapshot", "incident", "memory_assessment",
    "promotion", "conversation", "source", "citation", "candidate_hypothesis",
    "hypothesis_card", "proposal", "confirmation", "context_snapshot", "assistant_tool_call",
    "pine_export", "pine_import", "pine_parity", "tradingview_observation",
}
REQUIRED_IDENTITIES = {
    "program_id", "account_id", "stage", "strategy_spec_hash", "code_hash", "data_snapshot_id"
}
PAYLOAD_REQUIRED: dict[str, set[str]] = {
    "deployment": {"deployment_id", "connector_id", "mode", "status"},
    "connector": {"connector_id", "exchange", "environment", "status"},
    "signal_decision": {"symbol", "decision", "state_snapshot_id"},
    "order": {"order_id", "symbol", "side", "status"},
    "fill": {"fill_id", "order_id", "symbol", "price", "quantity"},
    "position": {"symbol", "quantity", "mark_price"},
    "portfolio": {"equity", "cash", "gross_exposure"},
    "trade_episode": {"trade_id", "symbol", "opened_at", "status"},
    "state_snapshot": {"state_snapshot_id", "features"},
    "incident": {"incident_id", "severity", "summary"},
    "memory_assessment": {"memory_item_id", "assessment", "confidence"},
    "promotion": {"from_stage", "to_stage", "decision", "evidence_hash"},
    "conversation": {"conversation_id", "message_id"},
    "source": {"source_id", "checksum_sha256"},
    "citation": {"source_id", "anchor"},
    "candidate_hypothesis": {"proposal_id", "claim"},
    "hypothesis_card": {"card_record_id", "card_id", "version", "status"},
    "proposal": {"proposal_id", "proposal_type", "status"},
    "confirmation": {"object_type", "object_id"},
    "context_snapshot": {"context_snapshot_id", "included_object_ids"},
    "assistant_tool_call": {"tool_call_id", "tool_name", "authorization_decision", "status"},
    "pine_export": {"script_hash", "strategy_spec_hash", "pine_version", "compatibility", "parity_status", "observation_only"},
    "pine_import": {"script_hash", "pine_version", "compatibility", "observation_only"},
    "pine_parity": {"script_hash", "strategy_spec_hash", "parity_status", "comparison_artifact_hash"},
    "tradingview_observation": {"script_hash", "observed_at", "symbol", "timeframe", "observation_only"},
}


def canonical_hash(payload: Any) -> str:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return sha256(body.encode("utf-8")).hexdigest()


def validate_stage_identity(identity: dict[str, Any]) -> list[str]:
    errors = [f"stage_identity_missing_{key}" for key in sorted(REQUIRED_IDENTITIES) if not identity.get(key)]
    if identity.get("stage") not in STAGES:
        errors.append("stage_identity_stage_invalid")
    if identity.get("stage") in {"demo", "live_canary", "live"} and not identity.get("deployment_id"):
        errors.append("stage_identity_deployment_id_required")
    return errors


def validate_lifecycle_event(event: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if event.get("schema_version") != LIFECYCLE_SCHEMA_VERSION:
        errors.append("lifecycle_schema_version_invalid")
    if event.get("event_type") not in EVENT_TYPES:
        errors.append("lifecycle_event_type_invalid")
    for key in ("event_id", "occurred_at", "identity", "payload", "actor"):
        if event.get(key) in (None, "", {}):
            errors.append(f"lifecycle_missing_{key}")
    identity = event.get("identity")
    if isinstance(identity, dict):
        errors.extend(validate_stage_identity(identity))
    else:
        errors.append("lifecycle_identity_must_be_object")
    if not isinstance(event.get("payload"), dict):
        errors.append("lifecycle_payload_must_be_object")
    else:
        required = PAYLOAD_REQUIRED.get(str(event.get("event_type")), set())
        errors.extend(f"lifecycle_payload_missing_{key}" for key in sorted(required) if event["payload"].get(key) in (None, ""))
    actor = event.get("actor")
    if not isinstance(actor, dict) or actor.get("type") not in {"user", "assistant", "worker", "engine", "exchange"}:
        errors.append("lifecycle_actor_invalid")
    return errors


def build_lifecycle_event(*, event_id: str, event_type: str, occurred_at: str, identity: dict[str, Any], payload: dict[str, Any], actor: dict[str, Any]) -> dict[str, Any]:
    event = {
        "schema_version": LIFECYCLE_SCHEMA_VERSION,
        "event_id": event_id,
        "event_type": event_type,
        "occurred_at": occurred_at,
        "identity": identity,
        "payload": payload,
        "actor": actor,
    }
    errors = validate_lifecycle_event(event)
    if errors:
        raise ValueError(";".join(errors))
    return {**event, "event_hash": canonical_hash(event)}
