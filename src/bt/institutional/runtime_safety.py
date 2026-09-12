"""EXEC-007 durable containment and explicit human recovery controls."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from collections.abc import Iterable
from typing import Any

from .receipt import ProducerReceipt, build_receipt, digest, is_sha256, verify_receipt

RUNTIME_SAFETY_SCHEMA_VERSION = "exec007-runtime-safety-v1.0.0"
RUNTIME_SAFETY_SPECIFICATION = {
    "schema_version": RUNTIME_SAFETY_SCHEMA_VERSION,
    "dependencies": ["EXEC-004", "EXEC-006", "RISK-005", "GOV-001"],
    "states": ["frozen", "killed", "ready"],
    "containment": "local durable freeze precedes network cancellation and is control-plane independent",
    "recovery": "exact reconciled OMS evidence and state-bound accountable human authority",
    "corruption": "missing, corrupt, or divergent safety state fails closed",
    "authority": {
        "allocation": False,
        "capital": False,
        "orders": False,
        "promotion": False,
    },
}


class RuntimeSafetyError(ValueError):
    pass


def _time(value: Any, field: str) -> datetime:
    try:
        result = datetime.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise RuntimeSafetyError(f"{field} must be ISO-8601") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise RuntimeSafetyError(f"{field} must be timezone-aware")
    return result.astimezone(UTC)


def _receipt(value: ProducerReceipt | dict[str, Any], milestone: str) -> dict[str, Any]:
    item = value.as_dict() if isinstance(value, ProducerReceipt) else dict(value)
    if not verify_receipt(item) or item.get("milestone") != milestone:
        raise RuntimeSafetyError(f"EXEC-007 requires an exact {milestone} receipt")
    return item


def _state_digest(state: dict[str, Any]) -> str:
    return digest({key: value for key, value in state.items() if key != "state_digest"})


def _write_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    payload = json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
    with temporary.open("w", encoding="ascii") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.chmod(0o600)
    os.replace(temporary, path)


def load_safety_state(path: str | Path) -> dict[str, Any]:
    target = Path(path)
    try:
        state = json.loads(target.read_text(encoding="ascii"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeSafetyError(
            "execution safety state is missing or unreadable"
        ) from exc
    if state.get("schema_version") != RUNTIME_SAFETY_SCHEMA_VERSION:
        raise RuntimeSafetyError("execution safety state schema is invalid")
    if state.get("status") not in {"frozen", "killed", "ready"}:
        raise RuntimeSafetyError("execution safety state status is invalid")
    if state.get("state_digest") != _state_digest(state):
        raise RuntimeSafetyError("execution safety state digest does not match content")
    return state


def _append_event(journal_path: Path, event: dict[str, Any]) -> None:
    journal_path.parent.mkdir(parents=True, exist_ok=True)
    with journal_path.open("a", encoding="ascii") as handle:
        handle.write(json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    journal_path.chmod(0o600)


def verify_safety_journal(path: str | Path) -> list[dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        return []
    events: list[dict[str, Any]] = []
    previous = "0" * 64
    for sequence, line in enumerate(
        target.read_text(encoding="ascii").splitlines(), start=1
    ):
        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            raise RuntimeSafetyError("execution safety journal is corrupt") from exc
        supplied = event.pop("event_digest", None)
        if (
            event.get("sequence") != sequence
            or event.get("previous_digest") != previous
        ):
            raise RuntimeSafetyError("execution safety journal chain is discontinuous")
        if supplied != digest(event):
            raise RuntimeSafetyError("execution safety journal digest does not match")
        event["event_digest"] = supplied
        previous = supplied
        events.append(event)
    return events


def _transition(
    *,
    state_path: Path,
    journal_path: Path,
    status: str,
    event_type: str,
    request_id: str,
    actor: str,
    reason: str,
    now: datetime,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    events = verify_safety_journal(journal_path)
    if any(item["request_id"] == request_id for item in events):
        return load_safety_state(state_path)
    previous_state = None
    if state_path.exists():
        previous_state = load_safety_state(state_path)
        if events and previous_state["event_digest"] != events[-1]["event_digest"]:
            raise RuntimeSafetyError("execution safety state and journal diverge")
    event = {
        "schema_version": RUNTIME_SAFETY_SCHEMA_VERSION,
        "sequence": len(events) + 1,
        "previous_digest": events[-1]["event_digest"] if events else "0" * 64,
        "event_type": event_type,
        "request_id": request_id,
        "actor": actor,
        "reason": reason,
        "occurred_at": _time(now, "now").isoformat(),
        "prior_state_digest": previous_state["state_digest"]
        if previous_state
        else None,
        "details": details or {},
    }
    event["event_digest"] = digest(event)
    state = {
        "schema_version": RUNTIME_SAFETY_SCHEMA_VERSION,
        "status": status,
        "sequence": event["sequence"],
        "reason": reason,
        "changed_at": event["occurred_at"],
        "changed_by": actor,
        "event_digest": event["event_digest"],
    }
    state["state_digest"] = _state_digest(state)
    _append_event(journal_path, event)
    _write_atomic(state_path, state)
    return state


def initialize_safety_state(
    *, state_path: str | Path, journal_path: str | Path, actor: str, now: datetime
) -> dict[str, Any]:
    if Path(state_path).exists():
        return load_safety_state(state_path)
    return _transition(
        state_path=Path(state_path),
        journal_path=Path(journal_path),
        status="frozen",
        event_type="initialized_frozen",
        request_id="initialization",
        actor=actor,
        reason="initialization_requires_reconciliation",
        now=now,
    )


def contain_runtime(
    *,
    state_path: str | Path,
    journal_path: str | Path,
    action: str,
    request_id: str,
    actor: str,
    reason: str,
    now: datetime,
) -> dict[str, Any]:
    if action not in {"freeze", "kill"}:
        raise RuntimeSafetyError("containment action must be freeze or kill")
    if not request_id or len(reason.strip()) < 5:
        raise RuntimeSafetyError("containment request and reason are required")
    return _transition(
        state_path=Path(state_path),
        journal_path=Path(journal_path),
        status="killed" if action == "kill" else "frozen",
        event_type=f"runtime_{action}",
        request_id=request_id,
        actor=actor,
        reason=reason.strip(),
        now=now,
    )


def require_runtime_ready(
    path: str | Path, journal_path: str | Path | None = None
) -> dict[str, Any]:
    state = load_safety_state(path)
    if journal_path is not None:
        events = verify_safety_journal(journal_path)
        if not events or events[-1]["event_digest"] != state["event_digest"]:
            raise RuntimeSafetyError("execution safety state and journal diverge")
    if state["status"] != "ready":
        raise RuntimeSafetyError(f"execution runtime is {state['status']}")
    return state


def recover_runtime(
    *,
    state_path: str | Path,
    journal_path: str | Path,
    request_id: str,
    actor: str,
    reason: str,
    now: datetime,
    exec004_receipt: ProducerReceipt | dict[str, Any],
    authority_record: dict[str, Any],
    maximum_authority_age_seconds: float = 900.0,
) -> dict[str, Any]:
    current = load_safety_state(state_path)
    if current["status"] not in {"frozen", "killed"}:
        raise RuntimeSafetyError("only a contained runtime can be recovered")
    oms = _receipt(exec004_receipt, "EXEC-004")
    reconciliation = oms["result"].get("reconciliation") or {}
    if (
        oms["result"].get("qualified") is not True
        or reconciliation.get("submission_allowed") is not True
    ):
        raise RuntimeSafetyError(
            "human recovery requires clean EXEC-004 reconciliation"
        )
    if (
        authority_record.get("decision_type") != "execution-recovery"
        or authority_record.get("action") != "resume"
    ):
        raise RuntimeSafetyError("human recovery authority is invalid")
    if authority_record.get("outcome") != "authorized":
        raise RuntimeSafetyError("human recovery was not authorized")
    if authority_record.get("object_digest") != current["state_digest"]:
        raise RuntimeSafetyError(
            "human recovery authority targets a different safety state"
        )
    if actor != authority_record.get("actor"):
        raise RuntimeSafetyError("recovery actor does not match authority record")
    if not set(authority_record.get("effective_roles") or []).intersection(
        {"founder", "production", "governance"}
    ):
        raise RuntimeSafetyError("recovery actor lacks an accountable role")
    if not is_sha256(authority_record.get("record_digest")):
        raise RuntimeSafetyError("authority record digest is invalid")
    decided_at = _time(authority_record.get("created_at"), "authority.created_at")
    age_seconds = (_time(now, "now") - decided_at).total_seconds()
    if age_seconds < 0 or age_seconds > maximum_authority_age_seconds:
        raise RuntimeSafetyError("human recovery authority is stale")
    return _transition(
        state_path=Path(state_path),
        journal_path=Path(journal_path),
        status="ready",
        event_type="human_recovery",
        request_id=request_id,
        actor=actor,
        reason=reason,
        now=now,
        details={
            "exec004_receipt_digest": oms["receipt_digest"],
            "authority_record_digest": authority_record["record_digest"],
        },
    )


def runtime_safety_receipt(
    *,
    dependency_receipts: dict[str, ProducerReceipt | dict[str, Any]],
    events: Iterable[dict[str, Any]],
    drill_results: dict[str, Any],
    dataset_digest: str,
    source_commit: str,
    configuration: dict[str, Any],
) -> ProducerReceipt:
    expected = {"EXEC-004", "EXEC-006", "RISK-005"}
    if set(dependency_receipts) != expected:
        raise RuntimeSafetyError("dependency receipt set is incomplete or unexpected")
    receipts = {key: _receipt(value, key) for key, value in dependency_receipts.items()}
    if any(item["dataset_digest"] != dataset_digest for item in receipts.values()):
        raise RuntimeSafetyError("dependency dataset digests do not match")
    event_list = list(events)
    if not event_list or not all(
        is_sha256(item.get("event_digest")) for item in event_list
    ):
        raise RuntimeSafetyError("verified safety journal events are required")
    required_drills = {"partition", "corrupt_state", "runaway_orders", "human_recovery"}
    if set(drill_results) != required_drills or not all(drill_results.values()):
        raise RuntimeSafetyError("all EXEC-007 drills must pass")
    result = {
        "schema_version": RUNTIME_SAFETY_SCHEMA_VERSION,
        "runtime_safety_schema_digest": digest(RUNTIME_SAFETY_SPECIFICATION),
        "dependency_receipts": {
            key.lower().replace("-", ""): item["receipt_digest"]
            for key, item in sorted(receipts.items())
        },
        "event_chain_head": event_list[-1]["event_digest"],
        "event_count": len(event_list),
        "drill_results": drill_results,
        "qualified": True,
        "kill_control_plane_independent": True,
        "human_recovery_required": True,
        "claim": "runtime containment and recovery qualification evidence only; no order, capital, allocation, or promotion authority",
    }
    return build_receipt(
        milestone="EXEC-007",
        producer="bt.institutional.runtime_safety.runtime_safety_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={"events": event_list, "dependencies": receipts},
        dataset_digest=dataset_digest,
        configuration=configuration,
        artifacts={
            "event_chain_head": result["event_chain_head"],
            "drill_results": drill_results,
        },
        result=result,
    )
