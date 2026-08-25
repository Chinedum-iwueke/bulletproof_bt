"""Hash-chained prospective execution journal and deterministic replay."""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from bt.logging.jsonl import to_jsonable

SCHEMA_VERSION = "shadow-journal-v1.0.0"
GENESIS_DIGEST = "0" * 64
REQUIRED_BINDINGS = (
    "candidate_digest",
    "dataset_digest",
    "strategy_digest",
    "cost_model_digest",
    "source_commit",
)


class JournalError(ValueError):
    """The prospective journal contract is invalid or has been violated."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        to_jsonable(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _validate_observed_at(value: str) -> None:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise JournalError("observed_at must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise JournalError("observed_at must be timezone-aware")


def _validate_bindings(bindings: dict[str, Any]) -> dict[str, str]:
    normalized = {key: str(value).strip().lower() for key, value in bindings.items()}
    missing = [key for key in REQUIRED_BINDINGS if not normalized.get(key)]
    if missing:
        raise JournalError(f"shadow journal bindings are missing: {', '.join(missing)}")
    for key in REQUIRED_BINDINGS[:-1]:
        value = normalized[key]
        if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
            raise JournalError(f"{key} must be a lowercase sha256 digest")
    commit = normalized["source_commit"]
    if len(commit) != 40 or any(char not in "0123456789abcdef" for char in commit):
        raise JournalError("source_commit must be a lowercase Git SHA-1")
    return normalized


class ProspectiveJournal:
    """Append-only journal whose records form a durable digest chain."""

    def __init__(self, path: Path, *, run_id: str, bindings: dict[str, Any]) -> None:
        self.path = path
        self.run_id = run_id
        self.bindings = _validate_bindings(bindings)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._sequence = 0
        self._previous_digest = GENESIS_DIGEST
        self._seen: set[str] = set()
        if self.path.exists() and self.path.stat().st_size:
            report = replay_journal(self.path, expected_bindings=self.bindings)
            if report["sealed"]:
                raise JournalError("sealed shadow journal cannot be appended")
            self._sequence = int(report["event_count"])
            self._previous_digest = str(report["journal_digest"])
            self._seen = set(report["event_ids"])
        self._file = self.path.open("a", encoding="ascii")
        if self._sequence == 0:
            self.append(
                "session_started",
                {
                    "authority": {
                        "capital": "prohibited",
                        "live_orders": "prohibited",
                        "venue_mutation": "prohibited",
                    },
                    "bindings": self.bindings,
                },
                event_id=f"{run_id}:session_started",
            )

    def append(
        self,
        event_type: str,
        payload: Any,
        *,
        event_id: str | None = None,
        observed_at: str | None = None,
    ) -> bool:
        if not event_type.strip():
            raise JournalError("event_type is required")
        normalized_payload = to_jsonable(
            asdict(cast(Any, payload)) if is_dataclass(payload) else payload
        )
        payload_digest = _digest(normalized_payload)
        stable_id = event_id or _digest(
            {"event_type": event_type, "payload_digest": payload_digest}
        )
        if stable_id in self._seen:
            return False
        sequence = self._sequence + 1
        observation_time = observed_at or _utc_now()
        _validate_observed_at(observation_time)
        core = {
            "schema_version": SCHEMA_VERSION,
            "run_id": self.run_id,
            "sequence": sequence,
            "event_id": stable_id,
            "event_type": event_type,
            "observed_at": observation_time,
            "payload": normalized_payload,
            "payload_digest": payload_digest,
            "previous_digest": self._previous_digest,
        }
        record = core | {"record_digest": _digest(core)}
        self._file.write(_canonical(record).decode("ascii") + "\n")
        self._file.flush()
        os.fsync(self._file.fileno())
        self._sequence = sequence
        self._previous_digest = record["record_digest"]
        self._seen.add(stable_id)
        return True

    def seal(self) -> dict[str, Any]:
        self.append(
            "session_sealed",
            {"event_count_before_seal": self._sequence},
            event_id=f"{self.run_id}:session_sealed",
        )
        self.close()
        return replay_journal(self.path, expected_bindings=self.bindings)

    def close(self) -> None:
        if not self._file.closed:
            self._file.flush()
            os.fsync(self._file.fileno())
            self._file.close()


def replay_journal(
    path: Path, *, expected_bindings: dict[str, Any] | None = None
) -> dict[str, Any]:
    previous = GENESIS_DIGEST
    expected_sequence = 1
    seen: set[str] = set()
    counts: dict[str, int] = {}
    bindings: dict[str, str] | None = None
    cost_totals = {"fee_cost": 0.0, "slippage_cost": 0.0, "spread_cost": 0.0}
    sealed = False
    for line_number, raw in enumerate(path.read_text(encoding="ascii").splitlines(), start=1):
        try:
            record = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise JournalError(f"invalid JSON at journal line {line_number}") from exc
        core = {key: value for key, value in record.items() if key != "record_digest"}
        if record.get("record_digest") != _digest(core):
            raise JournalError(f"record digest mismatch at sequence {expected_sequence}")
        if record.get("sequence") != expected_sequence:
            raise JournalError(f"out-of-order sequence at line {line_number}")
        if record.get("previous_digest") != previous:
            raise JournalError(f"digest chain mismatch at sequence {expected_sequence}")
        event_id = str(record.get("event_id", ""))
        if not event_id or event_id in seen:
            raise JournalError(f"duplicate or missing event_id at sequence {expected_sequence}")
        if record.get("payload_digest") != _digest(record.get("payload")):
            raise JournalError(f"payload digest mismatch at sequence {expected_sequence}")
        _validate_observed_at(str(record.get("observed_at", "")))
        event_type = str(record.get("event_type"))
        counts[event_type] = counts.get(event_type, 0) + 1
        if expected_sequence == 1:
            if event_type != "session_started":
                raise JournalError("first event must be session_started")
            payload = record.get("payload") or {}
            authority = payload.get("authority") or {}
            if any(authority.get(key) != "prohibited" for key in ("capital", "live_orders", "venue_mutation")):
                raise JournalError("shadow journal does not prohibit capital and venue mutation")
            bindings = _validate_bindings(payload.get("bindings") or {})
        if sealed:
            raise JournalError("records exist after session seal")
        sealed = event_type == "session_sealed"
        if event_type == "fill":
            payload = record.get("payload") or {}
            for field in cost_totals:
                cost_totals[field] += float(payload.get(field, 0.0) or 0.0)
        previous = str(record["record_digest"])
        seen.add(event_id)
        expected_sequence += 1
    if bindings is None:
        raise JournalError("journal is empty")
    if expected_bindings is not None and bindings != _validate_bindings(expected_bindings):
        raise JournalError("journal bindings do not match expected bindings")
    return {
        "schema_version": "shadow-replay-report-v1.0.0",
        "success": sealed,
        "sealed": sealed,
        "event_count": expected_sequence - 1,
        "event_ids": sorted(seen),
        "event_counts": counts,
        "journal_digest": previous,
        "bindings": bindings,
        "cost_totals": {key: round(value, 12) for key, value in cost_totals.items()},
        "capital_or_order_authority": False,
    }
