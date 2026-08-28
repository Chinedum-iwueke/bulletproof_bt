"""Canonical dual-clock execution events and deterministic journal replay."""

from __future__ import annotations

import sqlite3
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from .receipt import ProducerReceipt, build_receipt, digest, verify_receipt

EVENT_SCHEMA_VERSION = "canonical-execution-event-v1.0.0"
JOURNAL_SCHEMA_VERSION = "canonical-execution-journal-v1.0.0"


class CanonicalEventError(ValueError):
    """An event violates identity, clock, sequence, or correction invariants."""


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise CanonicalEventError(f"{name} must be timezone-aware UTC")
    return value.astimezone(UTC)


@dataclass(frozen=True)
class CanonicalExecutionEvent:
    schema_version: str
    event_id: str
    source: str
    stream: str
    kind: str
    instrument_id: str | None
    event_time: str
    receive_time: str
    source_sequence: int
    correction_of: str | None
    payload: dict[str, Any]
    payload_digest: str
    event_digest: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def canonical_event(
    *,
    event_id: str,
    source: str,
    stream: str,
    kind: str,
    event_time: datetime,
    receive_time: datetime,
    source_sequence: int,
    payload: dict[str, Any],
    instrument_id: str | None = None,
    correction_of: str | None = None,
    maximum_future_drift_seconds: float = 5.0,
) -> CanonicalExecutionEvent:
    event_clock = _utc(event_time, "event_time")
    receive_clock = _utc(receive_time, "receive_time")
    if not all(value and value.strip() for value in (event_id, source, stream, kind)):
        raise CanonicalEventError("event identity fields must be non-empty")
    if source_sequence < 0:
        raise CanonicalEventError("source_sequence must be non-negative")
    drift = (event_clock - receive_clock).total_seconds()
    if drift > maximum_future_drift_seconds:
        raise CanonicalEventError("event clock exceeds the receive clock drift limit")
    payload_hash = digest(payload)
    core = {
        "schema_version": EVENT_SCHEMA_VERSION,
        "event_id": event_id,
        "source": source,
        "stream": stream,
        "kind": kind,
        "instrument_id": instrument_id,
        "event_time": event_clock.isoformat(),
        "receive_time": receive_clock.isoformat(),
        "source_sequence": source_sequence,
        "correction_of": correction_of,
        "payload": payload,
        "payload_digest": payload_hash,
    }
    return CanonicalExecutionEvent(**core, event_digest=digest(core))


def verify_event(event: CanonicalExecutionEvent | dict[str, Any]) -> bool:
    document = event.as_dict() if isinstance(event, CanonicalExecutionEvent) else dict(event)
    event_hash = document.pop("event_digest", None)
    if document.get("schema_version") != EVENT_SCHEMA_VERSION:
        return False
    if document.get("payload_digest") != digest(document.get("payload")):
        return False
    return event_hash == digest(document)


class CanonicalEventJournal:
    """Append-only SQLite journal with point-in-time deterministic replay."""

    def __init__(self, path: str | Path) -> None:
        self._connection = sqlite3.connect(str(path))
        self._connection.row_factory = sqlite3.Row
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS canonical_execution_events (
                ingest_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL,
                source TEXT NOT NULL,
                stream TEXT NOT NULL,
                source_sequence INTEGER NOT NULL,
                receive_time TEXT NOT NULL,
                event_time TEXT NOT NULL,
                correction_of TEXT,
                event_digest TEXT NOT NULL UNIQUE,
                document TEXT NOT NULL,
                UNIQUE(source, stream, event_id)
            )
            """
        )
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()

    def append(self, event: CanonicalExecutionEvent) -> dict[str, Any]:
        import json

        if not verify_event(event):
            raise CanonicalEventError("event digest does not match its content")
        document = event.as_dict()
        existing = self._connection.execute(
            "SELECT event_digest FROM canonical_execution_events WHERE source=? AND stream=? AND event_id=?",
            (event.source, event.stream, event.event_id),
        ).fetchone()
        if existing:
            if existing["event_digest"] != event.event_digest:
                raise CanonicalEventError("event identity was reused with different content")
            return {"status": "duplicate", "gap": False, "late": False}
        if event.correction_of:
            target = self._connection.execute(
                "SELECT source, stream FROM canonical_execution_events WHERE event_id=?",
                (event.correction_of,),
            ).fetchone()
            if target is None:
                raise CanonicalEventError("correction target is absent from the journal")
            if target["source"] != event.source or target["stream"] != event.stream:
                raise CanonicalEventError("correction target belongs to another stream")
        maximum = self._connection.execute(
            "SELECT MAX(source_sequence) AS value FROM canonical_execution_events WHERE source=? AND stream=?",
            (event.source, event.stream),
        ).fetchone()["value"]
        late = maximum is not None and event.source_sequence <= maximum
        gap = maximum is not None and event.source_sequence > maximum + 1
        self._connection.execute(
            """INSERT INTO canonical_execution_events
            (event_id, source, stream, source_sequence, receive_time, event_time,
             correction_of, event_digest, document) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                event.event_id,
                event.source,
                event.stream,
                event.source_sequence,
                event.receive_time,
                event.event_time,
                event.correction_of,
                event.event_digest,
                json.dumps(document, sort_keys=True, separators=(",", ":")),
            ),
        )
        self._connection.commit()
        return {"status": "appended", "gap": gap, "late": late}

    def replay(self, *, known_at: datetime | None = None) -> dict[str, Any]:
        import json

        cutoff = _utc(known_at, "known_at").isoformat() if known_at else None
        rows = self._connection.execute(
            """SELECT document FROM canonical_execution_events
            WHERE (? IS NULL OR receive_time <= ?)
            ORDER BY event_time, source, stream, source_sequence, receive_time, event_digest""",
            (cutoff, cutoff),
        ).fetchall()
        events = [json.loads(row["document"]) for row in rows]
        superseded = {event["correction_of"] for event in events if event["correction_of"]}
        active = [event for event in events if event["event_id"] not in superseded]
        sequence_gaps: list[dict[str, Any]] = []
        by_stream: dict[tuple[str, str], list[int]] = {}
        for event in events:
            by_stream.setdefault((event["source"], event["stream"]), []).append(event["source_sequence"])
        for (source, stream), values in sorted(by_stream.items()):
            unique = sorted(set(values))
            for left, right in zip(unique, unique[1:]):
                if right > left + 1:
                    sequence_gaps.append({"source": source, "stream": stream, "after": left, "before": right})
        result = {
            "schema_version": JOURNAL_SCHEMA_VERSION,
            "known_at": cutoff,
            "event_count": len(events),
            "active_event_count": len(active),
            "events": active,
            "sequence_gaps": sequence_gaps,
            "replay_digest": digest(active),
        }
        return result


def execution_journal_receipt(
    *,
    events: Iterable[CanonicalExecutionEvent],
    data_reference_receipt: dict[str, Any],
    market_catalog_receipt: dict[str, Any],
    source_commit: str,
    dataset_digest: str,
    configuration: dict[str, Any],
) -> ProducerReceipt:
    if not verify_receipt(data_reference_receipt) or data_reference_receipt["milestone"] != "DATA-001":
        raise CanonicalEventError("DATA-001 receipt is not valid")
    if not verify_receipt(market_catalog_receipt) or market_catalog_receipt["milestone"] != "DATA-002":
        raise CanonicalEventError("DATA-002 receipt is not valid")
    event_list = list(events)
    journal = CanonicalEventJournal(":memory:")
    outcomes = []
    try:
        for event in event_list:
            outcomes.append(journal.append(event))
        replay = journal.replay()
    finally:
        journal.close()
    result = {
        "schema_version": "exec001-canonical-journal-dossier-v1.0.0",
        "data_reference_receipt_digest": data_reference_receipt["receipt_digest"],
        "market_catalog_receipt_digest": market_catalog_receipt["receipt_digest"],
        "event_schema_version": EVENT_SCHEMA_VERSION,
        "event_schema_digest": digest({"schema_version": EVENT_SCHEMA_VERSION}),
        "journal_schema_version": JOURNAL_SCHEMA_VERSION,
        "event_count": replay["event_count"],
        "active_event_count": replay["active_event_count"],
        "sequence_gaps": replay["sequence_gaps"],
        "late_event_count": sum(outcome["late"] for outcome in outcomes),
        "duplicate_event_count": sum(outcome["status"] == "duplicate" for outcome in outcomes),
        "replay_digest": replay["replay_digest"],
        "reconstructable": not replay["sequence_gaps"],
        "claim": "canonical event and replay evidence only; no allocation, capital or order authority",
    }
    event_documents = [event.as_dict() for event in event_list]
    return build_receipt(
        milestone="EXEC-001",
        producer="bt.institutional.execution.execution_journal_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={
            "events": event_documents,
            "data_reference_receipt": data_reference_receipt["receipt_digest"],
            "market_catalog_receipt": market_catalog_receipt["receipt_digest"],
        },
        dataset_digest=dataset_digest,
        configuration=configuration,
        artifacts={"event_digests": [event.event_digest for event in event_list], "replay_digest": replay["replay_digest"]},
        result=result,
    )
