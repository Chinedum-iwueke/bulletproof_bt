from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from bt.exec.shadow.journal import ProspectiveJournal
from bt.logging.formatting import write_json_deterministic
from bt.logging.jsonl import JsonlWriter


class ExecArtifactWriters:
    def __init__(
        self,
        *,
        run_dir: Path,
        run_id: str,
        mode: str,
        config: dict[str, Any],
        data_path: str,
        resumed_from_run_id: str | None = None,
    ) -> None:
        self.run_dir = run_dir
        self.run_id = run_id
        self.mode = mode
        self.resumed_from_run_id = resumed_from_run_id
        self._status_path = run_dir / "run_status.json"
        self.decisions = JsonlWriter(run_dir / "decisions.jsonl")
        self.orders = JsonlWriter(run_dir / "orders.jsonl")
        self.fills = JsonlWriter(run_dir / "fills.jsonl")
        self.heartbeat = JsonlWriter(run_dir / "heartbeat.jsonl")
        self.reconciliation = JsonlWriter(run_dir / "reconciliation.jsonl")
        self.incidents = JsonlWriter(run_dir / "incidents.jsonl")
        journal_cfg = config.get("shadow_journal", {})
        self.shadow_journal = None
        if mode == "shadow" and isinstance(journal_cfg, dict) and journal_cfg.get("enabled"):
            self.shadow_journal = ProspectiveJournal(
                run_dir / "prospective_journal.jsonl",
                run_id=run_id,
                bindings=dict(journal_cfg.get("bindings") or {}),
            )
        self.write_manifest(config=config, data_path=data_path)

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    def write_manifest(self, *, config: dict[str, Any], data_path: str) -> None:
        payload: dict[str, Any] = {
            "schema_version": 1,
            "run_id": self.run_id,
            "mode": self.mode,
            "created_at_utc": self._utc_now(),
            "data_path": data_path,
            "strategy": (config.get("strategy", {}) or {}).get("name"),
        }
        if self.resumed_from_run_id:
            payload["resumed_from_run_id"] = self.resumed_from_run_id
        write_json_deterministic(self.run_dir / "run_manifest.json", payload)

    def write_status(self, *, state: str, error: str | None = None, extra: dict[str, Any] | None = None) -> None:
        payload: dict[str, Any] = {
            "schema_version": 2,
            "run_id": self.run_id,
            "mode": self.mode,
            "state": state,
            "updated_at_utc": self._utc_now(),
        }
        if self._status_path.exists():
            existing = json.loads(self._status_path.read_text(encoding="utf-8"))
            if isinstance(existing, dict):
                payload = {**existing, **payload}
        if error:
            payload["error"] = error
        if self.resumed_from_run_id:
            payload["resumed_from_run_id"] = self.resumed_from_run_id
        if extra:
            payload.update(extra)
        write_json_deterministic(self._status_path, payload)

    def close(self) -> None:
        if self.shadow_journal is not None:
            self.shadow_journal.seal()
        self.decisions.close()
        self.orders.close()
        self.fills.close()
        self.heartbeat.close()
        self.reconciliation.close()
        self.incidents.close()

    @staticmethod
    def _normalize_record(record: object) -> dict[str, Any]:
        if is_dataclass(record):
            return asdict(cast(Any, record))
        if isinstance(record, dict):
            return record
        raise TypeError(f"Unsupported artifact record type: {type(record)!r}")

    def write_decision(self, record: object) -> None:
        normalized = self._normalize_record(record)
        self.decisions.write(normalized)
        self._journal("decision", normalized)

    def write_order(self, record: object) -> None:
        normalized = self._normalize_record(record)
        self.orders.write(normalized)
        self._journal("order_intent", normalized)

    def write_fill(self, record: object) -> None:
        normalized = self._normalize_record(record)
        self.fills.write(normalized)
        self._journal("fill", normalized)

    def write_heartbeat(self, record: object) -> None:
        normalized = self._normalize_record(record)
        self.heartbeat.write(normalized)
        self._journal("heartbeat", normalized)

    def write_reconciliation(self, record: object) -> None:
        normalized = self._normalize_record(record)
        self.reconciliation.write(normalized)
        self._journal("reconciliation", normalized)

    def write_incident(self, record: object) -> None:
        normalized = self._normalize_record(record)
        self.incidents.write(normalized)
        self._journal("incident", normalized)

    def _journal(self, event_type: str, record: dict[str, Any]) -> None:
        if self.shadow_journal is None:
            return
        explicit_id = record.get("event_id")
        order_id = record.get("order_id")
        lifecycle = record.get("event")
        event_id = (
            str(explicit_id)
            if explicit_id
            else f"{order_id}:{lifecycle}"
            if order_id and lifecycle
            else str(order_id)
            if order_id
            else None
        )
        observed_at = record.get("ts")
        self.shadow_journal.append(
            event_type,
            record,
            event_id=None if event_id is None else f"{event_type}:{event_id}",
            observed_at=None if observed_at is None else str(observed_at),
        )
