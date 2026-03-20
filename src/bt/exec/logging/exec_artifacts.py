from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from dataclasses import asdict, is_dataclass
from typing import Any

from bt.logging.formatting import write_json_deterministic
from bt.logging.jsonl import JsonlWriter


class ExecArtifactWriters:
    def __init__(self, *, run_dir: Path, run_id: str, mode: str, config: dict[str, Any], data_path: str) -> None:
        self.run_dir = run_dir
        self.run_id = run_id
        self.mode = mode
        self._status_path = run_dir / "run_status.json"
        self.decisions = JsonlWriter(run_dir / "decisions.jsonl")
        self.orders = JsonlWriter(run_dir / "orders.jsonl")
        self.fills = JsonlWriter(run_dir / "fills.jsonl")
        self.heartbeat = JsonlWriter(run_dir / "heartbeat.jsonl")
        self.reconciliation = JsonlWriter(run_dir / "reconciliation.jsonl")
        self.write_manifest(config=config, data_path=data_path)

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def write_manifest(self, *, config: dict[str, Any], data_path: str) -> None:
        write_json_deterministic(
            self.run_dir / "run_manifest.json",
            {
                "schema_version": 1,
                "run_id": self.run_id,
                "mode": self.mode,
                "created_at_utc": self._utc_now(),
                "data_path": data_path,
                "strategy": (config.get("strategy", {}) or {}).get("name"),
            },
        )

    def write_status(self, *, state: str, error: str | None = None) -> None:
        payload: dict[str, Any] = {"schema_version": 1, "run_id": self.run_id, "mode": self.mode, "state": state, "updated_at_utc": self._utc_now()}
        if error:
            payload["error"] = error
        write_json_deterministic(self._status_path, payload)

    def close(self) -> None:
        self.decisions.close()
        self.orders.close()
        self.fills.close()
        self.heartbeat.close()
        self.reconciliation.close()

    @staticmethod
    def _normalize_record(record: object) -> dict[str, Any]:
        if is_dataclass(record):
            return asdict(record)
        if isinstance(record, dict):
            return record
        raise TypeError(f"Unsupported artifact record type: {type(record)!r}")

    def write_decision(self, record: object) -> None:
        self.decisions.write(self._normalize_record(record))

    def write_order(self, record: object) -> None:
        self.orders.write(self._normalize_record(record))

    def write_fill(self, record: object) -> None:
        self.fills.write(self._normalize_record(record))

    def write_heartbeat(self, record: object) -> None:
        self.heartbeat.write(self._normalize_record(record))
