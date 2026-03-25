from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

import pandas as pd

from bt.logging.formatting import write_json_deterministic
from bt.logging.jsonl import JsonlWriter


class IncidentSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class IncidentTaxonomy(str, Enum):
    STARTUP = "startup"
    AUTH = "auth"
    TRANSPORT = "transport"
    STREAM_HEALTH = "stream_health"
    RECONCILE = "reconcile"
    LIFECYCLE = "lifecycle"
    CANARY = "canary"
    FREEZE = "freeze"
    RECOVERY = "recovery"
    CONFIG = "config"
    DOCTOR = "doctor"


@dataclass(frozen=True)
class IncidentRecord:
    ts: pd.Timestamp
    run_id: str
    incident_type: str
    taxonomy: IncidentTaxonomy
    severity: IncidentSeverity
    message: str
    context: dict[str, Any] = field(default_factory=dict)

    def to_jsonable(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["ts"] = self.ts.isoformat()
        payload["taxonomy"] = self.taxonomy.value
        payload["severity"] = self.severity.value
        return payload


@dataclass(frozen=True)
class IncidentSummary:
    run_id: str
    total_count: int
    counts_by_type: dict[str, int]
    counts_by_severity: dict[str, int]
    first_incident_ts: str | None
    last_incident_ts: str | None
    has_critical: bool
    unresolved_frozen_at_shutdown: bool


class IncidentRecorder:
    def __init__(self, *, run_dir: Path, run_id: str, enabled: bool = True) -> None:
        self.run_id = run_id
        self.run_dir = run_dir
        self.enabled = enabled
        self._writer = JsonlWriter(run_dir / "incidents.jsonl") if enabled else None

    def record(self, incident: IncidentRecord) -> None:
        if not self.enabled or self._writer is None:
            return
        self._writer.write(incident.to_jsonable())

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()


def load_incidents(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw:
            continue
        import json

        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            rows.append(parsed)
    return rows


def summarize_incidents(*, run_id: str, incidents: list[dict[str, Any]], final_status: dict[str, Any] | None = None) -> IncidentSummary:
    counts_by_type: dict[str, int] = {}
    counts_by_severity: dict[str, int] = {}
    times: list[str] = []
    has_critical = False
    for item in incidents:
        itype = str(item.get("incident_type", "unknown"))
        sev = str(item.get("severity", "info"))
        counts_by_type[itype] = counts_by_type.get(itype, 0) + 1
        counts_by_severity[sev] = counts_by_severity.get(sev, 0) + 1
        ts = item.get("ts")
        if isinstance(ts, str):
            times.append(ts)
        if sev == IncidentSeverity.CRITICAL.value:
            has_critical = True

    unresolved_frozen = bool((final_status or {}).get("frozen", False) and (final_status or {}).get("state") != "stopped")

    return IncidentSummary(
        run_id=run_id,
        total_count=len(incidents),
        counts_by_type=dict(sorted(counts_by_type.items())),
        counts_by_severity=dict(sorted(counts_by_severity.items())),
        first_incident_ts=min(times) if times else None,
        last_incident_ts=max(times) if times else None,
        has_critical=has_critical,
        unresolved_frozen_at_shutdown=unresolved_frozen,
    )


def write_incident_summary(*, run_dir: Path, summary: IncidentSummary) -> Path:
    path = run_dir / "incident_summary.json"
    write_json_deterministic(path, asdict(summary))
    return path
