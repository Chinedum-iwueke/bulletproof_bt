from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from bt.logging.formatting import write_json_deterministic


@dataclass(frozen=True)
class SessionSummary:
    run_id: str
    resumed_from_run_id: str | None
    mode: str
    environment: str
    broker_venue: str
    start_time: str | None
    end_time: str | None
    final_status: str
    frozen: bool
    freeze_reason: str | None
    startup_gate_result: str | None
    startup_gate_reason: str | None
    reconciliation_records: int
    reconciliation_material_mismatch_count: int
    order_count: int
    fill_count: int
    rejected_order_count: int
    transport_error_count: int
    stale_stream_incident_count: int
    canary_block_count: int
    trading_enabled_ever: bool
    mutation_enabled_ever: bool


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw:
            continue
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            rows.append(parsed)
    return rows


def build_session_summary(run_dir: Path) -> SessionSummary:
    manifest = _read_json(run_dir / "run_manifest.json")
    status = _read_json(run_dir / "run_status.json")
    orders = _read_jsonl(run_dir / "orders.jsonl")
    fills = _read_jsonl(run_dir / "fills.jsonl")
    recs = _read_jsonl(run_dir / "reconciliation.jsonl")
    incidents = _read_jsonl(run_dir / "incidents.jsonl")
    start_time = manifest.get("created_at_utc") if isinstance(manifest.get("created_at_utc"), str) else None
    end_time = status.get("updated_at_utc") if isinstance(status.get("updated_at_utc"), str) else None

    rejected = sum(1 for o in orders if str(o.get("event", "")) == "submit_rejected")
    transport_errors = sum(1 for i in incidents if str(i.get("incident_type", "")).startswith("transport"))
    stale_stream_incidents = sum(1 for i in incidents if str(i.get("incident_type", "")) == "private_stream_stale")
    canary_blocks = sum(1 for i in incidents if str(i.get("taxonomy", "")) == "canary")
    material_mismatch = sum(int((r.get("material_mismatch_count", 0) or 0)) for r in recs)

    return SessionSummary(
        run_id=str(manifest.get("run_id", run_dir.name)),
        resumed_from_run_id=manifest.get("resumed_from_run_id") if isinstance(manifest.get("resumed_from_run_id"), str) else None,
        mode=str(manifest.get("mode", "unknown")),
        environment=str(status.get("environment", "unknown")),
        broker_venue=str(status.get("broker_venue", "unknown")),
        start_time=start_time,
        end_time=end_time,
        final_status=str(status.get("state", "unknown")),
        frozen=bool(status.get("frozen", False)),
        freeze_reason=status.get("freeze_reason") if isinstance(status.get("freeze_reason"), str) else None,
        startup_gate_result=status.get("startup_gate_result") if isinstance(status.get("startup_gate_result"), str) else None,
        startup_gate_reason=status.get("startup_gate_reason") if isinstance(status.get("startup_gate_reason"), str) else None,
        reconciliation_records=len(recs),
        reconciliation_material_mismatch_count=material_mismatch,
        order_count=len(orders),
        fill_count=len(fills),
        rejected_order_count=rejected,
        transport_error_count=transport_errors,
        stale_stream_incident_count=stale_stream_incidents,
        canary_block_count=canary_blocks,
        trading_enabled_ever=bool(status.get("trading_enabled_ever", False)),
        mutation_enabled_ever=bool(status.get("mutation_enabled_ever", False)),
    )


def write_session_summary(run_dir: Path, summary: SessionSummary) -> Path:
    path = run_dir / "session_summary.json"
    write_json_deterministic(path, asdict(summary))
    return path
