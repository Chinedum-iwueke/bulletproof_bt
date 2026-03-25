from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from bt.exec.cli.export_run import export_run
from bt.exec.cli.incidents import list_incidents
from bt.exec.cli.status import build_status_view
from bt.exec.logging.export_bundle import list_runs
from bt.exec.logging.session_summary import build_session_summary
from bt.exec.observability.alerts import Alert, AlertEmitter, AlertEventType, AlertSeverity
from bt.exec.observability.channels import FileAlertChannel
from bt.exec.observability.incidents import IncidentRecord, IncidentSeverity, IncidentTaxonomy, summarize_incidents
from bt.exec.reconcile import ReconciliationResult, ReconciliationScope
from bt.exec.reconcile.policies import ReconciliationAction, ReconciliationDecision, ReconciliationPolicy
from bt.exec.reconcile.reports import reconciliation_record


def _mk_run_dir(tmp_path: Path, run_id: str = "exec_1") -> Path:
    run_dir = tmp_path / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run_manifest.json").write_text(json.dumps({"run_id": run_id, "mode": "paper_simulated", "created_at_utc": "2026-01-01T00:00:00Z"}), encoding="utf-8")
    (run_dir / "run_status.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "mode": "paper_simulated",
                "state": "stopped",
                "environment": "demo",
                "broker_venue": "simulated",
                "frozen": False,
                "read_only": False,
                "trading_enabled": True,
                "trading_enabled_ever": True,
                "mutation_enabled_ever": True,
                "startup_gate_result": "not_applicable",
                "private_stream_ready": True,
                "public_stream_ready": True,
                "updated_at_utc": "2026-01-01T00:01:00Z",
            }
        ),
        encoding="utf-8",
    )
    for name in ("decisions.jsonl", "orders.jsonl", "fills.jsonl", "heartbeat.jsonl", "reconciliation.jsonl", "incidents.jsonl"):
        (run_dir / name).write_text("", encoding="utf-8")
    (run_dir / "config_used.yaml").write_text("risk:\n  max_positions: 1\n", encoding="utf-8")
    return run_dir


def test_alert_serialization_and_file_channel(tmp_path: Path) -> None:
    alert_file = tmp_path / "alerts.jsonl"
    emitter = AlertEmitter(channels=[FileAlertChannel(alert_file)])
    alert = Alert(
        ts=pd.Timestamp("2026-01-01T00:00:00Z"),
        run_id="exec_1",
        event_type=AlertEventType.STARTUP_SUCCEEDED,
        severity=AlertSeverity.INFO,
        message="ok",
    )
    emitter.emit(alert)
    line = alert_file.read_text(encoding="utf-8").strip()
    payload = json.loads(line)
    assert payload["event_type"] == "startup_succeeded"
    assert payload["severity"] == "info"


def test_incident_summary_counts_and_severity() -> None:
    incidents = [
        IncidentRecord(
            ts=pd.Timestamp("2026-01-01T00:00:00Z"),
            run_id="exec_1",
            incident_type="startup_blocked",
            taxonomy=IncidentTaxonomy.STARTUP,
            severity=IncidentSeverity.ERROR,
            message="blocked",
        ).to_jsonable(),
        IncidentRecord(
            ts=pd.Timestamp("2026-01-01T00:01:00Z"),
            run_id="exec_1",
            incident_type="runtime_frozen",
            taxonomy=IncidentTaxonomy.FREEZE,
            severity=IncidentSeverity.CRITICAL,
            message="frozen",
        ).to_jsonable(),
    ]
    summary = summarize_incidents(run_id="exec_1", incidents=incidents, final_status={"state": "failed", "frozen": True})
    assert summary.counts_by_severity["critical"] == 1
    assert summary.counts_by_type["startup_blocked"] == 1
    assert summary.has_critical is True


def test_session_summary_and_export_bundle(tmp_path: Path) -> None:
    run_dir = _mk_run_dir(tmp_path)
    (run_dir / "orders.jsonl").write_text('{"event":"submit_rejected"}\n', encoding="utf-8")
    (run_dir / "reconciliation.jsonl").write_text('{"material_mismatch_count":2,"ts":"2026-01-01T00:01:00Z"}\n', encoding="utf-8")
    summary = build_session_summary(run_dir)
    assert summary.rejected_order_count == 1
    assert summary.reconciliation_material_mismatch_count == 2

    export_dir = export_run(run_dir=run_dir, export_root=tmp_path / "exports")
    manifest = json.loads((export_dir / "export_manifest.json").read_text(encoding="utf-8"))
    assert "run_status.json" in manifest["copied_files"]
    assert "session_summary.json" in manifest["copied_files"]


def test_admin_cli_views(tmp_path: Path) -> None:
    run_dir = _mk_run_dir(tmp_path)
    (run_dir / "heartbeat.jsonl").write_text('{"ts":"2026-01-01T00:00:30Z"}\n', encoding="utf-8")
    (run_dir / "reconciliation.jsonl").write_text('{"ts":"2026-01-01T00:00:40Z"}\n', encoding="utf-8")
    (run_dir / "incidents.jsonl").write_text(
        '{"ts":"2026-01-01T00:00:50Z","severity":"warning","incident_type":"private_stream_stale","message":"stale","taxonomy":"stream_health"}\n',
        encoding="utf-8",
    )

    status_view = build_status_view(run_dir)
    assert status_view["latest_heartbeat_ts"] == "2026-01-01T00:00:30Z"
    assert len(list_incidents(run_dir, limit=10)) == 1

    runs = list_runs(tmp_path)
    assert runs[0]["run_id"] == "exec_1"


def test_run_status_fields_and_reconciliation_serialization() -> None:
    decision = ReconciliationDecision(policy=ReconciliationPolicy.FREEZE_ON_MATERIAL, action=ReconciliationAction.FREEZE, reason="material")
    result = ReconciliationResult(
        ts=pd.Timestamp("2026-01-01T00:00:00Z"),
        scope=ReconciliationScope(True, True, True, True),
        mismatches=[],
        material_mismatch_count=1,
        decision=decision,
    )
    assert reconciliation_record(result)["decision"]["action"] == "freeze"
