"""Status and completion helpers for hypothesis parallel runs."""
from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from bt.logging.run_contract import REQUIRED_ARTIFACTS

STATUS_COLUMNS = [
    "row_id",
    "variant_id",
    "tier",
    "status",
    "return_code",
    "started_at",
    "ended_at",
    "duration_sec",
    "output_dir",
    "error_message",
]


@dataclass(frozen=True)
class RunArtifactStatus:
    state: str
    return_code: int | None
    message: str


def detect_run_artifact_status(run_dir: Path) -> RunArtifactStatus:
    try:
        exists = run_dir.exists()
    except OSError as exc:
        return RunArtifactStatus(state="INCOMPLETE", return_code=None, message=f"run directory inaccessible: {exc}")
    if not exists:
        return RunArtifactStatus(state="MISSING", return_code=None, message="run directory missing")
    status_path = run_dir / "run_status.json"
    if not status_path.exists():
        return RunArtifactStatus(state="INCOMPLETE", return_code=None, message="run_status.json missing")

    try:
        payload = json.loads(status_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return RunArtifactStatus(state="INCOMPLETE", return_code=None, message="run_status.json unreadable")
    if not isinstance(payload, dict):
        return RunArtifactStatus(state="INCOMPLETE", return_code=None, message="run_status.json invalid shape")

    status_value = str(payload.get("status", "")).upper()
    if status_value == "PASS":
        missing_required = sorted(name for name in REQUIRED_ARTIFACTS if not (run_dir / name).exists())
        if missing_required:
            return RunArtifactStatus(
                state="INCOMPLETE",
                return_code=None,
                message=f"missing artifacts: {','.join(missing_required)}",
            )
        return RunArtifactStatus(state="SUCCESS", return_code=0, message="PASS")
    if status_value == "FAIL":
        return RunArtifactStatus(state="FAILED", return_code=1, message=str(payload.get("error_message", "")))
    return RunArtifactStatus(state="INCOMPLETE", return_code=None, message=f"unknown status={status_value!r}")


def write_status_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=STATUS_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in STATUS_COLUMNS})
