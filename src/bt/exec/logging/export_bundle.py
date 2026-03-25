from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from bt.logging.formatting import write_json_deterministic


DEFAULT_EXPORT_FILES = [
    "run_manifest.json",
    "run_status.json",
    "decisions.jsonl",
    "orders.jsonl",
    "fills.jsonl",
    "heartbeat.jsonl",
    "reconciliation.jsonl",
    "incidents.jsonl",
    "session_summary.json",
    "incident_summary.json",
    "config_used.yaml",
]


def _contains_secret_like_key(obj: Any) -> bool:
    if isinstance(obj, dict):
        for key, value in obj.items():
            lk = str(key).lower()
            if any(token in lk for token in ("secret", "token", "password", "api_key")):
                return True
            if _contains_secret_like_key(value):
                return True
    if isinstance(obj, list):
        return any(_contains_secret_like_key(item) for item in obj)
    return False


def _safe_copy(src: Path, dst: Path) -> bool:
    if not src.exists():
        return False
    if src.name == "config_used.yaml":
        # conservative: do not include config snapshot if it appears to contain secret-like keys.
        import yaml

        parsed = yaml.safe_load(src.read_text(encoding="utf-8"))
        if _contains_secret_like_key(parsed):
            return False
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return True


def export_run_bundle(*, run_dir: Path, export_root: Path, include_files: list[str] | None = None) -> Path:
    files = include_files or list(DEFAULT_EXPORT_FILES)
    out_dir = export_root / run_dir.name
    out_dir.mkdir(parents=True, exist_ok=True)

    copied: list[str] = []
    skipped: list[str] = []
    for rel in files:
        if _safe_copy(run_dir / rel, out_dir / rel):
            copied.append(rel)
        else:
            skipped.append(rel)

    manifest = {
        "schema_version": 1,
        "run_id": run_dir.name,
        "copied_files": copied,
        "skipped_files": skipped,
    }
    write_json_deterministic(out_dir / "export_manifest.json", manifest)
    return out_dir


def list_runs(run_root: Path) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    if not run_root.exists():
        return results
    for candidate in sorted([p for p in run_root.iterdir() if p.is_dir()]):
        status_path = candidate / "run_status.json"
        manifest_path = candidate / "run_manifest.json"
        if not status_path.exists() or not manifest_path.exists():
            continue
        status = json.loads(status_path.read_text(encoding="utf-8"))
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        results.append(
            {
                "run_id": str(manifest.get("run_id", candidate.name)),
                "mode": str(manifest.get("mode", "unknown")),
                "environment": str(status.get("environment", "unknown")),
                "final_status": str(status.get("state", "unknown")),
                "frozen": str(bool(status.get("frozen", False))).lower(),
                "start_time": str(manifest.get("created_at_utc", "")),
                "end_time": str(status.get("updated_at_utc", "")),
                "resumed_from_run_id": str(manifest.get("resumed_from_run_id", "")),
            }
        )
    return results
