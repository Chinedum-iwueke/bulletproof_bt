from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from orchestrator.db import ResearchDB


ROOT = Path(__file__).resolve().parents[1]


def test_disc010_queue_is_deduplicated_and_lower_priority(tmp_path):
    specification = tmp_path / "specification.json"
    bindings = tmp_path / "bindings.json"
    output = tmp_path / "receipt.json"
    specification.write_text("{}", encoding="utf-8")
    bindings.write_text("{}", encoding="utf-8")
    assignment = tmp_path / "assignment.json"
    assignment.write_text(
        json.dumps(
            {
                "schema_version": "disc010-capacity-assignment-v1.0.0",
                "family_id": "cross-asset-v1",
                "specification": str(specification),
                "bindings": str(bindings),
                "data_root": str(tmp_path),
                "output": str(output),
                "source_commit": "a" * 40,
                "max_workers": 6,
            }
        ),
        encoding="utf-8",
    )
    database = tmp_path / "capacity.sqlite"
    command = [
        sys.executable,
        os.fspath(ROOT / "scripts" / "queue_disc010_signal_screen.py"),
        "--db",
        os.fspath(database),
        "--assignment",
        os.fspath(assignment),
        "--repository-root",
        os.fspath(ROOT),
    ]
    environment = {**os.environ, "PYTHONPATH": os.fspath(ROOT / "src")}

    first = subprocess.run(
        command, check=True, capture_output=True, text=True, env=environment
    )
    event = json.loads(first.stdout)
    assert event["workers"] == 6
    assert event["priority"] == 10
    db = ResearchDB(database)
    row = db.connect().execute("SELECT * FROM queues").fetchone()
    assert row["queue_name"] == "approved_backtests"
    assert row["item_type"] == "disc010_signal_screen"
    assert row["priority"] == 10
    payload = json.loads(row["payload_json"])
    assert payload["kind"] == "disc010_signal_screen"
    assert payload["max_workers"] == 6
    db.close()

    duplicate = subprocess.run(
        command, check=True, capture_output=True, text=True, env=environment
    )
    duplicate_event = json.loads(duplicate.stdout)
    assert duplicate_event == {
        "authority": "research_only",
        "event": "disc010_signal_screen_already_registered",
        "priority": 10,
        "queue_id": row["id"],
        "status": "PENDING",
        "workers": 6,
    }
    db = ResearchDB(database)
    count = db.connect().execute("SELECT COUNT(*) AS n FROM queues").fetchone()
    assert count["n"] == 1
    db.close()
