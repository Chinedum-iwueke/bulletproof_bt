"""Run an exact governed assignment claimed by the existing capacity scheduler."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
import os

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from orchestrator.db import ResearchDB  # noqa: E402
from orchestrator.alpha_capacity_owner import owner_alive  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--queue-id", required=True)
    parser.add_argument("--queue-name", required=True)
    parser.add_argument("--locked-by", required=True)
    args = parser.parse_args()
    db = ResearchDB(args.db)
    row = db.dequeue_by_id(args.queue_name, args.queue_id, args.locked_by)
    if row is None:
        return 0
    try:
        payload = json.loads(row["payload_json"])
        if not owner_alive(payload):
            raise ValueError("Hermes execution lease owner is no longer alive")
        if payload.get("kind") != "governed_alpha_assignment" or not 1 <= payload["max_workers"] <= 8:
            raise ValueError("Invalid governed capacity queue payload")
        assignment = Path(payload["assignment"]).resolve(strict=True)
        import hashlib
        if hashlib.sha256(assignment.read_bytes()).hexdigest() != payload["assignment_sha256"]:
            raise ValueError("Immutable assignment changed while queued")
        repository = Path(payload["repository_root"]).resolve(strict=True)
        command = [sys.executable, str(repository / "scripts/run_alpha_research_assignment.py"),
                   "--assignment", str(assignment), "--repository-root", payload["repository_root"],
                   "--output", payload["output"], "--receipt", payload["receipt"],
                   "--max-workers", str(payload["max_workers"])]
        result = subprocess.run(command, check=False, cwd=repository,
                                env={**os.environ, "PYTHONPATH": str(repository / "src")})
        state = db.connect().execute("SELECT status FROM queues WHERE id = ?", (args.queue_id,)).fetchone()
        if state["status"] == "FAILED":
            return 1
        if result.returncode:
            db.mark_queue_failed(args.queue_id, f"native alpha exit code {result.returncode}")
        else:
            db.mark_queue_done(args.queue_id)
        return result.returncode
    except Exception as exc:
        db.mark_queue_failed(args.queue_id, str(exc))
        raise
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
