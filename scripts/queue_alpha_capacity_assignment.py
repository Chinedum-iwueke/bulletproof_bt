"""Submit a governed assignment to the native global scheduler and await its receipt."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import time
import sys
import signal

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from orchestrator.db import ResearchDB  # noqa: E402
from orchestrator.alpha_capacity_owner import current_owner  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--assignment", type=Path, required=True)
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    assignment = json.loads(args.assignment.read_text())
    if assignment.get("stage", "execute") != "execute":
        raise ValueError("Only governed execution enters the capacity queue")
    if not 1 <= int(assignment["max_variants"]) <= 8:
        raise ValueError("The preregistered variant budget must be 1-8; never truncate a grid")
    db = ResearchDB(args.db)
    db.init_schema()
    payload = {"kind": "governed_alpha_assignment", "name": assignment["question_digest"],
               "max_workers": min(8, int(assignment["max_variants"])),
               "assignment": str(args.assignment.resolve()),
               "assignment_sha256": hashlib.sha256(args.assignment.read_bytes()).hexdigest(),
               "repository_root": str(args.repository_root.resolve()),
               "output": str(args.output.resolve()), "receipt": str(args.receipt.resolve()),
               **current_owner()}
    if payload["max_workers"] < 1:
        raise ValueError("Invalid variant budget")
    db.connect().execute("BEGIN IMMEDIATE")
    existing = db.connect().execute(
        "SELECT id FROM queues WHERE queue_name = ? AND item_type = ? AND item_id = ?",
        ("approved_backtests", "governed_alpha_assignment", payload["assignment_sha256"]),
    ).fetchone()
    if existing:
        db.close()
        raise RuntimeError("This immutable assignment already entered the queue; reconcile its receipt before retrying")
    queue_id = db.enqueue(queue_name="approved_backtests", item_type="governed_alpha_assignment",
                          item_id=payload["assignment_sha256"], payload=payload)
    def stop(signum, frame):
        db.mark_queue_failed(queue_id, "Hermes execution lease owner stopped")
        raise SystemExit(128 + signum)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    print(json.dumps({"event": "alpha_capacity_queued", "queue_id": queue_id,
                      "workers": payload["max_workers"]}), flush=True)
    try:
        while True:
            row = db.connect().execute("SELECT status FROM queues WHERE id = ?", (queue_id,)).fetchone()
            if row["status"] in {"DONE", "FAILED"}:
                return 0 if row["status"] == "DONE" else 1
            time.sleep(2)
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
