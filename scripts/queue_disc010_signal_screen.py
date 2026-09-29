#!/usr/bin/env python3
"""Queue one immutable lower-priority DISC-010 fallback screen."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from orchestrator.db import ResearchDB  # noqa: E402


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--assignment", required=True, type=Path)
    parser.add_argument("--repository-root", required=True, type=Path)
    args = parser.parse_args()
    assignment = json.loads(args.assignment.read_text(encoding="utf-8"))
    expected = {
        "schema_version",
        "family_id",
        "specification",
        "bindings",
        "output",
        "source_commit",
        "max_workers",
    }
    if set(assignment) != expected or assignment["schema_version"] != (
        "disc010-capacity-assignment-v1.0.0"
    ):
        raise ValueError("DISC-010 capacity assignment is malformed")
    workers = int(assignment["max_workers"])
    if not 1 <= workers <= 8:
        raise ValueError("DISC-010 worker budget must be 1-8")
    specification = Path(assignment["specification"]).resolve(strict=True)
    bindings = Path(assignment["bindings"]).resolve(strict=True)
    assignment_digest = sha256(args.assignment)
    payload = {
        "kind": "disc010_signal_screen",
        "name": assignment["family_id"],
        "max_workers": workers,
        "assignment": str(args.assignment.resolve()),
        "assignment_sha256": assignment_digest,
        "specification_sha256": sha256(specification),
        "bindings_sha256": sha256(bindings),
        "repository_root": str(args.repository_root.resolve(strict=True)),
    }
    db = ResearchDB(args.db)
    db.init_schema()
    existing = db.connect().execute(
        """
        SELECT id, status FROM queues
        WHERE queue_name = ? AND item_type = ? AND item_id = ?
          AND status != 'FAILED'
        ORDER BY created_at DESC LIMIT 1
        """,
        ("approved_backtests", "disc010_signal_screen", assignment_digest),
    ).fetchone()
    if existing:
        db.close()
        raise RuntimeError("this immutable DISC-010 screen is already queued")
    queue_id = db.enqueue(
        queue_name="approved_backtests",
        item_type="disc010_signal_screen",
        item_id=assignment_digest,
        priority=10,
        payload=payload,
    )
    db.close()
    print(
        json.dumps(
            {
                "event": "disc010_signal_screen_queued",
                "queue_id": queue_id,
                "workers": workers,
                "priority": 10,
                "authority": "research_only",
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
