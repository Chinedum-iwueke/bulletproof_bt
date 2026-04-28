#!/usr/bin/env python3
"""Initialize/reset research SQLite database schema.

Smoke command:
    python orchestrator/init_research_db.py --db research_db/research.sqlite
"""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from orchestrator.db import ResearchDB

TABLES = [
    "hypotheses",
    "experiments",
    "pipeline_runs",
    "runs",
    "verdicts",
    "queues",
    "state_findings",
    "artifacts",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize research SQLite database.")
    parser.add_argument("--db", default="research_db/research.sqlite")
    parser.add_argument("--reset", action="store_true", default=False)
    parser.add_argument("--yes", action="store_true", default=False)
    return parser.parse_args()


def reset_database_file(db_path: Path, yes: bool) -> None:
    if not db_path.exists():
        return
    if not yes:
        answer = input(f"Reset database at {db_path}? This will delete existing data. Type 'yes' to continue: ")
        if answer.strip().lower() != "yes":
            raise SystemExit("Aborted reset.")

    db_path.unlink()
    wal_path = db_path.with_suffix(db_path.suffix + "-wal")
    shm_path = db_path.with_suffix(db_path.suffix + "-shm")
    if wal_path.exists():
        wal_path.unlink()
    if shm_path.exists():
        shm_path.unlink()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if args.reset:
        reset_database_file(db_path, yes=args.yes)

    db = ResearchDB(db_path)
    db.init_schema()
    db.close()

    print(f"Database initialized: {db_path}")
    print(f"Tables: {', '.join(TABLES)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
