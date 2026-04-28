#!/usr/bin/env python3
"""Queue hypothesis jobs for the research daemon."""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from orchestrator.db import ResearchDB


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Queue one hypothesis into approved_backtests.")
    parser.add_argument("--db", default="research_db/research.sqlite")
    parser.add_argument("--hypothesis", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--priority", type=int, default=80)
    parser.add_argument("--max-workers", type=int, default=6)
    parser.add_argument("--phase", default="tier2")
    parser.add_argument("--queue-name", default="approved_backtests")
    parser.add_argument("--config", default="configs/engine.yaml")
    parser.add_argument("--local-config", default="configs/local/engine.lab.yaml")
    parser.add_argument(
        "--stable-data",
        default="/home/omenka/research_data/bt/curated/stable_data_1m_canonical",
    )
    parser.add_argument(
        "--vol-data",
        default="/home/omenka/research_data/bt/curated/vol_data_1m_canonical",
    )
    parser.add_argument("--outputs-root", default="outputs")
    parser.add_argument("--retain-top-n", type=int, default=2)
    parser.add_argument("--retain-median", type=int, default=1)
    parser.add_argument("--retain-worst", type=int, default=1)
    parser.add_argument("--no-cleanup-delete-logs", action="store_true", default=False)
    parser.add_argument("--no-cleanup-delete-nonretained-runs", action="store_true", default=False)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    hypothesis = Path(args.hypothesis)
    if not hypothesis.exists():
        raise FileNotFoundError(f"Hypothesis not found: {hypothesis}")

    db = ResearchDB(args.db, repo_root=PROJECT_ROOT)
    db.init_schema()

    hypothesis_id = db.upsert_hypothesis_by_name(
        name=args.name,
        yaml_path=hypothesis,
        status="IMPLEMENTED",
        metadata={"queued_from": "queue_hypothesis.py"},
    )

    payload = {
        "hypothesis": str(hypothesis),
        "name": args.name,
        "phase": args.phase,
        "max_workers": args.max_workers,
        "config": args.config,
        "local_config": args.local_config,
        "stable_data": args.stable_data,
        "vol_data": args.vol_data,
        "outputs_root": args.outputs_root,
        "retain_top_n": args.retain_top_n,
        "retain_median": args.retain_median,
        "retain_worst": args.retain_worst,
        "cleanup_delete_logs": not args.no_cleanup_delete_logs,
        "cleanup_delete_nonretained_runs": not args.no_cleanup_delete_nonretained_runs,
    }

    queue_id = db.enqueue(
        queue_name=args.queue_name,
        item_type="hypothesis",
        item_id=hypothesis_id,
        priority=args.priority,
        payload=payload,
    )
    db.close()

    print(f"Queued hypothesis: {args.name}")
    print(f"Queue ID: {queue_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
