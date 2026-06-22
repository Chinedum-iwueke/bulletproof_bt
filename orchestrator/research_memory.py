#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from research_memory.ingest import ingest_research_memory
from research_memory.query_engine import run_query
from research_memory.recommendation_engine import generate_recommendations
from research_memory.report_writer import write_reports
from research_memory.schema import ensure_research_memory_schema


def main() -> int:
    args = _parse_args()
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    ensure_research_memory_schema(conn)

    if args.query:
        result = run_query(conn, args.query, state_json=args.state_json, setup_class=args.setup_class, hypothesis_name=args.hypothesis_name, candidate_id=args.candidate_id, run_id=args.run_id)
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
        conn.close()
        return 0

    if args.write_db:
        manifest = ingest_research_memory(
            conn,
            outputs_root=Path(args.outputs_root),
            experiment_roots=[Path(path) for path in args.experiment_root],
            verdicts_dir=Path(args.verdicts_dir),
            state_findings_dir=Path(args.state_findings_dir),
            alpha_zoo_dir=Path(args.alpha_zoo_dir),
            output_dir=Path(args.output_dir),
        )
        print(json.dumps(manifest, indent=2, sort_keys=True))

    if args.recommend or args.write_db:
        recs = generate_recommendations(conn)
        print(json.dumps({"recommendations_generated": len(recs)}, indent=2, sort_keys=True))

    if args.output_dir:
        summary = write_reports(conn, Path(args.output_dir))
        print(json.dumps(summary, indent=2, sort_keys=True))

    conn.close()
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build and query deterministic research memory.")
    parser.add_argument("--db", required=True)
    parser.add_argument("--outputs-root", default="outputs")
    parser.add_argument("--experiment-root", action="append", default=[])
    parser.add_argument("--verdicts-dir", default="research/verdicts")
    parser.add_argument("--state-findings-dir", default="research/state_findings")
    parser.add_argument("--alpha-zoo-dir", default="research/alpha_zoo")
    parser.add_argument("--output-dir", default="research/memory")
    parser.add_argument("--write-db", action="store_true")
    parser.add_argument("--recommend", action="store_true")
    parser.add_argument("--query", choices=["similar_state", "best_states", "avoid_states", "setup_profile", "hypothesis_profile", "candidate_profile"])
    parser.add_argument("--state-json")
    parser.add_argument("--setup-class")
    parser.add_argument("--hypothesis-name")
    parser.add_argument("--candidate-id")
    parser.add_argument("--run-id")
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
