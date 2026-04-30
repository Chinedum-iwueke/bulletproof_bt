#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from orchestrator.alpha_zoo.candidate_loader import load_candidates
from orchestrator.alpha_zoo.candidate_ranker import rank_candidates
from orchestrator.alpha_zoo.correlation_analyzer import analyze_redundancy
from orchestrator.alpha_zoo.promotion_rules import apply_promotion_rules
from orchestrator.alpha_zoo.report_writer import write_outputs
from orchestrator.alpha_zoo.state_profile_analyzer import enrich_state_profiles


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase 12 Alpha Zoo Agent")
    p.add_argument("--db", default=None)
    p.add_argument("--experiments-root", default="outputs")
    p.add_argument("--state-findings-dir", default="research/state_findings")
    p.add_argument("--verdicts-dir", default="research/verdicts")
    p.add_argument("--output-dir", default="research/alpha_zoo")
    p.add_argument("--name", default=None)
    p.add_argument("--min-ev", type=float, default=0.05)
    p.add_argument("--min-trades", type=int, default=50)
    p.add_argument("--min-tail-5r-count", type=int, default=0)
    p.add_argument("--max-drawdown", type=float, default=999)
    p.add_argument("--include-inconclusive", action="store_true", default=False)
    p.add_argument("--include-negative-but-informative", action="store_true", default=False)
    p.add_argument("--top-n", type=int, default=50)
    p.add_argument("--write-db", action="store_true", default=False)
    p.add_argument("--dry-run", action="store_true", default=False)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    candidates = load_candidates(args)
    candidates = enrich_state_profiles(candidates, Path(args.state_findings_dir))
    candidates = rank_candidates(candidates)
    candidates, redundancy = analyze_redundancy(candidates)
    candidates = apply_promotion_rules(candidates, min_trades=args.min_trades)
    outputs = write_outputs(Path(args.output_dir), candidates, redundancy, top_n=args.top_n, dry_run=args.dry_run)
    if args.write_db and not args.dry_run and args.db:
        from orchestrator.alpha_zoo.candidate_loader import write_candidates_to_db

        write_candidates_to_db(Path(args.db), candidates, outputs)
    print(json.dumps({"candidates": len(candidates), "output_dir": args.output_dir}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
