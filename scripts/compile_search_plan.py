#!/usr/bin/env python3
"""Compile and register an immutable Bulletproof hypothesis search plan."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from bt.experiments.search_plan import (
    SearchBudget,
    SearchLedger,
    StoppingRule,
    compile_hypothesis_search_plan,
)
from bt.hypotheses.contract import HypothesisContract


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hypothesis", type=Path, required=True)
    parser.add_argument("--family-id", required=True)
    parser.add_argument("--dataset-snapshot-id", required=True)
    parser.add_argument("--dataset-digest", required=True)
    parser.add_argument("--repository-commit", required=True)
    parser.add_argument("--code-digest", required=True)
    parser.add_argument("--market-model-bundle-digest", required=True)
    parser.add_argument("--tier", action="append", required=True)
    parser.add_argument("--seed", action="append", type=int, required=True)
    parser.add_argument("--max-trials", type=int, required=True)
    parser.add_argument("--max-attempts-per-trial", type=int, default=1)
    parser.add_argument("--max-wallclock-seconds", type=int, required=True)
    parser.add_argument("--max-workers", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, required=True)
    args = parser.parse_args()

    contract = HypothesisContract.from_yaml(args.hypothesis)
    plan = compile_hypothesis_search_plan(
        contract=contract,
        family_id=args.family_id,
        hypothesis_digest=_sha256(args.hypothesis),
        dataset_snapshot_id=args.dataset_snapshot_id,
        dataset_digest=args.dataset_digest,
        repository_commit=args.repository_commit,
        code_digest=args.code_digest,
        market_model_bundle_digest=args.market_model_bundle_digest,
        tiers=tuple(args.tier),
        seeds=tuple(args.seed),
        resources={"max_workers": args.max_workers},
        budget=SearchBudget(
            max_trials=args.max_trials,
            max_attempts_per_trial=args.max_attempts_per_trial,
            max_wallclock_seconds=args.max_wallclock_seconds,
            max_workers=args.max_workers,
        ),
        stopping_rule=StoppingRule(kind="exhaustive"),
    )
    document = plan.document()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    ledger = SearchLedger(args.ledger)
    try:
        summary = ledger.register(plan)
    finally:
        ledger.close()
    print(json.dumps({"plan": document, "ledger": summary}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
