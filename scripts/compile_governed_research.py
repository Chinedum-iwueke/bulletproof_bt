#!/usr/bin/env python3
"""Compile a typed founder submission without executing research."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from bt.governance.research_bridge import (
    DatasetBinding,
    HypothesisSubmission,
    compile_submission,
    register_with_hermes,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--submission", type=Path, required=True)
    parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    parser.add_argument("--repository-commit", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-variants", type=int, default=64)
    parser.add_argument("--register", action="store_true")
    parser.add_argument("--api-url", default=os.environ.get("SWARM_API_URL"))
    parser.add_argument("--token-file", type=Path)
    args = parser.parse_args()
    source = json.loads(args.submission.read_text(encoding="utf-8"))
    dataset = source["dataset"]
    proposal = compile_submission(
        HypothesisSubmission(
            original_text=source["original_text"],
            hypothesis=source["hypothesis"],
            tier=source["tier"],
            legacy_tier_resolution=source.get("legacy_tier_resolution"),
            grid={key: tuple(value) for key, value in source["grid"].items()},
            dataset=DatasetBinding(
                snapshot_id=dataset["snapshot_id"],
                digest=dataset["digest"],
                available_fields=tuple(dataset["available_fields"]),
                universe=dataset["universe"],
                timeframe=dataset["timeframe"],
            ),
        ),
        repository_root=args.repository_root,
        repository_commit=args.repository_commit,
        max_variants=args.max_variants,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(proposal, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    receipt = None
    if args.register:
        if not args.api_url or args.token_file is None or not args.token_file.is_file():
            raise SystemExit("--register requires --api-url and --token-file")
        receipt = register_with_hermes(
            api_url=args.api_url,
            token=args.token_file.read_text(encoding="utf-8").strip(),
            proposal=proposal,
        )
    print(json.dumps({"output": str(args.output), "proposal_digest": proposal["proposal_digest"], "variant_count": proposal["search"]["variant_count"], "hermes": receipt}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
