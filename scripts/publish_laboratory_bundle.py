#!/usr/bin/env python3
"""Publish or resume one certified Bulletproof bundle through Hermes."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from bt.logging.laboratory_publication import (
    confirm_projections,
    publish_certified_bundle,
)


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser()
    value.add_argument("--api-url", default=os.environ.get("SWARM_API_URL"))
    value.add_argument("--token-file", type=Path)
    commands = value.add_subparsers(dest="command", required=True)
    publish = commands.add_parser("publish")
    publish.add_argument("--bundle-dir", type=Path, required=True)
    publish.add_argument("--trial-id", required=True)
    publish.add_argument("--result-id", required=True)
    publish.add_argument("--memory-db", type=Path, required=True)
    projection = commands.add_parser("confirm-projections")
    projection.add_argument("--publication-id", required=True)
    projection.add_argument("--graph-manifest-digest", required=True)
    projection.add_argument("--graph-source-epoch", type=int, required=True)
    projection.add_argument("--retrieval-corpus-digest", required=True)
    projection.add_argument("--retrieval-source-epoch", type=int, required=True)
    return value


def main() -> int:
    args = parser().parse_args()
    if not args.api_url:
        raise SystemExit("--api-url or SWARM_API_URL is required")
    token_file = args.token_file or Path(os.environ.get("SWARM_TOKEN_FILE", ""))
    if not str(token_file) or not token_file.is_file():
        raise SystemExit("--token-file or SWARM_TOKEN_FILE is required")
    token = token_file.read_text(encoding="utf-8").strip()
    if args.command == "publish":
        result = publish_certified_bundle(
            api_url=args.api_url,
            token=token,
            bundle_dir=args.bundle_dir,
            registry_trial_id=args.trial_id,
            registry_result_id=args.result_id,
            memory_database=args.memory_db,
        )
    else:
        result = confirm_projections(
            api_url=args.api_url,
            token=token,
            publication_id=args.publication_id,
            graph_manifest_digest=args.graph_manifest_digest,
            graph_source_epoch=args.graph_source_epoch,
            retrieval_corpus_digest=args.retrieval_corpus_digest,
            retrieval_source_epoch=args.retrieval_source_epoch,
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
