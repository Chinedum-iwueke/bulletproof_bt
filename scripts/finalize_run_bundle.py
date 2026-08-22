#!/usr/bin/env python3
"""Finalize a run directory and optionally publish its canonical Hermes record."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from bt.logging.run_bundle import finalize_run_bundle, publish_to_hermes


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--bundle-root", type=Path, required=True)
    parser.add_argument("--lineage", type=Path, required=True)
    parser.add_argument("--publish-api-url")
    args = parser.parse_args()
    lineage = json.loads(args.lineage.read_text(encoding="utf-8"))
    receipt = finalize_run_bundle(args.run_dir, args.bundle_root, lineage=lineage)
    result: dict[str, object] = {"bundle": receipt}
    if args.publish_api_url:
        token = os.environ.get("SWARM_ORCHESTRATOR_TOKEN", "")
        if not token:
            raise SystemExit("SWARM_ORCHESTRATOR_TOKEN is required for publication")
        result["publication"] = publish_to_hermes(args.publish_api_url, token, receipt, lineage)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
