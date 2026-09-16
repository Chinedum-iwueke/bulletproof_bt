#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess

from bt.institutional.strategy_catalog import build_strategy_capability_catalog


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", required=True, type=Path)
    parser.add_argument("--source-commit", required=True)
    args = parser.parse_args()
    actual = subprocess.run(
        ["git", "-C", str(args.repository), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if actual != args.source_commit:
        raise SystemExit("repository commit differs from requested strategy catalog")
    print(
        json.dumps(
            build_strategy_capability_catalog(
                args.repository, source_commit=args.source_commit
            ),
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
