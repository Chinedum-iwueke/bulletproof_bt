#!/usr/bin/env python3
"""Run the deterministic BT-001 collection and test shards."""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ROOT_TESTS = sorted(ROOT.glob("tests/test_*.py"))
SHARDS = {
    "collection": ["--collect-only", "tests"],
    "exec": ["tests/exec"],
    "hypotheses": ["tests/hypotheses"],
    "auxiliary": ["tests/indicators", "tests/portfolio_engine", "tests/strategies"],
    **{
        f"root-{index + 1}": [str(path.relative_to(ROOT)) for path in ROOT_TESTS[index::4]]
        for index in range(4)
    },
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("shard", choices=["all", *SHARDS])
    args = parser.parse_args()
    selected = list(SHARDS) if args.shard == "all" else [args.shard]
    for name in selected:
        print(f"=== BT-001 {name} ===", flush=True)
        completed = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", *SHARDS[name]],
            cwd=ROOT,
            check=False,
        )
        if completed.returncode:
            return completed.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
