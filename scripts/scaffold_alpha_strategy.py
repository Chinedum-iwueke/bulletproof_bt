#!/usr/bin/env python3
"""Fail-closed deterministic preflight and scaffold for ALPHA-003 authoring."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "src"))

from bt.governance.strategy_authoring import (  # noqa: E402
    StrategyIntentError,
    compile_strategy_intent,
    scaffold_strategy_package,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    evidence = json.loads(args.evidence.read_text(encoding="utf-8"))
    try:
        intent = compile_strategy_intent(evidence)
        result = scaffold_strategy_package(args.repository, intent)
    except StrategyIntentError as exc:
        result = {
            "schema_version": "strategy-scaffold-v1.0.0",
            "success": False,
            "reason": str(exc),
            "authority": {"capital": False, "orders": False},
        }
        args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        return 2
    result["success"] = True
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
