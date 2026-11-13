#!/usr/bin/env python3
"""Fail-closed deterministic preflight and scaffold for ALPHA-003 authoring."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "src"))

from bt.governance.data_requirements import (
    RequirementsError,
    preflight_evidence,
)
from bt.governance.strategy_authoring import (
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
    plans = {}
    successor_proposal = None
    try:
        evidence = json.loads(args.evidence.read_text(encoding="utf-8"))
        source_commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=args.repository,
            text=True,
        ).strip()
        plans = preflight_evidence(evidence, source_commit=source_commit)
        successor_context = evidence.get("coverage_successor_context")
        if plans["scientific"]["status"] == "deferred" and successor_context is not None:
            from bt.governance.coverage_successor import plan_coverage_successor

            package = evidence["validation_requirements"]
            successor_proposal = plan_coverage_successor(
                frozen_inputs={
                    "requirements": package["requirements"], "coverage": package["coverage"],
                    "dataset_bindings": evidence["dataset_bindings"],
                    "instruments": evidence["discovery_candidate"]["data"]["instruments"],
                    "source_commit": source_commit, "approved_window": evidence["window"],
                    "variants": evidence["discovery_candidate"]["parameter_budget"]["maximum_variants"],
                    "maximum_history_days": package["maximum_history_days"],
                    "maximum_bar_variant_work": package["maximum_bar_variant_work"],
                },
                parent_plan=plans["scientific"], **successor_context,
            )
        if any(plan["status"] != "ready" for plan in plans.values()):
            raise RequirementsError(
                "requirements_preflight_deferred: "
                + "; ".join(
                    f"{mode}: {','.join(plan['reasons'])}"
                    for mode, plan in plans.items()
                    if plan["status"] != "ready"
                )
            )
        intent = compile_strategy_intent(evidence)
        result = scaffold_strategy_package(args.repository, intent)
    except (
        StrategyIntentError,
        RequirementsError,
        KeyError,
        TypeError,
        ValueError,
        OSError,
        subprocess.CalledProcessError,
    ) as exc:
        result = {
            "schema_version": "strategy-scaffold-v1.0.0",
            "success": False,
            "reason": str(exc),
            "validation_plans": plans,
            "coverage_successor_proposal": successor_proposal,
            "retryable": False,
            "authority": {"capital": False, "orders": False},
        }
        args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        return 2
    result["success"] = True
    result["validation_plans"] = plans
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
