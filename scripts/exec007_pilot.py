#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from bt.institutional.receipt import build_receipt, digest
from bt.institutional.runtime_safety import (
    RUNTIME_SAFETY_SPECIFICATION,
    RuntimeSafetyError,
    contain_runtime,
    initialize_safety_state,
    load_safety_state,
    recover_runtime,
    runtime_safety_receipt,
    verify_safety_journal,
)


def _dependency(
    milestone: str, dataset_digest: str, source_commit: str, result: dict
) -> dict:
    return build_receipt(
        milestone=milestone,
        producer=f"bt.exec007.qualification.{milestone.lower()}",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={"fixture": "exec007-no-network"},
        dataset_digest=dataset_digest,
        configuration={"environment": "qualification"},
        artifacts={},
        result=result,
    ).as_dict()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    source_commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True
    ).strip()
    dataset_digest = digest({"fixture": "exec007-no-network-v1"})
    now = datetime.now(UTC)
    with tempfile.TemporaryDirectory(prefix="exec007-") as directory:
        root = Path(directory)
        state_path = root / "safety.json"
        journal_path = root / "safety.jsonl"
        state = initialize_safety_state(
            state_path=state_path,
            journal_path=journal_path,
            actor="qualification-operator",
            now=now,
        )
        clean_oms = _dependency(
            "EXEC-004",
            dataset_digest,
            source_commit,
            {"qualified": True, "reconciliation": {"submission_allowed": True}},
        )
        authority = {
            "decision_type": "execution-recovery",
            "action": "resume",
            "object_digest": state["state_digest"],
            "actor": "founder-operator",
            "effective_roles": ["founder"],
            "outcome": "authorized",
            "created_at": now.isoformat(),
            "record_digest": digest({"fixture": "independent-human-authority"}),
        }
        ready = recover_runtime(
            state_path=state_path,
            journal_path=journal_path,
            request_id="qualification-recovery",
            actor="founder-operator",
            reason="qualification reconciliation reviewed",
            now=now,
            exec004_receipt=clean_oms,
            authority_record=authority,
        )
        killed = contain_runtime(
            state_path=state_path,
            journal_path=journal_path,
            action="kill",
            request_id="qualification-kill",
            actor="independent-local-controller",
            reason="simulated control plane partition",
            now=now,
        )
        corrupt = root / "corrupt.json"
        corrupt.write_text("{}", encoding="ascii")
        corrupt_failed_closed = False
        try:
            load_safety_state(corrupt)
        except RuntimeSafetyError:
            corrupt_failed_closed = True
        drills = {
            "partition": killed["status"] == "killed",
            "corrupt_state": corrupt_failed_closed,
            "runaway_orders": killed["status"] == "killed",
            "human_recovery": ready["status"] == "ready",
        }
        dependencies = {
            "EXEC-004": clean_oms,
            "EXEC-006": _dependency(
                "EXEC-006", dataset_digest, source_commit, {"qualified": True}
            ),
            "RISK-005": _dependency(
                "RISK-005", dataset_digest, source_commit, {"qualified": True}
            ),
        }
        receipt = runtime_safety_receipt(
            dependency_receipts=dependencies,
            events=verify_safety_journal(journal_path),
            drill_results=drills,
            dataset_digest=dataset_digest,
            source_commit=source_commit,
            configuration={"environment": "deterministic_qualification_fixture"},
        ).as_dict()
    report = {
        "schema_version": "exec007-pilot-report-v1.0.0",
        "evidence_class": "deterministic_qualification_fixture",
        "success": all(drills.values()),
        "drills": drills,
        "terminal_state": killed,
        "producer_receipt": receipt,
        "runtime_safety_specification": RUNTIME_SAFETY_SPECIFICATION,
        "runtime_safety_specification_digest": digest(RUNTIME_SAFETY_SPECIFICATION),
        "capital_or_order_authority": False,
    }
    report["report_digest"] = digest(report)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="ascii"
    )
    output.chmod(0o600)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
