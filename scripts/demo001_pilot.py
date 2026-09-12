#!/usr/bin/env python3
"""Compile a secret-free DEMO-001 venue drill dossier into an exact receipt."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from bt.institutional.demo_certification import (
    DEMO_CERTIFICATION_SPECIFICATION,
    demo_certification_receipt,
)
from bt.institutional.receipt import digest


def _read(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"{path} must contain a JSON object")
    return value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--dependencies", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-commit", required=True)
    args = parser.parse_args()

    evidence = _read(args.evidence)
    dependencies = _read(args.dependencies)
    required = {
        "observed_at",
        "valid_until",
        "credential_fingerprint",
        "platform_observability_digest",
        "dataset_digest",
        "drill_evidence",
        "terminal_state",
    }
    missing = sorted(required - set(evidence))
    if missing:
        raise RuntimeError(f"venue evidence is incomplete: {', '.join(missing)}")
    receipt = demo_certification_receipt(
        observed_at=datetime.fromisoformat(evidence["observed_at"]),
        valid_until=datetime.fromisoformat(evidence["valid_until"]),
        endpoint_identity=evidence.get("endpoint_identity", ""),
        credential_fingerprint=evidence["credential_fingerprint"],
        drill_evidence=evidence["drill_evidence"],
        dependency_receipts=dependencies,
        platform_observability_digest=evidence["platform_observability_digest"],
        dataset_digest=evidence["dataset_digest"],
        source_commit=args.source_commit,
        configuration={
            "capital_environment": False,
            "withdrawal_authority": evidence.get("withdrawal_authority"),
            "terminal_state": evidence["terminal_state"],
            "instrument": evidence.get("instrument"),
            "bounded_order_limits": evidence.get("bounded_order_limits"),
        },
    )
    report = {
        "schema_version": "demo001-native-pilot-report-v1.0.0",
        "success": receipt.result["qualified"],
        "demo_certification_specification": DEMO_CERTIFICATION_SPECIFICATION,
        "demo_certification_specification_digest": digest(DEMO_CERTIFICATION_SPECIFICATION),
        "producer_receipt": receipt.as_dict(),
        "credential_values_retained": False,
        "capital_or_live_order_authority": False,
        "claim_boundary": receipt.result["claim"],
    }
    report["report_digest"] = digest(report)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="ascii")
    args.output.chmod(0o600)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
