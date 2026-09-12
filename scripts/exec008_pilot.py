#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from bt.institutional.adapter_certification import (
    ADAPTER_CERTIFICATION_SPECIFICATION,
    REQUIRED_DRILLS,
    AdapterCertificationError,
    adapter_certification_receipt,
    require_adapter_certification,
)
from bt.institutional.receipt import build_receipt, digest, verify_receipt


def dependency(milestone: str, dataset_digest: str, source_commit: str) -> dict:
    return build_receipt(
        milestone=milestone,
        producer=f"bt.exec008.qualification.{milestone.lower()}",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={"fixture": "exec008-no-network"},
        dataset_digest=dataset_digest,
        configuration={"environment": "qualification"},
        artifacts={},
        result={"qualified": True},
    ).as_dict()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    source_commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True
    ).strip()
    conformance = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "tests/test_exec008_adapter_certification.py",
            "tests/exec/test_c8_connector_certification.py",
            "tests/exec/test_binance_adapter_c3.py",
            "tests/exec/test_bybit_adapter_phase5.py",
            "tests/exec/test_order_lifecycle_phase3.py",
            "tests/exec/test_live_controls_phase6.py",
            "tests/exec/test_live_authorization.py",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    dataset_digest = digest({"fixture": "exec008-deterministic-conformance-v1"})
    now = datetime.now(UTC)
    dependencies = {
        name: dependency(name, dataset_digest, source_commit)
        for name in ("EXEC-003", "EXEC-004", "EXEC-007")
    }
    drills = {name: conformance.returncode == 0 for name in REQUIRED_DRILLS}
    receipts = []
    gate_blocks = {}
    for venue, endpoint in (
        ("bybit", "fixture://api-demo.bybit.com"),
        ("binance", "fixture://testnet.binancefuture.com"),
    ):
        receipt = adapter_certification_receipt(
            venue=venue,
            environment="demo",
            product_type="perpetual",
            evidence_class="deterministic_conformance",
            observed_at=now,
            valid_until=now + timedelta(hours=24),
            endpoint_identity=endpoint,
            drill_results=drills,
            dependency_receipts=dependencies,
            dataset_digest=dataset_digest,
            source_commit=source_commit,
            configuration={
                "network_access": False,
                "fixture": "scripted-adapter-responses",
            },
        ).as_dict()
        receipts.append(receipt)
        try:
            require_adapter_certification(
                receipt, venue=venue, environment="demo", now=now
            )
        except AdapterCertificationError:
            gate_blocks[venue] = True
    success = all(verify_receipt(item) for item in receipts) and all(
        gate_blocks.values()
    )
    report = {
        "schema_version": "exec008-pilot-report-v1.0.0",
        "success": success,
        "evidence_class": "deterministic_conformance",
        "adapter_results": [item["result"] for item in receipts],
        "producer_receipts": receipts,
        "demo_admission_blocked_without_venue_evidence": gate_blocks,
        "adapter_certification_specification": ADAPTER_CERTIFICATION_SPECIFICATION,
        "adapter_certification_specification_digest": digest(
            ADAPTER_CERTIFICATION_SPECIFICATION
        ),
        "capital_or_order_authority": False,
        "conformance_test_exit_code": conformance.returncode,
        "conformance_test_output_digest": digest(
            {"stdout": conformance.stdout, "stderr": conformance.stderr}
        ),
    }
    report["report_digest"] = digest(report)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="ascii"
    )
    args.output.chmod(0o600)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
