from datetime import UTC, datetime, timedelta

import pytest

from bt.institutional.adapter_certification import REQUIRED_DRILLS as EXEC008_DRILLS
from bt.institutional.adapter_certification import adapter_certification_receipt
from bt.institutional.demo_certification import (
    DEMO_CERTIFICATION_SPECIFICATION,
    DEPENDENCIES,
    REQUIRED_DRILLS,
    DemoCertificationError,
    demo_certification_receipt,
    require_demo_certification,
)
from bt.institutional.receipt import build_receipt, digest, verify_receipt

COMMIT = "d" * 40
DATASET = digest({"fixture": "demo001"})
NOW = datetime(2026, 9, 12, tzinfo=UTC)


def dep(name: str):
    return build_receipt(
        milestone=name,
        producer=f"test.{name.lower()}",
        producer_version="1.0.0",
        source_commit=COMMIT,
        inputs={},
        dataset_digest=DATASET,
        configuration={},
        artifacts={},
        result={"qualified": True, "allowed": True},
    )


def dependencies():
    values = {name: dep(name) for name in DEPENDENCIES if name != "EXEC-008"}
    values["EXEC-008"] = adapter_certification_receipt(
        venue="bybit",
        environment="demo",
        product_type="perpetual",
        evidence_class="venue_observed",
        observed_at=NOW,
        valid_until=NOW + timedelta(hours=24),
        endpoint_identity="api-demo.bybit.com",
        drill_results={name: True for name in EXEC008_DRILLS},
        dependency_receipts={name: dep(name) for name in ("EXEC-003", "EXEC-004", "EXEC-007")},
        dataset_digest=DATASET,
        source_commit=COMMIT,
        configuration={"demo": True},
    )
    return values


def evidence(origin="venue_observed"):
    return {
        name: {"passed": True, "origin": origin, "evidence_digest": digest({"drill": name})}
        for name in REQUIRED_DRILLS
    }


def produce(**overrides):
    values = {
        "observed_at": NOW,
        "valid_until": NOW + timedelta(hours=8),
        "endpoint_identity": "api-demo.bybit.com",
        "credential_fingerprint": digest({"key": "redacted-demo-identity"}),
        "drill_evidence": evidence(),
        "dependency_receipts": dependencies(),
        "platform_observability_digest": digest({"slo": "current"}),
        "dataset_digest": DATASET,
        "source_commit": COMMIT,
        "configuration": {
            "capital_environment": False,
            "withdrawal_authority": False,
            "terminal_state": {"open_orders": 0, "position_qty": 0},
        },
    }
    values.update(overrides)
    return demo_certification_receipt(**values)


def test_certified_receipt_is_exact_expiring_and_has_no_authority():
    receipt = produce()
    assert verify_receipt(receipt)
    assert not any(receipt.authority.values())
    assert require_demo_certification(receipt, now=NOW + timedelta(hours=1))["qualified"]
    assert DEMO_CERTIFICATION_SPECIFICATION["terminal_invariant"].startswith("no open")


def test_missing_venue_provenance_and_nonflat_terminal_state_block():
    drills = evidence("authenticated_demo_replay")
    receipt = produce(
        drill_evidence=drills,
        configuration={
            "capital_environment": False,
            "withdrawal_authority": False,
            "terminal_state": {"open_orders": 1, "position_qty": 0.001},
        },
    )
    assert receipt.result["status"] == "blocked"
    assert "provenance:authentication" in receipt.result["blockers"]
    assert "terminal_state_not_flat" in receipt.result["blockers"]
    with pytest.raises(DemoCertificationError):
        require_demo_certification(receipt, now=NOW)


def test_failed_drill_tamper_wrong_endpoint_and_expiry_fail_closed():
    drills = evidence()
    drills["restart_reconciliation"]["passed"] = False
    assert "drill:restart_reconciliation" in produce(drill_evidence=drills).result["blockers"]
    with pytest.raises(DemoCertificationError, match="endpoint"):
        produce(endpoint_identity="api.bybit.com")
    receipt = produce().as_dict()
    receipt["result"]["qualified"] = False
    with pytest.raises(DemoCertificationError):
        require_demo_certification(receipt, now=NOW)
    with pytest.raises(DemoCertificationError, match="expired"):
        require_demo_certification(produce(), now=NOW + timedelta(days=1))


def test_dependency_and_credential_boundaries_fail_closed():
    values = dependencies()
    del values["RISK-005"]
    with pytest.raises(DemoCertificationError, match="dependency"):
        produce(dependency_receipts=values)
    with pytest.raises(DemoCertificationError, match="digests"):
        produce(credential_fingerprint="secret")
    receipt = produce(
        configuration={
            "capital_environment": False,
            "withdrawal_authority": True,
            "terminal_state": {"open_orders": 0, "position_qty": 0},
        }
    )
    assert "credential_withdrawal_boundary_unproven" in receipt.result["blockers"]


def test_independently_versioned_dependency_datasets_are_bound():
    values = dependencies()
    values["RISK-005"] = build_receipt(
        milestone="RISK-005",
        producer="test.risk-005",
        producer_version="1.0.0",
        source_commit=COMMIT,
        inputs={},
        dataset_digest="a" * 64,
        configuration={},
        artifacts={},
        result={"qualified": True, "allowed": True},
    )
    receipt = produce(dependency_receipts=values)
    assert receipt.result["dependency_dataset_digests"]["risk005"] == "a" * 64
