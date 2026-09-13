from datetime import UTC, datetime, timedelta

import pytest

from bt.institutional.adapter_certification import (
    ADAPTER_CERTIFICATION_SPECIFICATION,
    REQUIRED_DRILLS,
    AdapterCertificationError,
    adapter_certification_receipt,
    capability_matrix,
    require_adapter_certification,
)
from bt.institutional.receipt import build_receipt, digest, verify_receipt

COMMIT = "a" * 40
DATASET = digest({"fixture": "exec008"})


def dependency(milestone: str) -> dict:
    return build_receipt(
        milestone=milestone,
        producer=f"test.{milestone.lower()}",
        producer_version="1.0.0",
        source_commit=COMMIT,
        inputs={},
        dataset_digest=DATASET,
        configuration={},
        artifacts={},
        result={"qualified": True},
    ).as_dict()


def produce(evidence_class: str = "venue_observed", **overrides):
    now = datetime(2026, 9, 12, tzinfo=UTC)
    values = {
        "venue": "bybit",
        "environment": "demo",
        "product_type": "perpetual",
        "evidence_class": evidence_class,
        "observed_at": now,
        "valid_until": now + timedelta(hours=24),
        "endpoint_identity": "api-demo.bybit.com",
        "drill_results": {name: True for name in REQUIRED_DRILLS},
        "dependency_receipts": {
            name: dependency(name) for name in ("EXEC-003", "EXEC-004", "EXEC-007")
        },
        "dataset_digest": DATASET,
        "source_commit": COMMIT,
        "configuration": {"bounded": True},
    }
    values.update(overrides)
    return adapter_certification_receipt(**values)


def test_capability_matrices_are_explicit_and_venue_specific() -> None:
    bybit = capability_matrix("bybit", "perpetual")
    binance = capability_matrix("binance", "perpetual")
    assert bybit["precision_source"] != binance["precision_source"]
    assert set(bybit["order_types"]) == {"market", "limit"}
    assert ADAPTER_CERTIFICATION_SPECIFICATION["authority"]["orders"] is False


def test_venue_observed_demo_receipt_is_exact_and_current() -> None:
    receipt = produce()
    assert verify_receipt(receipt)
    result = require_adapter_certification(
        receipt,
        venue="bybit",
        environment="demo",
        now=datetime(2026, 9, 12, 1, tzinfo=UTC),
    )
    assert result["demo_execution_eligible"] is True
    assert result["micro_live_eligible"] is False


def test_deterministic_conformance_never_becomes_demo_certification() -> None:
    receipt = produce("deterministic_conformance")
    assert receipt.result["status"] == "conformance_only"
    assert receipt.result["qualified"] is False
    with pytest.raises(AdapterCertificationError, match="venue-observed"):
        require_adapter_certification(
            receipt,
            venue="bybit",
            environment="demo",
            now=datetime(2026, 9, 12, 1, tzinfo=UTC),
        )


def test_failed_drill_and_expiry_fail_closed() -> None:
    drills = {name: True for name in REQUIRED_DRILLS}
    drills["partial_fill"] = False
    blocked = produce(drill_results=drills)
    assert blocked.result["status"] == "blocked"
    with pytest.raises(AdapterCertificationError):
        require_adapter_certification(
            blocked,
            venue="bybit",
            environment="demo",
            now=datetime(2026, 9, 12, 1, tzinfo=UTC),
        )
    with pytest.raises(AdapterCertificationError, match="expired"):
        require_adapter_certification(
            produce(),
            venue="bybit",
            environment="demo",
            now=datetime(2026, 9, 14, tzinfo=UTC),
        )


def test_independently_versioned_dependency_datasets_are_bound() -> None:
    dependencies = {
        name: dependency(name) for name in ("EXEC-003", "EXEC-004", "EXEC-007")
    }
    dependencies["EXEC-004"] = build_receipt(
        milestone="EXEC-004",
        producer="test.exec-004",
        producer_version="1.0.0",
        source_commit=COMMIT,
        inputs={},
        dataset_digest="b" * 64,
        configuration={},
        artifacts={},
        result={"qualified": True},
    ).as_dict()
    receipt = produce(dependency_receipts=dependencies)
    assert receipt.result["dependency_dataset_digests"]["exec004"] == "b" * 64
