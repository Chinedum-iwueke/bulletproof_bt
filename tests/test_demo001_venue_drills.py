from decimal import Decimal

import pytest

from scripts.demo001_venue_drills import (
    bounded_quantity,
    evidence,
    run,
    stepped_ceiling,
    stepped_floor,
)


def test_dynamic_rules_are_rounded_conservatively() -> None:
    assert stepped_ceiling(Decimal("0.00101"), Decimal("0.001")) == Decimal("0.002")
    assert stepped_floor(Decimal("100.19"), Decimal("0.10")) == Decimal("100.10")
    assert bounded_quantity(
        price=Decimal("100000"),
        minimum_quantity=Decimal("0.001"),
        quantity_step=Decimal("0.001"),
        minimum_notional=Decimal("5"),
        maximum_notional=Decimal("250"),
    ) == Decimal("0.001")


def test_minimum_order_must_fit_bounded_demo_notional() -> None:
    with pytest.raises(RuntimeError, match="exceeds"):
        bounded_quantity(
            price=Decimal("100000"),
            minimum_quantity=Decimal("0.001"),
            quantity_step=Decimal("0.001"),
            minimum_notional=Decimal("5"),
            maximum_notional=Decimal("50"),
        )


def test_drill_evidence_retains_only_digest_and_provenance() -> None:
    item = evidence(
        passed=True,
        origin="venue_observed",
        facts={"order_id": "private-to-dossier"},
    )
    assert set(item) == {"passed", "origin", "evidence_digest"}
    assert len(item["evidence_digest"]) == 64


def test_deployment_commit_is_explicit_and_full_length() -> None:
    with pytest.raises(ValueError, match="full lowercase Git commit"):
        run(
            symbol="BTCUSDT",
            maximum_notional=Decimal("250"),
            expected_egress_ip="176.97.192.188",
            source_commit="short",
        )
