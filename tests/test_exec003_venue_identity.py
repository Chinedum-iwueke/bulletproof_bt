from datetime import UTC, datetime, timedelta

import pytest

from bt.institutional.receipt import build_receipt, digest, verify_receipt
from bt.institutional.venue import (
    MAPPING_SCHEMA_VERSION,
    VenueIdentityError,
    venue_identity_map,
    venue_identity_receipt,
)

T0 = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)
COMMIT = "a" * 40
DATASET = digest({"dataset": "exec003"})


def dependency(milestone, result):
    return build_receipt(
        milestone=milestone,
        producer=f"fixture.{milestone.lower()}",
        producer_version="1.0.0",
        source_commit=COMMIT,
        inputs={},
        dataset_digest=DATASET,
        configuration={},
        artifacts={},
        result=result,
    ).as_dict()


def listing(venue, listing_id="BTCUSDT", **overrides):
    value = {
        "venue_id": venue,
        "listing_id": listing_id,
        "canonical_instrument_id": "BTC-USDT-LINEAR-PERP",
        "symbol": listing_id,
        "instrument_type": "linear_perpetual",
        "base_asset": "BTC",
        "quote_asset": "USDT",
        "settlement_asset": "USDT",
        "margin_asset": "USDT",
        "contract_size": "1",
        "inverse": False,
        "expiry": None,
        "price_tick": "0.10",
        "quantity_step": "0.001",
        "minimum_quantity": "0.001",
        "maximum_quantity": "100",
        "minimum_notional": "5",
        "status": "active",
        "effective_from": (T0 - timedelta(days=30)).isoformat(),
        "effective_to": None,
        "available_at": (T0 - timedelta(days=30)).isoformat(),
    }
    value.update(overrides)
    return value


def data001(*records):
    return dependency(
        "DATA-001",
        {"schema_version": "data001-reference-snapshot-v1.0.0", "records": list(records)},
    )


def relation(*members, **overrides):
    value = {
        "mapping_id": "btc-usdt-linear-perp-v1",
        "relationship_kind": "economic_equivalent",
        "members": [{"venue_id": venue, "listing_id": item} for venue, item in members],
        "effective_from": (T0 - timedelta(days=1)).isoformat(),
        "effective_to": None,
        "available_at": (T0 - timedelta(days=1)).isoformat(),
    }
    value.update(overrides)
    return value


def observations(*states):
    return [
        {
            "venue_id": venue,
            "status": status,
            "observed_at": (T0 - timedelta(seconds=2)).isoformat(),
            "available_at": (T0 - timedelta(seconds=1)).isoformat(),
        }
        for venue, status in states
    ]


def valid_map(**overrides):
    values = {
        "data001_receipt": data001(listing("bybit"), listing("binance")),
        "relationships": [relation(("bybit", "BTCUSDT"), ("binance", "BTCUSDT"))],
        "venue_observations": observations(("bybit", "operational"), ("binance", "operational")),
        "as_of": T0,
        "known_at": T0,
    }
    values.update(overrides)
    return venue_identity_map(**values)


def test_explicit_mapping_preserves_venue_constraints_and_non_fungibility():
    result = valid_map()
    assert result["qualified"] is True
    assert len(result["routes"]) == 2
    assert all(route["comparison_eligible"] for route in result["routes"])
    assert all(route["order_route_eligible"] is False for route in result["routes"])
    assert result["routes"][0]["target_constraints"]["price_tick"] == "0.10"
    assert "operationally distinct" in result["claim"]


def test_symbols_never_create_an_implicit_mapping():
    result = valid_map(relationships=[])
    assert result["mappings"] == []
    assert result["routes"] == []
    assert result["qualified"] is False
    assert result["unmapped_identities"] == ["binance:BTCUSDT", "bybit:BTCUSDT"]


def test_incompatible_settlement_is_quarantined():
    receipt = data001(
        listing("bybit"),
        listing("binance", settlement_asset="USDC", margin_asset="USDC"),
    )
    result = valid_map(data001_receipt=receipt)
    assert result["qualified"] is False
    assert result["mappings"][0]["state"] == "quarantined"
    assert "incompatible_settlement_asset" in result["mappings"][0]["blockers"]
    assert result["routes"] == []


def test_delisted_member_is_quarantined_at_effective_time():
    receipt = data001(
        listing("bybit"),
        listing("binance", effective_to=(T0 - timedelta(seconds=1)).isoformat()),
    )
    result = valid_map(data001_receipt=receipt)
    assert result["mappings"][0]["state"] == "quarantined"
    assert "member_inactive_or_unknown" in result["mappings"][0]["blockers"]


def test_outage_blocks_comparison_without_changing_identity():
    result = valid_map(
        venue_observations=observations(("bybit", "operational"), ("binance", "outage"))
    )
    assert result["mappings"][0]["state"] == "active"
    assert all(not route["comparison_eligible"] for route in result["routes"])
    assert all(any("venue_outage" in item for item in route["blockers"]) for route in result["routes"])


def test_stale_observation_fails_closed_as_outage():
    stale = observations(("bybit", "operational"), ("binance", "operational"))
    stale[0]["observed_at"] = (T0 - timedelta(minutes=5, seconds=1)).isoformat()
    stale[0]["available_at"] = (T0 - timedelta(minutes=5)).isoformat()
    result = valid_map(venue_observations=stale)
    assert result["venue_observations"]["bybit"]["status"] == "outage"
    assert result["venue_observations"]["bybit"]["reason"] == "stale_venue_observation"


def test_contract_size_conversion_is_explicit():
    receipt = data001(listing("bybit", contract_size="1"), listing("binance", contract_size="0.001"))
    result = valid_map(data001_receipt=receipt)
    route = next(item for item in result["routes"] if item["source"].startswith("bybit"))
    assert route["contract_conversion_ratio"] == "1E+3"


def test_ambiguous_active_relationships_fail_closed():
    relationships = [
        relation(("bybit", "BTCUSDT"), ("binance", "BTCUSDT")),
        relation(
            ("bybit", "BTCUSDT"),
            ("binance", "BTCUSDT"),
            mapping_id="other",
        ),
    ]
    with pytest.raises(VenueIdentityError, match="ambiguous relationships"):
        valid_map(relationships=relationships)


def test_point_in_time_future_relationship_is_quarantined():
    future = relation(
        ("bybit", "BTCUSDT"),
        ("binance", "BTCUSDT"),
        available_at=(T0 + timedelta(seconds=1)).isoformat(),
    )
    result = valid_map(relationships=[future])
    assert result["mappings"][0]["state"] == "quarantined"
    assert "relationship_inactive" in result["mappings"][0]["blockers"]


def test_quarantined_relationship_does_not_claim_active_members():
    future = relation(
        ("bybit", "BTCUSDT"),
        ("binance", "BTCUSDT"),
        mapping_id="future",
        available_at=(T0 + timedelta(seconds=1)).isoformat(),
    )
    current = relation(
        ("bybit", "BTCUSDT"),
        ("binance", "BTCUSDT"),
        mapping_id="current",
    )

    result = valid_map(relationships=[future, current])

    assert [item["state"] for item in result["mappings"]] == ["active", "quarantined"]
    assert len(result["routes"]) == 2


def test_naive_decision_clock_fails_closed():
    with pytest.raises(VenueIdentityError, match="as_of must be timezone-aware"):
        valid_map(as_of=datetime(2026, 8, 28, 12, 0))


def test_invalid_contract_bounds_fail_closed():
    receipt = data001(
        listing("bybit", minimum_quantity="1", maximum_quantity="0.5"),
        listing("binance"),
    )
    with pytest.raises(VenueIdentityError, match="maximum_quantity"):
        valid_map(data001_receipt=receipt)


def test_receipt_binds_all_dependencies_and_has_no_authority():
    data = data001(listing("bybit"), listing("binance"))
    receipt = venue_identity_receipt(
        data001_receipt=data,
        exec001_receipt=dependency("EXEC-001", {"reconstructable": True}),
        exec002_receipt=dependency("EXEC-002", {"state": {"coverage": {"observed": 1}}}),
        relationships=[relation(("bybit", "BTCUSDT"), ("binance", "BTCUSDT"))],
        venue_observations=observations(("bybit", "operational"), ("binance", "operational")),
        as_of=T0,
        known_at=T0,
        source_commit=COMMIT,
        dataset_digest=DATASET,
        configuration={"maximum_observation_age_seconds": 60},
    )
    assert verify_receipt(receipt)
    assert receipt.result["mapping_schema_digest"] == digest(
        {"schema_version": MAPPING_SCHEMA_VERSION}
    )
    assert receipt.result["qualified"] is True
    assert receipt.authority["orders"] is False


def test_receipt_rejects_invalid_dependency():
    wrong = dependency("EXEC-004", {})
    with pytest.raises(VenueIdentityError, match="EXEC-001"):
        venue_identity_receipt(
            data001_receipt=data001(listing("bybit"), listing("binance")),
            exec001_receipt=wrong,
            exec002_receipt=dependency("EXEC-002", {}),
            relationships=[],
            venue_observations=[],
            as_of=T0,
            known_at=T0,
            source_commit=COMMIT,
            dataset_digest=DATASET,
            configuration={},
        )
