from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from bt.institutional.receipt import build_receipt, digest, verify_receipt
from bt.institutional.venue_telemetry import (
    DEPENDENCY_PRODUCERS,
    VENUE_TELEMETRY_SPECIFICATION,
    VenueTelemetryError,
    replay_venue_telemetry,
    venue_event,
    venue_telemetry_receipt,
)

NOW = datetime(2026, 9, 12, 12, tzinfo=UTC)
COMMIT = "a" * 40
DATASET = "b" * 64
RAW = "c" * 64


def event(sequence: int, kind: str, payload: dict, **overrides):
    values = {
        "event_id": f"event-{sequence}",
        "venue": "bybit",
        "environment": "demo",
        "account_pseudonym": "acct-demo-7",
        "instrument_id": "bybit:linear:BTCUSDT",
        "stream": "private-orders",
        "kind": kind,
        "exchange_time": NOW + timedelta(seconds=sequence),
        "source_time": NOW + timedelta(seconds=sequence, milliseconds=10),
        "receive_time": NOW + timedelta(seconds=sequence, milliseconds=20),
        "sequence": sequence,
        "cursor": str(sequence),
        "raw_reference_digest": RAW,
        "normalization_version": "1.0.0",
        "payload": payload,
    }
    values.update(overrides)
    return venue_event(**values)


def dependencies():
    return {
        milestone: build_receipt(
            milestone=milestone,
            producer=producer,
            producer_version="1.0.0",
            source_commit=COMMIT,
            inputs={"milestone": milestone},
            dataset_digest=DATASET,
            configuration={},
            artifacts={},
            result={"success": True},
        )
        for milestone, producer in DEPENDENCY_PRODUCERS.items()
    }


def complete_events():
    return [
        event(
            1,
            "order",
            {"client_order_id": "o1", "status": "new", "side": "buy", "quantity": "1"},
        ),
        event(
            2,
            "fill",
            {
                "execution_id": "f1",
                "client_order_id": "o1",
                "side": "buy",
                "quantity": "1",
                "price": "100",
            },
        ),
        event(3, "fee", {"execution_id": "f1", "asset": "USDT", "amount": "0.10"}),
        event(4, "funding", {"asset": "USDT", "amount": "-0.02"}),
        event(
            5,
            "fill",
            {
                "execution_id": "f2",
                "client_order_id": "o2",
                "side": "sell",
                "quantity": "1",
                "price": "110",
            },
        ),
        event(6, "position", {"quantity": "0", "entry_price": "0"}),
        event(7, "cash", {"asset": "USDT", "balance": "1009.88"}),
        event(
            8, "margin", {"initial": "0", "maintenance": "0", "available": "1009.88"}
        ),
        event(
            9,
            "reconciliation",
            {"positions": {"bybit:linear:BTCUSDT": "0"}, "cash": {"USDT": "1009.88"}},
        ),
    ]


def test_replay_is_deterministic_duplicate_safe_and_builds_episode():
    events = complete_events()
    replay = replay_venue_telemetry(
        [*events, events[1]], known_at=NOW + timedelta(seconds=10)
    )
    replay_again = replay_venue_telemetry(
        [*reversed(events), events[1]], known_at=NOW + timedelta(seconds=10)
    )
    assert replay["projection_digest"] == replay_again["projection_digest"]
    assert replay["duplicate_event_count"] == 1
    assert replay["status"] == "current"
    assert replay["trade_episodes"] == [
        {
            "instrument_id": "bybit:linear:BTCUSDT",
            "opened_at": events[1].exchange_time,
            "closed_at": events[4].exchange_time,
            "side": "long",
            "quantity": "1",
            "entry_price": "100",
            "exit_price": "110",
            "gross_pnl": "10",
            "status": "closed",
        }
    ]
    assert replay["fees"] == "0.10"
    assert replay["funding"] == "-0.02"


def test_point_in_time_replay_excludes_future_and_applies_correction():
    original = event(1, "cash", {"asset": "USDT", "balance": "10"})
    correction = event(
        2, "cash", {"asset": "USDT", "balance": "11"}, correction_of=original.event_id
    )
    early = replay_venue_telemetry(
        [original, correction], known_at=NOW + timedelta(seconds=1, milliseconds=30)
    )
    final = replay_venue_telemetry(
        [original, correction], known_at=NOW + timedelta(seconds=3)
    )
    assert early["cash"][0]["balance"] == "10"
    assert final["cash"][0]["balance"] == "11"


def test_gap_and_rest_disagreement_fail_closed():
    events = [
        event(1, "position", {"quantity": "1"}),
        event(
            3,
            "reconciliation",
            {"positions": {"bybit:linear:BTCUSDT": "0"}, "cash": {}},
        ),
    ]
    replay = replay_venue_telemetry(events, known_at=NOW + timedelta(seconds=4))
    assert replay["status"] == "degraded"
    assert replay["sequence_gaps"]
    assert replay["reconciliation_discrepancies"] == ["position:bybit:linear:BTCUSDT"]


@pytest.mark.parametrize(
    "field", ["api_key", "password", "credential_blob", "signature"]
)
def test_secret_bearing_fields_are_rejected(field):
    with pytest.raises(VenueTelemetryError, match="secret-bearing"):
        event(1, "cash", {"asset": "USDT", "balance": "10", field: "sensitive"})


def test_identity_collision_and_missing_correction_are_rejected():
    first = event(1, "cash", {"asset": "USDT", "balance": "10"})
    collision = event(1, "cash", {"asset": "USDT", "balance": "20"})
    with pytest.raises(VenueTelemetryError, match="reused"):
        replay_venue_telemetry([first, collision], known_at=NOW + timedelta(seconds=2))
    orphan = event(
        2, "cash", {"asset": "USDT", "balance": "20"}, correction_of="missing"
    )
    with pytest.raises(VenueTelemetryError, match="absent"):
        replay_venue_telemetry([orphan], known_at=NOW + timedelta(seconds=3))


def test_clock_causality_and_normalization_version_are_fail_closed():
    with pytest.raises(VenueTelemetryError, match="receive_time"):
        event(
            1,
            "cash",
            {"asset": "USDT", "balance": "10"},
            source_time=NOW + timedelta(seconds=2),
            receive_time=NOW + timedelta(seconds=1),
        )
    events = complete_events()
    events[-1] = event(
        9,
        "reconciliation",
        {"positions": {"bybit:linear:BTCUSDT": "0"}, "cash": {"USDT": "1009.88"}},
        normalization_version="2.0.0",
    )
    with pytest.raises(VenueTelemetryError, match="mix normalization"):
        venue_telemetry_receipt(
            events=events,
            dependencies=dependencies(),
            known_at=NOW + timedelta(seconds=10),
            source_commit=COMMIT,
            dataset_digest=DATASET,
            telemetry_schema_digest=digest(VENUE_TELEMETRY_SPECIFICATION),
            configuration={},
            governance_digests={"GOV-003": "d" * 64},
        )


def test_receipt_requires_exact_dependencies_and_has_no_authority():
    receipt = venue_telemetry_receipt(
        events=complete_events(),
        dependencies=dependencies(),
        known_at=NOW + timedelta(seconds=10),
        source_commit=COMMIT,
        dataset_digest=DATASET,
        telemetry_schema_digest=digest(VENUE_TELEMETRY_SPECIFICATION),
        configuration={"freshness_seconds": 120},
        governance_digests={
            "GOV-003": "d" * 64,
            "PLAT-005": "e" * 64,
            "UI-007": "f" * 64,
        },
    )
    assert verify_receipt(receipt)
    assert receipt.milestone == "EXEC-011"
    assert receipt.result["reconstructable"] is True
    assert not any(receipt.authority.values())
    missing = dependencies()
    missing.pop("EXEC-009")
    with pytest.raises(VenueTelemetryError, match="every exact"):
        venue_telemetry_receipt(
            events=complete_events(),
            dependencies=missing,
            known_at=NOW + timedelta(seconds=10),
            source_commit=COMMIT,
            dataset_digest=DATASET,
            telemetry_schema_digest=digest(VENUE_TELEMETRY_SPECIFICATION),
            configuration={},
            governance_digests={"GOV-003": "d" * 64},
        )
