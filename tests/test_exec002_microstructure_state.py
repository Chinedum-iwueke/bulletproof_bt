from datetime import UTC, datetime, timedelta

import pytest

from bt.institutional.execution import canonical_event
from bt.institutional.microstructure import (
    MicrostructureStateError,
    microstructure_state,
    microstructure_state_receipt,
)
from bt.institutional.receipt import build_receipt, digest, verify_receipt

T0 = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)
COMMIT = "a" * 40
DATASET = digest({"dataset": "exec002"})


def event(sequence, kind, payload, *, received=None):
    return canonical_event(
        event_id=f"{kind}-{sequence}", source="bybit", stream="BTCUSDT:microstructure:epoch-1",
        kind=kind, instrument_id="BTCUSDT-PERP", event_time=T0 + timedelta(seconds=sequence),
        receive_time=T0 + timedelta(seconds=received if received is not None else sequence + 1),
        source_sequence=sequence, payload=payload,
    )


def dependencies():
    exec001 = build_receipt(
        milestone="EXEC-001", producer="fixture.exec001", producer_version="1.0.0", source_commit=COMMIT,
        inputs={}, dataset_digest=DATASET, configuration={}, artifacts={}, result={"reconstructable": True},
    ).as_dict()
    data003 = build_receipt(
        milestone="DATA-003", producer="fixture.data003", producer_version="1.0.0", source_commit=COMMIT,
        inputs={}, dataset_digest=DATASET, configuration={}, artifacts={}, result={"qualified": True},
    ).as_dict()
    return exec001, data003


def rich_events():
    return [
        event(1, "order_book", {"bids": [[100, 3], [99, 2]], "asks": [[101, 2], [102, 1]]}),
        event(2, "trade", {"price": 101, "size": 2, "aggressor_side": "buy"}),
        event(3, "trade", {"price": 100, "size": 1, "aggressor_side": "sell"}),
        event(4, "mark_price", {"price": 101}), event(5, "index_price", {"price": 100}),
        event(6, "funding", {"value": "0.0001"}), event(7, "open_interest", {"value": "1200000"}),
        event(8, "liquidation", {"notional": "25000"}),
    ]


def test_observed_state_has_typed_provenance_and_digest():
    state = microstructure_state(events=rich_events(), as_of=T0 + timedelta(seconds=20))
    assert state["fields"]["spread"]["value"] == "1"
    assert state["fields"]["depth_imbalance"]["status"] == "observed"
    assert state["fields"]["basis"]["value"] == "0.01"
    assert state["fields"]["liquidation_notional"]["status"] == "observed"
    assert state["coverage"]["inferred"] == 0
    assert len(state["state_digest"]) == 64


def test_crossed_book_fails_closed():
    crossed = event(1, "order_book", {"bids": [[101, 1]], "asks": [[100, 1]]})
    with pytest.raises(MicrostructureStateError, match="crossed"):
        microstructure_state(events=[crossed], as_of=T0 + timedelta(seconds=3))


def test_missing_levels_and_liquidations_are_unavailable_not_zero():
    state = microstructure_state(
        events=[event(1, "order_book", {"bids": [], "asks": []})], as_of=T0 + timedelta(seconds=3)
    )
    assert state["fields"]["order_book"]["status"] == "unavailable"
    liquidation = state["fields"]["liquidation_notional"]
    assert liquidation["value"] is None
    assert "liquidation_history_absent_not_zero" in liquidation["limitations"]


def test_liquidation_proxy_is_explicitly_inferred():
    state = microstructure_state(
        events=[event(1, "liquidation_proxy", {"value": "9000"})], as_of=T0 + timedelta(seconds=3)
    )
    field = state["fields"]["liquidation_notional"]
    assert field["status"] == "inferred"
    assert field["uncertainty"] == "model_dependent"


def test_venue_reset_discards_prior_snapshot():
    state = microstructure_state(
        events=[rich_events()[0], event(9, "venue_reset", {"reason": "sequence reset"})],
        as_of=T0 + timedelta(seconds=20),
    )
    assert state["venue_reset_observed"] is True
    assert state["fields"]["order_book"]["status"] == "unavailable"


def test_delayed_funding_and_oi_are_stale_not_observed():
    state = microstructure_state(
        events=[event(1, "funding", {"value": "0.001"}), event(2, "open_interest", {"value": "100"})],
        as_of=T0 + timedelta(seconds=100), maximum_auxiliary_age_seconds=10,
    )
    assert state["fields"]["funding_rate"]["status"] == "unavailable"
    assert state["fields"]["open_interest"]["uncertainty"] == "stale"


def test_receive_clock_excludes_future_known_event():
    future_known = event(1, "funding", {"value": "0.001"}, received=50)
    state = microstructure_state(events=[future_known], as_of=T0 + timedelta(seconds=10))
    assert state["fields"]["funding_rate"]["status"] == "unavailable"


def test_receipt_binds_exec_and_quality_dependencies():
    exec001, data003 = dependencies()
    receipt = microstructure_state_receipt(
        events=rich_events(), exec001_receipt=exec001, data003_receipt=data003,
        as_of=T0 + timedelta(seconds=20), source_commit=COMMIT, dataset_digest=DATASET,
        configuration={"maximum_auxiliary_age_seconds": 3600},
    )
    assert verify_receipt(receipt)
    assert receipt.result["state"]["coverage"]["observed"] > 0
    assert receipt.authority["orders"] is False


def test_receipt_rejects_wrong_exec_dependency():
    _, data003 = dependencies()
    wrong = build_receipt(
        milestone="EXEC-003", producer="fixture", producer_version="1.0.0", source_commit=COMMIT,
        inputs={}, dataset_digest=DATASET, configuration={}, artifacts={}, result={"reconstructable": True},
    ).as_dict()
    with pytest.raises(MicrostructureStateError, match="EXEC-001"):
        microstructure_state_receipt(
            events=[], exec001_receipt=wrong, data003_receipt=data003, as_of=T0,
            source_commit=COMMIT, dataset_digest=DATASET, configuration={},
        )
