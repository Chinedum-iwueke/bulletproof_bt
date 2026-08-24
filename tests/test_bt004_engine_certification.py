from __future__ import annotations

import itertools

import pandas as pd
import pytest

from bt.core.certification import (
    AccountingSnapshot,
    CausalEvent,
    CertificationError,
    CertifiedCheckpoint,
    EventKind,
    assert_decision_causality,
    canonical_event_order,
    digest,
    information_available_at,
    transition_digest,
)
from bt.data.feed import HistoricalDataFeed
from bt.hypotheses.l1_h2 import RollingQuantileGate


TS = pd.Timestamp("2026-01-01T00:00:00Z")


def event(
    identifier: str,
    kind: EventKind,
    *,
    available_at: pd.Timestamp = TS,
    sequence: int = 0,
) -> CausalEvent:
    return CausalEvent(
        event_id=identifier,
        kind=kind,
        event_time=TS,
        available_at=available_at,
        source="fixture",
        sequence=sequence,
        payload={"value": identifier},
    )


def test_equal_time_event_order_is_permutation_invariant() -> None:
    events = [
        event("fill", EventKind.FILL),
        event("market", EventKind.MARKET),
        event("decision", EventKind.DECISION),
        event("funding", EventKind.FUNDING),
    ]
    expected = ["market", "funding", "decision", "fill"]
    for permutation in itertools.permutations(events):
        assert [item.event_id for item in canonical_event_order(permutation)] == expected


def test_future_information_is_hidden_and_rejected() -> None:
    future = event(
        "future-funding",
        EventKind.FUNDING,
        available_at=TS + pd.Timedelta(minutes=1),
    )
    current = event("closed-bar", EventKind.MARKET)
    assert information_available_at([future, current], TS) == [current]
    with pytest.raises(CertificationError, match="future-funding"):
        assert_decision_causality([current, future], TS)


def test_accounting_identities_accept_valid_state_and_reject_drift() -> None:
    AccountingSnapshot(
        cash=9_990.0,
        equity=10_015.0,
        realized_pnl=5.0,
        unrealized_pnl=20.0,
        used_margin=1_000.0,
        free_margin=9_015.0,
    ).validate()
    with pytest.raises(CertificationError, match="equity"):
        AccountingSnapshot(9_990.0, 10_016.0, 5.0, 20.0, 1_000.0, 9_016.0).validate()


def test_checkpoint_replay_is_digest_bound_and_fails_closed() -> None:
    last = event("bar", EventKind.MARKET)
    state = {"cash": 10_000.0, "positions": {"BTCUSDT": 0.25}}
    checkpoint = CertifiedCheckpoint.create(
        dataset_digest="a" * 64,
        config_digest="b" * 64,
        last_event=last,
        state=state,
    )
    checkpoint.validate(dataset_digest="a" * 64, config_digest="b" * 64)
    assert checkpoint.state_digest == digest(state)
    assert checkpoint.state_digest == (
        "ab69932e82b802083fd7b862838649dbdc85f66afed23126f1cb5093d797f6e1"
    )
    with pytest.raises(CertificationError, match="dataset"):
        checkpoint.validate(dataset_digest="c" * 64, config_digest="b" * 64)
    tampered = CertifiedCheckpoint(
        dataset_digest=checkpoint.dataset_digest,
        config_digest=checkpoint.config_digest,
        last_event_key=checkpoint.last_event_key,
        state={"cash": 0.0},
        state_digest=checkpoint.state_digest,
    )
    with pytest.raises(CertificationError, match="state digest"):
        tampered.validate(dataset_digest="a" * 64, config_digest="b" * 64)


def test_transition_digest_replays_exactly_and_changes_with_state() -> None:
    item = event("bar", EventKind.MARKET)
    first = transition_digest(previous_digest="0" * 64, event=item, state={"cash": 1})
    assert first == transition_digest(
        previous_digest="0" * 64, event=item, state={"cash": 1}
    )
    assert first != transition_digest(
        previous_digest="0" * 64, event=item, state={"cash": 2}
    )


def test_canonical_state_rejects_unsupported_or_non_finite_values() -> None:
    with pytest.raises(CertificationError, match="unsupported type"):
        digest({"symbols": {"BTCUSDT"}})
    with pytest.raises(CertificationError, match="non-finite"):
        digest({"equity": float("nan")})


def test_historical_feed_equal_timestamp_symbols_are_stable() -> None:
    rows = [
        {"ts": TS, "symbol": symbol, "open": 1, "high": 2, "low": 0, "close": 1, "volume": 1}
        for symbol in ("ETHUSDT", "BTCUSDT")
    ]
    emitted = HistoricalDataFeed(pd.DataFrame(rows)).next()
    assert emitted is not None
    assert [bar.symbol for bar in emitted] == ["BTCUSDT", "ETHUSDT"]


def test_quantile_boundary_is_stable_across_float_roundoff() -> None:
    gate = RollingQuantileGate(lookback_bars=3, q=0.5)
    for value in (0.1, 0.1, 0.1):
        assert gate.update(value) == (None, None)
    threshold, passed = gate.update(0.1 + 1e-16)
    assert threshold == pytest.approx(0.1)
    assert passed is True
