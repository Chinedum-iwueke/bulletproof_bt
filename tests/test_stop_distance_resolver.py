from __future__ import annotations

from dataclasses import dataclass

import pytest

from bt.risk.stop_distance import resolve_stop_distance


@dataclass
class SignalStub:
    stop_price: float | None = None


@dataclass
class IndicatorStub:
    is_ready: bool
    value: float | None


def test_resolve_stop_distance_signal_stop_long() -> None:
    result = resolve_stop_distance(
        symbol="AAPL",
        side="long",
        entry_price=100.0,
        signal=SignalStub(stop_price=95.0),
        bars_by_symbol={},
        ctx={},
        config={},
    )

    assert result.stop_distance == 5.0
    assert result.source == "explicit_stop_price"


def test_resolve_stop_distance_signal_stop_invalid_side_long_uses_absolute_distance() -> None:
    result = resolve_stop_distance(
        symbol="AAPL",
        side="long",
        entry_price=100.0,
        signal={"stop_price": 101.0},
        bars_by_symbol={},
        ctx={},
        config={},
    )

    assert result.stop_distance == 1.0
    assert result.details["direction_mismatch_vs_entry"] is True


def test_resolve_stop_distance_invalid_side_near_entry_uses_absolute_distance() -> None:
    result = resolve_stop_distance(
        symbol="LDOUSDT:USDT",
        side="long",
        entry_price=1.2505,
        signal={"stop_price": 1.2520},
        bars_by_symbol={},
        ctx={},
        config={},
    )

    assert result.source == "explicit_stop_price"
    assert result.stop_distance == pytest.approx(0.0015)
    assert result.details["direction_mismatch_vs_entry"] is True


def test_resolve_stop_distance_invalid_side_near_entry_autocorrects() -> None:
    result = resolve_stop_distance(
        symbol="LDOUSDT:USDT",
        side="long",
        entry_price=1.2505,
        signal={"stop_price": 1.2520},
        bars_by_symbol={},
        ctx={},
        config={},
    )

    assert result.source == "explicit_stop_price"
    assert result.stop_distance == pytest.approx(0.0015)
    assert result.details["auto_corrected_invalid_side"] is True


def test_resolve_stop_distance_invalid_side_near_entry_can_be_disabled() -> None:
    with pytest.raises(ValueError, match=r"LDOUSDT:USDT: invalid stop_price for long: stop=1\.252 entry=1\.2505"):
        resolve_stop_distance(
            symbol="LDOUSDT:USDT",
            side="long",
            entry_price=1.2505,
            signal={"stop_price": 1.2520},
            bars_by_symbol={},
            ctx={},
            config={"risk": {"stop": {"invalid_side_tolerance_pct": 0.0}}},
        )


def test_resolve_stop_distance_atr_rule() -> None:
    config = {"risk": {"stop": {"mode": "atr", "atr_multiple": 2.0, "atr_indicator": "atr"}}}
    ctx = {"indicators": {"AAPL": {"atr": IndicatorStub(is_ready=True, value=1.5)}}}

    result = resolve_stop_distance(
        symbol="AAPL",
        side="short",
        entry_price=100.0,
        signal={},
        bars_by_symbol={},
        ctx=ctx,
        config=config,
    )

    assert result.stop_distance == 3.0
    assert result.source == "atr_multiple"


def test_resolve_stop_distance_atr_not_ready() -> None:
    config = {"risk": {"stop": {"mode": "atr", "atr_multiple": 2.0, "atr_indicator": "atr"}}}
    ctx = {"indicators": {"AAPL": {"atr": IndicatorStub(is_ready=False, value=None)}}}

    with pytest.raises(ValueError, match=r"ATR indicator 'atr' is not ready"):
        resolve_stop_distance(
            symbol="AAPL",
            side="long",
            entry_price=100.0,
            signal={},
            bars_by_symbol={},
            ctx=ctx,
            config=config,
        )


def test_resolve_stop_distance_missing_rules_actionable_error() -> None:
    with pytest.raises(
        ValueError,
        match=r"Provide signal\.stop_price or configure risk\.stop\.mode=atr",
    ):
        resolve_stop_distance(
            symbol="AAPL",
            side="long",
            entry_price=100.0,
            signal={},
            bars_by_symbol={},
            ctx={},
            config={},
        )


def test_resolve_stop_distance_sources_are_non_empty_allowed_values() -> None:
    explicit = resolve_stop_distance(
        symbol="AAPL",
        side="long",
        entry_price=100.0,
        signal={"stop_price": 99.0},
        bars_by_symbol={},
        ctx={},
        config={},
    )
    atr = resolve_stop_distance(
        symbol="AAPL",
        side="short",
        entry_price=100.0,
        signal={},
        bars_by_symbol={},
        ctx={"indicators": {"AAPL": {"atr": IndicatorStub(is_ready=True, value=1.0)}}},
        config={"risk": {"stop": {"mode": "atr", "atr_multiple": 2.0}}},
    )
    allowed = {"explicit_stop_price", "atr_multiple", "legacy_high_low_proxy"}
    assert explicit.source in allowed
    assert atr.source in allowed
    assert explicit.source
    assert atr.source
