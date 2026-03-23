from __future__ import annotations

import pytest

from bt.risk.risk_engine import RiskEngine


def _engine(*, qty_rounding: str = "none", min_stop_distance: float | None = None) -> RiskEngine:
    risk_cfg: dict[str, object] = {
        "mode": "r_fixed",
        "r_per_trade": 0.01,
        "qty_rounding": qty_rounding,
        "stop": {},
    }
    if min_stop_distance is not None:
        risk_cfg["min_stop_distance"] = min_stop_distance
    return RiskEngine(
        max_positions=1,
                config={"risk": risk_cfg},
    )


def test_compute_position_size_r_basic_with_explicit_stop() -> None:
    engine = _engine()

    qty, meta = engine.compute_position_size_r(
        symbol="BTC",
        side="long",
        entry_price=100.0,
        signal={"stop_price": 95.0},
        bars_by_symbol={},
        ctx={},
        equity=10_000.0,
    )

    assert qty == pytest.approx(20.0)
    assert meta["risk_amount"] == pytest.approx(100.0)
    assert meta["stop_distance"] == pytest.approx(5.0)
    assert meta["stop_source"] == "explicit_stop_price"
    assert "stop_details" in meta


def test_compute_position_size_r_min_stop_distance_clamps() -> None:
    engine = _engine(min_stop_distance=10.0)

    qty, meta = engine.compute_position_size_r(
        symbol="BTC",
        side="long",
        entry_price=100.0,
        signal={"stop_price": 95.0},
        bars_by_symbol={},
        ctx={},
        equity=10_000.0,
    )

    assert meta["stop_distance"] == pytest.approx(10.0)
    assert qty == pytest.approx(10.0)


def test_compute_position_size_r_zero_stop_distance_auto_widens() -> None:
    engine = _engine()

    qty, meta = engine.compute_position_size_r(
        symbol="BTC",
        side="long",
        entry_price=100.0,
        signal={"stop_price": 100.0},
        bars_by_symbol={},
        ctx={},
        equity=10_000.0,
    )

    assert qty > 0
    assert meta["stop_distance"] == pytest.approx(0.0001)


@pytest.mark.parametrize(
    ("qty_rounding", "expected_qty"),
    [
        ("floor", 33.33333333),
        ("round", 33.33333334),
    ],
)
def test_compute_position_size_r_rounding_deterministic(qty_rounding: str, expected_qty: float) -> None:
    engine = _engine(qty_rounding=qty_rounding)

    qty, _meta = engine.compute_position_size_r(
        symbol="BTC",
        side="long",
        entry_price=100.0,
        signal={"stop_price": 97.00000000024},
        bars_by_symbol={},
        ctx={},
        equity=10_000.0,
    )

    assert qty == pytest.approx(expected_qty)
