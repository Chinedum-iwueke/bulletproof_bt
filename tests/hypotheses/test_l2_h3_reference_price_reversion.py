from __future__ import annotations

from pathlib import Path

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar
from bt.experiments.hypothesis_runner import build_runtime_override
from bt.experiments.parallel_grid import build_hypothesis_manifest
from bt.hypotheses.contract import HypothesisContract
from bt.strategy.l2_h3_reference_price_reversion import L2H3ReferencePriceReversionStrategy


def _bar(i: int, close: float, *, low: float | None = None, high: float | None = None, volume: float = 1000.0) -> Bar:
    ts = pd.Timestamp("2024-01-01T00:00:00Z") + pd.Timedelta(minutes=i)
    return Bar(
        ts=ts,
        symbol="BTCUSDT",
        open=close,
        high=close if high is None else high,
        low=close if low is None else low,
        close=close,
        volume=volume,
    )


def _bar_at(ts: str, close: float, *, low: float | None = None, high: float | None = None) -> Bar:
    stamp = pd.Timestamp(ts)
    return Bar(
        ts=stamp,
        symbol="BTCUSDT",
        open=close,
        high=close if high is None else high,
        low=close if low is None else low,
        close=close,
        volume=1000.0,
    )


def _ctx(signal_bar: Bar, *, side: str | None = None) -> dict:
    positions = {} if side is None else {"BTCUSDT": {"side": side}}
    return {"htf": {"5m": {"BTCUSDT": signal_bar}}, "positions": positions}


def _warm_ready(strategy: L2H3ReferencePriceReversionStrategy) -> None:
    for i in range(20):
        b = _bar(i, 100.0, high=100.2, low=99.8)
        strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b))
    st = strategy._state_for("BTCUSDT")
    st.compression_gate._history.extend([0.03] * st.compression_gate._history.maxlen)
    st.liquidity_gate._history.extend([0.02] * st.liquidity_gate._history.maxlen)


def test_l2_h3_contract_grid_and_runtime_mapping(tmp_path: Path) -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l2_h3_reference_price_reversion.yaml")
    rows = contract.materialize_grid()
    assert len(rows) == 4
    assert {row["params"]["z0"] for row in rows} == {0.8, 1.2}
    assert {row["params"]["k_atr"] for row in rows} == {1.5, 2.0}

    spec = contract.to_run_specs()[0]
    override = build_runtime_override(contract, spec, "Tier2")
    assert override["strategy"]["name"] == "l2_h3_reference_price_reversion"
    assert override["htf_resampler"]["timeframes"] == ["5m"]

    manifest = build_hypothesis_manifest(
        hypothesis_path=Path("research/hypotheses/l2_h3_reference_price_reversion.yaml"),
        experiment_root=tmp_path / "exp",
        phase="tier2",
    )
    assert manifest.is_file()


def test_l2_h3_session_vwap_long_signal_logs_anchor_z_and_gates() -> None:
    strategy = L2H3ReferencePriceReversionStrategy(timeframe="5m", z0=0.8, k_atr=1.5)
    _warm_ready(strategy)

    trigger = _bar(21, 98.0, high=98.2, low=97.8)
    out = strategy.on_bars(trigger.ts, {"BTCUSDT": trigger}, {"BTCUSDT"}, _ctx(trigger))

    assert out
    assert out[0].side == Side.BUY
    meta = out[0].metadata
    assert meta["entry_reason"] == "session_vwap_reference_fade_long"
    assert meta["anchor_id"] == "2024-01-01"
    assert meta["session_vwap"] is not None
    assert meta["z"] <= -0.8
    assert meta["comp_gate_t"] is True
    assert meta["liq_gate_t"] is True
    assert meta["profit_exit_model"] == "session_vwap_touch"
    assert "decision_trace" in meta


def test_l2_h3_short_signal_and_liquidity_gate_block() -> None:
    strategy = L2H3ReferencePriceReversionStrategy(timeframe="5m", z0=0.8)
    _warm_ready(strategy)
    short_bar = _bar(21, 102.0, high=102.2, low=101.8)
    out = strategy.on_bars(short_bar.ts, {"BTCUSDT": short_bar}, {"BTCUSDT"}, _ctx(short_bar))
    assert out and out[0].side == Side.SELL
    assert out[0].metadata["entry_reason"] == "session_vwap_reference_fade_short"

    blocked = L2H3ReferencePriceReversionStrategy(timeframe="5m", z0=0.8)
    _warm_ready(blocked)
    st = blocked._state_for("BTCUSDT")
    st.liquidity_gate._history.clear()
    st.liquidity_gate._history.extend([0.001] * st.liquidity_gate._history.maxlen)
    noisy = _bar(21, 98.0, high=102.0, low=94.0)
    assert blocked.on_bars(noisy.ts, {"BTCUSDT": noisy}, {"BTCUSDT"}, _ctx(noisy)) == []


def test_l2_h3_hard_exits_at_utc_session_end() -> None:
    strategy = L2H3ReferencePriceReversionStrategy(timeframe="5m")
    st = strategy._state_for("BTCUSDT")
    st.position = Side.BUY
    st.entry_session_key = pd.Timestamp("2024-01-01T00:00:00Z")
    st.stop_price_frozen = 90.0
    st.stop_distance_frozen = 10.0
    st.atr_entry = 5.0

    b = _bar_at("2024-01-01T23:59:00Z", 99.0, high=99.5, low=98.5)
    out = strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b, side="buy"))

    assert out
    assert out[0].metadata["close_only"] is True
    assert out[0].metadata["exit_reason"] == "session_end"
    assert out[0].metadata["anchor_id"] == "2024-01-01"
