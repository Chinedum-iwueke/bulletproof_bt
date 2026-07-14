from __future__ import annotations

from pathlib import Path

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar
from bt.data.resample import HTFBar
from bt.experiments.hypothesis_runner import build_runtime_override
from bt.experiments.parallel_grid import build_hypothesis_manifest
from bt.hypotheses.contract import HypothesisContract
from bt.strategy.l2_h4_prior_day_extreme_traps import L2H4PriorDayExtremeTrapsStrategy


def _bar(i: int, close: float, *, high: float | None = None, low: float | None = None) -> Bar:
    ts = pd.Timestamp("2024-01-02T00:00:00Z") + pd.Timedelta(minutes=i)
    return Bar(
        ts=ts,
        symbol="BTCUSDT",
        open=close,
        high=close if high is None else high,
        low=close if low is None else low,
        close=close,
        volume=1000.0,
    )


def _daily() -> HTFBar:
    return HTFBar(
        ts=pd.Timestamp("2024-01-01T00:00:00Z"),
        symbol="BTCUSDT",
        open=100.0,
        high=105.0,
        low=95.0,
        close=100.0,
        volume=100000.0,
        timeframe="1d",
        n_bars=1440,
        expected_bars=1440,
        is_complete=True,
    )


def _ctx(signal_bar: Bar, *, side: str | None = None, include_daily: bool = True) -> dict:
    positions = {} if side is None else {"BTCUSDT": {"side": side}}
    htf = {"5m": {"BTCUSDT": signal_bar}}
    if include_daily:
        htf["1d"] = {"BTCUSDT": _daily()}
    return {"htf": htf, "positions": positions}


def _warm(strategy: L2H4PriorDayExtremeTrapsStrategy, *, include_daily: bool = True) -> None:
    for i in range(20):
        b = _bar(i, 100.0, high=101.0, low=99.0)
        strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b, include_daily=include_daily))


def test_l2_h4_contract_grid_and_runtime_htf_context(tmp_path: Path) -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l2_h4_prior_day_extreme_traps.yaml")
    rows = contract.materialize_grid()
    assert len(rows) == 8
    assert {row["params"]["epsilon_atr"] for row in rows} == {0.25, 0.5}
    assert {row["params"]["delta_atr"] for row in rows} == {0.1, 0.2}
    assert {row["params"]["k_atr"] for row in rows} == {2.0, 2.5}

    spec = contract.to_run_specs()[0]
    override = build_runtime_override(contract, spec, "Tier2")
    assert override["strategy"]["name"] == "l2_h4_prior_day_extreme_traps"
    assert sorted(override["htf_resampler"]["timeframes"]) == ["1d", "5m"]
    assert override["data"]["entry_timeframe"] is None

    manifest = build_hypothesis_manifest(
        hypothesis_path=Path("research/hypotheses/l2_h4_prior_day_extreme_traps.yaml"),
        experiment_root=tmp_path / "exp",
        phase="tier2",
    )
    assert manifest.is_file()


def test_l2_h4_high_vol_pdh_breakout_continuation() -> None:
    strategy = L2H4PriorDayExtremeTrapsStrategy(timeframe="5m", epsilon_atr=0.5, delta_atr=0.1)
    _warm(strategy)
    st = strategy._state_for("BTCUSDT")
    st.vol_gate._history.extend([0.001] * st.vol_gate._history.maxlen)
    st.compression_gate._history.extend([0.001] * st.compression_gate._history.maxlen)

    trigger = _bar(25, 105.25, high=105.3, low=105.0)
    out = strategy.on_bars(trigger.ts, {"BTCUSDT": trigger}, {"BTCUSDT"}, _ctx(trigger))

    assert out
    assert out[0].side == Side.BUY
    meta = out[0].metadata
    assert meta["trigger_type"] == "pdh_breakout_continuation"
    assert meta["regime_chosen"] == "high_vol_breakout"
    assert meta["pdh"] == 105.0
    assert meta["pdl"] == 95.0
    assert meta["prior_day_anchor_id"] == "2024-01-01"
    assert meta["high_vol_gate_t"] is True
    assert meta["one_attempt_per_day"] is True
    assert "decision_trace" in meta


def test_l2_h4_compression_rejection_fade_and_one_attempt_rule() -> None:
    strategy = L2H4PriorDayExtremeTrapsStrategy(timeframe="5m", epsilon_atr=0.5, delta_atr=0.2)
    _warm(strategy)
    st = strategy._state_for("BTCUSDT")
    st.vol_gate._history.extend([0.20] * st.vol_gate._history.maxlen)
    st.compression_gate._history.extend([0.03] * st.compression_gate._history.maxlen)

    rejection = _bar(25, 104.8, high=105.4, low=104.5)
    out = strategy.on_bars(rejection.ts, {"BTCUSDT": rejection}, {"BTCUSDT"}, _ctx(rejection))

    assert out
    assert out[0].side == Side.SELL
    assert out[0].metadata["trigger_type"] == "pdh_rejection_fade"
    assert out[0].metadata["regime_chosen"] == "compression_fade"
    assert out[0].metadata["comp_gate_t"] is True

    later = _bar(30, 104.7, high=105.2, low=104.4)
    assert strategy.on_bars(later.ts, {"BTCUSDT": later}, {"BTCUSDT"}, _ctx(later)) == []


def test_l2_h4_no_entry_without_closed_daily_reference() -> None:
    strategy = L2H4PriorDayExtremeTrapsStrategy(timeframe="5m")
    _warm(strategy, include_daily=False)
    trigger = _bar(25, 105.25, high=105.3, low=105.0)
    out = strategy.on_bars(trigger.ts, {"BTCUSDT": trigger}, {"BTCUSDT"}, _ctx(trigger, include_daily=False))
    assert out == []
