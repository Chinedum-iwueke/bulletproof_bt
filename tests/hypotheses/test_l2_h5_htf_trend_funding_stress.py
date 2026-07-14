from __future__ import annotations

from pathlib import Path

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar
from bt.data.resample import HTFBar
from bt.experiments.hypothesis_runner import build_runtime_override
from bt.experiments.parallel_grid import build_hypothesis_manifest
from bt.hypotheses.contract import HypothesisContract
from bt.strategy.l2_h5_htf_trend_funding_stress import L2H5HTFTrendFundingStressStrategy


def _bar(i: int, close: float, *, funding: float = 0.0001, funding_source_i: int | None = None) -> Bar:
    ts = pd.Timestamp("2025-01-01T00:00:00Z") + pd.Timedelta(minutes=i)
    source_i = i if funding_source_i is None else funding_source_i
    if source_i >= 99999:
        source_ts = ts + pd.Timedelta(hours=8)
    else:
        source_ts = ts - pd.Timedelta(hours=8) + pd.Timedelta(seconds=source_i)
    return Bar(
        ts=ts,
        symbol="BTCUSDT",
        open=close,
        high=close + 0.5,
        low=close - 0.5,
        close=close,
        volume=1000.0,
        extra={"funding_rate": funding, "funding_source_ts": source_ts},
    )


def _htf(i: int, close: float) -> HTFBar:
    return HTFBar(
        ts=pd.Timestamp("2024-12-01T00:00:00Z") + pd.Timedelta(hours=i),
        symbol="BTCUSDT",
        open=close,
        high=close + 1.0,
        low=close - 1.0,
        close=close,
        volume=10000.0,
        timeframe="1h",
        n_bars=60,
        expected_bars=60,
        is_complete=True,
    )


def _ctx(signal_bar: Bar, *, htf_bar: HTFBar | None = None, side: str | None = None) -> dict:
    positions = {} if side is None else {"BTCUSDT": {"side": side}}
    htf = {"5m": {"BTCUSDT": signal_bar}}
    if htf_bar is not None:
        htf["1h"] = {"BTCUSDT": htf_bar}
    return {"htf": htf, "positions": positions}


def _warm(strategy: L2H5HTFTrendFundingStressStrategy) -> None:
    for i in range(240):
        b = _bar(1000 + i, 100.0, funding=0.0001, funding_source_i=i)
        strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b, htf_bar=_htf(i, 100.0 + i * 0.2)))
    for i in range(35):
        b = _bar(i, 100.0, funding=0.0001, funding_source_i=500 + i)
        strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b))


def test_l2_h5_contract_grid_and_runtime_htf_context(tmp_path: Path) -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l2_h5_htf_trend_funding_stress.yaml")
    rows = contract.materialize_grid()
    assert len(rows) == 4
    assert {row["params"]["funding_z_threshold"] for row in rows} == {1.0, 1.5}
    assert {row["params"]["funding_lookback_days"] for row in rows} == {14, 30}

    spec = contract.to_run_specs()[0]
    override = build_runtime_override(contract, spec, "Tier2")
    assert override["strategy"]["name"] == "l2_h5_htf_trend_funding_stress"
    assert sorted(override["htf_resampler"]["timeframes"]) == ["1h", "5m"]

    manifest = build_hypothesis_manifest(
        hypothesis_path=Path("research/hypotheses/l2_h5_htf_trend_funding_stress.yaml"),
        experiment_root=tmp_path / "exp",
        phase="tier2",
    )
    assert manifest.is_file()


def test_l2_h5_low_funding_stress_entry_logs_provenance() -> None:
    strategy = L2H5HTFTrendFundingStressStrategy(timeframe="5m", funding_z_threshold=1.0)
    _warm(strategy)
    st = strategy._state_for("BTCUSDT")
    st.funding_stats.values.clear()
    st.funding_stats.values.extend([0.0001, 0.00011, 0.00009, 0.0001, 0.000105, 0.000095])

    below = _bar(100, 98.0, funding=0.0001, funding_source_i=900)
    strategy.on_bars(below.ts, {"BTCUSDT": below}, {"BTCUSDT"}, _ctx(below))
    reclaim = _bar(101, 101.0, funding=0.0001, funding_source_i=901)
    out = strategy.on_bars(reclaim.ts, {"BTCUSDT": reclaim}, {"BTCUSDT"}, _ctx(reclaim))

    assert out
    assert out[0].side == Side.BUY
    meta = out[0].metadata
    assert meta["entry_reason"] == "htf_trend_pullback_low_funding_stress"
    assert meta["funding_stress"] is False
    assert meta["funding_source_valid"] is True
    assert len(meta["funding_provenance_hash"]) == 64
    assert meta["entry_state_funding_z"] is not None


def test_l2_h5_high_funding_stress_blocks_entry() -> None:
    strategy = L2H5HTFTrendFundingStressStrategy(timeframe="5m", funding_z_threshold=1.0)
    _warm(strategy)
    st = strategy._state_for("BTCUSDT")
    st.funding_stats.values.clear()
    st.funding_stats.values.extend([0.0001, 0.00011, 0.00009, 0.0001, 0.000105, 0.000095])

    below = _bar(100, 98.0, funding=0.0001, funding_source_i=900)
    strategy.on_bars(below.ts, {"BTCUSDT": below}, {"BTCUSDT"}, _ctx(below))
    stressed = _bar(101, 101.0, funding=0.001, funding_source_i=901)
    assert strategy.on_bars(stressed.ts, {"BTCUSDT": stressed}, {"BTCUSDT"}, _ctx(stressed)) == []


def test_l2_h5_future_funding_source_ts_is_rejected() -> None:
    strategy = L2H5HTFTrendFundingStressStrategy(timeframe="5m", funding_z_threshold=1.0)
    _warm(strategy)
    st = strategy._state_for("BTCUSDT")
    st.funding_stats.values.clear()
    st.funding_stats.values.extend([0.0001] * 6)

    below = _bar(100, 98.0, funding=0.0001, funding_source_i=900)
    strategy.on_bars(below.ts, {"BTCUSDT": below}, {"BTCUSDT"}, _ctx(below))
    future = _bar(101, 101.0, funding=0.0001, funding_source_i=99999)
    assert strategy.on_bars(future.ts, {"BTCUSDT": future}, {"BTCUSDT"}, _ctx(future)) == []
    assert strategy._state_for("BTCUSDT").funding_causality_violations >= 1


def test_l2_h5_funding_flip_exit() -> None:
    strategy = L2H5HTFTrendFundingStressStrategy(timeframe="5m")
    st = strategy._state_for("BTCUSDT")
    st.entry_side = Side.BUY
    st.entry_funding_sign = 1
    st.entry_fund_z = 0.5
    st.stop_price_frozen = 90.0
    st.stop_distance_frozen = 10.0
    st.atr_entry = 5.0

    b = _bar(200, 100.0, funding=-0.0001, funding_source_i=900)
    st.funding_stats.values.extend([0.0001, 0.00011, 0.00009, 0.0001, 0.000105, 0.000095])
    out = strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b, side="buy"))

    assert out
    assert out[0].metadata["close_only"] is True
    assert out[0].metadata["exit_reason"] == "funding_flip"
