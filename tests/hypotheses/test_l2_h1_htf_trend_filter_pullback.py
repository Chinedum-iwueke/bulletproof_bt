from __future__ import annotations

from pathlib import Path

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar
from bt.data.resample import HTFBar
from bt.experiments.hypothesis_runner import build_runtime_override
from bt.experiments.parallel_grid import build_hypothesis_manifest
from bt.hypotheses.contract import HypothesisContract
from bt.strategy.l2_h1_htf_trend_filter_pullback import L2H1HTFTrendFilterPullbackStrategy


def _bar(i: int, close: float, *, extra: dict | None = None) -> Bar:
    ts = pd.Timestamp("2025-01-01T00:00:00Z") + pd.Timedelta(minutes=i)
    return Bar(
        ts=ts,
        symbol="BTCUSDT",
        open=close,
        high=close + 0.5,
        low=close - 0.5,
        close=close,
        volume=1000.0,
        extra=extra or {},
    )


def _htf_bar(i: int, close: float) -> HTFBar:
    ts = pd.Timestamp("2024-12-01T00:00:00Z") + pd.Timedelta(hours=i)
    return HTFBar(
        ts=ts,
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


def _ctx(*, htf_bar: HTFBar | None = None, side: Side | None = None) -> dict:
    positions = {} if side is None else {"BTCUSDT": {"side": side.name.lower()}}
    htf = {"1h": {"BTCUSDT": htf_bar}} if htf_bar is not None else {}
    return {"htf": htf, "positions": positions, "state": {}}


def _warm_ltf(strategy: L2H1HTFTrendFilterPullbackStrategy, n: int = 35) -> None:
    for i in range(n):
        b = _bar(i, 100.0)
        strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx())


def _warm_uptrend_htf(strategy: L2H1HTFTrendFilterPullbackStrategy, n: int = 240) -> None:
    for i in range(n):
        b = _bar(1000 + i, 100.0)
        strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(htf_bar=_htf_bar(i, 100.0 + (0.2 * i))))


def test_l2_h1_contract_grid_and_runtime_htf_context(tmp_path: Path) -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l2_h1_htf_trend_filter_pullback.yaml")
    rows = contract.materialize_grid()
    assert len(rows) == 16
    assert {row["params"]["timeframe"] for row in rows} == {"1m", "5m"}
    assert {row["params"]["use_htf_filter"] for row in rows} == {True, False}

    spec = next(row for row in contract.to_run_specs() if row["params"]["timeframe"] == "5m")
    override = build_runtime_override(contract, spec, "Tier2")
    assert override["strategy"]["name"] == "l2_h1_htf_trend_filter_pullback"
    assert sorted(override["htf_resampler"]["timeframes"]) == ["1h", "5m"]
    assert override["htf_resampler"]["strict"] is True

    manifest = build_hypothesis_manifest(
        hypothesis_path=Path("research/hypotheses/l2_h1_htf_trend_filter_pullback.yaml"),
        experiment_root=tmp_path / "exp",
        phase="tier2",
    )
    assert manifest.is_file()


def test_l2_h1_filtered_entry_uses_closed_htf_trend_and_logs_rich_state() -> None:
    strategy = L2H1HTFTrendFilterPullbackStrategy(timeframe="1m", K=3, k_atr=2.0, use_htf_filter=True)
    _warm_uptrend_htf(strategy)
    _warm_ltf(strategy)

    below = _bar(100, 98.0)
    assert strategy.on_bars(below.ts, {"BTCUSDT": below}, {"BTCUSDT"}, _ctx()) == []
    rich = {
        "mark_close": 101.1,
        "index_close": 101.0,
        "funding_rate": 0.0001,
        "funding_source_ts": "2025-01-01T00:00:00Z",
        "open_interest": 123456.0,
        "oi_source_ts": "2025-01-01T00:00:00Z",
        "basis_close_vs_index": 0.001,
    }
    reclaim = _bar(101, 101.0, extra=rich)
    out = strategy.on_bars(reclaim.ts, {"BTCUSDT": reclaim}, {"BTCUSDT"}, _ctx())

    assert out and out[0].side == Side.BUY
    meta = out[0].metadata
    assert meta["entry_reason"] == "htf_trend_ltf_pullback_recovery"
    assert meta["dir_htf"] == 1
    assert meta["htf_ready"] is True
    assert meta["stop_price"] < meta["entry_price"]
    assert meta["entry_state_mark_price"] == 101.1
    assert meta["entry_state_index_price"] == 101.0
    assert meta["entry_state_funding_raw"] == 0.0001
    assert meta["entry_state_oi_level"] == 123456.0
    assert "decision_trace" in meta


def test_l2_h1_filter_blocks_without_htf_readiness_but_baseline_enters() -> None:
    filtered = L2H1HTFTrendFilterPullbackStrategy(timeframe="1m", K=3, use_htf_filter=True)
    baseline = L2H1HTFTrendFilterPullbackStrategy(timeframe="1m", K=3, use_htf_filter=False)
    _warm_ltf(filtered)
    _warm_ltf(baseline)

    below = _bar(100, 98.0)
    filtered.on_bars(below.ts, {"BTCUSDT": below}, {"BTCUSDT"}, _ctx())
    baseline.on_bars(below.ts, {"BTCUSDT": below}, {"BTCUSDT"}, _ctx())
    reclaim = _bar(101, 103.0)

    assert filtered.on_bars(reclaim.ts, {"BTCUSDT": reclaim}, {"BTCUSDT"}, _ctx()) == []
    out = baseline.on_bars(reclaim.ts, {"BTCUSDT": reclaim}, {"BTCUSDT"}, _ctx())
    assert out and out[0].side == Side.BUY
    assert out[0].metadata["use_htf_filter"] is False
