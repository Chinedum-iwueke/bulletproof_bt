from pathlib import Path

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Bar
from bt.experiments.hypothesis_runner import build_runtime_override
from bt.experiments.parallel_grid import build_hypothesis_manifest
from bt.hypotheses.contract import HypothesisContract
from bt.strategy.l1_h8_trend_continuation_pullback import L1H8TrendContinuationPullbackStrategy


def test_l1_h8_variants_have_exactly_24_runs() -> None:
    for path in [
        "research/hypotheses/l1_h8a.yaml",
        "research/hypotheses/l1_h8b.yaml",
        "research/hypotheses/l1_h8c.yaml",
        "research/hypotheses/l1_h8d.yaml",
        "research/hypotheses/l1_h8e.yaml",
    ]:
        contract = HypothesisContract.from_yaml(path)
        rows = contract.materialize_grid()
        assert len(rows) == 24
        assert contract.schema.execution_semantics["base_data_frequency_expected"] == "1m"
        assert contract.schema.execution_semantics["exit_monitoring_timeframe"] == "1m"
        assert contract.schema.execution_semantics["risk_accounting"] == "engine_canonical_R"


def test_l1_h8_runtime_override_and_parallel_manifest() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h8d.yaml")
    spec = next(row for row in contract.to_run_specs() if row["params"]["signal_timeframe"] == "1h")
    override = build_runtime_override(contract, spec, "Tier2")
    assert override["strategy"]["name"] == "l1_h8_trend_continuation_pullback"
    assert override["strategy"]["timeframe"] == "1h"


def test_l1_h8_parallel_manifest_build(tmp_path: Path) -> None:
    manifest = build_hypothesis_manifest(
        hypothesis_path=Path("research/hypotheses/l1_h8a.yaml"),
        experiment_root=tmp_path / "exp",
        phase="tier2",
    )
    assert manifest.is_file()
    assert manifest.name == "l1_h8a_tier2_grid.csv"


def _bar(i: int, close: float, *, low: float | None = None, high: float | None = None) -> Bar:
    ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(minutes=i)
    return Bar(
        ts=ts,
        symbol="BTCUSDT",
        open=close,
        high=close if high is None else high,
        low=close if low is None else low,
        close=close,
        volume=1000,
    )


def _ctx(signal_bar: Bar, side: str | None = None, tf: str = "15m") -> dict:
    positions = {} if side is None else {"BTCUSDT": {"side": side}}
    return {"htf": {tf: {"BTCUSDT": signal_bar}}, "positions": positions}


def test_l1_h8_entry_logging_contains_required_fields() -> None:
    strategy = L1H8TrendContinuationPullbackStrategy(timeframe="15m", adx_min=0.0, pullback_max_bars=3, pullback_reference_mode="ema_only")

    for i in range(40):
        b = _bar(i, 100.0 + (0.01 * i), low=99.8, high=100.2 + (0.01 * i))
        strategy.on_bars(b.ts, {"BTCUSDT": b}, {"BTCUSDT"}, _ctx(b))

    st = strategy._state_for("BTCUSDT")
    st.pullback_direction = Side.BUY
    st.pullback_bars = 0
    st.pullback_extreme_low = 99.5
    st.pullback_extreme_high = 101.2
    st.pullback_hit_ema = True

    trigger = _bar(42, 102.5, low=101.8, high=102.8)
    out = strategy.on_bars(trigger.ts, {"BTCUSDT": trigger}, {"BTCUSDT"}, _ctx(trigger))
    assert out
    meta = out[0].metadata
    for key in [
        "trend_dir",
        "ema_fast",
        "ema_slow",
        "adx",
        "pullback_state",
        "pullback_depth",
        "reference_hit",
        "continuation_trigger",
        "stop_distance",
        "signal_timeframe",
        "risk_accounting",
    ]:
        assert key in meta
    assert meta["risk_accounting"] == "engine_canonical_R"
