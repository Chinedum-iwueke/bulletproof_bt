from __future__ import annotations

from pathlib import Path

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Signal
from bt.data.resampled_feed import EntryTimeframeGate
from bt.experiments.hypothesis_runner import build_runtime_override
from bt.experiments.parallel_grid import build_hypothesis_manifest
from bt.hypotheses.contract import HypothesisContract


class _DummyStrategy:
    def __init__(self, signals: list[Signal]) -> None:
        self._signals = signals

    def on_bars(self, ts, bars_by_symbol, tradeable, ctx):
        return list(self._signals)


def _signal(ts: str, *, close_only: bool = False) -> Signal:
    side = Side.SELL if close_only else Side.BUY
    return Signal(
        ts=pd.Timestamp(ts),
        symbol="BTCUSDT",
        side=side,
        signal_type="dummy_exit" if close_only else "dummy_entry",
        confidence=1.0,
        metadata={"close_only": True} if close_only else {},
    )


def test_l2_h2_contract_grid_and_entry_timeframe_runtime_mapping(tmp_path: Path) -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l2_h2_entry_timeframe_boundary_gate.yaml")
    rows = contract.materialize_grid()
    assert len(rows) == 4
    assert {row["params"]["entry_timeframe"] for row in rows} == {"none", "5m", "15m", "1h"}

    control_spec = next(row for row in contract.to_run_specs() if row["params"]["entry_timeframe"] == "none")
    control = build_runtime_override(contract, control_spec, "Tier2")
    assert control["strategy"]["name"] == "l1_h1_vol_floor_trend"
    assert control["data"]["entry_timeframe"] is None
    assert "entry_timeframe" not in control["strategy"]

    gated_spec = next(row for row in contract.to_run_specs() if row["params"]["entry_timeframe"] == "15m")
    gated = build_runtime_override(contract, gated_spec, "Tier2")
    assert gated["data"]["entry_timeframe"] == "15m"
    assert "entry_timeframe" not in gated["strategy"]

    manifest = build_hypothesis_manifest(
        hypothesis_path=Path("research/hypotheses/l2_h2_entry_timeframe_boundary_gate.yaml"),
        experiment_root=tmp_path / "exp",
        phase="tier2",
    )
    assert manifest.is_file()


def test_entry_timeframe_gate_blocks_entries_off_boundary_but_preserves_exits() -> None:
    ts = "2025-01-01T00:07:00Z"
    entry = _signal(ts)
    exit_signal = _signal(ts, close_only=True)
    gate = EntryTimeframeGate(inner=_DummyStrategy([entry, exit_signal]), entry_timeframe="15m")

    out = gate.on_bars(pd.Timestamp(ts), {}, {"BTCUSDT"}, {})

    assert len(out) == 1
    assert out[0].metadata["close_only"] is True
    assert out[0].metadata["allow_entries"] is False
    assert out[0].metadata["entry_timeframe_gate_applied"] is True

    artifacts = gate.strategy_artifacts()["entry_timeframe_gate"]
    assert artifacts["blocked_entry_signals"] == 1
    assert artifacts["exit_signals_preserved"] == 1


def test_entry_timeframe_gate_allows_entries_on_boundary_and_logs_flag() -> None:
    ts = "2025-01-01T00:15:00Z"
    entry = _signal(ts)
    gate = EntryTimeframeGate(inner=_DummyStrategy([entry]), entry_timeframe="15m")

    out = gate.on_bars(pd.Timestamp(ts), {}, {"BTCUSDT"}, {})

    assert len(out) == 1
    assert out[0].side == Side.BUY
    assert out[0].metadata["allow_entries"] is True
    assert out[0].metadata["entry_timeframe_boundary"] is True
    assert out[0].metadata["entry_timeframe"] == "15m"

    artifacts = gate.strategy_artifacts()["entry_timeframe_gate"]
    assert artifacts["allowed_entry_signals"] == 1
    assert artifacts["blocked_entry_signals"] == 0
