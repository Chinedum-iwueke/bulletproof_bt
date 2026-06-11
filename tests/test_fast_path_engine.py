from __future__ import annotations

import csv
import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from bt.api import run_backtest
from bt.core.enums import Side
from bt.core.types import Bar
from bt.engine.fast_path.l7_h1_kernel import L7H1KernelParams, build_l7_h1_feature_frame
from bt.engine.fast_path import run_fast_path_if_supported
from bt.engine.fast_path.timing import TimingRecorder
from bt.strategy.base import Strategy
from bt.strategy.htf_context import PrecomputedHTFContextStrategyAdapter
from bt.strategy.l7_h1_csi_gated_displacement_trend import L7H1CSIGatedDisplacementTrendStrategy
from bt.research_data.jobs.state_features import _with_l7_h1_kernel_features


def _write_dataset(dataset_dir: Path) -> None:
    dataset_dir.mkdir(parents=True)
    (dataset_dir / "manifest.yaml").write_text(
        yaml.safe_dump({"format": "per_symbol_parquet", "symbols": ["BTCUSDT"], "path": "symbols/{symbol}.parquet"}),
        encoding="utf-8",
    )
    ts0 = pd.Timestamp("2024-01-01T00:00:00Z")
    rows = []
    for idx in range(80):
        px = 100.0 + idx * 0.1
        rows.append(
            {
                "ts": ts0 + pd.Timedelta(minutes=idx),
                "symbol": "BTCUSDT",
                "open": px,
                "high": px + 0.3,
                "low": px - 0.3,
                "close": px + 0.05,
                "volume": 1000.0 + idx,
            }
        )
    frame = pd.DataFrame(rows)
    out = dataset_dir / "symbols" / "BTCUSDT.parquet"
    out.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), out)


def _write_config(path: Path, *, execution_engine: str) -> None:
    path.write_text(
        yaml.safe_dump(
            {
                "initial_cash": 1000.0,
                "execution_engine": execution_engine,
                "signal_delay_bars": 1,
                "model": "fixed_bps",
                "fixed_bps": 1.0,
                "data": {"mode": "streaming", "symbols_subset": ["BTCUSDT"], "chunksize": 1000},
                "execution": {"profile": "tier2", "intrabar_mode": "worst_case", "spread_mode": "none"},
                "benchmark": {"enabled": False},
                "risk": {
                    "mode": "equity_pct",
                    "r_per_trade": 0.001,
                    "max_positions": 1,
                    "max_leverage": 2.0,
                    "stop_resolution": "safe",
                    "allow_legacy_proxy": False,
                    "margin_buffer_tier": 1,
                    "slippage_k_proxy": 0.0,
                    "min_stop_distance_pct": 0.001,
                    "max_notional_pct_equity": 1.0,
                    "maintenance_free_margin_pct": 0.01,
                },
                "strategy": {"name": "coinflip", "seed": 7, "p_trade": 0.0, "cooldown_bars": 0},
                "outputs": {"root_dir": "outputs/runs", "jsonl": True},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def _read_equity(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_fast_path_auto_fallback_matches_classic_outputs(tmp_path: Path) -> None:
    dataset = tmp_path / "dataset"
    _write_dataset(dataset)
    classic_cfg = tmp_path / "classic.yaml"
    auto_cfg = tmp_path / "auto.yaml"
    _write_config(classic_cfg, execution_engine="classic")
    _write_config(auto_cfg, execution_engine="auto")

    classic = Path(run_backtest(config_path=str(classic_cfg), data_path=str(dataset), out_dir=str(tmp_path / "classic"), run_name="classic"))
    auto = Path(run_backtest(config_path=str(auto_cfg), data_path=str(dataset), out_dir=str(tmp_path / "auto"), run_name="auto"))

    assert _read_equity(classic / "equity.csv") == _read_equity(auto / "equity.csv")
    assert json.loads((classic / "performance.json").read_text(encoding="utf-8"))["final_equity"] == json.loads(
        (auto / "performance.json").read_text(encoding="utf-8")
    )["final_equity"]
    status = json.loads((auto / "fast_path_status.json").read_text(encoding="utf-8"))
    assert status["handled"] is False
    assert status["mode"] == "classic_fallback"
    timing = json.loads((auto / "run_timing.json").read_text(encoding="utf-8"))
    assert any(event["stage"] == "engine.run" for event in timing["events"])


def test_fast_path_status_falls_back_for_unsupported_research_panel(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    config = {"execution_engine": "fast_path", "data": {"dataset_kind": "research_panel"}, "strategy": {"name": "l7_h1"}}
    result = run_fast_path_if_supported(
        config=config,
        data_path="research_data",
        run_dir=run_dir,
        timing=TimingRecorder(run_dir / "run_timing.json"),
    )

    assert result.handled is False
    assert result.mode == "classic_fallback"
    status = json.loads((run_dir / "fast_path_status.json").read_text(encoding="utf-8"))
    assert "research_panel" in status["reason"]


def test_fast_path_status_attaches_l7h1_compiled_feature_kernel(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_l7h1"
    run_dir.mkdir()
    config = {
        "execution_engine": "auto",
        "data": {"dataset_kind": "research_panel"},
        "strategy": {"name": "l7_h1_csi_gated_displacement_trend"},
    }
    result = run_fast_path_if_supported(
        config=config,
        data_path="research_data",
        run_dir=run_dir,
        timing=TimingRecorder(run_dir / "run_timing.json"),
    )

    assert result.handled is False
    assert result.mode == "classic_with_compiled_l7h1_features"
    status = json.loads((run_dir / "fast_path_status.json").read_text(encoding="utf-8"))
    assert status["mode"] == "classic_with_compiled_l7h1_features"
    assert "L7-H1" in status["reason"]


def test_l7h1_kernel_emits_causal_decision_time_features() -> None:
    ts = pd.date_range(pd.Timestamp("2024-01-01T00:00:00Z"), periods=90, freq="1min")
    close = [100.0 + idx * 0.2 for idx in range(len(ts))]
    panel = pd.DataFrame(
        {
            "ts": ts,
            "symbol": "BTCUSDT",
            "open": close,
            "high": [px + 0.5 for px in close],
            "low": [px - 0.5 for px in close],
            "close": close,
            "volume": [1000.0 + idx for idx in range(len(ts))],
            "funding_rate": [0.0001 + idx * 0.000001 for idx in range(len(ts))],
            "open_interest": [10_000.0 + idx * 10.0 for idx in range(len(ts))],
            "mark_close": close,
            "index_close": [px * 0.999 for px in close],
        }
    )

    features = build_l7_h1_feature_frame(panel, params=L7H1KernelParams(signal_timeframe="15m"))

    assert not features.empty
    assert features["ts"].min() >= pd.Timestamp("2024-01-01T00:15:00Z")
    assert "l7h1_15m_CSI" in features.columns
    assert "l7h1_15m_D_t" in features.columns
    assert features["l7h1_15m_compiled_feature_ready"].all()


def test_l7h1_strategy_consumes_compiled_feature_columns() -> None:
    ts = pd.Timestamp("2024-01-01T00:15:00Z")
    signal_ts = pd.Timestamp("2024-01-01T00:00:00Z")
    strategy = L7H1CSIGatedDisplacementTrendStrategy(signal_timeframe="15m", d0=1.8, theta=0.7, k_stop=3.0)
    bar = Bar(
        ts=ts,
        symbol="BTCUSDT",
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=1000.0,
        extra={
            "l7h1_15m_compiled_feature_ready": True,
            "l7h1_15m_ATR_14": 1.0,
            "l7h1_15m_D_t": 2.0,
            "l7h1_15m_CSI": 0.8,
            "l7h1_15m_CSI_raw": 0.75,
            "l7h1_15m_funding_pct": 0.9,
            "l7h1_15m_basis_pct": 0.8,
            "l7h1_15m_oi_z": 1.0,
            "l7h1_15m_volume_z": 0.5,
            "l7h1_15m_S_t": 0.01,
            "l7h1_15m_spread_rank_desc": 0.6,
            "l7h1_15m_csi_component_funding": 0.9,
            "l7h1_15m_csi_component_oi": 0.7,
            "l7h1_15m_csi_component_displacement": 0.55,
            "l7h1_15m_csi_component_spread": 0.6,
            "l7h1_15m_side_code": 1,
        },
    )
    signal_bar = Bar(
        ts=signal_ts,
        symbol="BTCUSDT",
        open=99.0,
        high=101.0,
        low=98.0,
        close=100.0,
        volume=10_000.0,
    )

    signals = strategy.on_bars(
        ts,
        {"BTCUSDT": bar},
        {"BTCUSDT"},
        {"htf": {"15m": {"BTCUSDT": signal_bar}}, "positions": {}},
    )

    assert len(signals) == 1
    assert signals[0].side == Side.BUY
    assert signals[0].metadata["l7h1_feature_kernel"] == "compiled"
    assert signals[0].metadata["entry_state_funding_pctile"] == 0.9


def test_l7h1_feature_materializer_preserves_ts_symbol_order_for_multi_symbol_panels() -> None:
    ts = pd.date_range(pd.Timestamp("2024-01-01T00:00:00Z"), periods=40, freq="1min")
    rows = []
    for symbol in ("ETHUSDT", "BTCUSDT"):
        for idx, stamp in enumerate(ts):
            px = 100.0 + idx
            rows.append(
                {
                    "ts": stamp,
                    "symbol": symbol,
                    "open": px,
                    "high": px + 1.0,
                    "low": px - 1.0,
                    "close": px,
                    "volume": 1000.0,
                }
            )
    panel = pd.DataFrame(rows).sort_values(["ts", "symbol"]).reset_index(drop=True)

    enriched = _with_l7_h1_kernel_features(panel, signal_timeframes=["15m"], start=None, end=None)

    ordered = enriched[["ts", "symbol"]].sort_values(["ts", "symbol"], kind="mergesort").reset_index(drop=True)
    assert enriched[["ts", "symbol"]].reset_index(drop=True).equals(ordered)


def test_precomputed_htf_context_matches_streaming_cold_start() -> None:
    class Recorder(Strategy):
        def __init__(self) -> None:
            self.emitted: list[pd.Timestamp] = []

        def on_bars(self, ts, bars_by_symbol, tradeable, ctx):  # type: ignore[no-untyped-def]
            htf = ctx.get("htf", {}).get("15m", {})
            if "BTCUSDT" in htf:
                self.emitted.append(htf["BTCUSDT"].ts)
            return []

    inner = Recorder()
    adapter = PrecomputedHTFContextStrategyAdapter(inner=inner, timeframes=["15m"])

    def bar(ts: str, htf_ts: str) -> Bar:
        return Bar(
            ts=pd.Timestamp(ts),
            symbol="BTCUSDT",
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000.0,
            extra={
                "htf_15m_ready": True,
                "htf_15m_ts": pd.Timestamp(htf_ts),
                "htf_15m_open": 100.0,
                "htf_15m_high": 101.0,
                "htf_15m_low": 99.0,
                "htf_15m_close": 100.5,
                "htf_15m_volume": 1000.0,
                "htf_15m_n_bars": 15,
                "htf_15m_expected_bars": 15,
                "htf_15m_is_complete": True,
            },
        )

    # A full-history stamped panel can expose the previous bucket at the
    # backtest's first row. The streaming resampler starts cold and must not.
    first = bar("2024-01-01T00:00:00Z", "2023-12-31T23:45:00Z")
    adapter.on_bars(first.ts, {"BTCUSDT": first}, {"BTCUSDT"}, {})
    second = bar("2024-01-01T00:15:00Z", "2024-01-01T00:00:00Z")
    adapter.on_bars(second.ts, {"BTCUSDT": second}, {"BTCUSDT"}, {})

    assert inner.emitted == [pd.Timestamp("2024-01-01T00:00:00Z")]


def test_public_orchestrator_entrypoints_import_after_fast_path() -> None:
    import orchestrator.research_daemon  # noqa: F401
    import orchestrator.run_experiment_pipeline  # noqa: F401
    import scripts.run_parallel_hypothesis_grid  # noqa: F401
