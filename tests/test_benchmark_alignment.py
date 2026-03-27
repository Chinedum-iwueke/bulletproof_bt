from __future__ import annotations

from pathlib import Path

import pandas as pd

from bt.benchmarks.align import align_strategy_and_benchmark, prepare_strategy_daily_comparison_series
from bt.benchmarks.types import BenchmarkDatasetMetadata, EnabledBenchmarkConfig, LoadedBenchmarkDataset


def _loaded_benchmark(ts: list[str], close: list[float]) -> LoadedBenchmarkDataset:
    data = pd.DataFrame({"ts": pd.to_datetime(ts, utc=True), "symbol": "BTC", "close": close})
    config = EnabledBenchmarkConfig(
        enabled=True,
        mode="manual",
        id="BTC",
        source="platform_managed",
        library_root=Path("/"),
        library_revision="rev-1",
        frequency="1d",
        alignment_policy="common_daily_timestamps_only",
        comparison_frequency="1d",
        normalization_basis="100_at_first_common_timestamp",
    )
    metadata = BenchmarkDatasetMetadata(
        benchmark_id="BTC",
        dataset_path=Path("/benchmarks/BTC/daily.parquet"),
        row_count=len(data),
        start_ts=data["ts"].iloc[0],
        end_ts=data["ts"].iloc[-1],
    )
    return LoadedBenchmarkDataset(enabled=True, config=config, data=data, metadata=metadata)


def test_alignment_perfect_overlap() -> None:
    strategy = pd.DataFrame({"ts": pd.to_datetime(["2025-01-01", "2025-01-02"], utc=True), "equity": [100, 105]})
    strategy_daily = prepare_strategy_daily_comparison_series(strategy)
    aligned = align_strategy_and_benchmark(
        strategy_daily=strategy_daily,
        loaded_benchmark=_loaded_benchmark(["2025-01-01", "2025-01-02"], [200, 210]),
    )

    assert aligned.available is True
    assert aligned.point_count == 2


def test_alignment_partial_overlap() -> None:
    strategy = pd.DataFrame({"ts": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"], utc=True), "equity": [100, 105, 110]})
    strategy_daily = prepare_strategy_daily_comparison_series(strategy)
    aligned = align_strategy_and_benchmark(
        strategy_daily=strategy_daily,
        loaded_benchmark=_loaded_benchmark(["2025-01-02", "2025-01-03", "2025-01-04"], [210, 220, 230]),
    )

    assert aligned.available is True
    assert aligned.point_count == 2
    assert aligned.first_common_ts == pd.Timestamp("2025-01-02", tz="UTC")


def test_alignment_no_overlap_unavailable() -> None:
    strategy = pd.DataFrame({"ts": pd.to_datetime(["2025-01-01", "2025-01-02"], utc=True), "equity": [100, 105]})
    strategy_daily = prepare_strategy_daily_comparison_series(strategy)
    unavailable = align_strategy_and_benchmark(
        strategy_daily=strategy_daily,
        loaded_benchmark=_loaded_benchmark(["2025-01-10", "2025-01-11"], [200, 210]),
    )

    assert unavailable.available is False
    assert unavailable.reason == "no_benchmark_overlap"


def test_alignment_single_point_overlap_unavailable() -> None:
    strategy = pd.DataFrame({"ts": pd.to_datetime(["2025-01-01", "2025-01-02"], utc=True), "equity": [100, 105]})
    strategy_daily = prepare_strategy_daily_comparison_series(strategy)
    unavailable = align_strategy_and_benchmark(
        strategy_daily=strategy_daily,
        loaded_benchmark=_loaded_benchmark(["2025-01-02", "2025-01-10"], [200, 210]),
        min_points=2,
    )

    assert unavailable.available is False
    assert unavailable.reason == "insufficient_aligned_points"


def test_intraday_to_daily_conversion_uses_last_point_per_day() -> None:
    strategy = pd.DataFrame(
        {
            "ts": pd.to_datetime(
                [
                    "2025-01-01T00:01:00Z",
                    "2025-01-01T23:59:00Z",
                    "2025-01-02T10:00:00Z",
                    "2025-01-02T23:00:00Z",
                ]
            ),
            "equity": [100, 110, 111, 120],
        }
    )

    daily = prepare_strategy_daily_comparison_series(strategy)

    assert daily.source_frequency == "intraday"
    assert daily.point_count == 2
    assert daily.data["value"].tolist() == [110, 120]
