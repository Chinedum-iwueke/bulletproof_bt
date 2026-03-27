from __future__ import annotations

import pandas as pd
import pytest

from bt.benchmarks.metrics import compute_benchmark_comparison_metrics
from bt.benchmarks.types import NormalizedBenchmarkComparison


def _normalized(strategy: list[float], benchmark: list[float]) -> NormalizedBenchmarkComparison:
    ts = pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"], utc=True)
    return NormalizedBenchmarkComparison(
        available=True,
        benchmark_id="BTC",
        comparison_frequency="1d",
        source_frequency="1d",
        strategy_window_start=ts[0],
        strategy_window_end=ts[-1],
        first_common_ts=ts[0],
        last_common_ts=ts[-1],
        point_count=3,
        strategy_points=pd.DataFrame({"ts": ts, "normalized": strategy}),
        benchmark_points=pd.DataFrame({"ts": ts, "normalized": benchmark}),
    )


def test_metrics_return_calculations_and_excess_return() -> None:
    metrics = compute_benchmark_comparison_metrics(
        _normalized([100.0, 110.0, 120.0], [100.0, 105.0, 110.0])
    )

    assert metrics.available is True
    assert metrics.strategy_return == pytest.approx(0.20)
    assert metrics.benchmark_return == pytest.approx(0.10)
    assert metrics.excess_return_vs_benchmark == pytest.approx(0.10)


def test_metrics_flat_series_edge_case() -> None:
    metrics = compute_benchmark_comparison_metrics(
        _normalized([100.0, 100.0, 100.0], [100.0, 100.0, 100.0])
    )

    assert metrics.strategy_return == pytest.approx(0.0)
    assert metrics.benchmark_return == pytest.approx(0.0)
    assert metrics.excess_return_vs_benchmark == pytest.approx(0.0)


def test_metrics_identical_series_has_zero_excess_return() -> None:
    metrics = compute_benchmark_comparison_metrics(
        _normalized([100.0, 97.0, 103.0], [100.0, 97.0, 103.0])
    )

    assert metrics.excess_return_vs_benchmark == pytest.approx(0.0)
