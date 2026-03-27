from __future__ import annotations

import pandas as pd
import pytest

from bt.benchmarks.normalize import normalize_aligned_series
from bt.benchmarks.types import AlignedBenchmarkComparison


def _aligned(strategy_values: list[float], benchmark_values: list[float]) -> AlignedBenchmarkComparison:
    ts = pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"], utc=True)
    strategy_points = pd.DataFrame({"ts": ts, "strategy_value": strategy_values})
    benchmark_points = pd.DataFrame({"ts": ts, "benchmark_value": benchmark_values})
    return AlignedBenchmarkComparison(
        available=True,
        benchmark_id="BTC",
        comparison_frequency="1d",
        source_frequency="1d",
        strategy_window_start=ts[0],
        strategy_window_end=ts[-1],
        first_common_ts=ts[0],
        last_common_ts=ts[-1],
        point_count=3,
        strategy_points=strategy_points,
        benchmark_points=benchmark_points,
    )


def test_normalization_scales_first_points_to_100() -> None:
    normalized = normalize_aligned_series(_aligned([100.0, 110.0, 120.0], [50.0, 60.0, 75.0]))

    assert normalized.available is True
    assert normalized.strategy_points["normalized"].iloc[0] == pytest.approx(100.0)
    assert normalized.benchmark_points["normalized"].iloc[0] == pytest.approx(100.0)
    assert normalized.strategy_points["normalized"].iloc[-1] == pytest.approx(120.0)
    assert normalized.benchmark_points["normalized"].iloc[-1] == pytest.approx(150.0)


def test_normalization_fails_for_zero_or_negative_anchor() -> None:
    zero_anchor = normalize_aligned_series(_aligned([0.0, 110.0, 120.0], [50.0, 60.0, 75.0]))
    neg_anchor = normalize_aligned_series(_aligned([100.0, 110.0, 120.0], [-50.0, 60.0, 75.0]))

    assert zero_anchor.available is False
    assert zero_anchor.reason == "invalid_normalization_anchor"
    assert neg_anchor.available is False
    assert neg_anchor.reason == "invalid_normalization_anchor"
