from __future__ import annotations

import pandas as pd

from bt.benchmarks.types import BenchmarkComparisonFigure, BenchmarkComparisonFigureSeries, NormalizedBenchmarkComparison


def _format_ts(ts: pd.Timestamp) -> str:
    utc_ts = ts.tz_convert("UTC") if ts.tzinfo is not None else ts.tz_localize("UTC")
    return utc_ts.isoformat().replace("+00:00", "Z")


def _x_values(frame: pd.DataFrame) -> list[str]:
    return [_format_ts(ts) for ts in frame["ts"]]


def _series_values(frame: pd.DataFrame, *, value_column: str) -> list[float]:
    return [float(value) for value in frame[value_column]]


def build_benchmark_comparison_figure(
    normalized_result: NormalizedBenchmarkComparison,
) -> BenchmarkComparisonFigure:
    x_values = _x_values(normalized_result.strategy_points)
    strategy = BenchmarkComparisonFigureSeries(
        name="strategy_equity_normalized",
        values=_series_values(normalized_result.strategy_points, value_column="normalized"),
    )
    benchmark = BenchmarkComparisonFigureSeries(
        name=f"benchmark_{normalized_result.benchmark_id}_normalized",
        values=_series_values(normalized_result.benchmark_points, value_column="normalized"),
    )
    return BenchmarkComparisonFigure(
        id="benchmark_overlay",
        type="line_series",
        title="Strategy vs Benchmark",
        x_label="timestamp",
        y_label="normalized_index",
        x=x_values,
        series=[strategy, benchmark],
    )
