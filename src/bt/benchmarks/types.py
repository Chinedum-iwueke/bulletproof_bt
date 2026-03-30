from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, TypeAlias

import pandas as pd

BenchmarkId = Literal["BTC", "SPY", "XAUUSD", "DXY"]
BenchmarkModeEnabled = Literal["auto", "manual"]
BenchmarkMode = Literal["auto", "manual", "none"]
BenchmarkSource = Literal["platform_managed"]
BenchmarkFrequency = Literal["1d"]
ComparisonFrequency = Literal["1d"]
StrategySourceFrequency = Literal["1d", "intraday"]

BenchmarkComparisonUnavailableReason = Literal[
    "benchmark_disabled",
    "benchmark_not_configured",
    "benchmark_dataset_load_failed",
    "invalid_benchmark_config",
    "no_strategy_series",
    "no_benchmark_overlap",
    "insufficient_aligned_points",
    "invalid_normalization_anchor",
    "unsupported_comparison_frequency",
]


@dataclass(frozen=True)
class DisabledBenchmarkConfig:
    enabled: Literal[False]
    mode: Literal["none"]


@dataclass(frozen=True)
class EnabledBenchmarkConfig:
    enabled: Literal[True]
    mode: BenchmarkModeEnabled
    id: BenchmarkId
    source: BenchmarkSource
    library_root: Path
    library_revision: str
    frequency: BenchmarkFrequency
    alignment_policy: str
    comparison_frequency: BenchmarkFrequency
    normalization_basis: str


BenchmarkConfig: TypeAlias = DisabledBenchmarkConfig | EnabledBenchmarkConfig


@dataclass(frozen=True)
class BenchmarkDatasetMetadata:
    benchmark_id: BenchmarkId
    dataset_path: Path
    row_count: int
    start_ts: pd.Timestamp
    end_ts: pd.Timestamp


@dataclass(frozen=True)
class DisabledBenchmarkDataset:
    enabled: Literal[False]
    mode: Literal["none"]


@dataclass(frozen=True)
class LoadedBenchmarkDataset:
    enabled: Literal[True]
    config: EnabledBenchmarkConfig
    data: pd.DataFrame
    metadata: BenchmarkDatasetMetadata


LoadedBenchmarkResult: TypeAlias = DisabledBenchmarkDataset | LoadedBenchmarkDataset


@dataclass(frozen=True)
class BenchmarkComparisonUnavailable:
    available: Literal[False]
    reason: BenchmarkComparisonUnavailableReason
    benchmark_id: BenchmarkId | None = None
    detail: str | None = None


@dataclass(frozen=True)
class StrategyComparisonSeries:
    comparison_frequency: ComparisonFrequency
    source_frequency: StrategySourceFrequency
    data: pd.DataFrame
    window_start: pd.Timestamp
    window_end: pd.Timestamp
    point_count: int


@dataclass(frozen=True)
class AlignedBenchmarkComparison:
    available: Literal[True]
    benchmark_id: BenchmarkId
    comparison_frequency: ComparisonFrequency
    source_frequency: StrategySourceFrequency
    strategy_window_start: pd.Timestamp
    strategy_window_end: pd.Timestamp
    first_common_ts: pd.Timestamp
    last_common_ts: pd.Timestamp
    point_count: int
    strategy_points: pd.DataFrame
    benchmark_points: pd.DataFrame


@dataclass(frozen=True)
class NormalizedBenchmarkComparison:
    available: Literal[True]
    benchmark_id: BenchmarkId
    comparison_frequency: ComparisonFrequency
    source_frequency: StrategySourceFrequency
    strategy_window_start: pd.Timestamp
    strategy_window_end: pd.Timestamp
    first_common_ts: pd.Timestamp
    last_common_ts: pd.Timestamp
    point_count: int
    strategy_points: pd.DataFrame
    benchmark_points: pd.DataFrame


@dataclass(frozen=True)
class BenchmarkComparisonMetrics:
    available: Literal[True]
    benchmark_id: BenchmarkId
    comparison_frequency: ComparisonFrequency
    source_frequency: StrategySourceFrequency
    point_count: int
    first_common_ts: pd.Timestamp
    last_common_ts: pd.Timestamp
    strategy_return: float
    benchmark_return: float
    excess_return_vs_benchmark: float


BenchmarkComparisonResult: TypeAlias = AlignedBenchmarkComparison | BenchmarkComparisonUnavailable
BenchmarkMetricResult: TypeAlias = BenchmarkComparisonMetrics | BenchmarkComparisonUnavailable


@dataclass(frozen=True)
class BenchmarkComparisonFigureSeries:
    name: str
    values: list[float]


@dataclass(frozen=True)
class BenchmarkComparisonFigure:
    id: Literal["benchmark_overlay"]
    type: Literal["line_series"]
    title: str
    x_label: Literal["timestamp"]
    y_label: Literal["normalized_index"]
    x: list[str]
    series: list[BenchmarkComparisonFigureSeries]


@dataclass(frozen=True)
class BenchmarkComparisonSummaryMetrics:
    benchmark_selected: BenchmarkId
    strategy_return: float
    benchmark_return: float
    excess_return_vs_benchmark: float


@dataclass(frozen=True)
class BenchmarkComparisonMetadata:
    benchmark_id: BenchmarkId | None
    benchmark_source: BenchmarkSource | None
    library_revision: str | None
    benchmark_frequency: BenchmarkFrequency | None
    comparison_frequency: ComparisonFrequency | None
    alignment_basis: str
    normalization_basis: str
    comparison_window_start: str | None
    comparison_window_end: str | None
    point_count: int


@dataclass(frozen=True)
class BenchmarkComparisonOverviewAvailable:
    available: Literal[True]
    limited: Literal[False]
    summary_metrics: BenchmarkComparisonSummaryMetrics
    metadata: BenchmarkComparisonMetadata
    figure: BenchmarkComparisonFigure
    assumptions: list[str]
    limitations: list[str]


@dataclass(frozen=True)
class BenchmarkComparisonOverviewUnavailable:
    available: Literal[False]
    limited: Literal[True]
    reason: BenchmarkComparisonUnavailableReason
    message: str
    summary_metrics: None
    metadata: BenchmarkComparisonMetadata
    figure: None
    assumptions: list[str]
    limitations: list[str]


BenchmarkComparisonOverviewPayload: TypeAlias = (
    BenchmarkComparisonOverviewAvailable | BenchmarkComparisonOverviewUnavailable
)
