from __future__ import annotations

from pathlib import Path

import pandas as pd

from bt.benchmarks.artifacts import emit_benchmark_comparison_artifact
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


def test_emit_artifact_available_payload_structure_and_figure() -> None:
    strategy = pd.DataFrame(
        {
            "ts": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"], utc=True),
            "equity": [100.0, 110.0, 120.0],
        }
    )
    payload = emit_benchmark_comparison_artifact(
        strategy_series=strategy,
        loaded_benchmark=_loaded_benchmark(["2025-01-01", "2025-01-02", "2025-01-03"], [100.0, 105.0, 115.0]),
    )["benchmark_comparison"]

    assert payload["available"] is True
    assert payload["limited"] is False
    assert payload["summary_metrics"]["benchmark_selected"] == "BTC"
    assert payload["metadata"]["comparison_frequency"] == "1d"
    assert payload["metadata"]["point_count"] == 3

    figure = payload["figure"]
    assert figure["id"] == "benchmark_overlay"
    assert figure["type"] == "line_series"
    assert figure["x_label"] == "timestamp"
    assert figure["y_label"] == "normalized_index"
    assert len(figure["x"]) == 3
    assert len(figure["series"]) == 2
    assert len(figure["series"][0]["values"]) == 3
    assert len(figure["series"][1]["values"]) == 3


def test_emit_artifact_unavailable_payload_has_reason_and_no_fake_data() -> None:
    strategy = pd.DataFrame(
        {
            "ts": pd.to_datetime(["2025-01-01", "2025-01-02"], utc=True),
            "equity": [100.0, 101.0],
        }
    )
    payload = emit_benchmark_comparison_artifact(
        strategy_series=strategy,
        loaded_benchmark=_loaded_benchmark(["2025-02-01", "2025-02-02"], [100.0, 101.0]),
    )["benchmark_comparison"]

    assert payload["available"] is False
    assert payload["limited"] is True
    assert payload["reason"] == "no_benchmark_overlap"
    assert payload["summary_metrics"] is None
    assert payload["figure"] is None
    assert payload["metadata"]["point_count"] == 0
