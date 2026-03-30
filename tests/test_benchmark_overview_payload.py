from __future__ import annotations

import pandas as pd

from bt.analysis.overview.benchmark_overview import build_benchmark_overview_payload


def _enabled_config(library_root: str) -> dict:
    return {
        "enabled": True,
        "mode": "manual",
        "id": "BTC",
        "source": "platform_managed",
        "library_root": library_root,
        "library_revision": "2026-03-25",
        "frequency": "1d",
        "alignment_policy": "common_timestamps",
        "comparison_frequency": "1d",
        "normalization_basis": "100_first_common",
    }


def test_build_benchmark_overview_payload_available(tmp_path) -> None:
    benchmark_dir = tmp_path / "benchmarks" / "BTC"
    benchmark_dir.mkdir(parents=True)
    benchmark_df = pd.DataFrame(
        {
            "ts": pd.to_datetime(["2025-01-02", "2025-01-03", "2025-01-04"], utc=True),
            "symbol": ["BTC", "BTC", "BTC"],
            "close": [100.0, 110.0, 120.0],
        }
    )
    benchmark_df.to_parquet(benchmark_dir / "daily.parquet", index=False)

    strategy = pd.DataFrame(
        {
            "ts": pd.to_datetime(["2025-01-02T10:00:00Z", "2025-01-03T18:00:00Z", "2025-01-04T20:00:00Z"]),
            "equity": [1000.0, 1050.0, 1100.0],
        }
    )

    payload = build_benchmark_overview_payload(
        benchmark_config=_enabled_config(str(tmp_path / "benchmarks")),
        strategy_series=strategy,
    )

    comparison = payload["benchmark_comparison"]
    assert comparison["available"] is True
    assert comparison["limited"] is False
    assert comparison["summary_metrics"]["benchmark_selected"] == "BTC"
    assert comparison["metadata"]["point_count"] == 3
    assert comparison["figure"]["id"] == "benchmark_overlay"
    assert comparison["figure"]["type"] == "line_series"
    assert len(comparison["figure"]["series"]) == 2


def test_build_benchmark_overview_payload_disabled(tmp_path) -> None:
    strategy = pd.DataFrame(
        {
            "ts": pd.to_datetime(["2025-01-02T10:00:00Z", "2025-01-03T18:00:00Z"]),
            "equity": [1000.0, 1050.0],
        }
    )
    payload = build_benchmark_overview_payload(
        benchmark_config={"enabled": False, "mode": "none"},
        strategy_series=strategy,
    )

    comparison = payload["benchmark_comparison"]
    assert comparison["available"] is False
    assert comparison["limited"] is True
    assert comparison["reason"] == "benchmark_disabled"
    assert comparison["figure"] is None
