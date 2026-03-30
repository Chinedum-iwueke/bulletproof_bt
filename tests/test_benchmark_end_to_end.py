from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from bt.analysis.overview.benchmark_overview import build_benchmark_overview_payload


def _enabled_config(tmp_path: Path) -> dict[str, object]:
    return {
        "enabled": True,
        "mode": "manual",
        "id": "BTC",
        "source": "platform_managed",
        "library_root": str(tmp_path / "benchmarks"),
        "library_revision": "2026-03-25",
        "frequency": "1d",
        "alignment_policy": "common_daily_timestamps_only",
        "comparison_frequency": "1d",
        "normalization_basis": "100_at_first_common_timestamp",
    }


def _write_benchmark_dataset(tmp_path: Path, ts: list[str], close: list[float]) -> None:
    benchmark_dir = tmp_path / "benchmarks" / "BTC"
    benchmark_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "ts": pd.to_datetime(ts, utc=True),
            "symbol": ["BTC"] * len(ts),
            "close": close,
        }
    ).to_parquet(benchmark_dir / "daily.parquet", index=False)


def _strategy_series(ts: list[str], equity: list[float]) -> pd.DataFrame:
    return pd.DataFrame({"ts": pd.to_datetime(ts, utc=True), "equity": equity})


def _canonicalize(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _canonicalize(v) for k, v in sorted(obj.items())}
    if isinstance(obj, list):
        return [_canonicalize(x) for x in obj]
    if isinstance(obj, float):
        return round(obj, 12)
    return obj


def test_end_to_end_pipeline_produces_valid_payload_and_is_deterministic(tmp_path: Path) -> None:
    _write_benchmark_dataset(
        tmp_path,
        ts=["2025-01-01", "2025-01-02", "2025-01-03"],
        close=[100.0, 110.0, 120.0],
    )
    strategy = _strategy_series(
        ts=["2025-01-01T10:00:00Z", "2025-01-02T11:00:00Z", "2025-01-03T12:00:00Z"],
        equity=[1000.0, 1100.0, 1210.0],
    )

    payload_a = build_benchmark_overview_payload(
        benchmark_config=_enabled_config(tmp_path),
        strategy_series=strategy,
    )
    payload_b = build_benchmark_overview_payload(
        benchmark_config=_enabled_config(tmp_path),
        strategy_series=strategy,
    )

    comparison = payload_a["benchmark_comparison"]
    assert comparison["available"] is True
    assert comparison["limited"] is False
    assert comparison["summary_metrics"]["benchmark_selected"] == "BTC"
    assert comparison["metadata"]["point_count"] == 3
    assert comparison["metadata"]["comparison_window_start"] == "2025-01-01T00:00:00Z"
    assert comparison["metadata"]["comparison_window_end"] == "2025-01-03T00:00:00Z"
    assert comparison["figure"]["id"] == "benchmark_overlay"
    assert comparison["figure"]["type"] == "line_series"

    assert _canonicalize(payload_a) == _canonicalize(payload_b)


def test_failure_propagation_invalid_config_returns_unavailable_payload(tmp_path: Path) -> None:
    strategy = _strategy_series(["2025-01-01"], [1000.0])
    bad_cfg = _enabled_config(tmp_path)
    bad_cfg["id"] = "INVALID"

    payload = build_benchmark_overview_payload(benchmark_config=bad_cfg, strategy_series=strategy)

    comparison = payload["benchmark_comparison"]
    assert comparison["available"] is False
    assert comparison["reason"] == "invalid_benchmark_config"
    assert comparison["summary_metrics"] is None


def test_failure_propagation_dataset_load_failure_returns_unavailable_payload(tmp_path: Path) -> None:
    strategy = _strategy_series(["2025-01-01"], [1000.0])

    payload = build_benchmark_overview_payload(
        benchmark_config=_enabled_config(tmp_path),
        strategy_series=strategy,
    )

    comparison = payload["benchmark_comparison"]
    assert comparison["available"] is False
    assert comparison["reason"] == "benchmark_dataset_load_failed"
    assert comparison["summary_metrics"] is None


def test_failure_propagation_no_overlap_returns_unavailable_payload(tmp_path: Path) -> None:
    _write_benchmark_dataset(tmp_path, ts=["2025-02-01", "2025-02-02"], close=[100.0, 101.0])
    strategy = _strategy_series(["2025-01-01", "2025-01-02"], [1000.0, 1001.0])

    payload = build_benchmark_overview_payload(
        benchmark_config=_enabled_config(tmp_path),
        strategy_series=strategy,
    )

    comparison = payload["benchmark_comparison"]
    assert comparison["available"] is False
    assert comparison["reason"] == "no_benchmark_overlap"
    assert comparison["summary_metrics"] is None


def test_failure_propagation_normalization_failure_returns_unavailable_payload(tmp_path: Path) -> None:
    _write_benchmark_dataset(tmp_path, ts=["2025-01-01", "2025-01-02"], close=[100.0, 101.0])
    strategy = _strategy_series(["2025-01-01", "2025-01-02"], [0.0, 10.0])

    payload = build_benchmark_overview_payload(
        benchmark_config=_enabled_config(tmp_path),
        strategy_series=strategy,
    )

    comparison = payload["benchmark_comparison"]
    assert comparison["available"] is False
    assert comparison["reason"] == "invalid_normalization_anchor"
    assert comparison["summary_metrics"] is None


def test_payload_contract_always_contains_required_keys(tmp_path: Path) -> None:
    strategy = _strategy_series(["2025-01-01"], [1000.0])
    payload = build_benchmark_overview_payload(
        benchmark_config={"enabled": False, "mode": "none"},
        strategy_series=strategy,
    )["benchmark_comparison"]

    required = {
        "available",
        "limited",
        "summary_metrics",
        "metadata",
        "figure",
        "assumptions",
        "limitations",
    }
    assert required.issubset(set(payload.keys()))

    # JSON round-trip is deterministic and serializable
    serialized = json.dumps(payload, sort_keys=True)
    restored = json.loads(serialized)
    assert sorted(restored.keys()) == sorted(payload.keys())
