from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from bt.benchmarks.config import parse_benchmark_config
from bt.benchmarks.loader import BenchmarkDatasetError, load_benchmark_dataset, validate_benchmark_dataset


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


def _valid_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"], utc=True),
            "symbol": ["BTC", "BTC", "BTC"],
            "close": [100.0, 101.0, 99.5],
        }
    )


def _write_dataset(tmp_path: Path, frame: pd.DataFrame) -> Path:
    path = tmp_path / "benchmarks" / "BTC"
    path.mkdir(parents=True, exist_ok=True)
    parquet_path = path / "daily.parquet"
    frame.to_parquet(parquet_path, index=False)
    return parquet_path


def test_load_valid_parquet(tmp_path: Path) -> None:
    _write_dataset(tmp_path, _valid_frame())

    loaded = load_benchmark_dataset(parse_benchmark_config(_enabled_config(tmp_path)))

    assert loaded.enabled is True
    assert loaded.metadata.row_count == 3
    assert list(loaded.data.columns) == ["ts", "symbol", "close"]


def test_load_missing_file_raises_structured_error(tmp_path: Path) -> None:
    with pytest.raises(BenchmarkDatasetError, match="file not found"):
        load_benchmark_dataset(parse_benchmark_config(_enabled_config(tmp_path)))


def test_load_corrupted_file_raises_structured_error(tmp_path: Path) -> None:
    path = tmp_path / "benchmarks" / "BTC"
    path.mkdir(parents=True, exist_ok=True)
    (path / "daily.parquet").write_text("not parquet", encoding="utf-8")

    with pytest.raises(BenchmarkDatasetError, match="Unable to read benchmark parquet file"):
        load_benchmark_dataset(parse_benchmark_config(_enabled_config(tmp_path)))


def test_validate_missing_columns_raises() -> None:
    frame = _valid_frame().drop(columns=["close"])

    with pytest.raises(BenchmarkDatasetError, match="missing required column"):
        validate_benchmark_dataset(frame, expected_benchmark_id="BTC")


def test_validate_wrong_symbol_raises() -> None:
    frame = _valid_frame()
    frame["symbol"] = ["BTC", "SPY", "BTC"]

    with pytest.raises(BenchmarkDatasetError, match="symbol mismatch"):
        validate_benchmark_dataset(frame, expected_benchmark_id="BTC")


def test_validate_duplicate_timestamps_raises() -> None:
    frame = _valid_frame()
    frame.loc[1, "ts"] = frame.loc[0, "ts"]

    with pytest.raises(BenchmarkDatasetError, match="duplicate timestamps"):
        validate_benchmark_dataset(frame, expected_benchmark_id="BTC")


@pytest.mark.parametrize(
    "bad_close",
    [0.0, -1.0, float("inf"), float("nan")],
)
def test_validate_invalid_close_values_raises(bad_close: float) -> None:
    frame = _valid_frame()
    frame.loc[1, "close"] = bad_close

    with pytest.raises(BenchmarkDatasetError, match="close"):
        validate_benchmark_dataset(frame, expected_benchmark_id="BTC")
