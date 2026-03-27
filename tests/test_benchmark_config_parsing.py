from __future__ import annotations

from pathlib import Path

import pytest

from bt.benchmarks.config import BenchmarkConfigError, parse_benchmark_config


def _enabled_payload(tmp_path: Path) -> dict[str, object]:
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


def test_parse_valid_enabled_config(tmp_path: Path) -> None:
    parsed = parse_benchmark_config(_enabled_payload(tmp_path))

    assert parsed.enabled is True
    assert parsed.id == "BTC"
    assert parsed.frequency == "1d"
    assert parsed.comparison_frequency == "1d"


def test_parse_valid_disabled_config() -> None:
    parsed = parse_benchmark_config({"enabled": False, "mode": "none"})

    assert parsed.enabled is False
    assert parsed.mode == "none"


@pytest.mark.parametrize("bad_id", ["", "QQQ", "btc"])
def test_parse_invalid_benchmark_id_raises(tmp_path: Path, bad_id: str) -> None:
    payload = _enabled_payload(tmp_path)
    payload["id"] = bad_id

    with pytest.raises(BenchmarkConfigError, match="benchmark.id"):
        parse_benchmark_config(payload)


def test_parse_missing_required_fields_raises(tmp_path: Path) -> None:
    payload = _enabled_payload(tmp_path)
    del payload["library_revision"]

    with pytest.raises(BenchmarkConfigError, match="missing required field"):
        parse_benchmark_config(payload)


@pytest.mark.parametrize("field", ["frequency", "comparison_frequency"])
def test_parse_invalid_frequency_raises(tmp_path: Path, field: str) -> None:
    payload = _enabled_payload(tmp_path)
    payload[field] = "1h"

    with pytest.raises(BenchmarkConfigError, match=field):
        parse_benchmark_config(payload)


def test_parse_non_absolute_library_root_raises() -> None:
    payload = {
        "enabled": True,
        "mode": "manual",
        "id": "BTC",
        "source": "platform_managed",
        "library_root": "relative/path",
        "library_revision": "2026-03-25",
        "frequency": "1d",
        "alignment_policy": "common_daily_timestamps_only",
        "comparison_frequency": "1d",
        "normalization_basis": "100_at_first_common_timestamp",
    }

    with pytest.raises(BenchmarkConfigError, match="absolute"):
        parse_benchmark_config(payload)
