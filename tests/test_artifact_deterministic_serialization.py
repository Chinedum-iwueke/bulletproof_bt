from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
import yaml

from bt.api import run_backtest
from bt.logging.formatting import write_json_deterministic
from bt.logging.summary import write_summary_txt


def _strip_nondeterministic_summary_lines(text: str) -> str:
    return "\n".join(
        line
        for line in text.splitlines()
        if not line.startswith("Generated: ") and not line.startswith("Run Dir: ")
    )


def _read_manifest_without_created_at(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload.pop("created_at_utc", None)
    payload.pop("run_dir", None)
    return payload


def _write_dataset(dataset_dir: Path) -> None:
    dataset_dir.mkdir(parents=True, exist_ok=True)
    ts_index = pd.date_range("2024-01-01", periods=8, freq="h", tz="UTC")
    rows: list[dict[str, object]] = []
    closes = [100.0, 101.0, 100.5, 102.0, 103.0, 102.5, 104.0, 105.0]
    for ts, close in zip(ts_index, closes, strict=True):
        rows.append(
            {
                "ts": ts,
                "symbol": "BTCUSDT",
                "open": close,
                "high": close + 0.5,
                "low": close - 0.5,
                "close": close,
                "volume": 10.0,
            }
        )
    pd.DataFrame(rows).to_parquet(dataset_dir / "BTCUSDT.parquet", index=False)
    manifest = {
        "format": "per_symbol_parquet",
        "symbols": ["BTCUSDT"],
        "path": "{symbol}.parquet",
    }
    (dataset_dir / "manifest.yaml").write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")


def _write_config(path: Path) -> None:
    cfg = {
        "initial_cash": 1000.0,
        "max_leverage": 2.0,
        "signal_delay_bars": 1,
        "risk": {
            "max_positions": 1,
            "risk_per_trade_pct": 0.001,
        },
        "strategy": {
            "name": "coinflip",
            "seed": 7,
            "p_trade": 0.0,
            "cooldown_bars": 0,
        },
        "benchmark": {
            "enabled": True,
            "symbol": "BTCUSDT",
            "price_field": "close",
        },
    }
    path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")


def test_write_json_deterministic_rounds_and_sorts(tmp_path: Path) -> None:
    path = tmp_path / "payload.json"
    payload = {
        "z": 3.141592653589793,
        "a": {
            "y": [1.234567890123456, {"b": 2.345678901234567, "a": 0.3333333333333333}],
            "x": 2.0,
        },
    }

    write_json_deterministic(path, payload)
    first = path.read_bytes()
    write_json_deterministic(path, payload)
    second = path.read_bytes()

    assert first == second
    text = first.decode("utf-8")
    assert text.endswith("\n")
    assert text.splitlines()[1].strip().startswith('"a"')


def test_write_json_deterministic_rejects_non_finite(tmp_path: Path) -> None:
    path = tmp_path / "payload.json"

    with pytest.raises(ValueError, match=r"Non-finite float"):
        write_json_deterministic(path, {"bad": float("nan")})

    with pytest.raises(ValueError, match=r"Non-finite float"):
        write_json_deterministic(path, {"bad": float("inf")})


def test_e2e_artifacts_are_deterministic_across_re_runs(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    _write_dataset(dataset_dir)

    config_path = tmp_path / "engine.yaml"
    _write_config(config_path)

    run_dir_1 = Path(
        run_backtest(
            config_path=str(config_path),
            data_path=str(dataset_dir),
            out_dir=str(tmp_path / "out_1"),
        )
    )
    run_dir_2 = Path(
        run_backtest(
            config_path=str(config_path),
            data_path=str(dataset_dir),
            out_dir=str(tmp_path / "out_2"),
        )
    )

    performance_1 = json.loads((run_dir_1 / "performance.json").read_text(encoding="utf-8"))
    performance_2 = json.loads((run_dir_2 / "performance.json").read_text(encoding="utf-8"))
    performance_1.pop("run_id", None)
    performance_2.pop("run_id", None)
    assert performance_1 == performance_2

    write_summary_txt(run_dir_1)
    write_summary_txt(run_dir_2)

    benchmark_metrics_1 = run_dir_1 / "benchmark_metrics.json"
    benchmark_metrics_2 = run_dir_2 / "benchmark_metrics.json"
    if benchmark_metrics_1.exists() and benchmark_metrics_2.exists():
        assert benchmark_metrics_1.read_bytes() == benchmark_metrics_2.read_bytes()

    comparison_1 = run_dir_1 / "comparison_summary.json"
    comparison_2 = run_dir_2 / "comparison_summary.json"
    if comparison_1.exists() and comparison_2.exists():
        assert comparison_1.read_bytes() == comparison_2.read_bytes()

    summary_1 = _strip_nondeterministic_summary_lines((run_dir_1 / "summary.txt").read_text(encoding="utf-8"))
    summary_2 = _strip_nondeterministic_summary_lines((run_dir_2 / "summary.txt").read_text(encoding="utf-8"))
    assert summary_1 == summary_2

    manifest_1 = run_dir_1 / "run_manifest.json"
    manifest_2 = run_dir_2 / "run_manifest.json"
    if manifest_1.exists() and manifest_2.exists():
        assert _read_manifest_without_created_at(manifest_1) == _read_manifest_without_created_at(manifest_2)
