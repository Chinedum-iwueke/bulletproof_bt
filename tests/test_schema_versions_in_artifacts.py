from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml

from bt.api import run_backtest
from bt.contracts.schema_versions import PERFORMANCE_SCHEMA_VERSION


def _write_basic_config(path: Path, *, benchmark_enabled: bool = False) -> None:
    config: dict[str, object] = {
        "initial_cash": 100000.0,
        "max_leverage": 5.0,
        "signal_delay_bars": 1,
        "strategy": {"name": "coinflip", "p_trade": 0.0, "cooldown_bars": 0, "seed": 7},
        "risk": {"max_positions": 1, "risk_per_trade_pct": 0.001},
    }
    if benchmark_enabled:
        config["benchmark"] = {
            "enabled": True,
            "symbol": "BTCUSDT",
            "price_field": "close",
        }
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")


def _bars_frame(symbol: str, closes: list[float]) -> pd.DataFrame:
    ts_index = pd.date_range("2024-01-01", periods=len(closes), freq="min", tz="UTC")
    return pd.DataFrame(
        {
            "ts": ts_index,
            "symbol": symbol,
            "open": closes,
            "high": [float(v) + 1.0 for v in closes],
            "low": [float(v) - 1.0 for v in closes],
            "close": closes,
            "volume": [10.0] * len(closes),
        }
    )


def _write_dataset(dataset_dir: Path) -> None:
    dataset_dir.mkdir(parents=True, exist_ok=True)
    _bars_frame("BTCUSDT", [100.0, 110.0, 90.0]).to_parquet(dataset_dir / "BTCUSDT.parquet", index=False)
    manifest = {
        "format": "per_symbol_parquet",
        "symbols": ["BTCUSDT"],
        "path": "{symbol}.parquet",
    }
    (dataset_dir / "manifest.yaml").write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")


def test_run_status_has_schema_version(tmp_path: Path) -> None:
    config_path = tmp_path / "engine.yaml"
    _write_basic_config(config_path)

    run_dir = Path(
        run_backtest(
            config_path=str(config_path),
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "out"),
            run_name="schema_run_status",
        )
    )

    payload = json.loads((run_dir / "run_status.json").read_text(encoding="utf-8"))
    assert payload["status"] == "PASS"
    assert payload["schema_version"] == 1


def test_performance_has_schema_version(tmp_path: Path) -> None:
    config_path = tmp_path / "engine_perf.yaml"
    _write_basic_config(config_path)

    run_dir = Path(
        run_backtest(
            config_path=str(config_path),
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "out_perf"),
            run_name="schema_perf",
        )
    )

    payload = json.loads((run_dir / "performance.json").read_text(encoding="utf-8"))
    assert "total_trades" in payload
    assert payload["schema_version"] == PERFORMANCE_SCHEMA_VERSION


def test_benchmark_artifacts_have_schema_version_when_enabled(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    _write_dataset(dataset_dir)

    config_path = tmp_path / "engine_benchmark.yaml"
    _write_basic_config(config_path, benchmark_enabled=True)

    run_dir = Path(
        run_backtest(
            config_path=str(config_path),
            data_path=str(dataset_dir),
            out_dir=str(tmp_path / "out_benchmark"),
            run_name="schema_benchmark",
        )
    )

    benchmark_metrics = json.loads((run_dir / "benchmark_metrics.json").read_text(encoding="utf-8"))
    comparison_summary = json.loads((run_dir / "comparison_summary.json").read_text(encoding="utf-8"))

    assert benchmark_metrics["n_points"] >= 1
    assert benchmark_metrics["schema_version"] == 1
    assert "strategy" in comparison_summary
    assert comparison_summary["schema_version"] == 1
