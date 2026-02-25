from __future__ import annotations

import csv
import json
from pathlib import Path

import pandas as pd
import pytest
import yaml

import bt.strategy
from bt.experiments.grid_runner import run_grid


def _write_dataset(dataset_dir: Path) -> Path:
    dataset_dir.mkdir(parents=True, exist_ok=True)
    ts_index = pd.date_range("2024-01-01", periods=5, freq="min", tz="UTC")
    rows: list[dict[str, object]] = []
    for symbol in ["AAA", "BBB"]:
        for i, ts in enumerate(ts_index):
            base = 100 + i
            rows.append(
                {
                    "ts": ts,
                    "symbol": symbol,
                    "open": float(base),
                    "high": float(base + 1),
                    "low": float(base - 1),
                    "close": float(base + 0.5),
                    "volume": float(1000 + i),
                }
            )
    bars = pd.DataFrame(rows)
    bars.to_parquet(dataset_dir / "bars.parquet", index=False)
    with (dataset_dir / "manifest.yaml").open("w", encoding="utf-8") as handle:
        yaml.safe_dump({"version": 1, "format": "parquet", "files": ["bars.parquet"]}, handle, sort_keys=False)
    return dataset_dir / "bars.parquet"


def test_grid_runner_records_failures_and_continues(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    original_make_strategy = bt.strategy.make_strategy

    def flaky_make_strategy(name, **kwargs):
        if kwargs.get("seed") == 2:
            raise RuntimeError("intentional test failure")
        return original_make_strategy(name, **kwargs)

    monkeypatch.setattr(bt.strategy, "make_strategy", flaky_make_strategy)

    cfg = {
        "initial_cash": 10000.0,
        "max_leverage": 10.0,
        "risk": {"max_positions": 1, "risk_per_trade_pct": 0.001},
        "signal_delay_bars": 0,
        "strategy": {"name": "coinflip", "p_trade": 0.0},
        "maker_fee_bps": 0.0,
        "taker_fee_bps": 0.0,
        "slippage_k": 0.0,
        "audit": {"enabled": True, "level": "full"},
    }

    exp = {
        "version": 1,
        "grid": {"strategy.seed": [1, 2, 3]},
        "run_naming": {"template": "seed{strategy.seed}"},
    }

    data_path = _write_dataset(tmp_path / "dataset")
    out_path = tmp_path / "out"

    run_grid(config=cfg, experiment_cfg=exp, data_path=str(data_path), out_path=out_path)

    with (out_path / "summary.csv").open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 3

    fail_rows = [row for row in rows if row["status"] == "FAIL"]
    pass_rows = [row for row in rows if row["status"] == "PASS"]
    assert len(fail_rows) == 1
    assert len(pass_rows) == 2
    assert fail_rows[0]["error_type"] == "RuntimeError"
    assert "intentional test failure" in fail_rows[0]["error_message"]
    assert pass_rows[0]["error_type"] == ""
    assert pass_rows[0]["error_message"] == ""

    fail_run = out_path / "runs" / "run_002__seed2" / "run_status.json"
    assert fail_run.exists()
    payload = json.loads(fail_run.read_text(encoding="utf-8"))
    assert payload["status"] == "FAIL"
    assert payload["error_type"] == "RuntimeError"

    pass_run = out_path / "runs" / "run_001__seed1" / "run_status.json"
    assert pass_run.exists()
    payload_pass = json.loads(pass_run.read_text(encoding="utf-8"))
    assert payload_pass["status"] == "PASS"


    pass_stability = out_path / "runs" / "run_001__seed1" / "audit" / "stability_report.json"
    fail_stability = out_path / "runs" / "run_002__seed2" / "audit" / "stability_report.json"
    assert pass_stability.exists()
    assert fail_stability.exists()
