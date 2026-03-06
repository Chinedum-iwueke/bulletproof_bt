from __future__ import annotations

import csv
import json
from pathlib import Path

import yaml

from bt.experiments.parallel_grid import (
    GridSpec,
    build_grid_rows,
    build_override_payload,
    build_run_command,
    detect_run_artifact_status,
    write_manifest_csv,
)


def test_manifest_generation_is_deterministic_and_36_rows(tmp_path: Path) -> None:
    spec = GridSpec(strategy_name="volfloor_donchian", exit_type="donchian_reversal")
    rows = build_grid_rows(spec)

    assert len(rows) == 36
    assert rows[0]["run_id"] == "run_001__vol60_adx18_er035_n16"
    assert rows[1]["run_id"] == "run_002__vol60_adx18_er045_n16"
    assert rows[-1]["run_id"] == "run_036__vol85_adx25_er055_n16"

    manifest = tmp_path / "manifest.csv"
    write_manifest_csv(rows, manifest)
    with manifest.open("r", encoding="utf-8", newline="") as handle:
        written_rows = list(csv.DictReader(handle))
    assert [row["run_id"] for row in written_rows] == [row["run_id"] for row in rows]


def test_override_payload_injects_expected_keys() -> None:
    row = {
        "run_id": "run_001__vol60_adx18_er035_n16",
        "strategy_name": "volfloor_donchian",
        "exit_type": "donchian_reversal",
        "timeframe": "15m",
        "execution_tier": "tier2",
        "vol_floor": "60",
        "adx_min": "18",
        "er_min": "0.35",
        "er_lookback": "16",
    }
    payload = build_override_payload(row)

    assert payload["data"]["entry_timeframe"] == "15m"
    assert payload["execution"]["profile"] == "tier2"
    assert payload["strategy"]["name"] == "volfloor_donchian"
    assert payload["strategy"]["exit_type"] == "donchian_reversal"
    assert payload["strategy"]["er_lookback"] == 16
    assert payload["strategy"]["er_min"] == 0.35

    dumped = yaml.safe_dump(payload)
    loaded = yaml.safe_load(dumped)
    assert loaded["strategy"]["vol_floor_pct"] == 60.0


def test_completion_detection_success_failure_incomplete(tmp_path: Path) -> None:
    success_dir = tmp_path / "success"
    success_dir.mkdir()
    (success_dir / "run_status.json").write_text(json.dumps({"status": "PASS"}), encoding="utf-8")
    for name in [
        "config_used.yaml",
        "performance.json",
        "equity.csv",
        "trades.csv",
        "fills.jsonl",
        "decisions.jsonl",
        "performance_by_bucket.csv",
    ]:
        (success_dir / name).write_text("ok", encoding="utf-8")

    failure_dir = tmp_path / "failure"
    failure_dir.mkdir()
    (failure_dir / "run_status.json").write_text(
        json.dumps({"status": "FAIL", "error_message": "boom"}),
        encoding="utf-8",
    )

    incomplete_dir = tmp_path / "incomplete"
    incomplete_dir.mkdir()

    assert detect_run_artifact_status(success_dir).state == "SUCCESS"
    assert detect_run_artifact_status(failure_dir).state == "FAILED"
    assert detect_run_artifact_status(incomplete_dir).state == "INCOMPLETE"


def test_runner_command_construction() -> None:
    command = build_run_command(
        base_config=Path("configs/engine.yaml"),
        data_path=Path("data/sample"),
        out_dir=Path("outputs/exp/runs"),
        run_id="run_001__vol60_adx18_er035_n16",
        override_path=Path("outputs/exp/overrides/run_001__vol60_adx18_er035_n16.yaml"),
        python_executable="python",
    )

    assert command == [
        "python",
        "scripts/run_backtest.py",
        "--config",
        "configs/engine.yaml",
        "--data",
        "data/sample",
        "--run-id",
        "run_001__vol60_adx18_er035_n16",
        "--out-dir",
        "outputs/exp/runs",
        "--override",
        "outputs/exp/overrides/run_001__vol60_adx18_er035_n16.yaml",
    ]
