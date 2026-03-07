from __future__ import annotations

import csv
import os
import json
from pathlib import Path

import pytest
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
    assert payload["data"]["date_range"] == {"start": "2023-01-01", "end": None}
    assert payload["benchmark"]["enabled"] is False
    assert payload["execution"]["profile"] == "tier2"
    assert payload["execution"]["intrabar_mode"] == "worst_case"
    assert payload["strategy"]["name"] == "volfloor_donchian"
    assert payload["strategy"]["exit_type"] == "donchian_reversal"
    assert payload["strategy"]["er_lookback"] == 16
    assert payload["strategy"]["er_min"] == 0.35
    assert payload["risk"]["stop_resolution"] == "strict"
    assert payload["audit"]["max_events_per_file"] == 200000

    dumped = yaml.safe_dump(payload)
    loaded = yaml.safe_load(dumped)
    assert loaded["strategy"]["vol_floor_pct"] == 60.0


def test_override_payload_for_ema_contains_htf_and_default_exit() -> None:
    row = {
        "run_id": "run_001__vol60_adx18_er035_n16",
        "strategy_name": "volfloor_ema_pullback",
        "exit_type": "ema_trend_end",
        "timeframe": "15m",
        "execution_tier": "tier2",
        "vol_floor": "60",
        "adx_min": "18",
        "er_min": "0.35",
        "er_lookback": "16",
    }
    payload = build_override_payload(row)

    assert payload["data"]["date_range"] is None
    assert payload["strategy"]["stop_atr_mult"] == 2.0
    assert payload["htf_resampler"] == {"timeframes": ["15m"], "strict": True}
    assert payload["htf_timeframes"] == ["15m"]


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


def test_cli_default_max_workers_is_6(monkeypatch: pytest.MonkeyPatch) -> None:
    from bt.experiments import parallel_grid

    captured: dict[str, int] = {}

    monkeypatch.setattr(parallel_grid, "read_manifest_csv", lambda _path: [])

    def _fake_run_manifest_in_parallel(**kwargs):
        captured["max_workers"] = kwargs["max_workers"]
        return [], []

    monkeypatch.setattr(parallel_grid, "run_manifest_in_parallel", _fake_run_manifest_in_parallel)

    args = [
        "--experiment-root",
        "outputs/x",
        "--manifest",
        "outputs/x/manifests/a.csv",
        "--base-config",
        "configs/engine.yaml",
        "--data",
        "data",
        "--dry-run",
    ]
    exit_code = parallel_grid.cli_run_parallel_grid(args)
    assert exit_code == 0
    assert captured["max_workers"] == 6


def test_build_subprocess_env_includes_repo_src(monkeypatch: pytest.MonkeyPatch) -> None:
    from bt.experiments.parallel_grid import _build_subprocess_env

    monkeypatch.setenv("PYTHONPATH", "alpha:beta")
    env = _build_subprocess_env()

    assert "PYTHONPATH" in env
    assert str((Path.cwd() / "src").resolve()) in env["PYTHONPATH"]


def test_cli_rejects_missing_data_path() -> None:
    from bt.experiments.parallel_grid import cli_run_parallel_grid

    with pytest.raises(ValueError, match=r"--data path does not exist"):
        cli_run_parallel_grid([
            "--experiment-root",
            "outputs/x",
            "--manifest",
            "outputs/x/manifests/a.csv",
            "--base-config",
            "configs/engine.yaml",
            "--data",
            "home/not/real",
            "--dry-run",
        ])
