from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from bt.experiments.manifest import decode_params, encode_params, read_manifest_csv, write_manifest_csv
from bt.experiments.parallel_grid import (
    _normalized_output_dir,
    _variant_slug,
    build_hypothesis_manifest_rows,
    cli_run_parallel_hypothesis_grid,
    run_hypothesis_manifest_in_parallel,
)
from bt.experiments.status import detect_run_artifact_status
from bt.hypotheses.contract import HypothesisContract
from bt.logging.run_contract import REQUIRED_ARTIFACTS


def test_l1_h1_manifest_generation_is_deterministic() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h1_vol_floor_trend.yaml")
    rows_a = build_hypothesis_manifest_rows(
        contract=contract,
        hypothesis_path=Path("research/hypotheses/l1_h1_vol_floor_trend.yaml"),
        phase="tier2",
    )
    rows_b = build_hypothesis_manifest_rows(
        contract=contract,
        hypothesis_path=Path("research/hypotheses/l1_h1_vol_floor_trend.yaml"),
        phase="tier2",
    )
    assert rows_a == rows_b
    assert rows_a[0]["row_id"] == "row_00001"
    assert rows_a[0]["tier"] == "Tier2"

def test_l1_h1c_manifest_row_count_matches_registered_grid() -> None:
    contract = HypothesisContract.from_yaml("research/hypotheses/l1_h1c_volfloor_ema_pullback.yaml")
    rows = build_hypothesis_manifest_rows(
        contract=contract,
        hypothesis_path=Path("research/hypotheses/l1_h1c_volfloor_ema_pullback.yaml"),
        phase="tier2",
    )
    assert len(rows) == 54
    assert {row["tier"] for row in rows} == {"Tier2"}


def test_params_json_roundtrip() -> None:
    payload = {"theta_vol": 70, "tp_enabled": True, "k_atr": 2.0}
    assert decode_params(encode_params(payload)) == payload


def test_manifest_read_write_and_validation(tmp_path: Path) -> None:
    row = {
        "row_id": "row_00001",
        "hypothesis_id": "L1-H1",
        "hypothesis_path": "research/hypotheses/l1_h1_vol_floor_trend.yaml",
        "phase": "tier2",
        "tier": "Tier2",
        "variant_id": "g00000",
        "config_hash": "abc",
        "params_json": "{}",
        "run_slug": "g00000__tier2",
        "output_dir": "runs/row_00001__g00000__tier2",
        "expected_status": "pending",
        "enabled": "true",
        "notes": "",
    }
    manifest = tmp_path / "manifest.csv"
    write_manifest_csv([row], manifest)
    assert read_manifest_csv(manifest) == [row]


def test_completion_detection_requires_required_artifacts(tmp_path: Path) -> None:
    run_dir = tmp_path / "row"
    run_dir.mkdir()
    (run_dir / "run_status.json").write_text(json.dumps({"status": "PASS"}), encoding="utf-8")
    assert detect_run_artifact_status(run_dir).state == "INCOMPLETE"

    for name in REQUIRED_ARTIFACTS:
        (run_dir / name).write_text("ok", encoding="utf-8")
    assert detect_run_artifact_status(run_dir).state == "SUCCESS"


def test_skip_completed_behavior(tmp_path: Path) -> None:
    exp_root = tmp_path / "exp"
    completed = exp_root / "runs/row_00001__g00000__tier2"
    completed.mkdir(parents=True)
    (completed / "run_status.json").write_text(json.dumps({"status": "PASS"}), encoding="utf-8")
    for name in REQUIRED_ARTIFACTS:
        (completed / name).write_text("ok", encoding="utf-8")

    config = tmp_path / "engine.yaml"
    config.write_text("{}", encoding="utf-8")
    data_path = tmp_path / "data"
    data_path.mkdir()

    rows = [
        {
            "row_id": "row_00001",
            "hypothesis_id": "L1-H1",
            "hypothesis_path": "research/hypotheses/l1_h1_vol_floor_trend.yaml",
            "phase": "tier2",
            "tier": "Tier2",
            "variant_id": "g00000",
            "config_hash": "abc",
            "params_json": "{}",
            "run_slug": "g00000__tier2",
            "output_dir": "runs/row_00001__g00000__tier2",
            "expected_status": "pending",
            "enabled": "true",
            "notes": "",
        }
    ]

    statuses, failures = run_hypothesis_manifest_in_parallel(
        manifest_rows=rows,
        experiment_root=exp_root,
        config_path=config,
        local_config=None,
        data_path=data_path,
        max_workers=1,
        skip_completed=True,
        override_paths=[],
        dry_run=True,
    )

    assert failures == []
    assert statuses[0]["status"] == "SKIPPED"


def test_cli_rejects_missing_data_path(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.csv"
    write_manifest_csv(
        [
            {
                "row_id": "row_00001",
                "hypothesis_id": "L1-H1",
                "hypothesis_path": "research/hypotheses/l1_h1_vol_floor_trend.yaml",
                "phase": "tier2",
                "tier": "Tier2",
                "variant_id": "g00000",
                "config_hash": "abc",
                "params_json": "{}",
                "run_slug": "g00000__tier2",
                "output_dir": "runs/row_00001__g00000__tier2",
                "expected_status": "pending",
                "enabled": "true",
                "notes": "",
            }
        ],
        manifest,
    )
    config = tmp_path / "engine.yaml"
    config.write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match=r"--data path does not exist"):
        cli_run_parallel_hypothesis_grid(
            [
                "--experiment-root",
                str(tmp_path / "out"),
                "--manifest",
                str(manifest),
                "--config",
                str(config),
                "--data",
                str(tmp_path / "missing_data"),
                "--dry-run",
            ]
        )


def test_status_summary_written(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.csv"
    row = {
        "row_id": "row_00001",
        "hypothesis_id": "L1-H1",
        "hypothesis_path": "research/hypotheses/l1_h1_vol_floor_trend.yaml",
        "phase": "tier2",
        "tier": "Tier2",
        "variant_id": "g00000",
        "config_hash": "abc",
        "params_json": "{}",
        "run_slug": "g00000__tier2",
        "output_dir": "runs/row_00001__g00000__tier2",
        "expected_status": "pending",
        "enabled": "false",
        "notes": "",
    }
    write_manifest_csv([row], manifest)
    config = tmp_path / "engine.yaml"
    config.write_text("{}", encoding="utf-8")
    data_path = tmp_path / "data"
    data_path.mkdir()

    statuses, _ = run_hypothesis_manifest_in_parallel(
        manifest_rows=read_manifest_csv(manifest),
        experiment_root=tmp_path / "exp",
        config_path=config,
        local_config=None,
        data_path=data_path,
        max_workers=1,
        skip_completed=False,
        override_paths=[],
        dry_run=True,
    )
    status_csv = tmp_path / "exp" / "summaries" / "manifest_status.csv"
    with status_csv.open("r", encoding="utf-8", newline="") as handle:
        written = list(csv.DictReader(handle))
    assert len(written) == 1
    assert statuses[0]["status"] == "SKIPPED"


def test_variant_slug_is_bounded_and_stable_for_large_param_sets() -> None:
    params = {f"param_{idx:03d}": f"{'x' * 12}_{idx}" for idx in range(30)}
    slug_a = _variant_slug("g00007", params)
    slug_b = _variant_slug("g00007", params)
    assert slug_a == slug_b
    assert len(slug_a) <= 160
    assert "__h-" in slug_a


def test_completion_detection_handles_invalid_long_paths() -> None:
    too_long = Path("a" * 5000)
    status = detect_run_artifact_status(too_long)
    assert status.state == "INCOMPLETE"
    assert "inaccessible" in status.message


def test_legacy_manifest_output_dir_is_normalized(tmp_path: Path) -> None:
    config = tmp_path / "engine.yaml"
    config.write_text("{}", encoding="utf-8")
    data_path = tmp_path / "data"
    data_path.mkdir()
    params_json = json.dumps({"family_variant": "L1-H7A", "signal_timeframe": "15m"})
    long_output_dir = "runs/" + ("x" * 400)
    rows = [
        {
            "row_id": "row_00001",
            "hypothesis_id": "L1-H7A",
            "hypothesis_path": "research/hypotheses/l1_h7a_squeeze_expansion_pullback.yaml",
            "phase": "tier2",
            "tier": "Tier2",
            "variant_id": "g00000",
            "config_hash": "abc",
            "params_json": params_json,
            "run_slug": "legacy_slug",
            "output_dir": long_output_dir,
            "expected_status": "pending",
            "enabled": "true",
            "notes": "",
        }
    ]

    statuses, failures = run_hypothesis_manifest_in_parallel(
        manifest_rows=rows,
        experiment_root=tmp_path / "exp",
        config_path=config,
        local_config=None,
        data_path=data_path,
        max_workers=1,
        skip_completed=False,
        override_paths=[],
        dry_run=True,
    )

    assert failures == []
    expected_output = _normalized_output_dir(
        row_id="row_00001",
        variant_id="g00000",
        tier="Tier2",
        params_json=params_json,
    )
    assert statuses[0]["output_dir"] == expected_output
