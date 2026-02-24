from __future__ import annotations

import json
from pathlib import Path

import yaml

from bt.api import run_backtest
from bt.audit.gate import (
    EXIT_MISSING_REQUIRED,
    EXIT_OK,
    EXIT_VIOLATIONS,
    evaluate_gate,
)


def _write_yaml(path: Path, payload: dict) -> None:
    path.write_text(yaml.safe_dump(payload, sort_keys=True), encoding="utf-8")


def test_coverage_written_when_audit_enabled(tmp_path: Path) -> None:
    override = tmp_path / "audit_override.yaml"
    _write_yaml(
        override,
        {
            "audit": {
                "enabled": True,
                "level": "full",
                "required_layers": [],
            }
        },
    )

    run_dir = Path(
        run_backtest(
            config_path="configs/engine.yaml",
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "runs"),
            override_paths=[str(override)],
            run_name="audit_cov",
        )
    )

    coverage_path = run_dir / "audit" / "coverage.json"
    assert coverage_path.exists()

    coverage = json.loads(coverage_path.read_text(encoding="utf-8"))
    assert coverage["enabled"] is True
    assert "order_audit" in coverage["executed_layers"]
    assert "order_normalization_check" in coverage["executed_layers"]
    for key in (
        "run_id",
        "expected_layers",
        "required_layers",
        "executed_layers",
        "skipped_layers",
        "event_counts",
        "violations",
        "status",
    ):
        assert key in coverage


def test_gate_fails_on_violations(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    audit_dir = run_dir / "audit"
    audit_dir.mkdir(parents=True)
    (audit_dir / "coverage.json").write_text(
        json.dumps(
            {
                "required_layers": [],
                "executed_layers": ["order_audit"],
                "violations": {"order_audit": 1},
            }
        ),
        encoding="utf-8",
    )

    code, details = evaluate_gate(run_dir, strict=True)
    assert code == EXIT_VIOLATIONS
    assert details["reason"] == "violations"


def test_gate_fails_on_missing_required_layer(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    audit_dir = run_dir / "audit"
    audit_dir.mkdir(parents=True)
    (audit_dir / "coverage.json").write_text(
        json.dumps(
            {
                "required_layers": ["portfolio_audit"],
                "executed_layers": ["order_audit", "fill_audit"],
                "violations": {"order_audit": 0, "fill_audit": 0},
            }
        ),
        encoding="utf-8",
    )

    code, details = evaluate_gate(run_dir, strict=True)
    assert code == EXIT_MISSING_REQUIRED
    assert details["missing_required_layers"] == ["portfolio_audit"]


def test_gate_passes_with_clean_coverage(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    audit_dir = run_dir / "audit"
    audit_dir.mkdir(parents=True)
    (audit_dir / "coverage.json").write_text(
        json.dumps(
            {
                "required_layers": ["order_audit", "fill_audit"],
                "executed_layers": ["order_audit", "fill_audit"],
                "violations": {"order_audit": 0, "fill_audit": 0},
            }
        ),
        encoding="utf-8",
    )

    code, details = evaluate_gate(run_dir, strict=True)
    assert code == EXIT_OK
    assert details["status"] == "pass"
