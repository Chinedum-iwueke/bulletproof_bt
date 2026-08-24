from __future__ import annotations

from dataclasses import asdict, replace
import json
import os
from pathlib import Path
import subprocess
import sys

import jsonschema
import pandas as pd
import pytest

from bt.experiments.representation_contract import (
    EvaluationSplit,
    FieldContract,
    RepresentationContract,
    RepresentationContractError,
    audit_representation_frame,
    certify_representation_frame,
    validate_representation_document,
)


def _split() -> EvaluationSplit:
    return EvaluationSplit(
        train_start="2026-01-01T00:00:00Z",
        train_end="2026-01-01T00:04:00Z",
        validation_start="2026-01-01T00:05:00Z",
        validation_end="2026-01-01T00:07:00Z",
        test_start="2026-01-01T00:08:00Z",
        test_end="2026-01-01T00:10:00Z",
        fit_start="2026-01-01T00:00:00Z",
        fit_end="2026-01-01T00:04:00Z",
        purge_seconds=60,
        embargo_seconds=60,
    )


def _feature(**overrides) -> FieldContract:
    values = {
        "name": "ema_20",
        "kind": "feature",
        "source_columns": ("close",),
        "transformation": "bt.features.fixture:ema",
        "transformation_version": "1.0.0",
        "implementation_digest": "1" * 64,
        "observation_time_column": "feature_observed_at",
        "availability_time_column": "feature_available_at",
        "warmup_observations": 2,
        "missing_policy": "error",
        "fit_policy": "stateless",
        "completeness_column": "bar_complete",
    }
    values.update(overrides)
    return FieldContract(**values)


def _label() -> FieldContract:
    return FieldContract(
        name="return_1m",
        kind="label",
        source_columns=("close",),
        transformation="bt.labels.fixture:forward_return",
        transformation_version="1.0.0",
        implementation_digest="2" * 64,
        observation_time_column="label_observed_at",
        availability_time_column="label_available_at",
        warmup_observations=0,
        missing_policy="remain_missing",
        fit_policy="stateless",
        label_horizon_seconds=60,
    )


def _contract(**overrides) -> RepresentationContract:
    values = {
        "contract_id": "btc-ema-representation-v1",
        "dataset_snapshot_id": "snapshot-1",
        "dataset_digest": "3" * 64,
        "repository_commit": "4" * 40,
        "code_digest": "5" * 64,
        "decision_time_column": "decision_at",
        "entity_columns": ("symbol",),
        "membership_known_at_column": "membership_known_at",
        "membership_valid_from_column": "membership_valid_from",
        "membership_valid_to_column": "membership_valid_to",
        "fields": (_feature(), _label()),
        "split": _split(),
    }
    values.update(overrides)
    return RepresentationContract(**values)


def _frame() -> pd.DataFrame:
    decision = pd.date_range("2026-01-01T00:00:00Z", periods=11, freq="1min")
    return pd.DataFrame(
        {
            "symbol": ["BTCUSDT"] * 11,
            "decision_at": decision,
            "membership_known_at": [decision[0]] * 11,
            "membership_valid_from": [decision[0]] * 11,
            "membership_valid_to": [pd.NaT] * 11,
            "close": range(100, 111),
            "ema_20": [None, None, *range(102, 111)],
            "feature_observed_at": decision,
            "feature_available_at": decision,
            "bar_complete": [True] * 11,
            "return_1m": [0.01] * 10 + [None],
            "label_observed_at": decision + pd.Timedelta(minutes=1),
            "label_available_at": decision + pd.Timedelta(minutes=1),
        }
    )


def _codes(report: dict) -> set[str]:
    return {item["code"] for item in report["violations"]}


def test_contract_is_deterministic_schema_valid_and_tamper_evident() -> None:
    first = _contract().document()
    second = _contract().document()
    assert first == second
    schema = json.loads(Path("schemas/representation-contract-v1.schema.json").read_text())
    jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker()).validate(first)
    validate_representation_document(first)
    first["fields"][0]["warmup_observations"] = 0
    with pytest.raises(RepresentationContractError, match="digest mismatch"):
        validate_representation_document(first)


def test_clean_point_in_time_frame_certifies_and_replays() -> None:
    report = certify_representation_frame(_contract(), _frame())
    assert report["status"] == "certified"
    assert report["row_count"] == 11
    assert len(report["report_digest"]) == 64


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        (lambda frame: frame.assign(feature_available_at=frame["decision_at"] + pd.Timedelta(minutes=1)), "future_information"),
        (lambda frame: frame.assign(feature_observed_at=frame["decision_at"] + pd.Timedelta(minutes=1)), "future_information"),
        (lambda frame: frame.assign(membership_known_at=frame["decision_at"] + pd.Timedelta(minutes=1)), "universe_leakage"),
        (lambda frame: frame.assign(bar_complete=[True, True, False, *([True] * 8)]), "incomplete_period_value"),
        (lambda frame: frame.assign(label_available_at=frame["decision_at"]), "premature_label"),
    ],
)
def test_future_revision_universe_incomplete_bar_and_label_leakage_fail(mutation, code) -> None:
    assert code in _codes(audit_representation_frame(_contract(), mutation(_frame())))


def test_warmup_and_missingness_are_enforced_per_entity() -> None:
    frame = _frame()
    frame.loc[0, "ema_20"] = 100.0
    frame.loc[4, "ema_20"] = None
    report = audit_representation_frame(_contract(), frame)
    assert {"warmup_leakage", "undeclared_missingness"}.issubset(_codes(report))


def test_fit_state_and_split_boundaries_fail_closed() -> None:
    with pytest.raises(RepresentationContractError, match="fitted-state digest"):
        _feature(fit_policy="train_only").validate()
    with pytest.raises(RepresentationContractError, match="unsupported fit policy"):
        _feature(fit_policy="global").validate()
    with pytest.raises(RepresentationContractError, match="inside the training"):
        replace(_split(), fit_end="2026-01-01T00:06:00Z").validate()
    with pytest.raises(RepresentationContractError, match="purge_seconds"):
        replace(_split(), purge_seconds=61).validate()
    with pytest.raises(RepresentationContractError, match="embargo_seconds"):
        replace(_split(), embargo_seconds=61).validate()


def test_contract_rejects_duplicate_fields_and_unqualified_label() -> None:
    with pytest.raises(RepresentationContractError, match="unique"):
        _contract(fields=(_feature(), _feature())).validate()
    with pytest.raises(RepresentationContractError, match="positive horizon"):
        replace(_label(), label_horizon_seconds=None).validate()


def test_fitted_feature_binds_training_artifact_and_certifies() -> None:
    fitted = _feature(fit_policy="train_only", fit_artifact_digest="6" * 64)
    contract = _contract(fields=(fitted, _label()))
    assert certify_representation_frame(contract, _frame())["status"] == "certified"


def test_fresh_cli_reconstructs_manifest_and_leakage_report(tmp_path) -> None:
    source = tmp_path / "contract.json"
    frame = tmp_path / "frame.parquet"
    manifest = tmp_path / "representation.json"
    report = tmp_path / "leakage.json"
    source.write_text(json.dumps(asdict(_contract())), encoding="utf-8")
    _frame().to_parquet(frame, index=False)
    completed = subprocess.run(
        [
            sys.executable,
            "scripts/certify_representation.py",
            "--contract",
            str(source),
            "--frame",
            str(frame),
            "--manifest-output",
            str(manifest),
            "--report-output",
            str(report),
        ],
        check=True,
        capture_output=True,
        env={**os.environ, "PYTHONPATH": str(Path.cwd() / "src")},
        text=True,
    )
    result = json.loads(completed.stdout)
    assert result["report"]["status"] == "certified"
    validate_representation_document(json.loads(manifest.read_text()))
    assert json.loads(report.read_text())["representation_digest"] == result["representation_digest"]
