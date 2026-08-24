from __future__ import annotations

import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import jsonschema
import pandas as pd
import pytest
import yaml

from bt.logging.run_bundle import (
    RunBundleError,
    finalize_run_bundle,
    hermes_run_payload,
    publish_to_hermes,
    validate_bundle_manifest,
)
from bt.execution.model_registry import declared_classic_bundle
from bt.experiments.representation_contract import (
    EvaluationSplit,
    FieldContract,
    RepresentationContract,
    audit_representation_frame,
)


def _representation() -> RepresentationContract:
    return RepresentationContract(
        contract_id="bundle-fixture",
        dataset_snapshot_id="11111111-1111-4111-8111-111111111111",
        dataset_digest="3" * 64,
        repository_commit="1" * 40,
        code_digest="2" * 64,
        decision_time_column="decision_at",
        entity_columns=("symbol",),
        membership_known_at_column="membership_known_at",
        membership_valid_from_column="membership_valid_from",
        membership_valid_to_column=None,
        fields=(
            FieldContract(
                name="close_feature",
                kind="feature",
                source_columns=("close",),
                transformation="fixture:identity",
                transformation_version="1",
                implementation_digest="a" * 64,
                observation_time_column="observed_at",
                availability_time_column="available_at",
                warmup_observations=0,
                missing_policy="error",
                fit_policy="stateless",
            ),
        ),
        split=EvaluationSplit(
            train_start="2026-01-01T00:00:00Z",
            train_end="2026-01-01T00:01:00Z",
            validation_start="2026-01-01T00:01:00Z",
            validation_end="2026-01-01T00:02:00Z",
            test_start="2026-01-01T00:02:00Z",
            test_end="2026-01-01T00:03:00Z",
            fit_start="2026-01-01T00:00:00Z",
            fit_end="2026-01-01T00:01:00Z",
            purge_seconds=0,
            embargo_seconds=0,
        ),
    )


def _lineage() -> dict[str, object]:
    model_digest = declared_classic_bundle(
        profile="tier2",
        parameters={"taker_fee_bps": 6.0, "slippage_bps": 2.0, "spread_bps": 1.0, "delay_bars": 1},
    ).digest
    return {
        "repository_commit": "1" * 40,
        "code_digest": "2" * 64,
        "dataset_snapshot_id": "11111111-1111-4111-8111-111111111111",
        "dataset_digest": "3" * 64,
        "specification_digest": "4" * 64,
        "environment_digest": "5" * 64,
        "market_model_bundle_digest": model_digest,
        "representation_contract_digest": _representation().digest,
        "search_plan_digest": "7" * 64,
        "search_family_id": "fixture-family",
        "trial_id": "8" * 64,
        "attempt": 1,
    }


def _run(path: Path, *, run_id: str = "run-a", absolute_manifest_paths: bool = True) -> Path:
    path.mkdir(parents=True)
    (path / "config_used.yaml").write_text(
        yaml.safe_dump({"strategy": {"name": "fixture"}, "seed": 7}), encoding="utf-8"
    )
    (path / "performance.json").write_text(
        json.dumps({"schema_version": 1, "run_id": run_id, "total_trades": 0}) + "\n",
        encoding="utf-8",
    )
    model_bundle = declared_classic_bundle(
        profile="tier2",
        parameters={"taker_fee_bps": 6.0, "slippage_bps": 2.0, "spread_bps": 1.0, "delay_bars": 1},
    )
    (path / "market_model_bundle.json").write_text(
        json.dumps(model_bundle.document(), sort_keys=True) + "\n", encoding="utf-8"
    )
    representation = _representation()
    representation_document = representation.document()
    frame = pd.DataFrame(
        {
            "symbol": ["BTCUSDT"],
            "decision_at": [pd.Timestamp("2026-01-01T00:00:00Z")],
            "membership_known_at": [pd.Timestamp("2026-01-01T00:00:00Z")],
            "membership_valid_from": [pd.Timestamp("2026-01-01T00:00:00Z")],
            "close": [100.0],
            "close_feature": [100.0],
            "observed_at": [pd.Timestamp("2026-01-01T00:00:00Z")],
            "available_at": [pd.Timestamp("2026-01-01T00:00:00Z")],
        }
    )
    report = audit_representation_frame(representation, frame)
    (path / "representation_contract.json").write_text(
        json.dumps(representation_document, sort_keys=True) + "\n", encoding="utf-8"
    )
    (path / "representation_leakage_report.json").write_text(
        json.dumps(report, sort_keys=True) + "\n", encoding="utf-8"
    )
    for name, header in (
        ("equity.csv", "ts,equity\n"),
        ("trades.csv", "entry_ts,exit_ts,symbol\n"),
        ("performance_by_bucket.csv", "bucket,trades\n"),
    ):
        (path / name).write_text(header, encoding="utf-8")
    (path / "fills.jsonl").write_text("", encoding="utf-8")
    (path / "decisions.jsonl").write_text("", encoding="utf-8")
    run_dir_value = str(path) if absolute_manifest_paths else "runs/current"
    (path / "run_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "run_id": run_id,
                "run_dir": run_dir_value,
                "data_path": "/srv/protected/data.parquet" if absolute_manifest_paths else "data/fixture.parquet",
                "created_at_utc": "2026-08-22T12:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    return path


def _manifest(root: Path, receipt: dict[str, object]) -> tuple[dict, Path]:
    directory = root / "bundles" / str(receipt["bundle_digest"])
    return json.loads((directory / "run_bundle_manifest.json").read_text()), directory


def test_finalization_is_atomic_schema_valid_and_integrity_replayable(tmp_path) -> None:
    root = tmp_path / "registry"
    receipt = finalize_run_bundle(_run(tmp_path / "run"), root, lineage=_lineage())
    manifest, directory = _manifest(root, receipt)
    schema = json.loads(Path("schemas/run-bundle-v1.schema.json").read_text())
    jsonschema.Draft202012Validator(schema).validate(manifest)
    validate_bundle_manifest(manifest, directory)
    legacy = next(item for item in manifest["artifacts"] if item["name"] == "run_manifest.json")
    assert legacy["normalization"] == "stable-runtime-aliases-v1"
    assert legacy["source_content_digest"] != legacy["content_digest"]
    assert not any(path.name.startswith(".staging") for path in root.iterdir())


def test_semantically_equal_runs_share_bundle_digest_and_publish_once(tmp_path) -> None:
    root = tmp_path / "registry"
    first = finalize_run_bundle(_run(tmp_path / "run-a", run_id="a"), root, lineage=_lineage())
    second = finalize_run_bundle(_run(tmp_path / "run-b", run_id="b"), root, lineage=_lineage())
    assert first == second
    assert len(list((root / "bundles").iterdir())) == 1
    assert len(list((root / "receipts").iterdir())) == 1


def test_interruption_retains_failed_attempt_and_no_partial_bundle(tmp_path) -> None:
    root = tmp_path / "registry"

    def interrupt(_):
        raise RuntimeError("simulated interruption")

    with pytest.raises(RunBundleError, match="failure retained"):
        finalize_run_bundle(_run(tmp_path / "run"), root, lineage=_lineage(), before_commit=interrupt)
    failures = list((root / "failures").glob("*.json"))
    assert len(failures) == 1
    assert json.loads(failures[0].read_text())["state"] == "failed"
    assert not (root / "bundles").exists()


def test_missing_corrupt_and_unregistered_artifacts_fail_closed(tmp_path) -> None:
    root = tmp_path / "registry"
    missing = _run(tmp_path / "missing")
    (missing / "trades.csv").unlink()
    with pytest.raises(RunBundleError):
        finalize_run_bundle(missing, root, lineage=_lineage())
    corrupt = _run(tmp_path / "corrupt")
    (corrupt / "performance.json").write_text("not json", encoding="utf-8")
    with pytest.raises(RunBundleError):
        finalize_run_bundle(corrupt, root, lineage=_lineage())
    unknown = _run(tmp_path / "unknown")
    (unknown / "model.bin").write_bytes(b"binary")
    with pytest.raises(RunBundleError, match="registered structural schema"):
        finalize_run_bundle(unknown, root, lineage=_lineage())


def test_market_model_bundle_is_required_digest_bound_and_tamper_evident(tmp_path) -> None:
    missing = _run(tmp_path / "missing-model")
    (missing / "market_model_bundle.json").unlink()
    with pytest.raises(RunBundleError, match="market_model_bundle.json is required"):
        finalize_run_bundle(missing, tmp_path / "registry-a", lineage=_lineage())

    mismatched = _lineage()
    mismatched["market_model_bundle_digest"] = "f" * 64
    with pytest.raises(RunBundleError, match="does not match lineage"):
        finalize_run_bundle(_run(tmp_path / "mismatch"), tmp_path / "registry-b", lineage=mismatched)

    tampered = _run(tmp_path / "tampered")
    document = json.loads((tampered / "market_model_bundle.json").read_text())
    document["models"][0]["parameters"]["delay_bars"] = 99
    (tampered / "market_model_bundle.json").write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(RunBundleError, match="bundle digest mismatch"):
        finalize_run_bundle(tampered, tmp_path / "registry-c", lineage=_lineage())


def test_representation_evidence_is_required_certified_and_digest_bound(tmp_path) -> None:
    missing = _run(tmp_path / "missing-representation")
    (missing / "representation_leakage_report.json").unlink()
    with pytest.raises(RunBundleError, match="representation_contract.json"):
        finalize_run_bundle(missing, tmp_path / "registry-a", lineage=_lineage())

    uncertified = _run(tmp_path / "uncertified-representation")
    report_path = uncertified / "representation_leakage_report.json"
    report = json.loads(report_path.read_text())
    report["status"] = "failed"
    report["report_digest"] = hashlib.sha256(
        json.dumps(
            {key: value for key, value in report.items() if key != "report_digest"},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
    ).hexdigest()
    report_path.write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(RunBundleError, match="not certified"):
        finalize_run_bundle(uncertified, tmp_path / "registry-b", lineage=_lineage())

    mismatched = _lineage()
    mismatched["representation_contract_digest"] = "f" * 64
    with pytest.raises(RunBundleError, match="does not match lineage"):
        finalize_run_bundle(_run(tmp_path / "mismatch-representation"), tmp_path / "registry-c", lineage=mismatched)


def test_manifest_corruption_and_incompatible_schema_fail_replay(tmp_path) -> None:
    root = tmp_path / "registry"
    receipt = finalize_run_bundle(_run(tmp_path / "run"), root, lineage=_lineage())
    manifest, directory = _manifest(root, receipt)
    incompatible = {**manifest, "schema_version": "run-bundle-v2.0.0"}
    with pytest.raises(RunBundleError, match="unsupported"):
        validate_bundle_manifest(incompatible, directory)
    artifact = directory / "artifacts/performance.json"
    artifact.write_text("{}", encoding="utf-8")
    with pytest.raises(RunBundleError, match="integrity mismatch"):
        validate_bundle_manifest(manifest, directory)


@pytest.mark.parametrize(
    "name,content,match",
    [
        ("notes.txt", "source=/home/founder/private\n", "absolute protected path"),
        ("notes.txt", "api_key=supersecretvalue\n", "sensitive material"),
    ],
)
def test_path_and_secret_scan_blocks_publication(tmp_path, name, content, match) -> None:
    run = _run(tmp_path / "run")
    (run / name).write_text(content, encoding="utf-8")
    with pytest.raises(RunBundleError, match=match):
        finalize_run_bundle(run, tmp_path / "registry", lineage=_lineage())


def test_symlinked_artifact_cannot_escape_run_directory(tmp_path) -> None:
    outside = tmp_path / "outside.txt"
    outside.write_text("private", encoding="utf-8")
    run = _run(tmp_path / "run")
    (run / "linked.txt").symlink_to(outside)
    with pytest.raises(RunBundleError, match="symlinked artifacts are forbidden"):
        finalize_run_bundle(run, tmp_path / "registry", lineage=_lineage())


def test_hermes_payload_and_idempotent_receipt_are_digest_bound(tmp_path) -> None:
    receipt = finalize_run_bundle(
        _run(tmp_path / "run"), tmp_path / "registry", lineage=_lineage()
    )
    payload = hermes_run_payload(receipt, _lineage())
    assert payload["payload"]["bundle_digest"] == receipt["bundle_digest"]
    response = json.dumps({"object_id": payload["object_id"]}).encode()

    class Response:
        def __enter__(self):
            return io.BytesIO(response)

        def __exit__(self, *_):
            return False

    import io

    with patch("urllib.request.urlopen", return_value=Response()) as call:
        published = publish_to_hermes("http://control-plane", "not-logged", receipt, _lineage())
    assert published["state"] == "published"
    assert call.call_args.args[0].headers["Authorization"] == "Bearer not-logged"
