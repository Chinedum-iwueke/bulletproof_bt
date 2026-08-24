import json
from pathlib import Path
from unittest.mock import patch

import pytest

from bt.logging.laboratory_publication import (
    publish_certified_bundle,
    record_memory_publication,
)
from bt.logging.run_bundle import RunBundleError


def publication() -> dict:
    return {
        "id": "10000000-0000-4000-8000-000000000001",
        "trial_id": "10000000-0000-4000-8000-000000000002",
        "result_id": "10000000-0000-4000-8000-000000000003",
        "request_digest": "a" * 64,
        "bundle_digest": "b" * 64,
        "canonical_receipt": {
            "result_object_id": "10000000-0000-4000-8000-000000000004"
        },
        "state": "awaiting_memory",
    }


def test_memory_publication_is_idempotent_and_digest_bound(tmp_path: Path) -> None:
    database = tmp_path / "memory.sqlite"
    created = record_memory_publication(database, publication())
    existing = record_memory_publication(database, publication())
    assert created["disposition"] == "created"
    assert existing["disposition"] == "existing"
    assert created["memory_database_digest"] == existing["memory_database_digest"]


def test_memory_publication_refuses_identity_mutation(tmp_path: Path) -> None:
    database = tmp_path / "memory.sqlite"
    record_memory_publication(database, publication())
    changed = publication() | {"result_id": "20000000-0000-4000-8000-000000000003"}
    with pytest.raises(RunBundleError, match="identity is immutable"):
        record_memory_publication(database, changed)


def test_certified_bundle_resumes_through_local_memory_confirmation(
    tmp_path: Path,
) -> None:
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    manifest = {
        "bundle_digest": "b" * 64,
        "manifest_digest": "c" * 64,
        "lineage": {
            "repository_commit": "d" * 40,
            "dataset_digest": "e" * 64,
            "market_model_bundle_digest": "f" * 64,
            "representation_contract_digest": "1" * 64,
        },
    }
    (bundle / "run_bundle_manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    responses = [
        {"object_id": "10000000-0000-4000-8000-000000000010"},
        publication(),
        publication() | {"state": "complete"},
    ]
    with (
        patch("bt.logging.laboratory_publication.validate_bundle_manifest"),
        patch(
            "bt.logging.laboratory_publication.hermes_run_payload",
            return_value={"object_id": "10000000-0000-4000-8000-000000000010"},
        ),
        patch(
            "bt.logging.laboratory_publication._request", side_effect=responses
        ) as request,
    ):
        result = publish_certified_bundle(
            api_url="http://hermes",
            token="secret",
            bundle_dir=bundle,
            registry_trial_id=publication()["trial_id"],
            registry_result_id=publication()["result_id"],
            memory_database=tmp_path / "memory.sqlite",
        )
    assert result["state"] == "complete"
    assert request.call_count == 3
    submitted = request.call_args_list[1].args[3]
    assert submitted["bundle_digest"] == manifest["bundle_digest"]
    assert len(submitted["request_digest"]) == 64
    memory_receipt = request.call_args_list[2].args[3]
    assert memory_receipt["bundle_digest"] == manifest["bundle_digest"]


def test_bundle_integrity_is_checked_before_network_publication(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "run_bundle_manifest.json").write_text("{}", encoding="utf-8")
    with patch(
        "bt.logging.laboratory_publication.validate_bundle_manifest",
        side_effect=RunBundleError("bundle manifest digest mismatch"),
    ):
        with pytest.raises(RunBundleError, match="manifest digest mismatch"):
            publish_certified_bundle(
                api_url="http://hermes",
                token="secret",
                bundle_dir=bundle,
                registry_trial_id=publication()["trial_id"],
                registry_result_id=publication()["result_id"],
                memory_database=tmp_path / "memory.sqlite",
            )
