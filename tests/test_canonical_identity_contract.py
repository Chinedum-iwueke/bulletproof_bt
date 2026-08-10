from __future__ import annotations

import json
from pathlib import Path

import pytest

from bt.contracts.canonical_identity import (
    IdentityCompatibilityError,
    assert_immutable_identity,
    normalize_identity,
)

FIXTURES = Path(__file__).parent / "fixtures" / "canonical_identity"


def load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_canonical_record_round_trips() -> None:
    document = load("canonical-v1.json")
    assert normalize_identity(document) == document


def test_legacy_bulletproof_record_receives_immutable_alias() -> None:
    legacy = load("legacy-bulletproof-v0.json")
    value = normalize_identity(
        legacy,
        object_type="research.trial",
        namespace="bulletproof",
        id_field="run_id",
        digest_field="artifact_digest",
        producer_schema_version="run-bundle-v0",
        canonical_object_id="33333333-3333-4333-8333-333333333333",
    )
    assert value["object_id"] == "33333333-3333-4333-8333-333333333333"
    assert value["content_digest"] == legacy["artifact_digest"]
    assert value["aliases"][0]["value"] == "run-btc-momentum-legacy-001"


def test_digest_mutation_is_rejected() -> None:
    previous = load("canonical-v1.json")
    current = {**previous, "content_digest": "c" * 64}
    with pytest.raises(IdentityCompatibilityError, match="digest mutation"):
        assert_immutable_identity(previous, current)


def test_unknown_major_version_is_rejected() -> None:
    document = {**load("canonical-v1.json"), "schema_version": "canonical-identity-v2.0.0"}
    with pytest.raises(IdentityCompatibilityError, match="unsupported"):
        normalize_identity(document)
