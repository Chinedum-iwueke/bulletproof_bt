import json
from pathlib import Path

from bt.contracts.lifecycle import build_lifecycle_event, validate_lifecycle_event


def test_c1_lifecycle_fixture_is_valid_and_hashes_deterministically() -> None:
    fixture = json.loads(Path("tests/fixtures/contracts/research_lifecycle_event_v1.json").read_text())
    assert validate_lifecycle_event(fixture) == []
    one = build_lifecycle_event(**{key: fixture[key] for key in ("event_id", "event_type", "occurred_at", "identity", "payload", "actor")})
    two = build_lifecycle_event(**{key: fixture[key] for key in ("event_id", "event_type", "occurred_at", "identity", "payload", "actor")})
    assert one["event_hash"] == two["event_hash"]


def test_c1_deployment_stage_requires_deployment_identity() -> None:
    fixture = json.loads(Path("tests/fixtures/contracts/research_lifecycle_event_v1.json").read_text())
    fixture["identity"]["stage"] = "live"
    assert "stage_identity_deployment_id_required" in validate_lifecycle_event(fixture)
