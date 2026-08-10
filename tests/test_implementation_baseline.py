import importlib.util
import json
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts" / "implementation_baseline.py"
SCHEMA = Path(__file__).parents[1] / "schemas" / "implementation-baseline-v1.schema.json"
SPEC = importlib.util.spec_from_file_location("implementation_baseline", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
baseline = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(baseline)


def git(repository: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def repository(tmp_path: Path, *, lock: bool = True) -> Path:
    root = tmp_path / "bulletproof_bt"
    root.mkdir()
    git(root, "init", "-b", "main")
    git(root, "config", "user.name", "Baseline Test")
    git(root, "config", "user.email", "baseline@example.invalid")
    (root / "pyproject.toml").write_text("[project]\nname='fixture'\n", encoding="utf-8")
    schema = root / "fixture.schema.json"
    schema.write_text('{"type":"object"}\n', encoding="utf-8")
    if lock:
        (root / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    git(root, "add", "pyproject.toml", "fixture.schema.json")
    if lock:
        git(root, "add", "uv.lock")
    git(root, "commit", "-m", "fixture")
    return root


def test_clean_repository_is_stable_and_schema_shaped(tmp_path: Path) -> None:
    root = repository(tmp_path)
    first = baseline.collect(root)
    second = baseline.collect(root)

    assert first == second
    assert first["repository"]["dirty"] is False
    assert first["dependencies"]["lock_present"] is True
    assert first["schemas"][0]["path"] == "fixture.schema.json"
    assert len(first["baseline_digest"]) == 64
    baseline.validate(json.loads(json.dumps(first)))


def test_dirty_repository_is_guarded_and_can_be_recorded(tmp_path: Path) -> None:
    root = repository(tmp_path)
    (root / "pyproject.toml").write_text("[project]\nname='changed'\n", encoding="utf-8")

    with pytest.raises(baseline.BaselineError, match="Repository is dirty"):
        baseline.collect(root)

    document = baseline.collect(root, require_clean=False)
    assert document["repository"]["dirty"] is True
    assert document["repository"]["dirty_paths"] == ["pyproject.toml"]


def test_missing_lock_is_explicit_not_fatal(tmp_path: Path) -> None:
    document = baseline.collect(repository(tmp_path, lock=False))
    assert document["dependencies"]["lock_present"] is False
    assert document["dependencies"]["locks"] == []


def test_remote_credentials_are_not_retained(tmp_path: Path) -> None:
    root = repository(tmp_path)
    credential = "fixture-" + "credential"
    query = "access_" + "token=fixture"
    git(
        root,
        "remote",
        "add",
        "origin",
        f"https://user:{credential}@example.invalid/org/repo.git?{query}",
    )

    document = baseline.collect(root)
    encoded = json.dumps(document)
    assert document["repository"]["origin"] == "https://example.invalid/org/repo.git"
    assert credential not in encoded
    assert query not in encoded


def test_digest_tampering_is_rejected(tmp_path: Path) -> None:
    document = baseline.collect(repository(tmp_path))
    document["repository"]["branch"] = "tampered"
    with pytest.raises(baseline.BaselineError, match="digest mismatch"):
        baseline.validate(document)


def test_nested_unknown_fields_are_rejected(tmp_path: Path) -> None:
    document = baseline.collect(repository(tmp_path))
    document["repository"]["unexpected"] = True
    with pytest.raises(baseline.BaselineError, match="Repository fields"):
        baseline.validate(document)


def test_written_evidence_is_private_and_repeatable(tmp_path: Path) -> None:
    document = baseline.collect(repository(tmp_path))
    first = tmp_path / "one" / "implementation-baseline.json"
    second = tmp_path / "two" / "implementation-baseline.json"
    baseline._write(document, first)
    baseline._write(document, second)
    assert first.read_bytes() == second.read_bytes()
    assert first.stat().st_mode & 0o777 == 0o600


def test_json_schema_is_strict_and_matches_contract() -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    assert schema["$schema"].endswith("2020-12/schema")
    assert schema["additionalProperties"] is False
    assert schema["properties"]["schema_version"]["const"] == baseline.SCHEMA_VERSION
    assert schema["properties"]["claim_vocabulary"]["const"] == baseline.CLAIM_VOCABULARY
