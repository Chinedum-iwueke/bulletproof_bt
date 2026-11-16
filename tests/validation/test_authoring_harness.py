"""Unit tests of harness rejection rules, not native-strategy certification."""

import importlib.util
import json
import sys
from copy import deepcopy
from hashlib import sha256

import pandas as pd
import pytest

from bt.validation.authoring_harness import (
    HarnessError,
    _digest,
    assert_native_implementation_unmodified,
    assert_runner_governance_unmodified,
    assert_separate_fixture_bindings,
    assert_terminal_case,
    assert_unchanged_scientific_contract,
    write_fixture_binding,
)


@pytest.mark.parametrize(
    "name",
    [
        "execute_registered",
        "verify_trusted_dataset_bindings",
        "governed_review_verified",
        "complete_independent_review",
    ],
)
def test_runner_guard_rejects_mocked_governance(monkeypatch, name):
    from pathlib import Path

    import scripts.run_alpha_research_assignment as runner

    repository = Path(__file__).resolve().parents[2]
    assert_runner_governance_unmodified(repository)
    monkeypatch.setattr(runner, name, lambda *args, **kwargs: True)
    with pytest.raises(HarnessError, match="runner governance replaced"):
        assert_runner_governance_unmodified(repository)


def test_native_guard_rejects_monkeypatched_science(tmp_path, monkeypatch):
    path = tmp_path / "src/bt/strategy/fixture_native.py"
    path.parent.mkdir(parents=True)
    path.write_text(
        "MINIMUM_HISTORY = 5000\ndef evaluate_alpha_intent():\n    return {}\n"
    )
    name = "bt.strategy.fixture_native"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setitem(sys.modules, name, module)
    contract = {
        "evaluation": {
            "native_implementation": {"path": "src/bt/strategy/fixture_native.py"}
        }
    }
    assert_native_implementation_unmodified(tmp_path, contract)
    monkeypatch.setattr(module, "MINIMUM_HISTORY", 12)
    with pytest.raises(HarnessError, match="constant changed"):
        assert_native_implementation_unmodified(tmp_path, contract)
    monkeypatch.setattr(module, "MINIMUM_HISTORY", 5000)
    monkeypatch.setattr(module, "evaluate_alpha_intent", dict)
    with pytest.raises(HarnessError, match="function replaced"):
        assert_native_implementation_unmodified(tmp_path, contract)


def panel():
    return pd.DataFrame(
        {
            "ts": pd.date_range("2024-01-01", periods=10, freq="min", tz="UTC"),
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "volume": 10.0,
        }
    )


def test_fixture_provenance_is_content_bound_and_separate(tmp_path):
    binding = write_fixture_binding(
        tmp_path,
        panel(),
        instrument="TRBUSDT",
        venue="bybit",
        source_commit="a" * 40,
        case="negative",
    )
    assert_separate_fixture_bindings(
        [binding], [{"producer_receipt_digest": "production"}]
    )
    directory = tmp_path / "negative" / "TRBUSDT"
    for kind in ("catalog", "manifest", "producer_receipt", "lake_governance"):
        document = json.loads((directory / f"{kind}.json").read_text())
        assert _digest(document) == binding[f"{kind}_digest"]
        assert document["scientific_certification"] is False
    with pytest.raises(HarnessError, match="production provenance"):
        assert_separate_fixture_bindings([binding], [binding])
    (directory / "panel.parquet").write_bytes(b"changed")
    with pytest.raises(HarnessError, match="bytes differ"):
        assert_separate_fixture_bindings([binding], [])


@pytest.mark.parametrize(
    "field", ["producer_receipt_id", "catalog_id", "manifest_digest", "dataset_key"]
)
def test_fixture_rejects_copied_production_identity(tmp_path, field):
    binding = write_fixture_binding(
        tmp_path,
        panel(),
        instrument="TRBUSDT",
        venue="bybit",
        source_commit="a" * 40,
        case="negative",
    )
    production = {field: "real-production-identity"}
    binding[field] = production[field]
    with pytest.raises(HarnessError, match="production provenance"):
        assert_separate_fixture_bindings([binding], [production])


@pytest.mark.parametrize(
    "field", ["warmup_observations", "minimum_history", "costs", "window", "grid"]
)
def test_fixture_does_not_silently_relax_semantics(field):
    source = {
        "immutable_contract": {"dataset_bindings": [{"dataset_digest": "production"}]},
        field: {"value": 5000},
    }
    fixture = deepcopy(source)
    fixture["immutable_contract"]["dataset_bindings"] = [{"dataset_digest": "fixture"}]
    assert_unchanged_scientific_contract(source, fixture)
    fixture[field]["value"] = 12
    with pytest.raises(HarnessError, match="scientific contract"):
        assert_unchanged_scientific_contract(source, fixture)
    assert source[field]["value"] == 5000


def test_fixture_only_rebinds_provenance_at_known_contract_paths():
    source = {
        "immutable_contract": {"dataset_bindings": [{"dataset_digest": "production"}]},
        "evaluation": {"evidence_digest": "frozen-evaluation-evidence"},
    }
    fixture = deepcopy(source)
    fixture["evaluation"]["evidence_digest"] = "replacement"
    with pytest.raises(HarnessError, match="scientific contract"):
        assert_unchanged_scientific_contract(source, fixture)


def terminal(tmp_path, outcome="negative"):
    result = {
        "disposition": "commissioning_complete",
        "publication_envelope": {
            "qualification_authority": False,
            "trial": {
                "held_out_evaluation": {
                    "outcome": outcome,
                    "passed": outcome == "positive",
                    "held_out_evaluated": True,
                    "metrics": {"support": 100},
                }
            },
        },
    }
    for index in range(2):
        path = tmp_path / "run-bundles-test" / "bundles" / str(index) / "artifacts"
        path.mkdir(parents=True)
        record = {"parameters": {"threshold": index}, "outcome": outcome}
        record["record_digest"] = _digest(record)
        artifact = path / "strategy_intent_validation_evaluation.json"
        artifact.write_text(json.dumps(record))
        (path / "downstream_reuse_manifest.json").write_text(
            json.dumps(
                {
                    "artifacts": {
                        artifact.name: sha256(artifact.read_bytes()).hexdigest()
                    }
                }
            )
        )
    return result


def check(result, tmp_path, outcome="negative"):
    return assert_terminal_case(
        result,
        output=tmp_path,
        expected_outcome=outcome,
        expected_parameters=[{"threshold": 0}, {"threshold": 1}],
        required_metrics=["support"],
    )


@pytest.mark.parametrize("outcome", ["positive", "negative", "invalid", "failed"])
def test_assertion_can_check_each_explicit_outcome(tmp_path, outcome):
    report = check(terminal(tmp_path, outcome), tmp_path, outcome)
    assert report["outcome"] == outcome
    assert report["scientific_certification"] is False


def test_assertion_rejects_any_outcome_shortcut(tmp_path):
    with pytest.raises(HarnessError, match="terminal outcome"):
        check(terminal(tmp_path, "invalid"), tmp_path, "positive")


def test_native_scientific_terminal_is_not_trade_log_holdout(tmp_path):
    result = terminal(tmp_path, "invalid")
    trial = result["publication_envelope"]["trial"]
    trial["hypothesis_evaluation"] = trial.pop("held_out_evaluation")
    trial["held_out_evaluation"] = None
    report = check(result, tmp_path, "invalid")
    assert report["outcome"] == "invalid"
    assert report["scientific_certification"] is False


def test_missing_scientific_and_trade_terminal_fails_closed(tmp_path):
    result = terminal(tmp_path)
    result["publication_envelope"]["trial"]["held_out_evaluation"] = None
    with pytest.raises(HarnessError, match="evidence is missing"):
        check(result, tmp_path)


def test_malformed_native_terminal_does_not_fall_back_to_trade_score(tmp_path):
    result = terminal(tmp_path)
    result["publication_envelope"]["trial"]["hypothesis_evaluation"] = []
    with pytest.raises(HarnessError, match="evidence is missing"):
        check(result, tmp_path)


def test_assertion_rejects_closed_positive_selection(tmp_path):
    result = terminal(tmp_path, "positive")
    result["publication_envelope"]["trial"]["held_out_evaluation"][
        "held_out_evaluated"
    ] = False
    with pytest.raises(HarnessError, match="actually open"):
        check(result, tmp_path, "positive")


def test_assertion_rejects_missing_metrics(tmp_path):
    result = terminal(tmp_path)
    result["publication_envelope"]["trial"]["held_out_evaluation"]["metrics"] = {}
    with pytest.raises(HarnessError, match="metrics"):
        check(result, tmp_path)


def test_assertion_rejects_tampered_variant(tmp_path):
    result = terminal(tmp_path)
    path = next(tmp_path.rglob("strategy_intent_validation_evaluation.json"))
    value = json.loads(path.read_text())
    value["parameters"] = {"threshold": 99}
    path.write_text(json.dumps(value))
    with pytest.raises(HarnessError, match="record digest"):
        check(result, tmp_path)


def test_assertion_rejects_qualification_authority(tmp_path):
    result = terminal(tmp_path)
    result["publication_envelope"]["qualification_authority"] = True
    with pytest.raises(HarnessError, match="qualification authority"):
        check(result, tmp_path)
