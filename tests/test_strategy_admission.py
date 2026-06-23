from __future__ import annotations

from pathlib import Path

import yaml

from bt.hypotheses.contract import HypothesisContract
from bt.hypotheses.logging import REQUIRED_LOG_FIELDS
from bt.validation.strategy_admission import validate_hypothesis_admission


FIXTURE = Path("research/hypotheses/l1_h11a.yaml")


def test_existing_production_hypothesis_passes_admission() -> None:
    report = validate_hypothesis_admission(FIXTURE)

    assert report.status == "PASS"
    assert report.strategy_sha256
    assert report.hypothesis_sha256


def test_admission_fails_without_truth_contract(tmp_path: Path) -> None:
    payload = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
    payload.pop("truth_contract")
    path = tmp_path / "missing_truth.yaml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    report = validate_hypothesis_admission(path)

    assert report.status == "FAIL"
    assert any(issue.check == "truth_contract" for issue in report.issues)


def test_admission_fails_changed_causal_contract(tmp_path: Path) -> None:
    payload = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
    payload["truth_contract"]["aux_join_direction"] = "forward"
    path = tmp_path / "future_join.yaml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    report = validate_hypothesis_admission(path)

    assert report.status == "FAIL"
    assert any(issue.check == "truth_contract.aux_join_direction" for issue in report.issues)


def test_contract_logging_always_includes_canonical_truth_fields() -> None:
    payload = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
    payload["logging"] = {"required_fields": ["custom_family_field"]}

    contract = HypothesisContract.from_dict(payload)

    assert set(REQUIRED_LOG_FIELDS).issubset(contract.logging_fields())
    assert "custom_family_field" in contract.logging_fields()
