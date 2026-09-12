from datetime import UTC, datetime, timedelta

import pytest

from bt.institutional.candidate_admission import CandidateAdmissionError, candidate_admission_receipt
from bt.institutional.receipt import build_receipt, digest, verify_receipt

NOW = datetime(2026, 9, 12, 20, tzinfo=UTC)
DATASET = digest({"dataset": "risk004"})
CANDIDATE = digest({"candidate": "risk004"})


def producer(milestone, qualifier, *, result=None):
    body = {qualifier: True} if qualifier else {}
    body.update(result or {})
    return build_receipt(milestone=milestone, producer=f"test.{milestone}", producer_version="1.0.0", source_commit="a" * 40, inputs={}, dataset_digest=DATASET, configuration={}, artifacts={}, result=body)


def dependencies(*, shadow=None):
    qualifiers = {"PORT-002": "qualified", "PORT-003": "valid", "PORT-004": "qualified", "RISK-001": "admissible", "RISK-002": "allowed"}
    values = {key: producer(key, qualifier) for key, qualifier in qualifiers.items()}
    values["RISK-003"] = producer("RISK-003", "qualified", result={"effective_risk_fraction": 0.01, "budget_capital": 1000})
    values["SHADOW-002"] = producer("SHADOW-002", None, result=shadow or {"candidate_digest": CANDIDATE, "qualified_for_continued_shadow": True, "status": "monitoring", "recommended_action": "continue_monitoring"})
    return values


def dossier():
    value = {"schema_version": "portfolio-candidate-dossier-v1.0.0", "authority": {"portfolio_allocation": "prohibited", "capital": "prohibited", "orders": "prohibited", "self_promotion": "prohibited"}, "allocated": False, "decision": "candidate", "candidate_digests": [CANDIDATE]}
    value["dossier_digest"] = digest(value)
    return value


def assess(**updates):
    values = {"candidate_digest": CANDIDATE, "port001_dossier": dossier(), "dependency_receipts": dependencies(), "requested_action": "admit", "prior_operations_state": "candidate", "prior_capital_state": "no-authority", "requested_capital": 1000, "requested_risk_fraction": 0.01, "known_at": NOW, "evidence_epoch": NOW - timedelta(minutes=1), "expires_at": NOW + timedelta(days=7), "dataset_digest": DATASET, "source_commit": "b" * 40}
    values.update(updates)
    return candidate_admission_receipt(**values)


def test_admission_is_replayable_and_has_no_authority():
    receipt = assess()
    assert verify_receipt(receipt)
    assert receipt.result["eligible_for_authority_review"] is True
    assert receipt.result["automatic_transition"] is False
    assert not any(receipt.authority.values())


@pytest.mark.parametrize("action,operations,capital", [("allocate", "shadow", "eligible"), ("scale", "live", "allocated")])
def test_positive_capital_paths_are_separate_recommendations(action, operations, capital):
    receipt = assess(requested_action=action, prior_operations_state=operations, prior_capital_state=capital)
    assert receipt.result["recommended_action"] == action


def test_deterioration_supports_demotion_but_not_positive_admission():
    shadow = {"candidate_digest": CANDIDATE, "qualified_for_continued_shadow": False, "status": "demotion_pending", "recommended_action": "demotion_review_required"}
    assert assess(requested_action="demote", prior_operations_state="shadow", dependency_receipts=dependencies(shadow=shadow)).result["eligible_for_authority_review"]
    assert not assess(dependency_receipts=dependencies(shadow=shadow)).result["eligible_for_authority_review"]


def test_limits_fail_closed():
    result = assess(requested_capital=1001, requested_risk_fraction=0.02).result
    assert result["recommended_action"] == "reject"
    assert set(result["failures"]) == {"capital_exceeds_budget", "risk_fraction_exceeds_budget"}


def test_dataset_candidate_and_receipt_tamper_are_rejected():
    bad = dependencies()
    bad["PORT-002"] = {**bad["PORT-002"].as_dict(), "dataset_digest": "f" * 64}
    with pytest.raises(CandidateAdmissionError):
        assess(dependency_receipts=bad)
    with pytest.raises(CandidateAdmissionError, match="binding"):
        assess(dependency_receipts=dependencies(shadow={"candidate_digest": "c" * 64, "qualified_for_continued_shadow": True}))


def test_expired_or_future_evidence_is_rejected():
    with pytest.raises(CandidateAdmissionError):
        assess(expires_at=NOW)
    with pytest.raises(CandidateAdmissionError):
        assess(evidence_epoch=NOW + timedelta(seconds=1))
