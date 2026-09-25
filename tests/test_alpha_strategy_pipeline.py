from datetime import UTC, datetime
from copy import deepcopy
import json
from pathlib import Path

import pytest
import yaml

from bt.governance.alpha_strategy_pipeline import (
    confirm_card,
    canonical_hash,
    draft_research_card,
    governed_review_verified,
    draft_weekend_momentum_card,
    qualify_card,
)
from bt.institutional.strategy_catalog import build_strategy_capability_catalog


def assignment() -> dict:
    return {
        "question": "Does the BTC weekend liquidity regime alter the net predictive value of short-horizon momentum?",
        "question_digest": "a" * 64,
        "campaign_id": "11111111-1111-4111-8111-111111111111",
        "dataset_build_id": "22222222-2222-4222-8222-222222222222",
        "dataset_digest": "b" * 64,
        "instrument": "BTCUSDT",
        "timeframe": "1m",
        "venue": "bybit",
        "window_start": "2025-01-01T00:00:00Z",
        "window_end": "2026-01-01T00:00:00Z",
        "research_context": {"citations": []},
    }


def test_weekend_question_compiles_to_approved_portable_graph() -> None:
    draft = draft_weekend_momentum_card(assignment())
    assert draft["status"] == "draft"
    confirmed = confirm_card(
        draft,
        actor="founder-operator",
        confirmed_at=datetime.now(UTC).isoformat(),
    )
    result = qualify_card(confirmed, repository_root=".")
    assert result["qualified"] is True
    assert result["artifact_bundle"]["compile_readiness"]["status"] == "registry_ready"
    assert (
        result["artifact_bundle"]["run_config"]["strategy"]["name"]
        == "alpha_weekend_momentum"
    )
    assert result["variant_count"] == 8
    assert result["qualification_scope"] == "deterministic_compilation_only"
    assert result["review"]["independent_of_drafter"] is False
    assert result["review"]["gates"]["independent_review_complete"] is False


def test_unknown_question_is_not_mapped_to_weekend_strategy() -> None:
    value = assignment()
    value["question"] = "Does funding predict liquidation cascades?"
    try:
        draft_weekend_momentum_card(value)
    except ValueError as exc:
        assert str(exc) == "question_requires_bounded_strategy_engineering"
    else:
        raise AssertionError("unsupported question was silently mapped")


def engineered_fixture(tmp_path):
    value = assignment()
    card = draft_weekend_momentum_card(value)
    value["question"] = "Does ETH momentum predict its next tradable return?"
    value["instrument"] = "ETHUSDT"
    value["question_digest"] = canonical_hash({"question": value["question"]})
    card["research_question"] = value["question"]
    card["dataset_binding"]["instrument"] = "ETHUSDT"
    directory = tmp_path / "research" / "hypotheses" / "cards"
    directory.mkdir(parents=True)
    path = directory / f"{value['question_digest']}.json"
    path.write_text(json.dumps(card))
    return value, card, path


def test_exact_engineered_card_discovery(tmp_path):
    value, card, _ = engineered_fixture(tmp_path)
    assert draft_research_card(value, repository_root=str(tmp_path)) == card


def test_engineered_card_binds_exact_adaptive_representation(tmp_path):
    value, card, path = engineered_fixture(tmp_path)
    plan = {
        "schema_version": "adaptive-representation-plan-v1.0.0",
        "candidate_key": "eth-momentum",
        "instruments": ["ETHUSDT"],
        "basket_members": [{
            "instrument": "ETHUSDT",
            "role": "primary",
            "legacy_groups": ["stable"],
            "selection_rationale": "ETH is the exact target and predictor instrument.",
        }],
        "source_timeframe": "1m",
        "research_timeframe": "15m",
        "resampling_policy": "left_closed_left_labeled_complete_bars",
        "transformations": [{
            "output_field": "eth_return",
            "operation": "log_return",
            "input_fields": ["ETHUSDT__close"],
            "parameters": {"periods": 1},
            "fit_policy": "stateless",
            "rationale": "Returns remove the nonstationary price-level scale.",
        }],
        "transformation_rationale": "Use completed 15-minute returns for the horizon.",
        "rejected_alternatives": ["Raw price levels retain an avoidable scale trend."],
        "selection_data_boundary": "metadata_predictors_only_no_targets",
        "outcome_data_consulted": False,
    }
    value["representation_plan"] = plan
    with pytest.raises(ValueError, match="adaptive_representation_mismatch"):
        draft_research_card(value, repository_root=str(tmp_path))

    fields = ["eth_return"]
    card["execution_semantics"].update({
        "adaptive_representation_plan_digest": canonical_hash(plan),
        "adaptive_representation_fields": fields,
        "required_extra_columns": fields,
    })
    path.write_text(json.dumps(card))
    assert draft_research_card(value, repository_root=str(tmp_path)) == card


@pytest.mark.parametrize("mutation,reason", [
    ("question", "question_mismatch"),
    ("dataset", "dataset_mismatch"),
    ("window", "window_mismatch"),
    ("approved", "cannot_self_approve"),
    ("budget", "parameter_budget_exceeded"),
])
def test_engineered_card_bindings_fail_closed(tmp_path, mutation, reason):
    value, card, path = engineered_fixture(tmp_path)
    if mutation == "question":
        card["research_question"] = "An unrelated question"
    elif mutation == "dataset":
        card["dataset_binding"]["dataset_digest"] = "c" * 64
    elif mutation == "window":
        card["execution_window"]["end"] = "2027-01-01T00:00:00Z"
    elif mutation == "approved":
        card["confirmed_by"] = "self"
    else:
        value["max_variants"] = 4
    path.write_text(json.dumps(card))
    with pytest.raises(ValueError, match=reason):
        draft_research_card(value, repository_root=str(tmp_path))


def test_engineered_card_symlink_rejected(tmp_path):
    value, _, path = engineered_fixture(tmp_path)
    target = path.with_suffix(".source")
    path.rename(target)
    path.symlink_to(target)
    with pytest.raises(OSError):
        draft_research_card(value, repository_root=str(tmp_path))


def test_non_btc_question_never_uses_btc_fallback(tmp_path):
    value = assignment()
    value["question"] = "Does ETH weekend momentum predict returns?"
    value["instrument"] = "ETHUSDT"
    value["question_digest"] = canonical_hash({"question": value["question"]})
    with pytest.raises(ValueError, match="exact_engineered_strategy_card_missing"):
        draft_research_card(value, repository_root=str(tmp_path))


def test_weekend_question_also_requires_exact_reviewed_card(tmp_path):
    value = assignment()
    value["question_digest"] = canonical_hash({"question": value["question"]})
    with pytest.raises(ValueError, match="exact_engineered_strategy_card_missing"):
        draft_research_card(value, repository_root=str(tmp_path))


def test_frozen_native_capability_drafts_exact_registered_contract_card():
    root = Path(__file__).parents[1]
    value = assignment()
    value["question_digest"] = canonical_hash({"question": value["question"]})
    value["research_timeframe"] = "1m"
    value["max_variants"] = 8
    catalog = build_strategy_capability_catalog(root, source_commit="a" * 40)
    value["reusable_strategy"] = next(
        item
        for item in catalog["capabilities"]
        if item["hypothesis_id"] == "ALPHA-WEEKEND-MOMENTUM"
    )
    draft = draft_research_card(value, repository_root=str(root))
    assert draft["representation_mode"] == "registered_engine_contract"
    assert draft["engine_hypothesis_template_digest"] == value[
        "reusable_strategy"
    ]["contract_digest"]
    assert draft["features"] == []
    confirmed = confirm_card(
        draft,
        actor="founder-operator",
        confirmed_at=datetime.now(UTC).isoformat(),
    )
    result = qualify_card(confirmed, repository_root=str(root))
    assert result["qualified"] is True
    assert result["variant_count"] == 8
    assert result["artifact_bundle"]["compile_readiness"]["status"] == "registry_ready"


def test_single_instrument_registered_strategy_rejects_basket_assignment():
    root = Path(__file__).parents[1]
    value = assignment()
    value["question_digest"] = canonical_hash({"question": value["question"]})
    value["research_timeframe"] = "1m"
    value["max_variants"] = 8
    value["instruments"] = ["BTCUSDT", "ETHUSDT"]
    catalog = build_strategy_capability_catalog(root, source_commit="a" * 40)
    value["reusable_strategy"] = next(
        item
        for item in catalog["capabilities"]
        if item["hypothesis_id"] == "ALPHA-WEEKEND-MOMENTUM"
    )
    with pytest.raises(ValueError, match="input_cardinality_mismatch"):
        draft_research_card(value, repository_root=str(root))


def test_registered_basket_requires_exact_instruments_and_representation():
    root = Path(__file__).parents[1]
    contract_path = (
        root
        / "research/hypotheses/alpha_003_eth_liquidity_displacement_btc_residual_60m.yaml"
    )
    payload = yaml.safe_load(contract_path.read_text(encoding="utf-8"))
    question = payload["immutable_contract"]["question"]
    value = assignment()
    value.update({
        "question": question,
        "question_digest": canonical_hash({"question": question}),
        "instrument": "BTCUSDT",
        "instruments": ["BTCUSDT", "ETHUSDT"],
        "research_timeframe": "15m",
        "max_variants": 8,
        "representation_plan": payload["representation_plan"],
    })
    catalog = build_strategy_capability_catalog(root, source_commit="a" * 40)
    value["reusable_strategy"] = next(
        item
        for item in catalog["capabilities"]
        if item["hypothesis_id"]
        == "ALPHA-003-ETH-LIQUIDITY-DISPLACEMENT-BTC-RESIDUAL-60M"
    )

    draft = draft_research_card(value, repository_root=str(root))
    assert draft["engine_strategy_name"] == (
        "eth_liquidity_displacement_btc_residual_60m"
    )

    changed_basket = deepcopy(value)
    changed_basket["instruments"] = ["BTCUSDT", "SOLUSDT"]
    with pytest.raises(ValueError, match="instrument_contract_mismatch"):
        draft_research_card(changed_basket, repository_root=str(root))

    changed_plan = deepcopy(value)
    changed_plan["representation_plan"]["transformations"][0]["rationale"] += " Changed."
    with pytest.raises(ValueError, match="adaptive_representation_mismatch"):
        draft_research_card(changed_plan, repository_root=str(root))


def review_packet_fixture():
    value = {"campaign_digest": "a" * 64, "question_digest": "b" * 64, "base_ref": "c" * 40}
    qualification = {"card": {"claim": "example"}, "artifact_bundle": {"compiled": "example"}}
    subject = {
        "campaign_digest": value["campaign_digest"], "question_digest": value["question_digest"],
        "source_commit": value["base_ref"], "card_digest": canonical_hash(qualification["card"]),
        "artifact_bundle_digest": canonical_hash(qualification["artifact_bundle"]),
        "qualification_task_id": "reviewed-task",
        "producer_agent_ids": ["10000000-0000-4000-8000-000000000001"],
        "producer_identities": [{
            "agent_id": "10000000-0000-4000-8000-000000000001", "package_digest": "e" * 64,
            "context_group": "producer", "profile_digest": "f" * 64,
            "machine": "vm1", "provider": "deterministic", "model_family": "none", "runtime": "python",
        }],
    }
    subject["qualifier_identity"] = dict(subject["producer_identities"][0])
    assertion = {
        "schema_version": "evaluation-independence-assertion-v1.0.0", "route_digest": "d" * 64,
        "subject_digest": canonical_hash(subject), "assignments": [],
        "producer": dict(subject["producer_identities"][0]),
        "policy": {"required_review_kinds": ["strategy_spec", "causality_leakage"], "max_pairwise_shared_dimensions": 4},
    }
    for index, kind in enumerate(assertion["policy"]["required_review_kinds"]):
        review = {"subject_digest": canonical_hash(subject), "verdict": "approve", "blockers": [], "checks": ["causality"], "rationale": "Independent specification checks completed."}
        assertion["assignments"].append({
            "assignment_digest": str(index + 2) * 64, "review_kind": kind,
            "review_digest": canonical_hash(review), "alpha_strategy_review": review,
            "correlation_report": {},
            "evaluator_identity": {
                "agent_id": f"{index + 2}0000000-0000-4000-8000-000000000001", "package_digest": str(index + 3) * 64,
                "context_group": f"review-{index}", "profile_digest": str(index + 5) * 64,
                "machine": "vm1", "provider": "openai", "model_family": "codex", "runtime": "codex-cli",
            },
        })
    qualification["governed_review"] = {
        "subject": subject, "assertion": assertion, "receipt_digest": canonical_hash(assertion),
        "route_id": "40000000-0000-4000-8000-000000000001", "verdict": "independence_demonstrated",
    }
    return value, qualification


def test_governed_packet_binds_reviewed_card_artifacts_and_execution_scope():
    value, qualification = review_packet_fixture()
    assert governed_review_verified(value, qualification)


def test_same_agent_package_rollover_preserves_explicit_qualifier_identity():
    value, qualification = review_packet_fixture()
    packet = qualification["governed_review"]
    subject = packet["subject"]
    older = dict(subject["qualifier_identity"], package_digest="9" * 64, context_group="older-draft")
    subject["producer_identities"].insert(0, older)
    packet["assertion"]["subject_digest"] = canonical_hash(subject)
    for entry in packet["assertion"]["assignments"]:
        entry["alpha_strategy_review"]["subject_digest"] = canonical_hash(subject)
        entry["review_digest"] = canonical_hash(entry["alpha_strategy_review"])
    packet["receipt_digest"] = canonical_hash(packet["assertion"])
    assert governed_review_verified(value, qualification)


@pytest.mark.parametrize("mutation", ["card", "bundle", "source", "question", "receipt", "kinds"])
def test_governed_packet_changes_fail_closed(mutation):
    value, qualification = review_packet_fixture()
    if mutation == "card":
        qualification["card"]["claim"] = "changed"
    elif mutation == "bundle":
        qualification["artifact_bundle"]["compiled"] = "changed"
    elif mutation == "source":
        value["base_ref"] = "d" * 40
    elif mutation == "question":
        value["question_digest"] = "d" * 64
    elif mutation == "receipt":
        qualification["governed_review"]["receipt_digest"] = "d" * 64
    else:
        qualification["governed_review"]["assertion"]["assignments"] = []
    assert not governed_review_verified(value, qualification)


def test_self_declared_or_missing_review_never_proves_independence():
    assert not governed_review_verified({}, None)
    assert not governed_review_verified({}, {"review": {"gates": {"independent_review_complete": True}}})


def test_minimal_self_hashed_assertion_is_not_a_governed_review():
    value, qualification = review_packet_fixture()
    subject = qualification["governed_review"]["subject"]
    assertion = {"subject_digest": canonical_hash(subject), "assignments": [
        {"review_kind": "strategy_spec"}, {"review_kind": "causality_leakage"},
    ]}
    qualification["governed_review"] = {"subject": subject, "assertion": assertion, "receipt_digest": canonical_hash(assertion)}
    assert not governed_review_verified(value, qualification)


@pytest.mark.parametrize("mutation", ["drafter", "peer_package", "rejected", "changed_review", "no_route"])
def test_rehashed_packets_cannot_bypass_review_content_or_separation(mutation):
    value, qualification = review_packet_fixture()
    packet = qualification["governed_review"]
    first, second = packet["assertion"]["assignments"]
    if mutation == "drafter":
        first["evaluator_identity"]["agent_id"] = packet["subject"]["producer_agent_ids"][0]
    elif mutation == "peer_package":
        second["evaluator_identity"]["package_digest"] = first["evaluator_identity"]["package_digest"]
    elif mutation == "rejected":
        first["alpha_strategy_review"]["verdict"] = "reject"
        first["review_digest"] = canonical_hash(first["alpha_strategy_review"])
    elif mutation == "changed_review":
        first["alpha_strategy_review"]["rationale"] = "Changed without changing its review digest."
    else:
        packet.pop("route_id")
    packet["receipt_digest"] = canonical_hash(packet["assertion"])
    assert not governed_review_verified(value, qualification)
