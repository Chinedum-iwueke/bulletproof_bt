from datetime import UTC, datetime
import json

import pytest

from bt.governance.alpha_strategy_pipeline import (
    confirm_card,
    canonical_hash,
    draft_research_card,
    draft_weekend_momentum_card,
    qualify_card,
)


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
    with pytest.raises(ValueError, match="bounded_strategy_engineering"):
        draft_research_card(value, repository_root=str(tmp_path))
