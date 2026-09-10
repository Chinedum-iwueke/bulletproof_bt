from datetime import UTC, datetime

from bt.governance.alpha_strategy_pipeline import (
    confirm_card,
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
