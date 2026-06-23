from bt.contracts.research_copilot import validate_research_copilot_turn


def valid_turn() -> dict[str, object]:
    return {
        "schema_version": "research_copilot_turn_v1",
        "mode": "exploratory",
        "response": "A falsifiable interpretation is available for review.",
        "next_question": "Which observable event should define entry?",
        "candidates": [{
            "schema_version": "candidate_hypothesis_v1",
            "claim": "BTC continuation follows forced-flow displacement.",
            "mechanism": "Liquidation cascades amplify the initial shock.",
            "observable_proxy": "Closed displacement bar after liquidation notional spike.",
            "expected_direction": "Continuation in displacement direction.",
            "horizon": "One to twelve 15-minute bars.",
            "entry_idea": "Enter after closed-bar confirmation.",
            "exit_idea": "Compare invalidation and time exits.",
            "required_datasets": ["ohlcv", "liquidations"],
            "falsification_test": "No net effect after costs and holdout.",
            "failure_modes": ["Volatility proxy masquerades as forced flow."],
            "implementation_readiness": "needs_capability_check",
            "rationale": "The mechanism is explicit but not yet engine-approved.",
            "source_citations": [],
        }],
        "research_state": {
            "entry": {"value": "Closed-bar trigger", "provenance": "recommended", "confidence": 0.5}
        },
        "warnings": [],
    }


def test_research_copilot_turn_contract_accepts_provisional_candidate() -> None:
    assert validate_research_copilot_turn(valid_turn()) == []


def test_research_copilot_turn_contract_rejects_silent_confirmation_and_bad_confidence() -> None:
    turn = valid_turn()
    turn["research_state"] = {"entry": {"value": "Trigger", "provenance": "invented", "confidence": 2}}
    errors = validate_research_copilot_turn(turn)
    assert "research_state_entry_provenance_invalid" in errors
    assert "research_state_entry_confidence_invalid" in errors
