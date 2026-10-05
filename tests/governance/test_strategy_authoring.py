from __future__ import annotations

import json
from pathlib import Path

import pytest

from bt.governance.strategy_authoring import (
    StrategyIntentError,
    compile_strategy_intent,
    scaffold_strategy_package,
)
from bt.contracts.research_specs_v2 import validate_hypothesis_card


DIGEST = "a" * 64


def evidence() -> dict:
    question = (
        "Does completed SOLUSDT 2h return predict ETHUSDT return over the next 4h?"
    )
    from bt.governance.strategy_authoring import digest

    instruments = ["ETHUSDT", "SOLUSDT"]
    plan = {
        "schema_version": "adaptive-representation-plan-v1.0.0",
        "candidate_key": "sol-eth-diffusion",
        "instruments": instruments,
        "source_timeframe": "1m",
        "research_timeframe": "2h",
        "resampling_policy": "left_closed_left_labeled_complete_bars",
        "transformations": [
            {
                "output_field": "sol_return_2h",
                "operation": "log_return",
                "input_fields": ["SOLUSDT__close"],
                "parameters": {"periods": 1},
                "fit_policy": "stateless",
            }
        ],
        "selection_data_boundary": "metadata_predictors_only_no_targets",
        "outcome_data_consulted": False,
    }
    candidate = {
        "candidate_key": "sol-eth-diffusion",
        "question": question,
        "predictor": "completed SOLUSDT 2h log return",
        "target": "ETHUSDT close-to-close log return",
        "horizon": "next 4 hours",
        "causal_timing": "Predictor closes before the next-bar decision timestamp.",
        "null_hypothesis": "The conditional future ETHUSDT return is zero after costs.",
        "predicted_direction": "positive",
        "mechanism": "Information propagates from SOLUSDT to ETHUSDT with a delay.",
        "rival_explanations": ["Common market beta"],
        "falsification_criteria": ["Held-out net effect is not positive"],
        "parameter_budget": {
            "maximum_parameters": 2,
            "maximum_variants": 4,
            "parameter_names": ["tail_percentile", "minimum_support"],
        },
        "data": {
            "venue": "bybit",
            "instrument": "ETHUSDT",
            "instruments": instruments,
            "timeframe": "1m",
            "research_timeframe": "2h",
            "resampling_policy": "left_closed_left_labeled_complete_bars",
        },
        "equations": [],
    }
    bindings = [
        {
            "instruments": [instrument],
            "partition_digests": [DIGEST],
            **{
                field: DIGEST
                for field in (
                    "dataset_build_id",
                    "dataset_digest",
                    "catalog_digest",
                    "manifest_digest",
                    "producer_receipt_digest",
                    "lake_governance_digest",
                )
            },
        }
        for instrument in instruments
    ]
    return {
        "question": question,
        "question_digest": digest({"question": question}),
        "discovery_candidate": candidate,
        "representation_plan": plan,
        "dataset_bindings": bindings,
        "window": {"start": "2025-01-01T00:00:00Z", "end": "2026-01-01T00:00:00Z"},
        "maximum_variants": 8,
        "authority": {
            "capital": False,
            "orders": False,
            "production_promotion": False,
            "self_approval": False,
        },
    }


def test_intent_compiles_only_complete_research_handoff() -> None:
    intent = compile_strategy_intent(evidence())
    document = intent.document()
    assert document["strategy_name"] == "alpha_003_sol_eth_diffusion"
    assert document["instruments"] == ["ETHUSDT", "SOLUSDT"]
    assert document["maximum_variants"] == 4
    assert len(document["intent_digest"]) == 64
    assert document["authority"]["capital"] is False


def test_intent_rejects_unverified_equation() -> None:
    payload = evidence()
    payload["discovery_candidate"]["equations"] = [{"verification": "source_replayed"}]
    with pytest.raises(StrategyIntentError, match="independent assurance"):
        compile_strategy_intent(payload)


def test_intent_rejects_incomplete_dataset_identity() -> None:
    payload = evidence()
    del payload["dataset_bindings"][0]["manifest_digest"]
    with pytest.raises(StrategyIntentError, match="identity is incomplete"):
        compile_strategy_intent(payload)


def test_scaffold_is_deterministic_and_idempotent(tmp_path: Path) -> None:
    root = tmp_path
    intent = compile_strategy_intent(evidence())
    first = scaffold_strategy_package(root, intent)
    second = scaffold_strategy_package(root, intent)
    assert set(first["paths"].values()) == {"created"}
    assert set(second["paths"].values()) == {"reused"}
    intent_path = root / "research/hypotheses/intents/alpha_003_sol_eth_diffusion.json"
    assert (
        json.loads(intent_path.read_text())["intent_digest"] == first["intent_digest"]
    )
    assert (
        "__CODEX_REQUIRED__"
        in (root / "src/bt/strategy/alpha_003_sol_eth_diffusion.py").read_text()
    )
    generated_test = (
        root / "tests/hypotheses/test_alpha_003_sol_eth_diffusion_scaffold.py"
    )
    assert "__CODEX_REQUIRED__" not in generated_test.read_text()
    card_path = root / f"research/hypotheses/cards/{intent.question_digest}.json"
    card = json.loads(card_path.read_text())
    card["gates"][0]["op"] = ">="
    card["entry"]["direction"] = "conditional"
    card["exit"]["policy"] = "fixed_horizon"
    card["parameters"] = {"tail_percentile": [0.95], "minimum_support": [30]}
    card["evaluation"]["metrics"] = ["mean_net_return"]
    assert validate_hypothesis_card(card, require_confirmed=False) == []


def test_scaffold_refuses_unbound_existing_file(tmp_path: Path) -> None:
    intent = compile_strategy_intent(evidence())
    path = tmp_path / "src/bt/strategy/alpha_003_sol_eth_diffusion.py"
    path.parent.mkdir(parents=True)
    path.write_text("unrelated\n")
    with pytest.raises(StrategyIntentError, match="not bound to intent"):
        scaffold_strategy_package(tmp_path, intent)
