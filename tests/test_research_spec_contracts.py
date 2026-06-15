from __future__ import annotations

import json
from pathlib import Path

import pytest

from bt.contracts.research_specs import (
    B7_CARD_TYPES,
    CARD_BUNDLE_SCHEMA_VERSION,
    CARD_SCHEMA_VERSION,
    SpecValidationError,
    build_experiment_plan,
    compile_strategy_run_config,
    execute_experiment_contract,
    load_spec,
    validate_experiment_plan,
    validate_hypothesis_spec,
    validate_strategy_spec,
)


ROOT = Path(__file__).resolve().parents[1]


def test_hypothesis_spec_reference_is_valid() -> None:
    spec = load_spec(ROOT / "examples/hypothesis_specs/trend_continuation_reference.json")

    assert validate_hypothesis_spec(spec) == []


@pytest.mark.parametrize(
    "path",
    sorted((ROOT / "examples/strategy_specs").glob("*_reference.json")),
    ids=lambda path: path.name,
)
def test_strategy_spec_templates_are_valid_except_invalid_fixture(path: Path) -> None:
    spec = load_spec(path)
    errors = validate_strategy_spec(spec)
    if path.name.startswith("invalid_"):
        assert "signal_0_contains_lookahead_language" in errors
    else:
        assert errors == []


def test_strategy_spec_compiler_emits_run_config_contract() -> None:
    spec = load_spec(ROOT / "examples/strategy_specs/trend_continuation_reference.json")

    run_config = compile_strategy_run_config(spec)

    assert run_config["schema_version"] == "run_config_from_strategy_spec_v1"
    assert run_config["strategy_spec_id"] == spec["strategy_spec_id"]
    assert run_config["strategy"]["family"] == "trend_continuation"
    assert run_config["strategy"]["params"]["lookback_bars"] == 55
    assert run_config["execution_semantics"]["lookahead_allowed"] is False


def test_strategy_compiler_fails_closed_on_lookahead() -> None:
    spec = load_spec(ROOT / "examples/strategy_specs/invalid_lookahead_reference.json")

    with pytest.raises(SpecValidationError) as exc:
        compile_strategy_run_config(spec)

    assert "signal_0_contains_lookahead_language" in exc.value.errors


def test_schema_files_are_valid_json() -> None:
    for schema in ("hypothesis_spec_v1.schema.json", "strategy_spec_v1.schema.json", "experiment_plan_v1.schema.json"):
        payload = json.loads((ROOT / "schemas" / schema).read_text(encoding="utf-8"))
        assert payload["type"] == "object"


def test_experiment_planner_emits_falsification_matrix() -> None:
    spec = load_spec(ROOT / "examples/strategy_specs/trend_continuation_reference.json")

    plan = build_experiment_plan(spec)

    assert plan["schema_version"] == "experiment_plan_v1"
    types = {item["experiment_type"] for item in plan["items"]}
    assert {"baseline", "cost_sensitivity", "slippage_sensitivity", "parameter_grid", "holdout_split", "benchmark_null"} <= types
    assert validate_experiment_plan(plan) == []


def test_experiment_plan_reference_is_valid() -> None:
    plan = load_spec(ROOT / "examples/experiment_plans/trend_continuation_plan_reference.json")

    assert validate_experiment_plan(plan) == []


def test_experiment_contract_execution_writes_auditable_artifacts(tmp_path: Path) -> None:
    spec = load_spec(ROOT / "examples/strategy_specs/trend_continuation_reference.json")
    plan = build_experiment_plan(spec)

    result = execute_experiment_contract(
        strategy_spec=spec,
        experiment_plan=plan,
        item_id="cost_sensitivity_2x",
        output_dir=tmp_path,
        runtime_limits={"max_minutes": 30},
    )

    assert result["manifest"]["schema_version"] == "experiment_execution_manifest_v1"
    assert result["manifest"]["status"] == "contract_ready"
    assert result["manifest"]["experiment_item_id"] == "cost_sensitivity_2x"
    assert result["run_config"]["cost_model"]["round_trip_bps"] == spec["cost_model"]["round_trip_bps"] * 2
    assert result["cards"]["schema_version"] == CARD_BUNDLE_SCHEMA_VERSION
    assert {card["card_type"] for card in result["cards"]["cards"]} == set(B7_CARD_TYPES)
    assert all(card["schema_version"] == CARD_SCHEMA_VERSION for card in result["cards"]["cards"])
    verdict_card = next(card for card in result["cards"]["cards"] if card["card_type"] == "VerdictCard")
    assert verdict_card["data"]["decision_grade"] is False
    assert (tmp_path / "run_config.json").exists()
    assert (tmp_path / "execution_manifest.json").exists()
    assert (tmp_path / "verdict.json").exists()
    assert (tmp_path / "execution_log.md").exists()
    assert (tmp_path / "cards.json").exists()
    assert (tmp_path / "cards.md").exists()
    assert (tmp_path / "VerdictCard.json").exists()


def test_experiment_contract_execution_fails_closed_for_unknown_item(tmp_path: Path) -> None:
    spec = load_spec(ROOT / "examples/strategy_specs/trend_continuation_reference.json")
    plan = build_experiment_plan(spec)

    with pytest.raises(SpecValidationError) as exc:
        execute_experiment_contract(
            strategy_spec=spec,
            experiment_plan=plan,
            item_id="not-a-real-item",
            output_dir=tmp_path,
        )

    assert "experiment_item_not_found_not-a-real-item" in exc.value.errors
