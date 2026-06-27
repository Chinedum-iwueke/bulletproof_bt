import json
from pathlib import Path

import pandas as pd
from jsonschema import Draft7Validator

from bt.contracts.research_specs_v2 import (
    ENGINE_TRUTH_BLOCK,
    ResearchSpecV2Error,
    build_artifact_bundle,
    canonical_json,
    compile_readiness,
    upgrade_hypothesis_spec_v1,
    validate_hypothesis_card,
)
from bt.core.types import Bar
from bt.hypotheses.contract import HypothesisContract
from bt.strategy import make_strategy


def fixture_card() -> dict:
    return json.loads(Path("tests/fixtures/contracts/csi_hypothesis_card_v1.json").read_text())


def portable_card() -> dict:
    card = fixture_card()
    card["card_id"] = "card-portable"
    card.pop("engine_strategy_name", None)
    card.pop("engine_hypothesis_template", None)
    card["features"] = [
        {"id": "atr_3", "source": "ohlcv", "transform": "atr", "window": 3, "lag": 1},
        {"id": "range_atr", "source": "derived", "transform": "true_range_over", "inputs": ["atr_3"], "lag": 0},
    ]
    card["gates"] = [{"left": "range_atr", "op": ">=", "right_param": "threshold"}]
    card["entry"] = {"direction": "bar_direction", "timing": "bar_close_submit_next_bar_execution", "pyramiding": False}
    card["exit"] = {"type": "fixed_stop_time_exit", "stop_param": "stop_atr_multiple", "max_hold_param": "max_hold_bars"}
    card["parameters"] = {"threshold": [0.5], "stop_atr_multiple": [2.0], "max_hold_bars": [3]}
    card["data_requirements"] = ["ohlcv"]
    return card


def test_c1_5_golden_card_emits_admissible_byte_stable_bundle() -> None:
    card = fixture_card()
    assert validate_hypothesis_card(card) == []
    first = build_artifact_bundle(card, repo_root=".")
    second = build_artifact_bundle(card, repo_root=".")
    assert canonical_json(first) == canonical_json(second)
    assert first["compile_readiness"]["status"] == "registry_ready"
    assert first["engine_hypothesis_yaml"]["truth_contract"] == ENGINE_TRUTH_BLOCK
    assert HypothesisContract.from_dict(first["engine_hypothesis_yaml"]).schema.metadata.hypothesis_id == "L7-H1"
    for schema_name, payload_key in (
        ("hypothesis_card_v1", "card"),
        ("hypothesis_spec_v2", "hypothesis_spec"),
        ("engine_hypothesis_yaml_v1", "engine_hypothesis_yaml"),
        ("strategy_spec_v2", "strategy_spec"),
        ("compile_readiness_report_v1", "compile_readiness"),
    ):
        schema = json.loads(Path(f"schemas/{schema_name}.schema.json").read_text())
        Draft7Validator(schema).validate(first[payload_key])


def test_c1_5_portable_graph_is_executable_and_missing_bars_do_not_advance() -> None:
    bundle = build_artifact_bundle(portable_card(), available_datasets={"ohlcv"})
    assert bundle["compile_readiness"]["status"] == "graph_compilable"
    config = bundle["run_config"]["strategy"]
    strategy = make_strategy(config["name"], research_graph=config["research_graph"], parameters=config["parameters"])
    t0 = pd.Timestamp("2026-01-01T00:00:00Z")
    assert strategy.on_bars(t0, {}, {"BTCUSDT"}, {}) == []
    signals = []
    for index in range(5):
        ts = t0 + pd.Timedelta(minutes=index)
        bar = Bar(ts=ts, symbol="BTCUSDT", open=100 + index, high=103 + index, low=99 + index, close=102 + index, volume=10)
        signals.extend(strategy.on_bars(ts, {"BTCUSDT": bar}, {"BTCUSDT"}, {}))
    assert signals
    assert signals[-1].metadata["compiler_version"] == "research_graph_compiler_v1"


def test_c1_5_portable_graph_applies_feature_lag_before_decision() -> None:
    card = portable_card()
    card["features"] = [{"id": "ret", "source": "ohlcv", "source_field": "close", "transform": "return", "lag": 1}]
    card["gates"] = [{"left": "ret", "op": ">=", "right_param": "threshold"}]
    card["parameters"]["threshold"] = [0.01]
    bundle = build_artifact_bundle(card, available_datasets={"ohlcv"})
    config = bundle["run_config"]["strategy"]
    strategy = make_strategy(config["name"], research_graph=config["research_graph"], parameters=config["parameters"])
    t0 = pd.Timestamp("2026-01-01T00:00:00Z")
    bars = [
        Bar(ts=t0, symbol="BTCUSDT", open=100, high=101, low=99, close=100, volume=1),
        Bar(ts=t0 + pd.Timedelta(minutes=1), symbol="BTCUSDT", open=100, high=111, low=99, close=110, volume=1),
        Bar(ts=t0 + pd.Timedelta(minutes=2), symbol="BTCUSDT", open=110, high=111, low=109, close=110, volume=1),
    ]
    assert strategy.on_bars(bars[0].ts, {"BTCUSDT": bars[0]}, {"BTCUSDT"}, {}) == []
    assert strategy.on_bars(bars[1].ts, {"BTCUSDT": bars[1]}, {"BTCUSDT"}, {}) == []
    assert strategy.on_bars(bars[2].ts, {"BTCUSDT": bars[2]}, {"BTCUSDT"}, {})


def test_c1_5_unsupported_state_machine_becomes_implementation_task() -> None:
    card = portable_card()
    card["exit"] = {"type": "state_machine"}
    bundle = build_artifact_bundle(card, available_datasets={"ohlcv"})
    assert bundle["compile_readiness"]["status"] == "implementation_required"
    assert bundle["implementation_task"]["approval_required"] is True
    assert "run_config" not in bundle


def test_c1_5_truth_and_data_blockers_are_distinct() -> None:
    bundle = build_artifact_bundle(portable_card(), available_datasets=set())
    assert bundle["compile_readiness"]["status"] == "data_blocked"
    spec = bundle["strategy_spec"]
    spec["execution_semantics"]["interpolation"] = "allowed"
    assert compile_readiness(spec, available_datasets={"ohlcv"})["status"] == "semantics_blocked"


def test_c1_5_rejects_future_features_and_template_mismatch() -> None:
    card = fixture_card()
    card["features"][0]["lag"] = -1
    assert "card_feature_0_future_lag_forbidden" in validate_hypothesis_card(card)
    card = fixture_card()
    card["parameters"]["d0"] = [9.9]
    try:
        build_artifact_bundle(card, repo_root=".")
    except ResearchSpecV2Error as error:
        assert "engine_hypothesis_template_parameter_mismatch_d0" in error.errors
    else:
        raise AssertionError("template mismatch was accepted")


def test_c1_5_v1_reader_marks_legacy_spec_for_confirmation() -> None:
    upgraded = upgrade_hypothesis_spec_v1({"schema_version": "hypothesis_spec_v1", "hypothesis_id": "legacy", "program_id": "p", "title": "Legacy", "claim": "Claim"})
    assert upgraded["schema_version"] == "hypothesis_spec_v2"
    assert upgraded["compatibility"]["execution_status"] == "requires_card_confirmation"
