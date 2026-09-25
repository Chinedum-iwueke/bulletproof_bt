from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest
import yaml

from bt.core.types import Bar
from bt.evaluation.alpha_research import required_observation_logging_evaluation
from bt.hypotheses.contract import HypothesisContract
from bt.experiments.hypothesis_runner import build_runtime_override
from bt.governance.alpha_strategy_pipeline import (
    confirm_card,
    draft_research_card,
    qualify_card,
)
from bt.strategy.bybit_cross_sectional_liquidity_dispersion_reversal import (
    INSTRUMENTS,
    OUTPUT_FIELDS,
    BybitCrossSectionalLiquidityDispersionReversalStrategy,
    cross_sectional_reversal_evaluation,
    verify_contiguous_overlap,
)
from bt.validation.strategy_admission import validate_hypothesis_admission
import scripts.run_alpha_research_assignment as assignment_runner

ROOT = Path(__file__).parents[2]
YAML_PATH = (
    ROOT
    / "research/hypotheses/alpha_003_bybit_cross_sectional_liquidity_dispersion_reversal.yaml"
)


def _frame(bars: int = 340, *, start: str = "2025-01-01T00:00:00Z") -> pd.DataFrame:
    rows = []
    start_ts = pd.Timestamp(start)
    prices = {symbol: 100.0 for symbol in INSTRUMENTS}
    for minute in range(bars * 5):
        bucket = minute // 5
        for index, symbol in enumerate(INSTRUMENTS):
            if minute % 5 == 4:
                shock = (0.0002, -0.0001, 0.0)[index]
                if bucket % 31 == 0:
                    shock = (0.04, -0.03, 0.002)[index]
                if bucket % 31 in range(1, 7):
                    shock = (-0.009, 0.007, 0.0)[index]
                prices[symbol] *= 1 + shock
            rows.append(
                {
                    "ts": start_ts + pd.Timedelta(minutes=minute),
                    "symbol": symbol,
                    "open": prices[symbol],
                    "high": prices[symbol],
                    "low": prices[symbol],
                    "close": prices[symbol],
                    "volume": 10.0,
                    "quote_volume": 250_000.0 if bucket % 31 == 0 else 2_000_000.0,
                }
            )
    return pd.DataFrame(rows)


def _params(direction: str = "symmetric_reversal") -> dict:
    return {
        "dispersion_percentile": 0.95,
        "liquidity_stress_percentile": 0.8,
        "direction_specification": direction,
        "quote_volume_rank_window": 288,
    }


def _representation_plan() -> dict:
    transformations = []
    for symbol in INSTRUMENTS:
        lower = symbol.lower()
        transformations.extend(
            [
                {
                    "output_field": f"{lower}_log_return_5m",
                    "operation": "log_return",
                    "input_fields": [f"{symbol}__close"],
                    "parameters": {"periods": 1},
                    "fit_policy": "stateless",
                    "rationale": "Causal completed-bar return for basket dispersion.",
                },
                {
                    "output_field": f"{lower}_quote_volume_5m",
                    "operation": "identity",
                    "input_fields": [f"{symbol}__quote_volume"],
                    "parameters": {},
                    "fit_policy": "stateless",
                    "rationale": "Completed quote volume for prior-only stress ranks.",
                },
                {
                    "output_field": f"{lower}_volume_5m",
                    "operation": "identity",
                    "input_fields": [f"{symbol}__volume"],
                    "parameters": {},
                    "fit_policy": "stateless",
                    "rationale": "Completed base volume for validity checks.",
                },
            ]
        )
    by_name = {item["output_field"]: item for item in transformations}
    return {
        "schema_version": "adaptive-representation-plan-v1.0.0",
        "candidate_key": "cross-sectional-liquidity-dispersion-reversal",
        "instruments": list(INSTRUMENTS),
        "basket_members": [
            {
                "instrument": symbol,
                "role": "primary" if symbol == "BTCUSDT" else "predictor",
                "legacy_groups": ["volatile" if symbol == "SOLUSDT" else "stable"],
                "selection_rationale": "Frozen liquid basket member required by the causal question.",
            }
            for symbol in INSTRUMENTS
        ],
        "source_timeframe": "1m",
        "research_timeframe": "5m",
        "resampling_policy": "left_closed_left_labeled_complete_bars",
        "transformations": [by_name[field] for field in OUTPUT_FIELDS],
        "transformation_rationale": "Represent synchronized returns and liquidity causally.",
        "rejected_alternatives": [
            "Raw levels do not represent the preregistered mechanism."
        ],
        "selection_data_boundary": "metadata_predictors_only_no_targets",
        "outcome_data_consulted": False,
    }


def test_contract_is_exact_bounded_deterministic_and_admitted() -> None:
    raw = yaml.safe_load(YAML_PATH.read_text())
    card = json.loads(
        (
            ROOT
            / "research/hypotheses/cards/b0ea787bddb0e593439e84fbe9c533eb69ca2bbe99e0448986bd26c39fb0e690.json"
        ).read_text()
    )
    assert (
        card["engine_hypothesis_template_digest"]
        == hashlib.sha256(YAML_PATH.read_bytes()).hexdigest()
    )
    assert (
        raw["immutable_contract"]["question_digest"]
        == "b0ea787bddb0e593439e84fbe9c533eb69ca2bbe99e0448986bd26c39fb0e690"
    )
    assert raw["immutable_contract"]["instruments"] == list(INSTRUMENTS)
    assert raw["evaluation"]["outcome_retention"] == [
        "positive",
        "negative",
        "invalid",
        "failed",
    ]
    contract = HypothesisContract.from_yaml(YAML_PATH)
    assert contract.materialize_grid() == contract.materialize_grid()
    assert len(contract.materialize_grid()) == 8
    runtime = build_runtime_override(contract, contract.to_run_specs()[0], "Tier2")
    assert (
        runtime["strategy"]["name"]
        == "bybit_cross_sectional_liquidity_dispersion_reversal"
    )
    assert runtime["strategy"].get("use_compiled_features") is not True
    report = validate_hypothesis_admission(YAML_PATH)
    assert report.status == "PASS", report.to_dict()


def test_runtime_binds_exact_reviewed_representation_digest() -> None:
    raw = yaml.safe_load(YAML_PATH.read_text())
    expected = "a" * 64
    raw["execution_semantics"]["adaptive_representation_plan_digest"] = expected
    contract = HypothesisContract.from_dict(raw)
    runtime = build_runtime_override(contract, contract.to_run_specs()[0], "Tier2")
    assert runtime["strategy"]["adaptive_representation_plan_digest"] == expected


def test_overlap_gate_rejects_short_and_gapped_common_history() -> None:
    frame = _frame(3)
    assert (
        verify_contiguous_overlap(frame, minimum_days=1)[1]
        == "less_than_365_days_overlapping_1m_data"
    )
    full = _frame(288)
    assert verify_contiguous_overlap(full, minimum_days=1) == (True, "admitted")
    target = full["ts"].sort_values().unique()[30]
    missing = full.drop(full[(full.symbol == "ETHUSDT") & (full.ts == target)].index)
    assert verify_contiguous_overlap(missing, minimum_days=1)[0] is False


def test_evaluator_bounds_targets_and_implements_distinct_direction_grid() -> None:
    frame = _frame()
    start = "2025-01-02T00:00:00Z"
    end = "2025-01-02T03:00:00Z"
    symmetric = cross_sectional_reversal_evaluation(
        frame, params=_params(), start=start, end=end, enforce_overlap=False
    )
    winner = cross_sectional_reversal_evaluation(
        frame,
        params=_params("winner_only"),
        start=start,
        end=end,
        enforce_overlap=False,
    )
    for record in symmetric["decision_records"]:
        if "target_ts" in record:
            assert pd.Timestamp(record["target_ts"]) <= pd.Timestamp(end)
    assert symmetric["parameters"]["direction_specification"] == "symmetric_reversal"
    assert winner["parameters"]["direction_specification"] == "winner_only"
    assert [x.get("gross_target_return") for x in symmetric["decision_records"]] != [
        x.get("gross_target_return") for x in winner["decision_records"]
    ]
    assert symmetric["outcome"] in {"positive", "negative", "invalid"}


def test_evaluator_retains_invalid_and_failed_outcomes() -> None:
    invalid = cross_sectional_reversal_evaluation(
        _frame(2), params=_params(), enforce_overlap=False
    )
    assert invalid["outcome"] == "invalid"
    broken = _frame()
    broken.loc[broken.index[-1], "close"] = float("inf")
    failed = cross_sectional_reversal_evaluation(
        broken, params=_params(), enforce_overlap=False
    )
    assert failed["outcome"] == "failed"
    assert failed["passed"] is False


def test_thresholds_are_prior_only_and_observation_logging_is_complete() -> None:
    short = cross_sectional_reversal_evaluation(
        _frame(340),
        params=_params(),
        enforce_overlap=False,
        representation_plan_digest="a" * 64,
    )
    long = cross_sectional_reversal_evaluation(
        _frame(420),
        params=_params(),
        enforce_overlap=False,
        representation_plan_digest="a" * 64,
    )
    short_records = {item["decision_ts"]: item for item in short["observation_records"]}
    long_records = {item["decision_ts"]: item for item in long["observation_records"]}
    assert short_records
    assert {key: long_records[key]["decision_trace"] for key in short_records} == {
        key: item["decision_trace"] for key, item in short_records.items()
    }
    required = yaml.safe_load(YAML_PATH.read_text())["logging"]["required_fields"]
    report = required_observation_logging_evaluation(short, required)
    assert report["passed"] is True
    assert report["observation_count"] == len(short_records)


def _bar(ts: pd.Timestamp, symbol: str, digest: str, decision_ts: pd.Timestamp) -> Bar:
    extra = {field: 1.0 for field in OUTPUT_FIELDS}
    extra.update(
        {
            "representation_plan_digest": digest,
            "representation_output_fields": json.dumps(
                list(OUTPUT_FIELDS), separators=(",", ":")
            ),
            "representation_decision_ts": decision_ts.isoformat(),
        }
    )
    return Bar(ts, symbol, 1, 1, 1, 1, 1, extra)


def test_strategy_validates_every_members_ordered_fields_digest_and_causal_timestamp() -> (
    None
):
    ts = pd.Timestamp("2025-01-01T00:05:00Z")
    digest = "f" * 64
    strategy = BybitCrossSectionalLiquidityDispersionReversalStrategy(
        adaptive_representation_plan_digest=digest
    )
    bars = {symbol: _bar(ts, symbol, digest, ts) for symbol in INSTRUMENTS}
    assert strategy.on_bars(ts, bars, set(INSTRUMENTS), {}) == []
    assert strategy.records[-1].outcome == "consumed"
    bars["SOLUSDT"] = _bar(ts, "SOLUSDT", digest, ts + pd.Timedelta(minutes=5))
    assert strategy.on_bars(ts, bars, set(INSTRUMENTS), {}) == []
    assert strategy.records[-1].outcome == "invalid"
    assert strategy.records[-1].reason == "representation_decision_timestamp_mismatch"
    unbound = BybitCrossSectionalLiquidityDispersionReversalStrategy()
    assert unbound.on_bars(ts, bars, set(INSTRUMENTS), {}) == []
    assert unbound.records[-1].reason == "representation_plan_digest_missing_or_invalid"


def test_runner_binds_both_heldout_boundaries_and_overlap_admission() -> None:
    source = (ROOT / "scripts/run_alpha_research_assignment.py").read_text()
    assert "end=rep.split.test_end" in source
    assert "verify_contiguous_overlap(combined)" in source
    assert "cross_sectional_reversal_evaluation" in source


def test_execute_registered_materializes_multi_asset_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = yaml.safe_load(YAML_PATH.read_text())
    plan = _representation_plan()
    frame = _frame(120, start=raw["immutable_contract"]["window"]["start"])
    paths = {}
    for symbol in INSTRUMENTS:
        path = tmp_path / f"{symbol}.parquet"
        frame.loc[frame["symbol"] == symbol].to_parquet(path, index=False)
        paths[symbol] = path
    assignment = {
        "base_ref": "a" * 40,
        "campaign_id": "11111111-1111-4111-8111-111111111111",
        "campaign_digest": "b" * 64,
        "source_candidate_id": "22222222-2222-4222-8222-222222222222",
        "source_candidate_digest": "c" * 64,
        "question": raw["immutable_contract"]["question"],
        "question_digest": raw["immutable_contract"]["question_digest"],
        "domain_key": "market-microstructure",
        "dataset_build_id": raw["immutable_contract"]["dataset_bindings"][0][
            "dataset_build_id"
        ],
        "dataset_digest": raw["immutable_contract"]["dataset_bindings"][0][
            "dataset_digest"
        ],
        "dataset_path": str(paths["BTCUSDT"]),
        "dataset_key": "btcusdt",
        "dataset_bindings": [
            binding
            | {
                "dataset_path": str(paths[binding["instrument"]]),
                "dataset_key": binding["instrument"].lower(),
            }
            for binding in raw["immutable_contract"]["dataset_bindings"]
        ],
        "instrument": "BTCUSDT",
        "instruments": list(INSTRUMENTS),
        "venue": "bybit",
        "timeframe": "1m",
        "research_timeframe": "5m",
        "window_start": raw["immutable_contract"]["window"]["start"],
        "window_end": raw["immutable_contract"]["window"]["end"],
        "tier": "Tier2B",
        "max_variants": 8,
        "representation_plan": plan,
        "bundle_root": str(tmp_path / "retained"),
        "memory_database": str(tmp_path / "memory.sqlite3"),
        "research_context": {
            "corpus_digest": "d" * 64,
            "abstained": True,
            "citations": [],
        },
    }
    card = confirm_card(
        draft_research_card(assignment, repository_root=str(ROOT)),
        actor="founder-operator",
        confirmed_at="2026-09-25T00:00:00Z",
    )
    qualification = qualify_card(card, repository_root=str(ROOT))
    qualification["qualified"] = True
    qualification["review"]["gates"]["independent_review_complete"] = True
    qualification["review"]["independent_of_drafter"] = True
    assignment["qualification"] = qualification
    monkeypatch.setattr(assignment_runner, "governed_review_verified", lambda *_: True)
    monkeypatch.setattr(
        assignment_runner, "verify_contiguous_overlap", lambda *_: (True, "test")
    )
    monkeypatch.setattr(
        "bt.strategy.bybit_cross_sectional_liquidity_dispersion_reversal.verify_contiguous_overlap",
        lambda *_: (True, "test"),
    )
    result = assignment_runner.execute_registered(
        assignment, ROOT, tmp_path / "output", max_workers=1
    )
    assert result["disposition"] == "native_execution_complete"
    assert result["alpha_campaign_attempt"]["outcome"] in {
        "positive",
        "negative",
        "invalid",
        "failed",
    }
    bundles = list(
        (tmp_path / "output").glob(
            "run-bundles-*/bundles/*/artifacts/cross_sectional_reversal_evaluation.json"
        )
    )
    assert len(bundles) == 8
    logging = list(
        (tmp_path / "output").glob(
            "run-bundles-*/bundles/*/artifacts/required_trade_logging_evaluation.json"
        )
    )
    assert len(logging) == 8
    assert all(json.loads(path.read_text())["passed"] for path in logging)
