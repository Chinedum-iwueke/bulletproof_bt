from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml

from bt.core.types import Bar
from bt.hypotheses.contract import HypothesisContract
from bt.experiments.hypothesis_runner import build_runtime_override
from bt.strategy.bybit_cross_sectional_liquidity_dispersion_reversal import (
    INSTRUMENTS, OUTPUT_FIELDS,
    BybitCrossSectionalLiquidityDispersionReversalStrategy,
    cross_sectional_reversal_evaluation,
    verify_contiguous_overlap,
)
from bt.validation.strategy_admission import validate_hypothesis_admission

ROOT = Path(__file__).parents[2]
YAML_PATH = ROOT / "research/hypotheses/alpha_003_bybit_cross_sectional_liquidity_dispersion_reversal.yaml"


def _frame(bars: int = 340) -> pd.DataFrame:
    rows = []
    start = pd.Timestamp("2025-01-01", tz="UTC")
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
            rows.append({"ts": start + pd.Timedelta(minutes=minute), "symbol": symbol,
                         "open": prices[symbol], "high": prices[symbol], "low": prices[symbol],
                         "close": prices[symbol], "volume": 10.0,
                         "quote_volume": 250_000.0 if bucket % 31 == 0 else 2_000_000.0})
    return pd.DataFrame(rows)


def _params(direction: str = "symmetric_reversal") -> dict:
    return {"dispersion_percentile": .95, "liquidity_stress_percentile": .8,
            "direction_specification": direction, "quote_volume_rank_window": 288}


def test_contract_is_exact_bounded_deterministic_and_admitted() -> None:
    raw = yaml.safe_load(YAML_PATH.read_text())
    assert raw["immutable_contract"]["question_digest"] == "b0ea787bddb0e593439e84fbe9c533eb69ca2bbe99e0448986bd26c39fb0e690"
    assert raw["immutable_contract"]["instruments"] == list(INSTRUMENTS)
    assert raw["evaluation"]["outcome_retention"] == ["positive", "negative", "invalid", "failed"]
    contract = HypothesisContract.from_yaml(YAML_PATH)
    assert contract.materialize_grid() == contract.materialize_grid()
    assert len(contract.materialize_grid()) == 8
    runtime = build_runtime_override(contract, contract.to_run_specs()[0], "Tier2")
    assert runtime["strategy"]["name"] == "bybit_cross_sectional_liquidity_dispersion_reversal"
    assert runtime["strategy"].get("use_compiled_features") is not True
    report = validate_hypothesis_admission(YAML_PATH)
    assert report.status == "PASS", report.to_dict()


def test_overlap_gate_rejects_short_and_gapped_common_history() -> None:
    frame = _frame(3)
    assert verify_contiguous_overlap(frame, minimum_days=1)[1] == "less_than_365_days_overlapping_1m_data"
    full = _frame(288)
    assert verify_contiguous_overlap(full, minimum_days=1) == (True, "admitted")
    target = full["ts"].sort_values().unique()[30]
    missing = full.drop(full[(full.symbol == "ETHUSDT") & (full.ts == target)].index)
    assert verify_contiguous_overlap(missing, minimum_days=1)[0] is False


def test_evaluator_bounds_targets_and_implements_distinct_direction_grid() -> None:
    frame = _frame()
    start = "2025-01-02T00:00:00Z"
    end = "2025-01-02T03:00:00Z"
    symmetric = cross_sectional_reversal_evaluation(frame, params=_params(), start=start, end=end, enforce_overlap=False)
    winner = cross_sectional_reversal_evaluation(frame, params=_params("winner_only"), start=start, end=end, enforce_overlap=False)
    for record in symmetric["decision_records"]:
        if "target_ts" in record:
            assert pd.Timestamp(record["target_ts"]) <= pd.Timestamp(end)
    assert symmetric["parameters"]["direction_specification"] == "symmetric_reversal"
    assert winner["parameters"]["direction_specification"] == "winner_only"
    assert [x.get("gross_target_return") for x in symmetric["decision_records"]] != [x.get("gross_target_return") for x in winner["decision_records"]]
    assert symmetric["outcome"] in {"positive", "negative", "invalid"}


def test_evaluator_retains_invalid_and_failed_outcomes() -> None:
    invalid = cross_sectional_reversal_evaluation(_frame(2), params=_params(), enforce_overlap=False)
    assert invalid["outcome"] == "invalid"
    broken = _frame()
    broken.loc[broken.index[-1], "close"] = float("inf")
    failed = cross_sectional_reversal_evaluation(broken, params=_params(), enforce_overlap=False)
    assert failed["outcome"] == "failed"
    assert failed["passed"] is False


def _bar(ts: pd.Timestamp, symbol: str, digest: str, decision_ts: pd.Timestamp) -> Bar:
    extra = {field: 1.0 for field in OUTPUT_FIELDS}
    extra.update({"representation_plan_digest": digest,
                  "representation_output_fields": json.dumps(list(OUTPUT_FIELDS), separators=(",", ":")),
                  "representation_decision_ts": decision_ts.isoformat()})
    return Bar(ts, symbol, 1, 1, 1, 1, 1, extra)


def test_strategy_validates_every_members_ordered_fields_digest_and_causal_timestamp() -> None:
    ts = pd.Timestamp("2025-01-01T00:05:00Z")
    digest = "f" * 64
    strategy = BybitCrossSectionalLiquidityDispersionReversalStrategy(adaptive_representation_plan_digest=digest)
    bars = {symbol: _bar(ts, symbol, digest, ts) for symbol in INSTRUMENTS}
    assert strategy.on_bars(ts, bars, set(INSTRUMENTS), {}) == []
    assert strategy.records[-1].outcome == "consumed"
    bars["SOLUSDT"] = _bar(ts, "SOLUSDT", digest, ts + pd.Timedelta(minutes=5))
    assert strategy.on_bars(ts, bars, set(INSTRUMENTS), {}) == []
    assert strategy.records[-1].outcome == "invalid"
    assert strategy.records[-1].reason == "representation_decision_timestamp_mismatch"


def test_runner_binds_both_heldout_boundaries_and_overlap_admission() -> None:
    source = (ROOT / "scripts/run_alpha_research_assignment.py").read_text()
    assert "end=rep.split.test_end" in source
    assert "verify_contiguous_overlap(combined)" in source
    assert "cross_sectional_reversal_evaluation" in source
