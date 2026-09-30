from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

from bt.contracts.research_specs_v2 import canonical_hash
from bt.core.types import Bar
from bt.governance.alpha_strategy_pipeline import draft_research_card
from bt.hypotheses.contract import HypothesisContract
from bt.strategy.xrp_signed_impact_future_volatility_30m import (
    OUTPUT_FIELDS, PLAN_DIGEST, QUESTION,
    XrpSignedImpactFutureVolatility30mStrategy,
    _compile_rows, signed_impact_volatility_evaluation,
    signed_impact_volatility_grid_evaluation,
)


ROOT = Path(__file__).parents[2]
YAML = ROOT / "research/hypotheses/alpha_003_xrp_signed_impact_future_volatility_30m.yaml"


def _contract() -> dict:
    return yaml.safe_load(YAML.read_text(encoding="utf-8"))


def _frame(size: int = 1700) -> pd.DataFrame:
    rng = np.random.default_rng(17)
    returns = rng.normal(0.0, .002, size)
    quote = 2_000_000.0 + np.arange(size) * 10.0
    rv = (
        pd.Series(returns).rolling(12, min_periods=12).std(ddof=0).shift(1)
        * np.sqrt(12)
    )
    timestamps = pd.date_range("2023-01-01", periods=size, freq="30min", tz="UTC")
    return pd.DataFrame({
        "ts": timestamps,
        OUTPUT_FIELDS[0]: quote, OUTPUT_FIELDS[1]: 10.0,
        OUTPUT_FIELDS[2]: returns, OUTPUT_FIELDS[3]: returns / quote,
        OUTPUT_FIELDS[4]: rv,
        "representation_plan_digest": PLAN_DIGEST,
        "representation_output_fields": json.dumps(list(OUTPUT_FIELDS)),
        "representation_decision_ts": timestamps,
    })


def test_exact_card_contract_plan_and_deterministic_compilation() -> None:
    raw = _contract()
    contract = HypothesisContract.from_yaml(YAML)
    assert raw["immutable_contract"]["question"] == QUESTION
    assert len(contract.to_run_specs()) == 4
    assert canonical_hash(raw["representation_plan"]) == PLAN_DIGEST
    assert raw["execution_semantics"]["adaptive_representation_fields"] == list(OUTPUT_FIELDS)
    one = _compile_rows(_frame(), .95, 12)
    two = _compile_rows(_frame(), .95, 12)
    pd.testing.assert_frame_equal(one, two)
    # Current impact is absent from its own strict prior-only threshold.
    assert one.loc[:1439, "shock_category"].isna().all()


def test_signed_tails_are_not_absolute_impact_proxy_and_grid_opens_test_once() -> None:
    frame = _frame(2600)
    frame.loc[1500, OUTPUT_FIELDS[2]] = -.1
    frame.loc[1500, OUTPUT_FIELDS[3]] = -.1 / frame.loc[1500, OUTPUT_FIELDS[0]]
    rows = _compile_rows(frame, .975, 12)
    assert rows.loc[1500, "shock_category"] == "negative"
    grid = signed_impact_volatility_grid_evaluation(
        frame,
        parameter_grid={
            "signed_impact_tail_quantile": [.95, .975],
            "volatility_control_window_bars": [12, 24],
        },
    )
    assert grid["outcome"] in {"positive", "negative", "invalid", "failed"}
    assert grid["test_open_count"] in {0, 1}


def test_invalid_failed_and_negative_outcomes_are_retained_without_mocking() -> None:
    params = {"signed_impact_tail_quantile": .95, "volatility_control_window_bars": 12}
    invalid = signed_impact_volatility_evaluation(
        _frame().assign(ts=lambda x: x.ts.dt.tz_localize(None)), params=params
    )
    assert invalid["outcome"] == "invalid"
    failed = signed_impact_volatility_evaluation(_frame(2600), params=params)
    assert failed["outcome"] == "failed"
    observed = signed_impact_volatility_evaluation(_frame(10000), params=params)
    assert observed["outcome"] in {"negative", "positive"}
    assert observed["covariance"] == "Newey-West-HAC-lag-5"
    tail_records = [
        record for record in observed["decision_records"]
        if record["shock_category"] is not None and record["future_volatility_3h"] is not None
    ]
    assert tail_records
    assert all(record["terminal_outcome"] == observed["outcome"] for record in tail_records)
    assert {record["terminal_outcome"] for record in observed["decision_records"]} >= {
        "invalid", "no_decision",
    }
    required = {
        "representation_plan_digest", "representation_output_fields",
        "representation_decision_ts", "shock_category", "shock_magnitude",
        "prior_volatility", "completed_quote_volume", "future_volatility_3h",
        "terminal_outcome",
    }
    assert required <= observed["decision_records"][0].keys()
    assert required <= invalid["decision_records"][0].keys()


def test_native_adapter_rejects_future_stale_and_semantically_invalid_payloads() -> None:
    ts = pd.Timestamp("2023-02-01T00:00:00Z")
    values = dict(zip(OUTPUT_FIELDS, [2_000_000.0, 10.0, .02, .02 / 2_000_000.0, .01]))
    extra = values | {"representation_plan_digest": PLAN_DIGEST, "representation_output_fields": json.dumps(list(OUTPUT_FIELDS)), "representation_decision_ts": ts.isoformat()}
    strategy = XrpSignedImpactFutureVolatility30mStrategy()
    bar = Bar(ts, "XRPUSDT", 1., 1., 1., 1., 1., extra)
    signal = strategy.on_bars(ts, {"XRPUSDT": bar}, {"XRPUSDT"}, {})[0]
    assert signal.side is None
    assert signal.metadata["native_payload_outcome"] == "consumed"
    for bad_ts in (ts + pd.Timedelta(minutes=1), ts - pd.Timedelta(minutes=30)):
        bad = Bar(ts, "XRPUSDT", 1., 1., 1., 1., 1., extra | {"representation_decision_ts": bad_ts.isoformat()})
        assert strategy.on_bars(ts, {"XRPUSDT": bad}, {"XRPUSDT"}, {})[0].metadata["native_payload_outcome"] == "invalid"
    inconsistent = Bar(ts, "XRPUSDT", 1., 1., 1., 1., 1., extra | {OUTPUT_FIELDS[3]: 99.0})
    assert strategy.on_bars(ts, {"XRPUSDT": inconsistent}, {"XRPUSDT"}, {})[0].metadata["native_payload_outcome"] == "invalid"


def test_evaluator_enforces_provenance_complete_targets_and_prior_only_controls() -> None:
    frame = _frame(1700)
    rows = _compile_rows(frame, .95, 12)
    assert rows.loc[:11, "prior_volatility"].isna().all()
    assert rows.loc[12, "prior_volatility"] == pytest.approx(
        frame.loc[12, OUTPUT_FIELDS[4]]
    )

    missing_future = frame.copy()
    missing_future.loc[1501, OUTPUT_FIELDS[2]] = np.nan
    compiled = _compile_rows(missing_future, .95, 12)
    assert not bool(compiled.loc[1500, "target_complete"])

    for column, value in (
        ("representation_plan_digest", "0" * 64),
        ("representation_output_fields", json.dumps(list(reversed(OUTPUT_FIELDS)))),
        ("representation_decision_ts", frame["ts"] + pd.Timedelta(minutes=30)),
    ):
        bad = frame.copy()
        bad[column] = value
        assert not _compile_rows(bad, .95, 12)["semantic_valid"].any()


def test_timestamp_gap_invalidates_only_dependent_windows_then_recovers() -> None:
    frame = _frame(3200).drop(index=1500).reset_index(drop=True)
    frame.loc[2941, OUTPUT_FIELDS[2]] = -.1
    frame.loc[2941, OUTPUT_FIELDS[3]] = (
        frame.loc[2941, OUTPUT_FIELDS[2]] / frame.loc[2941, OUTPUT_FIELDS[0]]
    )
    rows = _compile_rows(frame, .95, 12)
    assert not bool(rows.loc[1499, "target_complete"])
    assert rows.loc[1510, "semantic_valid"]
    assert rows.loc[2941, "shock_category"] == "negative"


def test_validation_candidates_cannot_materialize_held_out_targets() -> None:
    frame = _frame(10000)
    cutoff = int(len(frame) * .8)
    test_start = frame.loc[cutoff, "ts"]
    grid = signed_impact_volatility_grid_evaluation(
        frame,
        parameter_grid={
            "signed_impact_tail_quantile": [.95, .975],
            "volatility_control_window_bars": [12, 24],
        },
    )
    for candidate in grid["selection_candidates"]:
        assert candidate["held_out_evaluated"] is False
        assert candidate["test"] is None
        assert all(
            pd.Timestamp(record["decision_ts"]) < test_start
            for record in candidate["decision_records"]
            if record["decision_ts"] is not None
        )


def test_invalid_rows_keep_row_level_terminal_outcomes() -> None:
    frame = _frame(3000)
    frame.loc[1600, "representation_plan_digest"] = "0" * 64
    result = signed_impact_volatility_evaluation(
        frame,
        params={
            "signed_impact_tail_quantile": .95,
            "volatility_control_window_bars": 12,
        },
    )
    record = next(
        item for item in result["decision_records"]
        if item["decision_ts"] == frame.loc[1600, "ts"].isoformat()
    )
    assert record["terminal_outcome"] == "invalid"


def test_evaluator_requires_exact_prior_window_signed_tails_and_frozen_grid() -> None:
    frame = _frame(1700)
    frame.loc[100, OUTPUT_FIELDS[3]] = np.nan
    rows = _compile_rows(frame, .95, 12)
    assert rows.loc[1440, "shock_category"] is None

    positive_only = _frame(1700)
    positive_only[OUTPUT_FIELDS[2]] = positive_only[OUTPUT_FIELDS[2]].abs() + 1e-6
    positive_only[OUTPUT_FIELDS[3]] = (
        positive_only[OUTPUT_FIELDS[2]] / positive_only[OUTPUT_FIELDS[0]]
    )
    categories = _compile_rows(positive_only, .95, 12)["shock_category"]
    assert "negative" not in set(categories.dropna())

    with pytest.raises(ValueError, match="frozen four-variant"):
        signed_impact_volatility_grid_evaluation(
            _frame(1700),
            parameter_grid={
                "signed_impact_tail_quantile": [.95],
                "volatility_control_window_bars": [12, 24],
            },
        )


def test_native_draft_discovery_does_not_map_question_to_another_template() -> None:
    raw = _contract()
    immutable = raw["immutable_contract"]
    assignment = {
        "question": QUESTION, "question_digest": immutable["question_digest"],
        "tier": "Tier2B", "max_variants": 8,
        "representation_plan": raw["representation_plan"],
        "dataset_build_id": immutable["dataset_build_id"], "dataset_digest": immutable["dataset_digest"],
        "instrument": "XRPUSDT", "instruments": ["XRPUSDT"], "venue": "bybit", "timeframe": "1m",
        "window_start": immutable["window"]["start"], "window_end": immutable["window"]["end"],
    }
    card = draft_research_card(assignment, repository_root=str(ROOT))
    assert card["engine_strategy_name"] == "xrp_signed_impact_future_volatility_30m"
    assert card["research_question"] == QUESTION
