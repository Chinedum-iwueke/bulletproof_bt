from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

import bt.strategy.btc_oi_expansion_return_asymmetry_60m as strategy_module
from bt.contracts.research_specs_v2 import canonical_hash
from bt.core.types import Bar
from bt.core.engine import BacktestEngine
from bt.data.feed import HistoricalDataFeed
from bt.execution.execution_model import ExecutionModel
from bt.execution.fees import FeeModel
from bt.execution.slippage import SlippageModel
from bt.experiments.adaptive_representation import materialize_adaptive_representation
from bt.governance.alpha_strategy_pipeline import confirm_card, draft_research_card, qualify_card
from bt.hypotheses.contract import HypothesisContract
from bt.logging.jsonl import JsonlWriter
from bt.logging.trades import TradesCsvWriter
from bt.portfolio.portfolio import Portfolio
from bt.risk.risk_engine import RiskEngine
from bt.strategy.btc_oi_expansion_return_asymmetry_60m import (
    OUTPUT_FIELDS,
    PLAN_DIGEST,
    BtcOiExpansionReturnAsymmetry60mStrategy,
    _complete_decision_rows,
    oi_expansion_evaluation,
    oi_expansion_grid_evaluation,
)
from bt.validation.strategy_admission import validate_hypothesis_admission
from bt.universe.universe import UniverseEngine
from scripts.run_alpha_research_assignment import attach_adaptive_features


ROOT = Path(__file__).parents[2]
YAML_PATH = ROOT / "research/hypotheses/alpha_003_btc_oi_expansion_return_asymmetry_60m.yaml"
QUESTION = "Does point-in-time open-interest expansion accompanying a completed 15m BTCUSDT return predict direction-dependent BTCUSDT close-to-close return over the next 60m, after controlling for prior return, realized volatility, funding-cycle position, and liquidity?"
DIGEST = "44689a57199bfac4a435371976e7361e46ec2eff4b28bf521aced9076d1b40e6"


def _contract() -> dict:
    return yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))


def _assignment() -> dict:
    contract = _contract()
    return {
        "question": QUESTION,
        "question_digest": DIGEST,
        "campaign_id": "11111111-1111-4111-8111-111111111111",
        "dataset_build_id": "fbb81c42-953b-42fb-8fe1-89c75b45e1aa",
        "dataset_digest": "9a211d8818c5ab8ec82ad5a7d38957e63eb387ea83d4b00541922a0eeca4aacb",
        "instrument": "BTCUSDT", "instruments": ["BTCUSDT"],
        "timeframe": "1m", "research_timeframe": "15m", "venue": "bybit",
        "window_start": "2025-05-01T00:00:00Z", "window_end": "2026-05-01T00:00:00Z",
        "max_variants": 8, "research_context": {"citations": []},
        "representation_plan": contract["representation_plan"],
    }


def _panel(decisions: int = 620, *, edge: float = 0.012) -> pd.DataFrame:
    """Raw 1m panel with sparse, non-overlapping causal OI/return episodes."""
    log_close = np.arange(decisions, dtype=float) * 0.00001
    oi = np.arange(decisions, dtype=float) * 0.001 + 10.0
    for index in range(32, decisions - 5, 8):
        sign = 1.0 if (index // 8) % 2 == 0 else -1.0
        log_close[index] = log_close[index - 1] + sign * 0.006
        log_close[index + 4] = log_close[index] + sign * edge
        log_close[index + 5] = log_close[index + 4] + 0.00001
        # The point-in-time observation at the completed bucket's decision
        # boundary is carried by the first 1m row of the following bucket.
        oi[index + 1] = oi[index] + 0.08
        oi[index + 2] = oi[index] + 0.002
    rows = []
    start = pd.Timestamp("2025-05-01T00:00:00Z")
    for bucket in range(decisions):
        close = float(100.0 * np.exp(log_close[bucket]))
        interest = float(np.exp(oi[bucket]))
        for minute in range(15):
            ts = start + pd.Timedelta(minutes=bucket * 15 + minute)
            rows.append({
                "ts": ts, "symbol": "BTCUSDT", "open": close,
                "high": close + 0.1, "low": close - 0.1, "close": close,
                "volume": 100.0, "quote_volume": 100_000.0,
                "open_interest": interest, "oi_source_ts": ts,
            })
    return pd.DataFrame(rows)


def _params(direction: str = "continuation") -> dict:
    return {
        "open_interest_expansion_percentile": 0.8,
        "absolute_return_percentile": 0.5,
        "direction": direction,
    }


def test_exact_card_is_discovered_and_compiles_deterministically() -> None:
    assert canonical_hash({"question": QUESTION}) == DIGEST
    first = draft_research_card(_assignment(), repository_root=str(ROOT))
    second = draft_research_card(_assignment(), repository_root=str(ROOT))
    assert first == second
    assert first["research_question"] == QUESTION
    confirmed = confirm_card(first, actor="founder-operator", confirmed_at="2026-09-25T00:00:00Z")
    one = qualify_card(confirmed, repository_root=str(ROOT))
    two = qualify_card(confirmed, repository_root=str(ROOT))
    assert one["qualified"] is True and one["variant_count"] == 8
    assert one["artifact_bundle"] == two["artifact_bundle"]
    assert one["artifact_bundle"]["run_config"]["strategy"]["name"] == "btc_oi_expansion_return_asymmetry_60m"
    assert len(HypothesisContract.from_yaml(YAML_PATH).to_run_specs()) == 8
    assert validate_hypothesis_admission(YAML_PATH).status == "PASS"


def test_real_raw_panel_enforces_schema_contiguity_source_time_and_target() -> None:
    raw = _panel(80)
    rows = _complete_decision_rows(raw)
    assert rows.loc[30, "predictor_history_complete"]
    assert rows.loc[30, "target_complete"]
    missing = raw.loc[raw["ts"] != raw["ts"].iloc[30 * 15 + 7]]
    broken = _complete_decision_rows(missing)
    assert not broken.loc[30, "complete"]
    assert not broken.loc[31, "predictor_history_complete"]
    assert not broken.loc[29, "target_complete"]
    future = raw.copy()
    future.loc[future.index[30 * 15:31 * 15], "oi_source_ts"] = (
        future.loc[future.index[30 * 15:31 * 15], "ts"] + pd.Timedelta(days=1)
    )
    future_rows = _complete_decision_rows(future)
    assert future_rows.loc[30, "oi_source_ts"] <= future_rows.loc[30, "decision_ts"]
    with pytest.raises(ValueError, match="open.*high.*low|immutable fields"):
        _complete_decision_rows(raw.drop(columns=["open", "high", "low"]))


def test_full_evaluator_retains_positive_negative_failed_and_invalid_outcomes() -> None:
    raw = _panel()
    positive = oi_expansion_evaluation(raw, params=_params(), minimum_direction_support=2)
    negative = oi_expansion_evaluation(raw, params=_params("reversal"), minimum_direction_support=2)
    failed = oi_expansion_evaluation(raw, params=_params(), minimum_direction_support=10_000)
    invalid = oi_expansion_evaluation(_panel(20), params=_params(), minimum_direction_support=2)
    assert positive["outcome"] == "negative"
    assert negative["outcome"] == "positive"
    assert failed["outcome"] == "failed"
    assert invalid["outcome"] == "invalid"
    assert positive["thresholds_fit_split"] == "train"
    assert positive["split_counts"]["purged"] > 0
    assert positive["maximum_drawdown"] <= 0
    assert positive["doubled_cost_directional_effect"] < positive["test_directional_effect"]


def test_placebo_contrast_is_paired_by_identical_timestamp_identity() -> None:
    result = oi_expansion_evaluation(_panel(), params=_params("reversal"), minimum_direction_support=2)
    assert result["pairs"]
    assert all(
        item["actual_minus_placebo"]
        == item["actual_predictor_payoff"] - item["placebo_difference"]
        for item in result["pairs"]
    )
    assert any(abs(item["placebo_difference"]) > 0 for item in result["pairs"])
    assert all(
        item["actual_predictor_contrast"] != item["placebo_predictor_contrast"]
        for item in result["pairs"]
    )
    identities = {
        (item["treated_decision_ts"], item["control_decision_ts"])
        for item in result["pairs"]
    }
    assert len(identities) == len(result["pairs"])
    assert result["paired_actual_minus_placebo_confidence_interval_95"][0] > 0


def test_grid_selects_on_validation_and_opens_test_once() -> None:
    grid = _contract()["parameter_grid"]
    result = oi_expansion_grid_evaluation(
        _panel(300), parameter_grid=grid, minimum_direction_support=1
    )
    assert len(result["selection_candidates"]) == 8
    assert result["test_open_count"] == 1
    assert result["selected_parameters"] == max(
        result["selection_candidates"],
        key=lambda item: (
            item["validation_directional_effect"],
            tuple(
                str(item["parameters"][key])
                for key in (
                    "open_interest_expansion_percentile",
                    "absolute_return_percentile",
                    "direction",
                )
            ),
        ),
    )["parameters"]


def test_grid_keeps_test_closed_when_no_validation_effect_is_positive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def nonpositive_evaluation(*args, params, evaluate_test, **kwargs):
        assert evaluate_test is False
        return {"validation_directional_effect": -0.001}

    monkeypatch.setattr(
        strategy_module,
        "oi_expansion_evaluation",
        nonpositive_evaluation,
    )
    result = strategy_module.oi_expansion_grid_evaluation(
        _panel(40),
        parameter_grid=_contract()["parameter_grid"],
        minimum_direction_support=1,
    )
    assert result["outcome"] == "negative"
    assert result["selected_parameters"] is None
    assert result["selection_reason"] == "no_positive_validation_directional_effect"
    assert result["test_open_count"] == 0


def test_compiler_attachment_is_deterministic_and_provenance_bound(tmp_path: Path) -> None:
    raw = _panel(80)
    plan = _contract()["representation_plan"]
    first = materialize_adaptive_representation(plan, {"BTCUSDT": raw})
    second = materialize_adaptive_representation(plan, {"BTCUSDT": raw})
    pd.testing.assert_frame_equal(first.frame, second.frame)
    assert first.receipt == second.receipt
    assert first.receipt["plan_digest"] == PLAN_DIGEST
    source = tmp_path / "source.parquet"
    raw.to_parquet(source, index=False)
    path = attach_adaptive_features(
        source, first, output=tmp_path, declared_fields=list(OUTPUT_FIELDS)
    )
    attached = pd.read_parquet(path)
    decisions = attached.dropna(subset=["representation_decision_ts"])
    assert not decisions.empty
    assert decisions["representation_plan_digest"].eq(PLAN_DIGEST).all()
    assert decisions["representation_output_fields"].map(
        lambda value: tuple(json.loads(value)) == OUTPUT_FIELDS
    ).all()
    assert (
        pd.to_datetime(decisions["representation_decision_ts"], utc=True)
        == pd.to_datetime(decisions["ts"], utc=True)
    ).all()


def _classic_run(frame: pd.DataFrame, output: Path) -> list[dict]:
    output.mkdir()
    engine = BacktestEngine(
        datafeed=HistoricalDataFeed(frame),
        universe=UniverseEngine(
            min_history_bars=1, lookback_bars=1, min_avg_volume=0.0, lag_bars=0
        ),
        strategy=BtcOiExpansionReturnAsymmetry60mStrategy(
            history_window=2,
            open_interest_expansion_percentile=0.5,
            absolute_return_percentile=0.5,
        ),
        risk=RiskEngine(
            max_positions=1,
            config={"risk": {"mode": "r_fixed", "r_per_trade": 0.005, "stop": {}}},
        ),
        execution=ExecutionModel(
            fee_model=FeeModel(maker_fee_bps=6.0, taker_fee_bps=6.0),
            slippage_model=SlippageModel(k=0.0), delay_bars=1,
        ),
        portfolio=Portfolio(initial_cash=10_000.0, max_leverage=1.0),
        decisions_writer=JsonlWriter(output / "decisions.jsonl"),
        fills_writer=JsonlWriter(output / "fills.jsonl"),
        trades_writer=TradesCsvWriter(output / "trades.csv"),
        equity_path=output / "equity.csv",
        config={},
    )
    engine.run()
    return [
        json.loads(line)
        for line in (output / "decisions.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_compiled_representation_executes_deterministically_in_classic_engine(tmp_path: Path) -> None:
    raw = _panel(80)
    materialized = materialize_adaptive_representation(
        _contract()["representation_plan"], {"BTCUSDT": raw}
    )
    source = tmp_path / "classic-source.parquet"
    raw.to_parquet(source, index=False)
    execution_path = attach_adaptive_features(
        source, materialized, output=tmp_path, declared_fields=list(OUTPUT_FIELDS)
    )
    execution = pd.read_parquet(execution_path)
    first = _classic_run(execution, tmp_path / "first")
    second = _classic_run(execution, tmp_path / "second")
    assert first == second
    entries = [
        row for row in first
        if row.get("signal", {}).get("signal_type")
        == "btc_oi_expansion_return_asymmetry_entry"
    ]
    assert entries


def _entry_timestamps(records: list[dict]) -> set[pd.Timestamp]:
    return {
        pd.Timestamp(row["ts"])
        for row in records
        if row.get("signal", {}).get("signal_type")
        == "btc_oi_expansion_return_asymmetry_entry"
    }


def test_compiler_to_strategy_fails_closed_across_missing_decision(
    tmp_path: Path,
) -> None:
    raw = _panel(80)
    plan = _contract()["representation_plan"]
    complete = materialize_adaptive_representation(plan, {"BTCUSDT": raw})
    complete_source = tmp_path / "complete-source.parquet"
    raw.to_parquet(complete_source, index=False)
    (tmp_path / "complete-attachment").mkdir()
    complete_path = attach_adaptive_features(
        complete_source,
        complete,
        output=tmp_path / "complete-attachment",
        declared_fields=list(OUTPUT_FIELDS),
    )
    baseline = _classic_run(
        pd.read_parquet(complete_path),
        tmp_path / "complete-run",
    )
    entry_ts = min(_entry_timestamps(baseline))

    # Remove one constituent minute from the decision immediately preceding
    # the entry. The following valid decision must not bridge that OI gap.
    missing_ts = entry_ts - pd.Timedelta(minutes=23)
    broken_raw = raw.loc[pd.to_datetime(raw["ts"], utc=True) != missing_ts].copy()
    broken = materialize_adaptive_representation(plan, {"BTCUSDT": broken_raw})
    broken_source = tmp_path / "broken-source.parquet"
    broken_raw.to_parquet(broken_source, index=False)
    (tmp_path / "broken-attachment").mkdir()
    broken_path = attach_adaptive_features(
        broken_source,
        broken,
        output=tmp_path / "broken-attachment",
        declared_fields=list(OUTPUT_FIELDS),
    )
    broken_records = _classic_run(
        pd.read_parquet(broken_path),
        tmp_path / "broken-run",
    )
    assert entry_ts not in _entry_timestamps(broken_records)


def test_compiler_to_strategy_rejects_malformed_provenance(
    tmp_path: Path,
) -> None:
    raw = _panel(80)
    materialized = materialize_adaptive_representation(
        _contract()["representation_plan"], {"BTCUSDT": raw}
    )
    source = tmp_path / "source.parquet"
    raw.to_parquet(source, index=False)
    (tmp_path / "attachment").mkdir()
    execution_path = attach_adaptive_features(
        source,
        materialized,
        output=tmp_path / "attachment",
        declared_fields=list(OUTPUT_FIELDS),
    )
    execution = pd.read_parquet(execution_path)
    baseline = _classic_run(execution, tmp_path / "baseline-run")
    entry_ts = min(_entry_timestamps(baseline))
    corrupt = execution.copy()
    mask = pd.to_datetime(corrupt["ts"], utc=True) == entry_ts
    assert mask.sum() == 1
    corrupt.loc[mask, "representation_decision_ts"] = "malformed"
    rejected = _classic_run(corrupt, tmp_path / "corrupt-run")
    assert entry_ts not in _entry_timestamps(rejected)


def _decision_bar(ts: pd.Timestamp, **overrides) -> Bar:
    extra = {
        "btc_15m_log_return": 0.02,
        "btc_past_60m_log_return": 0.01,
        "btc_past_6h_realized_volatility": 0.02,
        "btc_15m_quote_volume": 2_000_000.0,
        "open_interest": 110.0,
        "oi_source_ts": ts.isoformat(),
        "representation_plan_digest": PLAN_DIGEST,
        "representation_output_fields": json.dumps(list(OUTPUT_FIELDS)),
        "representation_decision_ts": ts.isoformat(),
    }
    extra.update(overrides)
    return Bar(ts, "BTCUSDT", 100.0, 101.0, 99.0, 100.0, 1.0, extra)


@pytest.mark.parametrize("field,value", [
    ("representation_plan_digest", "wrong"),
    ("representation_output_fields", json.dumps(list(reversed(OUTPUT_FIELDS)))),
    ("representation_decision_ts", "malformed"),
    ("oi_source_ts", "malformed"),
])
def test_strategy_fails_closed_for_bad_provenance_and_malformed_timestamps(field, value) -> None:
    ts = pd.Timestamp("2025-05-02T00:00:00Z")
    strategy = BtcOiExpansionReturnAsymmetry60mStrategy(history_window=2)
    assert strategy.on_bars(ts, {"BTCUSDT": _decision_bar(ts, **{field: value})}, {"BTCUSDT"}, {"positions": {}}) == []


def test_strategy_consumes_compiled_fields_deterministically_and_direction_is_real() -> None:
    start = pd.Timestamp("2025-05-02T00:00:00Z")
    outputs = {}
    for direction in ("continuation", "reversal"):
        strategy = BtcOiExpansionReturnAsymmetry60mStrategy(
            history_window=2, direction=direction,
            open_interest_expansion_percentile=0.5,
            absolute_return_percentile=0.5,
        )
        signals = []
        for index, (ret, oi) in enumerate(((0.001, 100.0), (0.002, 101.0), (0.02, 110.0))):
            ts = start + pd.Timedelta(minutes=15 * index)
            signals.extend(strategy.on_bars(
                ts,
                {"BTCUSDT": _decision_bar(ts, btc_15m_log_return=ret, open_interest=oi)},
                {"BTCUSDT"}, {"positions": {}},
            ))
        assert len(signals) == 1
        outputs[direction] = signals[0]
        assert signals[0].metadata["representation_plan_digest"] == PLAN_DIGEST
        assert tuple(signals[0].metadata["representation_output_fields"]) == OUTPUT_FIELDS
    assert outputs["continuation"].side != outputs["reversal"].side
    assert outputs["continuation"].metadata["decision_trace"]


def test_strategy_does_not_bridge_missing_15m_oi_decision() -> None:
    start = pd.Timestamp("2025-05-02T00:00:00Z")
    strategy = BtcOiExpansionReturnAsymmetry60mStrategy(
        history_window=1,
        open_interest_expansion_percentile=0.5,
        absolute_return_percentile=0.5,
    )
    first = _decision_bar(start, open_interest=100.0)
    missing = _decision_bar(
        start + pd.Timedelta(minutes=15),
        open_interest=105.0,
        representation_plan_digest="invalid",
    )
    after_gap = _decision_bar(
        start + pd.Timedelta(minutes=30),
        open_interest=120.0,
    )
    assert strategy.on_bars(start, {"BTCUSDT": first}, {"BTCUSDT"}, {"positions": {}}) == []
    assert strategy.on_bars(
        start + pd.Timedelta(minutes=15),
        {"BTCUSDT": missing},
        {"BTCUSDT"},
        {"positions": {}},
    ) == []
    assert strategy.on_bars(
        start + pd.Timedelta(minutes=30),
        {"BTCUSDT": after_gap},
        {"BTCUSDT"},
        {"positions": {}},
    ) == []
