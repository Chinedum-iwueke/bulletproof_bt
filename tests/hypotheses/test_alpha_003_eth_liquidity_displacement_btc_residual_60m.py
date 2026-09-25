from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
import pytest

from bt.contracts.research_specs_v2 import canonical_hash
from bt.core.engine import BacktestEngine
from bt.data.feed import HistoricalDataFeed
from bt.execution.execution_model import ExecutionModel
from bt.execution.fees import FeeModel
from bt.execution.slippage import SlippageModel
from bt.experiments.adaptive_representation import materialize_adaptive_representation
from bt.governance.alpha_strategy_pipeline import complete_independent_review, confirm_card, draft_research_card, qualify_card
import bt.governance.alpha_strategy_pipeline as pipeline_module
from bt.hypotheses.contract import HypothesisContract
from bt.logging.jsonl import JsonlWriter
from bt.logging.trades import TradesCsvWriter
from bt.portfolio.portfolio import Portfolio
from bt.risk.risk_engine import RiskEngine
from bt.strategy.eth_liquidity_displacement_btc_residual_60m import (
    MIN_HISTORY, OUTPUT_FIELDS, PLAN_DIGEST,
    EthLiquidityDisplacementBtcResidual60mStrategy,
    compile_decision_rows, liquidity_displacement_evaluation,
    liquidity_displacement_grid_evaluation,
)
import bt.strategy.eth_liquidity_displacement_btc_residual_60m as strategy_module
from bt.universe.universe import UniverseEngine
from bt.validation.strategy_admission import validate_hypothesis_admission
from scripts.run_alpha_research_assignment import attach_adaptive_features


ROOT = Path(__file__).parents[2]
YAML_PATH = ROOT / "research/hypotheses/alpha_003_eth_liquidity_displacement_btc_residual_60m.yaml"
CARD_PATH = ROOT / "research/hypotheses/cards/f504661fcff41ac7ebe0b37a45139baef2718e0307fc7cfbbdfcc87fe2bcbe44.json"
QUESTION = "Does an extreme point-in-time ETHUSDT 15m liquidity-adjusted displacement predict a nonzero BTCUSDT residual close-to-close log return over the next 60m, after controlling for the contemporaneously completed BTCUSDT 15m return and prior-only BTCUSDT realized volatility?"


def _contract() -> dict:
    return yaml.safe_load(YAML_PATH.read_text())


def _panels(decisions: int = 260, edge: float = 0.02) -> dict[str, pd.DataFrame]:
    btc = np.arange(decisions, dtype=float) * 0.00001
    eth = np.arange(decisions, dtype=float) * 0.00001
    for index in range(105, decisions - 5, 9):
        sign = 1.0 if index % 2 else -1.0
        eth[index] = eth[index - 1] + sign * 0.04
        btc[index + 4] = btc[index] + sign * edge
        btc[index + 5] = btc[index + 4] + 0.00001
    result = {}
    start = pd.Timestamp("2025-05-01T00:00:00Z")
    for symbol, values, base in (("BTCUSDT", btc, 100.0), ("ETHUSDT", eth, 50.0)):
        rows = []
        for bucket, value in enumerate(values):
            close = base * np.exp(value)
            for minute in range(15):
                rows.append({"ts": start + pd.Timedelta(minutes=bucket * 15 + minute), "symbol": symbol, "open": close, "high": close + .1, "low": close - .1, "close": close, "volume": 100.0, "quote_volume": 100_000.0})
        result[symbol] = pd.DataFrame(rows)
    return result


def _assignment() -> dict:
    return {"question": QUESTION, "question_digest": canonical_hash({"question": QUESTION}), "campaign_id": "11111111-1111-4111-8111-111111111111", "dataset_build_id": "25d9958c-71b6-4c2b-949a-3c0433f70f38", "dataset_digest": "9a211d8818c5ab8ec82ad5a7d38957e63eb387ea83d4b00541922a0eeca4aacb", "instrument": "BTCUSDT", "instruments": ["BTCUSDT", "ETHUSDT"], "timeframe": "1m", "research_timeframe": "15m", "venue": "bybit", "window_start": "2025-05-01T00:00:00Z", "window_end": "2026-05-01T00:00:00Z", "max_variants": 8, "representation_plan": _contract()["representation_plan"]}


def test_exact_card_plan_and_template_are_independently_bound() -> None:
    assert PLAN_DIGEST == canonical_hash(_contract()["representation_plan"])
    card = json.loads(CARD_PATH.read_text())
    assert card["research_question"] == QUESTION
    assert card["engine_hypothesis_template_digest"] == hashlib.sha256(YAML_PATH.read_bytes()).hexdigest()
    first = draft_research_card(_assignment(), repository_root=str(ROOT))
    assert first == draft_research_card(_assignment(), repository_root=str(ROOT))
    qualification = qualify_card(confirm_card(first, actor="founder-operator", confirmed_at="2026-09-25T00:00:00Z"), repository_root=str(ROOT))
    assert qualification["qualified"] is False
    assert qualification["review"]["gates"]["independent_review_complete"] is False
    assert len(HypothesisContract.from_yaml(YAML_PATH).to_run_specs()) == 4
    assert validate_hypothesis_admission(YAML_PATH).status == "PASS"


def test_bound_external_review_completes_the_declared_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    card = confirm_card(draft_research_card(_assignment(), repository_root=str(ROOT)), actor="founder-operator", confirmed_at="2026-09-25T00:00:00Z")
    qualification = qualify_card(card, repository_root=str(ROOT))
    qualification["governed_review"] = {"immutable": "externally-bound-test-packet"}
    monkeypatch.setattr(pipeline_module, "governed_review_verified", lambda assignment, item: True)
    completed = complete_independent_review({}, qualification)
    assert completed["qualified"] is True
    assert completed["review"]["gates"]["independent_review_complete"] is True
    assert completed["review"]["independent_of_drafter"] is True


def test_complete_rows_enforce_constituents_liquidity_target_and_prior_only_volatility() -> None:
    panels = _panels(130)
    rows = compile_decision_rows(panels)
    assert rows.loc[100, "prior_only_volatility_source_end_ts"] == rows.loc[100, "decision_ts"] - pd.Timedelta(minutes=15)
    broken = {key: value.copy() for key, value in panels.items()}
    broken["ETHUSDT"] = broken["ETHUSDT"].drop(broken["ETHUSDT"].index[100 * 15 + 7])
    compiled = compile_decision_rows(broken)
    item = compiled.loc[compiled.decision_ts == rows.loc[100, "decision_ts"]].iloc[0]
    assert not item["complete"]
    assert not compiled.loc[96, "target_complete"]
    negative = {key: value.copy() for key, value in panels.items()}
    negative["BTCUSDT"].loc[10, "quote_volume"] = -1
    assert not compile_decision_rows(negative).loc[0, "complete"]

    missing_bucket = {key: value.copy() for key, value in panels.items()}
    missing_start = panels["BTCUSDT"]["ts"].min() + pd.Timedelta(minutes=75)
    missing_end = missing_start + pd.Timedelta(minutes=15)
    for symbol in missing_bucket:
        timestamps = missing_bucket[symbol]["ts"]
        missing_bucket[symbol] = missing_bucket[symbol].loc[
            ~((timestamps >= missing_start) & (timestamps < missing_end))
        ]
    gap_rows = compile_decision_rows(missing_bucket)
    gap_decision = missing_end
    assert not gap_rows.loc[gap_rows["decision_ts"] == gap_decision, "complete"].item()
    following = gap_rows.loc[gap_rows["decision_ts"] == gap_decision + pd.Timedelta(minutes=15)].iloc[0]
    assert pd.isna(following["btc_return"])
    assert pd.isna(following["eth_return"])


def test_evaluator_fits_train_only_and_retains_all_terminal_outcomes(monkeypatch: pytest.MonkeyPatch) -> None:
    panels = _panels()
    params = {"eth_displacement_tail_percentile": .8, "response_direction": "continuation"}
    negative = liquidity_displacement_evaluation(panels, params={**params, "response_direction": "reversal"}, minimum_history=20, minimum_extreme_support=1, minimum_matched_support=1)
    failed = liquidity_displacement_evaluation(panels, params=params, minimum_history=20, minimum_extreme_support=10_000, minimum_matched_support=10_000)
    invalid = liquidity_displacement_evaluation(_panels(80), params=params, minimum_history=10_000)
    # Exercise the positive retention branch independently of this synthetic
    # sample's deliberately noisy interval; the underlying pairs, costs,
    # residualization, support, and matching remain the evaluator's own.
    monkeypatch.setattr(strategy_module, "_ci", lambda values: (0.01, [0.009, 0.011]))
    positive = liquidity_displacement_evaluation(panels, params=params, minimum_history=20, minimum_extreme_support=1, minimum_matched_support=1)
    assert positive["outcome"] == "positive" and negative["outcome"] == "negative"
    assert positive["residual_coefficients_fit_split"] == "train"
    assert positive["threshold_fit_policy"] == "rolling_prior_only"
    assert positive["registered_round_trip_cost"] == pytest.approx(0.0018)
    assert positive["doubled_cost_directional_effect"] == pytest.approx(
        0.01 - 0.0036
    )
    assert failed["outcome"] == "failed" and invalid["outcome"] == "invalid"
    assert all(item["treated_decision_ts"] != item["control_decision_ts"] for item in positive["pairs"])


def test_grid_is_deterministic_and_opens_test_at_most_once() -> None:
    kwargs = {"minimum_history": 20, "minimum_extreme_support": 1, "minimum_matched_support": 1}
    one = liquidity_displacement_grid_evaluation(_panels(), parameter_grid=_contract()["parameter_grid"], **kwargs)
    two = liquidity_displacement_grid_evaluation(_panels(), parameter_grid=_contract()["parameter_grid"], **kwargs)
    assert one == two
    assert len(one["selection_candidates"]) == 4
    assert one["test_open_count"] in (0, 1)


def _classic_run(frame: pd.DataFrame, output: Path) -> list[dict]:
    output.mkdir()
    strategy = EthLiquidityDisplacementBtcResidual60mStrategy(eth_displacement_tail_percentile=.975)
    strategy.history["BTCUSDT"].extend([0.0] * MIN_HISTORY)
    engine = BacktestEngine(datafeed=HistoricalDataFeed(frame), universe=UniverseEngine(min_history_bars=1, lookback_bars=1, min_avg_volume=0.0, lag_bars=0), strategy=strategy, risk=RiskEngine(max_positions=1, config={"risk": {"mode": "r_fixed", "r_per_trade": .005, "stop": {}}}), execution=ExecutionModel(fee_model=FeeModel(maker_fee_bps=6, taker_fee_bps=6), slippage_model=SlippageModel(k=0), delay_bars=1), portfolio=Portfolio(initial_cash=10_000, max_leverage=1), decisions_writer=JsonlWriter(output / "decisions.jsonl"), fills_writer=JsonlWriter(output / "fills.jsonl"), trades_writer=TradesCsvWriter(output / "trades.csv"), equity_path=output / "equity.csv", config={})
    engine.run()
    return [json.loads(line) for line in (output / "decisions.jsonl").read_text().splitlines() if line]


def test_compiler_attachment_and_classic_engine_are_causal_and_deterministic(tmp_path: Path) -> None:
    panels = _panels(240)
    materialized = materialize_adaptive_representation(_contract()["representation_plan"], panels)
    source = tmp_path / "source.parquet"
    pd.concat(panels.values(), ignore_index=True).to_parquet(source, index=False)
    attached_path = attach_adaptive_features(source, materialized, output=tmp_path, declared_fields=list(OUTPUT_FIELDS))
    attached = pd.read_parquet(attached_path)
    # The strategy is deliberately pre-warmed with the immutable 35,040-value
    # prior tail in _classic_run; begin after the synthetic feed's opening
    # boundary so that warm-up is not (correctly) reset as a missing decision.
    attached = attached.loc[attached["ts"] != attached["ts"].min()].copy()
    decisions = attached.dropna(subset=["representation_decision_ts"])
    assert (pd.to_datetime(decisions["prior_only_volatility_source_end_ts"], utc=True) == pd.to_datetime(decisions["ts"], utc=True) - pd.Timedelta(minutes=15)).all()
    first = _classic_run(attached, tmp_path / "first")
    second = _classic_run(attached, tmp_path / "second")
    assert first == second
    entries = [row for row in first if row.get("signal", {}).get("signal_type") == "eth_liquidity_displacement_btc_residual_entry"]
    assert entries
    corrupt = attached.copy()
    corrupt.loc[corrupt["representation_decision_ts"].notna(), "prior_only_volatility_source_end_ts"] = corrupt.loc[corrupt["representation_decision_ts"].notna(), "representation_decision_ts"]
    rejected = _classic_run(corrupt, tmp_path / "corrupt")
    assert not [row for row in rejected if row.get("signal", {}).get("signal_type") == "eth_liquidity_displacement_btc_residual_entry"]
