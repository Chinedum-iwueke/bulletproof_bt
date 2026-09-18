from __future__ import annotations

from pathlib import Path
import pandas as pd
import yaml

from bt.contracts.research_specs_v2 import canonical_hash
from bt.core.engine import BacktestEngine
from bt.core.types import Bar
from bt.data.feed import HistoricalDataFeed
from bt.execution.execution_model import ExecutionModel
from bt.execution.fees import FeeModel
from bt.execution.slippage import SlippageModel
from bt.governance.alpha_strategy_pipeline import confirm_card, draft_research_card, qualify_card
from bt.hypotheses.contract import HypothesisContract
from bt.logging.jsonl import JsonlWriter
from bt.logging.trades import TradesCsvWriter
from bt.portfolio.portfolio import Portfolio
from bt.risk.risk_engine import RiskEngine
from bt.strategy.btc_funding_basis_crowding_60m import (
    QUESTION, BtcFundingBasisCrowding60mStrategy, _complete_decisions,
    _funding_cycle_distance, _overlap_cluster_standard_error,
    funding_basis_matched_evaluation,
)
from bt.universe.universe import UniverseEngine
from bt.validation.strategy_admission import validate_hypothesis_admission
from scripts import run_alpha_research_assignment as assignment_runner


ROOT = Path(__file__).parents[2]
DIGEST = "7267bed217cb13005e9463b53592235976af731a434e62ec98dfc61823f8fec2"
YAML_PATH = ROOT / "research/hypotheses/alpha_003_btc_funding_basis_crowding_60m.yaml"


def _assignment() -> dict:
    return {"question": QUESTION, "question_digest": DIGEST, "dataset_build_id": "fbb81c42-953b-42fb-8fe1-89c75b45e1aa", "dataset_digest": "9a211d8818c5ab8ec82ad5a7d38957e63eb387ea83d4b00541922a0eeca4aacb", "venue": "bybit", "instrument": "BTCUSDT", "timeframe": "1m", "window_start": "2023-01-01T00:00:00Z", "window_end": "2024-01-01T00:00:00Z", "max_variants": 8}


def _frame(minutes: int = 70) -> pd.DataFrame:
    ts = pd.date_range("2023-01-01", periods=minutes, freq="1min", tz="UTC")
    return pd.DataFrame({"ts": ts, "symbol": "BTCUSDT", "close": [100 + i / 100 for i in range(minutes)], "quote_volume": 250_000.0, "mark_close": 101.0, "index_close": 100.0, "funding_rate": .001, "funding_source_ts": ts})


def test_exact_card_compiles_deterministically_without_template_substitution() -> None:
    assert canonical_hash({"question": QUESTION}) == DIGEST
    card = draft_research_card(_assignment(), repository_root=str(ROOT))
    assert card["research_question"] == QUESTION
    confirmed = confirm_card(card, actor="founder-operator", confirmed_at="2026-09-18T00:00:00Z")
    first = qualify_card(confirmed, repository_root=str(ROOT))
    second = qualify_card(confirmed, repository_root=str(ROOT))
    assert first["qualified"] and first["artifact_bundle"] == second["artifact_bundle"]
    assert first["artifact_bundle"]["run_config"]["strategy"]["name"] == "btc_funding_basis_crowding_60m"
    assert first["authority"] == {"capital": False, "orders": False, "promotion": False, "self_approval": False}


def test_contract_is_frozen_classic_only_and_admitted() -> None:
    contract = HypothesisContract.from_yaml(YAML_PATH)
    assert contract.materialize_grid() == contract.materialize_grid()
    assert len(contract.materialize_grid()) == 4
    raw = yaml.safe_load(YAML_PATH.read_text())
    assert raw["immutable_contract"]["question"] == QUESTION
    assert raw["evaluation"]["selection_metric"] == "validation_treated_minus_control_mean"
    assert raw["evaluation"]["outcome_retention"] == ["positive", "negative", "invalid", "failed"]
    assert validate_hypothesis_admission(YAML_PATH).status == "PASS"


def test_latest_backward_funding_join_uses_source_time_not_row_order() -> None:
    frame = _frame(10)
    frame["funding_source_ts"] = frame.loc[0, "ts"]
    frame.loc[8, ["funding_source_ts", "funding_rate"]] = [frame.loc[0, "ts"], .1]
    frame.loc[1, ["funding_source_ts", "funding_rate"]] = [frame.loc[1, "ts"], .2]
    decisions = _complete_decisions(frame)
    assert decisions.iloc[1]["funding_rate"] == .2
    assert decisions.iloc[1]["funding_source_ts"] == frame.loc[1, "ts"]


def test_incomplete_target_is_invalid_and_all_outcomes_are_retained() -> None:
    frame = _frame(70).drop(index=31).reset_index(drop=True)
    result = funding_basis_matched_evaluation(frame, params={"funding_percentile_threshold": .95, "basis_threshold_bps": 0.0})
    assert len(result["decision_records"]) == len(frame.assign(bucket=frame.ts.dt.floor("5min")).bucket.unique())


def test_decision_evidence_is_scoped_to_the_declared_partition() -> None:
    frame = _frame(180)
    start = pd.Timestamp("2023-01-01T00:30:00Z")
    end = pd.Timestamp("2023-01-01T01:30:00Z")
    result = funding_basis_matched_evaluation(
        frame,
        params={"funding_percentile_threshold": .95, "basis_threshold_bps": 0.0},
        start=start,
        end=end,
    )
    timestamps = [pd.Timestamp(item["decision_ts"]) for item in result["decision_records"]]
    assert timestamps
    assert min(timestamps) >= start
    assert max(timestamps) <= end
    assert any(item["status"] == "invalid" for item in result["decision_records"])
    assert result["outcome"] == "invalid"
    assert result["passed"] is False


def test_entirely_missing_bucket_cannot_form_a_sixty_minute_target() -> None:
    frame = _frame(80)
    missing_bucket = pd.Timestamp("2023-01-01T00:30:00Z")
    frame = frame.loc[frame["ts"].dt.floor("5min") != missing_bucket].reset_index(drop=True)
    decisions = _complete_decisions(frame)
    missing = decisions.loc[decisions["bucket_ts"] == missing_bucket].iloc[0]
    before_gap = decisions.loc[
        decisions["decision_ts"] == pd.Timestamp("2023-01-01T00:20:00Z")
    ].iloc[0]
    assert not bool(missing["complete"])
    assert pd.isna(missing["close"])
    assert not bool(before_gap["target_complete"])
    assert pd.isna(before_gap["target_return_60m"])


def test_negative_outcome_and_all_decision_classes_are_retained(monkeypatch) -> None:
    history = 25_920
    rows = []
    start = pd.Timestamp("2023-01-01T00:05:00Z")
    for index in range(history + 80):
        treated = index >= history and index % 2 == 0
        rows.append({
            "decision_ts": start + pd.Timedelta(minutes=5 * index),
            "bucket_ts": start + pd.Timedelta(minutes=5 * (index - 1)),
            "complete": index != history + 3,
            "close": 100.0,
            "quote_volume_5m": 1_250_000.0,
            "funding_rate": .002 if treated else .001,
            "funding_source_ts": start + pd.Timedelta(minutes=5 * index),
            "basis": .001 if treated else 0.0,
            "trailing_return_60m": .01 if index % 4 < 2 else -.01,
            "realized_volatility_6h": .02,
            "target_return_60m": .01 if treated else 0.0,
            "target_complete": True,
        })
    monkeypatch.setattr(
        "bt.strategy.btc_funding_basis_crowding_60m._complete_decisions",
        lambda frame: pd.DataFrame(rows),
    )
    result = funding_basis_matched_evaluation(
        pd.DataFrame(),
        params={"funding_percentile_threshold": .95, "basis_threshold_bps": 1.0},
        minimum_support=30,
    )
    assert result["outcome"] == "negative"
    assert result["passed"] is False
    assert result["matched_support"] >= 30
    assert {item["status"] for item in result["decision_records"]} >= {
        "treated", "control", "invalid"
    }


def test_positive_outcome_requires_supported_disjoint_matched_windows(monkeypatch) -> None:
    history = 25_920
    start = pd.Timestamp("2023-01-01T00:05:00Z")
    rows = [
        {
            "decision_ts": start + pd.Timedelta(minutes=5 * index),
            "bucket_ts": start + pd.Timedelta(minutes=5 * index - 5),
            "complete": True,
            "close": 100.0,
            "quote_volume_5m": 1_250_000.0,
            "funding_rate": .001,
            "funding_source_ts": start + pd.Timedelta(minutes=5 * index),
            "basis": 0.0,
            "trailing_return_60m": 0.0,
            "realized_volatility_6h": .02,
            "target_return_60m": 0.0,
            "target_complete": True,
        }
        for index in range(history)
    ]
    tail_start = rows[-1]["decision_ts"] + pd.Timedelta(hours=2)
    for index in range(80):
        treated = index % 2 == 0
        decision = tail_start + pd.Timedelta(hours=2 * index)
        rows.append(
            {
                "decision_ts": decision,
                "bucket_ts": decision - pd.Timedelta(minutes=5),
                "complete": True,
                "close": 100.0,
                "quote_volume_5m": 1_250_000.0,
                "funding_rate": .002 if treated else .001,
                "funding_source_ts": decision,
                "basis": .001 if treated else 0.0,
                "trailing_return_60m": (
                    .01 if treated and index % 4 == 0 else -.01 if treated else 0.0
                ),
                "realized_volatility_6h": .02,
                "target_return_60m": -.01 if treated else 0.0,
                "target_complete": True,
            }
        )
    monkeypatch.setattr(
        "bt.strategy.btc_funding_basis_crowding_60m._complete_decisions",
        lambda frame: pd.DataFrame(rows),
    )

    result = funding_basis_matched_evaluation(
        pd.DataFrame(),
        params={"funding_percentile_threshold": .95, "basis_threshold_bps": 1.0},
    )

    assert result["outcome"] == "positive"
    assert result["passed"] is True
    assert result["matched_support"] == 40
    assert result["confidence_interval_95"]["upper"] < 0
    assert result["doubled_cost_treated_minus_control"] < 0


def test_future_mutation_cannot_change_prior_completed_decisions() -> None:
    original = _frame(70)
    mutated = original.copy()
    mutated.loc[mutated.index[-10]:, ["close", "mark_close", "funding_rate"]] = [1.0, 10_000.0, 99.0]
    cutoff = pd.Timestamp("2023-01-01T00:50:00Z")
    left = _complete_decisions(original)
    right = _complete_decisions(mutated)
    columns = ["decision_ts", "close", "funding_rate", "basis", "trailing_return_60m", "realized_volatility_6h"]
    pd.testing.assert_frame_equal(left.loc[left.decision_ts <= cutoff, columns], right.loc[right.decision_ts <= cutoff, columns])


def test_doubled_cost_moves_negative_claim_toward_zero() -> None:
    result = funding_basis_matched_evaluation(_frame(), params={"funding_percentile_threshold": .95, "basis_threshold_bps": 0.0}, cost_bps=9.0)
    assert result["doubled_cost_treated_minus_control"] > result["treated_minus_control_mean"]


def test_overlapping_treated_or_control_outcomes_share_dependence_cluster() -> None:
    start = pd.Timestamp("2023-01-01T00:00:00Z")
    pairs = [
        {
            "treated_decision_ts": start + pd.Timedelta(hours=index * 3),
            "control_decision_ts": start + pd.Timedelta(hours=24 + index * 3),
            "difference": difference,
        }
        for index, difference in enumerate((1.0, 1.0, -1.0, -1.0))
    ]
    independent = _overlap_cluster_standard_error(pairs)
    pairs[1]["control_decision_ts"] = pairs[0]["control_decision_ts"] + pd.Timedelta(minutes=5)
    clustered = _overlap_cluster_standard_error(pairs)
    assert clustered > independent


def test_single_overlap_component_cannot_claim_finite_precision() -> None:
    start = pd.Timestamp("2023-01-01T00:00:00Z")
    pairs = [
        {
            "treated_decision_ts": start + pd.Timedelta(minutes=5 * index),
            "control_decision_ts": start + pd.Timedelta(days=1, minutes=5 * index),
            "difference": float(index % 2),
        }
        for index in range(12)
    ]
    assert _overlap_cluster_standard_error(pairs) == float("inf")


def test_funding_cycle_matching_wraps_across_eight_hour_boundary() -> None:
    assert _funding_cycle_distance(475, 5) == 10 / 480
    assert _funding_cycle_distance(475, 5) < _funding_cycle_distance(475, 240)


def test_missing_basis_is_invalid_not_a_control(monkeypatch) -> None:
    decision = pd.DataFrame([{
        "decision_ts": pd.Timestamp("2023-01-01T06:00:00Z"),
        "bucket_ts": pd.Timestamp("2023-01-01T05:55:00Z"),
        "complete": True,
        "close": 100.0,
        "quote_volume_5m": 1_250_000.0,
        "funding_rate": .001,
        "funding_source_ts": pd.Timestamp("2023-01-01T05:59:00Z"),
        "basis": None,
        "trailing_return_60m": .01,
        "realized_volatility_6h": .02,
        "target_return_60m": -.01,
        "target_complete": True,
    }])
    monkeypatch.setattr(
        "bt.strategy.btc_funding_basis_crowding_60m._complete_decisions",
        lambda frame: decision,
    )
    result = funding_basis_matched_evaluation(
        pd.DataFrame(),
        params={"funding_percentile_threshold": .95, "basis_threshold_bps": 0.0},
    )
    assert result["outcome"] == "invalid"
    assert result["decision_records"][0]["status"] == "invalid"


def test_funding_observed_before_its_source_time_is_not_available() -> None:
    frame = _frame(10)
    frame["funding_source_ts"] = frame["ts"] + pd.Timedelta(hours=1)
    decisions = _complete_decisions(frame)
    assert decisions["funding_rate"].isna().all()
    assert decisions["funding_source_ts"].isna().all()


def test_native_strategy_does_not_evaluate_stale_bucket_after_whole_gap() -> None:
    strategy = BtcFundingBasisCrowding60mStrategy()
    strategy.funding_history["BTCUSDT"].extend([.001] * 25_920)
    strategy.completed_buckets["BTCUSDT"].extend(
        (
            pd.Timestamp("2022-12-31T18:00:00Z") + pd.Timedelta(minutes=5 * index),
            99.0,
            True,
        )
        for index in range(72)
    )

    emitted = []
    for minute in (*range(5), 10):
        ts = pd.Timestamp("2023-01-01T00:00:00Z") + pd.Timedelta(minutes=minute)
        bar = Bar(
            ts=ts, symbol="BTCUSDT", open=100.0, high=101.0, low=99.0,
            close=100.0, volume=1.0,
            extra={
                "quote_volume": 250_000.0,
                "mark_close": 101.0,
                "index_close": 100.0,
                "funding_rate": .002,
                "funding_source_ts": ts,
            },
        )
        emitted.extend(
            strategy.on_bars(ts, {"BTCUSDT": bar}, {"BTCUSDT"}, {"positions": {}})
        )
    assert emitted == []


def test_runner_selects_matched_control_evidence_not_engine_pnl() -> None:
    import importlib.util
    spec = importlib.util.spec_from_file_location("assignment_runner", ROOT / "scripts/run_alpha_research_assignment.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    evaluations = [
        {"outcome": "negative", "treated_minus_control_mean": -.001, "matched_support": 100},
        {"outcome": "negative", "treated_minus_control_mean": -.002, "matched_support": 40},
    ]
    assert module.select_funding_basis_variant(evaluations) == 1


def test_runner_never_selects_invalid_or_under_supported_validation() -> None:
    evaluations = [
        {"outcome": "invalid", "treated_minus_control_mean": -.50, "matched_support": 0},
        {"outcome": "failed", "treated_minus_control_mean": -.25, "matched_support": 12},
        {"outcome": "negative", "treated_minus_control_mean": -.01, "matched_support": 40},
    ]
    assert assignment_runner.select_funding_basis_variant(evaluations) == 2
    assert assignment_runner.select_funding_basis_variant(evaluations[:2]) is None


def test_classic_engine_executes_fills_costs_exit_and_trade_metadata(tmp_path: Path) -> None:
    frame = _frame(70)
    for column in ("open", "high", "low", "volume"):
        frame[column] = frame["close"]
    strategy = BtcFundingBasisCrowding60mStrategy(
        funding_percentile_threshold=.95, basis_threshold_bps=0.0,
    )
    strategy.funding_history["BTCUSDT"].extend([.001] * 25_920)
    strategy.completed_buckets["BTCUSDT"].extend(
        (
            pd.Timestamp("2022-12-31T18:00:00Z") + pd.Timedelta(minutes=5 * index),
            99.0 + index / 100,
            True,
        )
        for index in range(72)
    )
    run = tmp_path / "run"
    engine = BacktestEngine(
        datafeed=HistoricalDataFeed(frame),
        universe=UniverseEngine(min_history_bars=1, lookback_bars=1, min_avg_volume=0.0, lag_bars=0),
        strategy=strategy,
        risk=RiskEngine(max_positions=1, config={"risk": {"mode": "r_fixed", "r_per_trade": .005, "stop": {}}}),
        execution=ExecutionModel(fee_model=FeeModel(maker_fee_bps=6, taker_fee_bps=6), slippage_model=SlippageModel(k=.0002), delay_bars=1),
        portfolio=Portfolio(initial_cash=10_000, max_leverage=1),
        decisions_writer=JsonlWriter(run / "decisions.jsonl"),
        fills_writer=JsonlWriter(run / "fills.jsonl"),
        trades_writer=TradesCsvWriter(run / "trades.csv"),
        equity_path=run / "equity.csv", config={},
    )
    engine.run()
    fills = (run / "fills.jsonl").read_text().splitlines()
    trades = pd.read_csv(run / "trades.csv")
    assert len(fills) >= 2
    assert not trades.empty
    assert trades.iloc[0]["funding_source_ts"] == "2023-01-01T00:04:00+00:00"
    assert float(trades.iloc[0]["fees"]) > 0


def test_execute_registered_retains_per_variant_truth_and_finalized_bundles(
    tmp_path: Path, monkeypatch,
) -> None:
    frame = _frame(600)
    for column in ("open", "high", "low", "volume"):
        frame[column] = frame["close"]
    frame["basis_close_vs_index"] = frame["mark_close"] / frame["index_close"] - 1.0
    data_path = tmp_path / "panel.parquet"
    frame.to_parquet(data_path, index=False)

    base = _assignment()
    card = draft_research_card(base, repository_root=str(ROOT))
    qualification = qualify_card(
        confirm_card(card, actor="founder-operator", confirmed_at="2026-09-18T00:00:00Z"),
        repository_root=str(ROOT),
    )
    assignment = base | {
        "base_ref": "1" * 40,
        "campaign_id": "11111111-1111-4111-8111-111111111111",
        "campaign_digest": "2" * 64,
        "source_candidate_id": "22222222-2222-4222-8222-222222222222",
        "source_candidate_digest": "3" * 64,
        "domain_key": "crypto-perpetuals",
        "dataset_path": str(data_path),
        "tier": "Tier2B",
        "bundle_root": str(tmp_path / "retained"),
        "memory_database": str(tmp_path / "memory.sqlite3"),
        "research_context": {"corpus_digest": "4" * 64, "abstained": True, "citations": []},
        "qualification": qualification,
    }
    monkeypatch.setattr(assignment_runner, "governed_review_verified", lambda *_: True)
    monkeypatch.setattr(
        assignment_runner,
        "required_trade_logging_evaluation",
        lambda path, fields: {
            "schema_version": "alpha-required-trade-logging-v1.1.0",
            "trade_count": 0,
            "required_fields": list(fields),
            "missing_columns": [],
            "null_fields": {},
            "invalid_fields": {},
            "passed": True,
            "record_digest": "5" * 64,
        },
    )
    result = assignment_runner.execute_registered(
        assignment, ROOT, tmp_path / "output", max_workers=1
    )
    assert result["disposition"] == "native_execution_complete"
    assert result["alpha_campaign_attempt"]["outcome"] == "failed"
    assert result["alpha_campaign_attempt"]["failure_stage"] == "truth_gate"
    assert result["alpha_campaign_attempt"]["gate_report"]["point_in_time_valid"] is True
    assert result["alpha_campaign_attempt"]["gate_report"]["out_of_sample_evaluated"] is False
    assert result["alpha_campaign_attempt"]["gate_report"]["cost_stress_evaluated"] is False
    assert result["publication_envelope"]["trial"]["held_out_evaluation"] is None
    assert not {
        "oos_trade_count", "oos_mean_net_r", "double_cost_oos_mean_net_r"
    } & set(result["metrics"])
    assert not {
        "oos_trade_support", "positive_oos_net_edge", "double_cost_oos_edge"
    } & set(result["alpha_campaign_attempt"]["gate_report"]["failed_gates"])
    manifests = sorted((tmp_path / "output").glob("run-bundles-*/bundles/*/run_bundle_manifest.json"))
    assert len(manifests) == 4
    evaluations = []
    for manifest in manifests:
        bundle = manifest.parent / "artifacts"
        evaluations.append(
            yaml.safe_load((bundle / "funding_basis_validation_evaluation.json").read_text())
        )
        assert (bundle / "selection_bias_audit.json").is_file()
        assert (bundle / "representation_leakage_report.json").is_file()
        fast_path = yaml.safe_load((bundle / "fast_path_status.json").read_text())
        assert fast_path["mode"] == "classic"
        assert fast_path["actual_engine"] == "classic"
    assert {tuple(sorted(item["parameters"].items())) for item in evaluations} == {
        (("basis_threshold_bps", 0.0), ("funding_percentile_threshold", .95)),
        (("basis_threshold_bps", 0.0), ("funding_percentile_threshold", .99)),
        (("basis_threshold_bps", 1.0), ("funding_percentile_threshold", .95)),
        (("basis_threshold_bps", 1.0), ("funding_percentile_threshold", .99)),
    }
    assert result["truth"]["status"] == "PASS"
    assert not list((tmp_path / "output").glob(
        "run-bundles-*/bundles/*/artifacts/funding_basis_matched_evaluation.json"
    ))
    not_evaluated = list((tmp_path / "output").glob(
        "run-bundles-*/bundles/*/artifacts/funding_basis_heldout_not_evaluated.json"
    ))
    assert len(not_evaluated) == 1
    assert yaml.safe_load(not_evaluated[0].read_text())["held_out_evaluated"] is False

    def positive_evaluation(frame, *, params, **_kwargs):
        return {
            "schema_version": "btc-funding-basis-matched-evaluation-v1.0.0",
            "question": QUESTION,
            "parameters": dict(params),
            "outcome": "positive",
            "decision_records": [],
            "pairs": [],
            "matched_support": 40,
            "treated_support": 40,
            "control_support": 80,
            "treated_minus_control_mean": -0.01,
            "confidence_interval_95": {"lower": -0.015, "upper": -0.005},
            "confidence_interval_method": "overlap_component_cluster_robust_60m",
            "doubled_cost_treated_minus_control": -0.0064,
            "directional_support": {
                "positive_trailing_return": 20,
                "nonpositive_trailing_return": 20,
            },
            "passed": True,
        }

    monkeypatch.setattr(
        assignment_runner, "funding_basis_matched_evaluation", positive_evaluation
    )
    positive_assignment = assignment | {
        "bundle_root": str(tmp_path / "retained-positive"),
        "memory_database": str(tmp_path / "memory-positive.sqlite3"),
    }
    positive = assignment_runner.execute_registered(
        positive_assignment, ROOT, tmp_path / "output-positive", max_workers=1
    )
    positive_attempt = positive["alpha_campaign_attempt"]
    assert positive_attempt["outcome"] == "positive"
    assert positive_attempt["failure_stage"] is None
    assert positive_attempt["gate_report"]["failed_gates"] == []
    assert positive_attempt["gate_report"]["shadow_eligible"] is False
    assert positive_attempt["gate_report"]["qualification_authority"] is True
    assert positive["publication_envelope"]["trial"]["held_out_evaluation"] is not None
    assert (tmp_path / "output-positive" / "execution-result.json").is_file()
    assert len(positive_attempt["evidence_digests"]) >= 4
    heldout = list((tmp_path / "output-positive").glob(
        "run-bundles-*/bundles/*/artifacts/funding_basis_matched_evaluation.json"
    ))
    assert len(heldout) == 1
