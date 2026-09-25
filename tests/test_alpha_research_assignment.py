from pathlib import Path
from types import SimpleNamespace
import pandas as pd
import pytest

from bt.evaluation.alpha_research import (
    complete_five_minute_bars,
    complete_timeframe_bars,
    held_out_trade_evaluation,
    impact_proxy_evaluation,
    required_trade_logging_evaluation,
)
from bt.governance.research_bridge import BridgeError

from scripts.run_alpha_research_assignment import (
    AUTHORITY,
    attach_adaptive_features,
    causal_warmup_receipt,
    engineering_required,
    execute_registered,
    execution_scope,
    downstream_reuse_manifest,
    file_digest,
    independent_review_required,
    hypothesis_identity,
    materialize_execution_panel,
    representation,
    record_alpha_memory,
    retain_bundle,
    prepare_execution_output,
    finalize_execution_output,
)


def assignment():
    return {
        "base_ref": "a" * 40,
        "campaign_id": "11111111-1111-4111-8111-111111111111",
        "campaign_digest": "b" * 64,
        "source_candidate_id": "22222222-2222-4222-8222-222222222222",
        "source_candidate_digest": "c" * 64,
        "question": "Does a new weekend interaction survive costs?",
        "question_digest": "d" * 64,
        "domain_key": "market-microstructure",
        "dataset_build_id": "33333333-3333-4333-8333-333333333333",
        "dataset_digest": "e" * 64,
        "instrument": "BTCUSDT",
        "timeframe": "1m",
        "research_context": {
            "corpus_digest": "f" * 64,
            "abstained": False,
            "citations": [
                {
                    "object_id": "44444444-4444-4444-8444-444444444444",
                    "content_digest": "1" * 64,
                    "coordinates": {"page": 4, "line_start": 2, "line_end": 9},
                    "text": "Weekend liquidity is structurally distinct.",
                    "confidence": 0.8,
                }
            ],
        },
    }


def test_explicit_registered_identity_is_only_taken_from_typed_marker() -> None:
    assert hypothesis_identity("hypothesis_id: L1-H9B") == "L1-H9B"
    prose = "Run something similar to L1-H9B but with a weekend interaction"
    assert hypothesis_identity(prose) == prose


def test_adaptive_fields_are_attached_only_at_their_decision_timestamp(tmp_path):
    source = pd.DataFrame({
        "ts": pd.to_datetime([
            "2025-01-01T00:14:00Z",
            "2025-01-01T00:15:00Z",
        ]),
        "symbol": ["BTCUSDT", "BTCUSDT"],
        "open": [1.0, 1.0],
        "high": [1.0, 1.0],
        "low": [1.0, 1.0],
        "close": [1.0, 1.0],
        "volume": [1.0, 1.0],
    })
    source_path = tmp_path / "source.parquet"
    source.to_parquet(source_path, index=False)
    materialized = SimpleNamespace(
        receipt={"output_fields": ["btc_log_return"]},
        frame=pd.DataFrame({
            "decision_at": pd.to_datetime(["2025-01-01T00:15:00Z"]),
            "btc_log_return": [0.02],
        }),
    )

    result_path = attach_adaptive_features(
        source_path,
        materialized,
        output=tmp_path,
        declared_fields=["btc_log_return"],
    )

    result = pd.read_parquet(result_path)
    assert pd.isna(result.loc[0, "btc_log_return"])
    assert result.loc[1, "btc_log_return"] == pytest.approx(0.02)


def test_adaptive_fields_must_match_reviewed_strategy_contract(tmp_path):
    source_path = tmp_path / "source.parquet"
    pd.DataFrame({"ts": pd.to_datetime(["2025-01-01T00:00:00Z"])}).to_parquet(
        source_path, index=False
    )
    materialized = SimpleNamespace(
        receipt={"output_fields": ["expected_field"]},
        frame=pd.DataFrame(),
    )
    with pytest.raises(BridgeError, match="differ from the frozen plan"):
        attach_adaptive_features(
            source_path,
            materialized,
            output=tmp_path,
            declared_fields=["other_field"],
        )


def test_unreviewed_qualification_stops_before_compute_or_output_creation(tmp_path):
    value = assignment()
    value["qualification"] = {"qualified": True}
    output = tmp_path / "must-not-be-created"
    with pytest.raises(BridgeError, match="no compute started"):
        execute_registered(value, tmp_path, output)
    assert not output.exists()


def test_interrupted_execution_is_preserved_and_completed_retry_is_idempotent(
    tmp_path,
) -> None:
    value = assignment()
    output = tmp_path / "attempt"
    assert prepare_execution_output(output, value) is None
    (output / "partial-artifact.json").write_text("{}\n", encoding="utf-8")

    assert prepare_execution_output(output, value) is None
    partials = list(tmp_path.glob(".attempt.partial-*"))
    assert len(partials) == 1
    assert (partials[0] / "partial-artifact.json").is_file()

    result = {"disposition": "native_execution_complete", "value": 7}
    finalize_execution_output(output, value, result)
    assert prepare_execution_output(output, value) == result


def test_execution_panel_materializes_every_digest_bound_basket_member(tmp_path) -> None:
    timestamps = pd.date_range("2025-01-01T00:00:00Z", periods=10, freq="1min")
    bindings = []
    for position, instrument in enumerate(("BTCUSDT", "ETHUSDT")):
        path = tmp_path / f"{instrument}.parquet"
        pd.DataFrame(
            {
                "ts": timestamps,
                "symbol": instrument,
                "open": 100.0 + position,
                "high": 101.0 + position,
                "low": 99.0 + position,
                "close": 100.5 + position,
                "volume": 1000.0,
            }
        ).to_parquet(path, index=False)
        bindings.append(
            {
                "dataset_build_id": f"{position + 3}" * 8 + "-3333-4333-8333-333333333333",
                "dataset_digest": file_digest(path),
                "dataset_path": str(path),
                "dataset_key": instrument.lower(),
                "instrument": instrument,
                "venue": "bybit",
            }
        )
    value = assignment() | {
        "dataset_path": str(tmp_path / "BTCUSDT.parquet"),
        "dataset_key": "btcusdt",
        "dataset_bindings": bindings,
        "instruments": ["BTCUSDT", "ETHUSDT"],
        "window_start": "2025-01-01T00:00:00Z",
        "window_end": "2025-01-01T00:10:00Z",
    }
    output = tmp_path / "output"
    output.mkdir()
    path, aggregate_digest, panels = materialize_execution_panel(value, output)
    combined = pd.read_parquet(path)
    assert set(combined["symbol"]) == {"BTCUSDT", "ETHUSDT"}
    assert len(combined) == 20
    assert set(panels) == {"BTCUSDT", "ETHUSDT"}
    assert len(aggregate_digest) == 64


def test_execution_panel_materializes_explicit_causal_warmup(tmp_path) -> None:
    timestamps = pd.date_range("2024-12-31T23:55:00Z", periods=15, freq="1min")
    path = tmp_path / "BTCUSDT.parquet"
    pd.DataFrame(
        {
            "ts": timestamps,
            "symbol": "BTCUSDT",
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 1000.0,
        }
    ).to_parquet(path, index=False)
    value = assignment() | {
        "dataset_path": str(path),
        "window_start": "2025-01-01T00:00:00Z",
        "window_end": "2025-01-01T00:10:00Z",
    }
    output = tmp_path / "output"
    output.mkdir()
    destination, _, panels = materialize_execution_panel(
        value, output, warmup_bars=5, warmup_timeframe="1m"
    )
    materialized = pd.read_parquet(destination)
    assert materialized["ts"].min() == pd.Timestamp("2024-12-31T23:55:00Z")
    assert len(materialized) == 15
    assert len(panels["BTCUSDT"]) == 15


def test_causal_warmup_receipt_requires_complete_pre_evaluation_decisions() -> None:
    materialized = SimpleNamespace(
        frame=pd.DataFrame(
            {
                "decision_at": pd.date_range(
                    "2024-12-31T23:56:00Z", periods=5, freq="1min"
                )
            }
        )
    )
    receipt = causal_warmup_receipt(
        materialized,
        official_start="2025-01-01T00:00:00Z",
        warmup_bars=5,
        warmup_timeframe="1m",
    )
    assert receipt["materialized_complete_bars"] == 5
    assert receipt["orders_permitted_during_warmup"] is False
    with pytest.raises(BridgeError, match="lacks the declared causal warmup"):
        causal_warmup_receipt(
            materialized,
            official_start="2025-01-01T00:00:00Z",
            warmup_bars=6,
            warmup_timeframe="1m",
        )


def test_commissioning_scope_is_review_contained_and_non_qualifying() -> None:
    value = assignment() | {
        "execution_class": "commissioning",
        "window_start": "2023-01-01T00:00:00Z",
        "window_end": "2023-02-01T00:00:00Z",
        "max_variants": 8,
    }
    qualification = {
        "window": {
            "start": "2023-01-01T00:00:00Z",
            "end": "2024-01-01T00:00:00Z",
        }
    }
    scope = execution_scope(value, qualification)
    assert scope["qualification_authority"] is False
    assert scope["execution_class"] == "commissioning"
    assert scope["commissioning_window"] == {
        "start": "2023-01-01T00:00:00+00:00",
        "end": "2023-02-01T00:00:00+00:00",
    }
    assert scope["reviewed_window"] == qualification["window"]


def test_downstream_manifest_separates_learning_ml_and_rl_authority(tmp_path) -> None:
    for name in (
        "decisions.jsonl",
        "trades.csv",
        "representation_contract.json",
        "search_plan.json",
        "evaluation.json",
    ):
        (tmp_path / name).write_text(name, encoding="utf-8")
    document = downstream_reuse_manifest(
        tmp_path,
        assignment=assignment(),
        representation_digest="2" * 64,
        search_plan_digest="3" * 64,
        evaluation_artifact="evaluation.json",
        variant_index=2,
        selected_for_holdout=True,
    )
    assert document["readiness"]["institutional_learning"]["state"] == "ready"
    assert document["readiness"]["ml002"]["state"] == "materialization_candidate"
    assert document["readiness"]["rl001"]["state"] == "not_ready"
    assert document["authority"] == AUTHORITY
    assert document["artifacts"]["trades.csv"] == file_digest(tmp_path / "trades.csv")


def test_qualification_window_must_equal_independently_reviewed_window() -> None:
    value = assignment() | {
        "window_start": "2023-01-01T00:00:00Z",
        "window_end": "2023-02-01T00:00:00Z",
        "max_variants": 8,
    }
    qualification = {
        "window": {
            "start": "2023-01-01T00:00:00Z",
            "end": "2024-01-01T00:00:00Z",
        }
    }
    with pytest.raises(BridgeError, match="differs from review"):
        execution_scope(value, qualification)


@pytest.mark.parametrize(
    ("start", "end", "message"),
    [
        ("2022-12-31T00:00:00Z", "2023-01-15T00:00:00Z", "outside"),
        ("2023-01-01T00:00:00Z", "2023-03-01T00:00:00Z", "31 days"),
    ],
)
def test_commissioning_rejects_scope_expansion(start, end, message) -> None:
    value = assignment() | {
        "execution_class": "commissioning",
        "window_start": start,
        "window_end": end,
        "max_variants": 8,
    }
    qualification = {
        "window": {
            "start": "2023-01-01T00:00:00Z",
            "end": "2024-01-01T00:00:00Z",
        }
    }
    with pytest.raises(BridgeError, match=message):
        execution_scope(value, qualification)


def test_independence_failure_retains_zero_trial_operational_evidence(tmp_path):
    result = independent_review_required(assignment(), tmp_path, "unbound review")
    attempt = result["alpha_campaign_attempt"]
    assert attempt["outcome"] == "failed"
    assert attempt["failure_stage"] == "independent_evaluation"
    assert attempt["trial_count"] == 0
    assert (tmp_path / "independent-review-failure.json").is_file()


def test_unsupported_question_retains_citations_and_zero_trials(tmp_path: Path) -> None:
    result = engineering_required(assignment(), tmp_path, "not registered")
    attempt = result["alpha_campaign_attempt"]
    assert result["disposition"] == "engineering_required"
    assert attempt["trial_count"] == 0
    assert attempt["outcome"] == "failed"
    assert attempt["failure_stage"] == "strategy_generation"
    assert attempt["gate_report"]["shadow_eligible"] is False
    assert result["hypothesis_card"]["research_context"]["citations"]
    assert set(AUTHORITY.values()) == {False}
    assert (tmp_path / "strategy-engineering-requirement.json").is_file()


def test_held_out_evaluation_is_temporal_and_doubles_observed_costs(
    tmp_path: Path,
) -> None:
    import pandas as pd

    pd.DataFrame(
        {
            "entry_ts": [
                "2026-01-01T00:00:00Z",
                "2026-02-01T00:00:00Z",
                "2026-02-02T00:00:00Z",
            ],
            "identity_ts_signal": [
                "2026-01-01T00:00:00Z",
                "2026-02-01T00:00:00Z",
                "2026-02-02T00:00:00Z",
            ],
            "r_net": [9.0, 0.5, 0.25],
            "cost_drag_r": [0.1, 0.1, 0.1],
        }
    ).to_csv(tmp_path / "trades.csv", index=False)
    report = held_out_trade_evaluation(tmp_path, "2026-02-01T00:00:00Z")
    assert report["trade_count"] == 2
    assert report["mean_net_r"] == 0.375
    assert report["double_cost_mean_net_r"] == 0.275
    assert report["adequate_support"] is False


def test_held_out_membership_uses_decision_not_entry_fill(tmp_path: Path) -> None:
    pd.DataFrame(
        {
            "entry_ts": ["2026-02-01T00:00:00Z"],
            "identity_ts_signal": ["2026-01-31T23:59:00Z"],
            "r_net": [10.0],
            "cost_drag_r": [0.1],
        }
    ).to_csv(tmp_path / "trades.csv", index=False)
    report = held_out_trade_evaluation(tmp_path, "2026-02-01T00:00:00Z")
    assert report["trade_count"] == 0


def test_required_trade_logging_fails_closed_on_null_risk_fields(tmp_path: Path) -> None:
    complete = {
        "identity_ts_signal": ["2026-01-01T00:00:00Z"],
        "requested_risk_amount": [100.0],
        "risk_amount": [80.0],
        "risk_utilization_pct": [0.8],
        "under_risked_trade": [True],
    }
    pd.DataFrame(complete).to_csv(tmp_path / "trades.csv", index=False)
    assert required_trade_logging_evaluation(tmp_path)["passed"] is True
    complete["requested_risk_amount"] = [None]
    pd.DataFrame(complete).to_csv(tmp_path / "trades.csv", index=False)
    report = required_trade_logging_evaluation(tmp_path)
    assert report["passed"] is False
    assert report["null_fields"] == {"requested_risk_amount": 1}


def test_required_trade_logging_enforces_complete_declared_contract(
    tmp_path: Path,
) -> None:
    row = {
        "identity_ts_signal": "2026-01-01T00:00:00Z",
        "requested_risk_amount": 100.0,
        "risk_amount": 80.0,
        "risk_utilization_pct": 0.8,
        "under_risked_trade": True,
        "decision_trace": '{"reason_code":"impact-reversal"}',
        "impact_proxy": 0.01,
    }
    pd.DataFrame([row]).to_csv(tmp_path / "trades.csv", index=False)
    report = required_trade_logging_evaluation(
        tmp_path, ["decision_trace", "impact_proxy", "target_exit_ts"]
    )
    assert report["passed"] is False
    assert report["missing_columns"] == ["target_exit_ts"]
    assert report["declared_fields"] == [
        "decision_trace",
        "impact_proxy",
        "target_exit_ts",
    ]

    row["target_exit_ts"] = "2026-01-01T00:30:00Z"
    row["decision_trace"] = "not-json"
    pd.DataFrame([row]).to_csv(tmp_path / "trades.csv", index=False)
    report = required_trade_logging_evaluation(
        tmp_path, ["decision_trace", "impact_proxy", "target_exit_ts"]
    )
    assert report["passed"] is False
    assert report["invalid_fields"] == {"decision_trace": 1}


def test_representation_applies_horizon_aware_split_gaps() -> None:
    import pandas as pd

    timestamps = pd.date_range(
        "2026-01-01T00:00:00Z", periods=240, freq="1min"
    )
    frame = pd.DataFrame(
        {"ts": timestamps, "symbol": "BTCUSDT", "close": 100.0}
    )
    contract, report = representation(
        assignment(),
        frame,
        "f" * 64,
        purge_seconds=1800,
        embargo_seconds=1800,
    )
    split = contract.split
    assert pd.Timestamp(split.train_start) == timestamps[0] + pd.Timedelta(minutes=1)
    assert pd.Timestamp(split.validation_start) - pd.Timestamp(split.train_end) > pd.Timedelta(minutes=30)
    assert pd.Timestamp(split.test_start) - pd.Timestamp(split.validation_end) > pd.Timedelta(minutes=30)
    assert split.purge_seconds == 1800
    assert split.embargo_seconds == 1800
    assert report["status"] == "certified"


def test_representation_sorts_source_and_rejects_duplicate_minutes() -> None:
    frame = pd.DataFrame(
        {
            "ts": [
                "2026-01-01T00:01:00Z",
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:01:00Z",
            ],
            "symbol": ["BTCUSDT"] * 3,
            "close": [101.0, 100.0, 101.0],
        }
    )
    with pytest.raises(BridgeError, match="duplicate instrument timestamps"):
        representation(assignment(), frame, "f" * 64)


def test_representation_uses_complete_five_minute_decision_rows() -> None:
    timestamps = pd.date_range("2026-01-01T00:00:00Z", periods=240, freq="1min")
    frame = pd.DataFrame(
        {
            "ts": timestamps,
            "symbol": "BTCUSDT",
            "close": 100.0,
            "quote_volume": 1_000_000.0,
        }
    )
    contract, _ = representation(
        assignment(), frame, "f" * 64,
        purge_seconds=1800, embargo_seconds=1800, decision_timeframe="5m",
    )
    assert pd.Timestamp(contract.split.train_start) == timestamps[0] + pd.Timedelta(minutes=5)


def test_incomplete_five_minute_bucket_is_excluded_from_decision_row_splits() -> None:
    timestamps = pd.date_range("2026-01-01T00:00:00Z", periods=240, freq="1min")
    frame = pd.DataFrame(
        {
            "ts": timestamps,
            "symbol": "BTCUSDT",
            "close": 100.0,
            "quote_volume": 1_000_000.0,
        }
    )
    complete, _ = representation(
        assignment(), frame, "f" * 64,
        purge_seconds=1800, embargo_seconds=1800, decision_timeframe="5m",
    )
    missing, _ = representation(
        assignment(), frame.drop(index=100), "f" * 64,
        purge_seconds=1800, embargo_seconds=1800, decision_timeframe="5m",
    )
    assert missing.split != complete.split
    assert pd.Timestamp(missing.split.train_end) == (
        pd.Timestamp(complete.split.train_end) + pd.Timedelta(minutes=5)
    )
    assert pd.Timestamp(missing.split.validation_start) == (
        pd.Timestamp(complete.split.validation_start) + pd.Timedelta(minutes=5)
    )


def test_impact_proxy_evaluation_uses_complete_causal_five_minute_bars() -> None:
    import pandas as pd

    rows = []
    timestamp = pd.Timestamp("2026-01-01T00:00:00Z")
    close = 100.0
    # 25 complete bars provide a trailing window plus held-out extremes and controls.
    for minute in range(125):
        bucket = minute // 5
        step = 0.0001
        if bucket in {12, 18} and minute % 5 == 4:
            step = 0.02
        elif bucket in {14, 20} and minute % 5 == 4:
            step = 0.019
        close *= 1.0 + step
        rows.append(
            {
                "ts": timestamp + pd.Timedelta(minutes=minute),
                "symbol": "BTCUSDT",
                "close": close,
                "quote_volume": 400_000.0 if bucket in {12, 18} else 2_000_000.0,
            }
        )
    report = impact_proxy_evaluation(
        pd.DataFrame(rows),
        test_start="2026-01-01T00:50:00Z",
        params={
            "impact_proxy_threshold": 0.8,
            "normalization_window": 4,
            "return_shock_control_band": 0.2,
        },
    )
    assert report["schema_version"] == "alpha-impact-proxy-evaluation-v1.2.0"
    assert report["direction_balance"]["short"] >= 1
    assert report["matched_return_shock_control"]["extreme_observations"] >= 1
    assert report["matched_return_shock_control"]["control_reuse"] is False
    assert "paired_difference_lower_95" in report["matched_return_shock_control"]
    assert report["direction_balance"]["minimum_per_direction"] == 10
    assert report["direction_balance"]["balanced_positive_reversal"] is False
    assert "record_digest" in report


def _impact_rows(bucket_starts: list[pd.Timestamp]) -> pd.DataFrame:
    rows = []
    close = 100.0
    for bucket_start in bucket_starts:
        for minute in range(5):
            close *= 1.001
            rows.append(
                {
                    "ts": bucket_start + pd.Timedelta(minutes=minute),
                    "symbol": "BTCUSDT",
                    "close": close,
                    "quote_volume": 2_000_000.0,
                }
            )
    return pd.DataFrame(rows)


def test_impact_proxy_evaluation_resets_state_across_bucket_gaps() -> None:
    start = pd.Timestamp("2026-01-01T00:00:00Z")
    bucket_starts = [start + pd.Timedelta(minutes=5 * index) for index in range(12)]
    bucket_starts += [
        start + pd.Timedelta(minutes=65 + 5 * index) for index in range(12)
    ]

    report = impact_proxy_evaluation(
        _impact_rows(bucket_starts),
        test_start=start.isoformat(),
        params={
            "impact_proxy_threshold": 0.8,
            "normalization_window": 2,
            "return_shock_control_band": 0.2,
        },
    )

    assert report["evaluated_observations"] == 6
    assert report["gap_policy"] == "reset_return_normalization_and_atr_state"
    assert report["target_path_policy"] == (
        "six_contiguous_complete_5m_buckets_after_decision"
    )


def test_impact_proxy_evaluation_filters_on_observable_decision_time() -> None:
    start = pd.Timestamp("2026-01-01T00:00:00Z")
    bucket_starts = [start + pd.Timedelta(minutes=5 * index) for index in range(12)]

    report = impact_proxy_evaluation(
        _impact_rows(bucket_starts),
        test_start=(start + pd.Timedelta(minutes=20)).isoformat(),
        params={
            "impact_proxy_threshold": 0.8,
            "normalization_window": 2,
            "return_shock_control_band": 0.2,
        },
    )

    assert report["evaluated_observations"] == 3
    assert report["decision_time"] == "bucket_start_plus_5m"


def test_impact_proxy_evaluation_rejects_duplicate_minutes() -> None:
    import pandas as pd

    frame = pd.DataFrame(
        {
            "ts": ["2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"],
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "close": [100.0, 100.0],
            "quote_volume": [1_000_000.0, 1_000_000.0],
        }
    )
    with pytest.raises(BridgeError, match="duplicate minute bars"):
        impact_proxy_evaluation(
            frame,
            test_start="2026-01-01T00:00:00Z",
            params={
                "impact_proxy_threshold": 0.8,
                "normalization_window": 4,
                "return_shock_control_band": 0.2,
            },
        )


def test_impact_proxy_target_requires_exact_wall_clock_horizon() -> None:
    rows = []
    start = pd.Timestamp("2026-01-01T00:00:00Z")
    for minute in range(60):
        if 30 <= minute < 35:
            continue
        rows.append(
            {
                "ts": start + pd.Timedelta(minutes=minute),
                "symbol": "BTCUSDT",
                "close": 100.0 + minute,
                "quote_volume": 1_000_000.0,
            }
        )
    bars = complete_five_minute_bars(pd.DataFrame(rows))
    assert pd.Timestamp("2026-01-01T00:30:00Z") not in set(bars["ts"])


def test_structural_reconstruction_supports_nonstandard_minute_timeframes() -> None:
    start = pd.Timestamp("2026-01-01T00:00:00Z")
    frame = pd.DataFrame(
        {
            "ts": pd.date_range(start, periods=14, freq="1min"),
            "symbol": "BTCUSDT",
            "close": range(14),
        }
    )
    bars = complete_timeframe_bars(frame, "7m")
    assert list(bars["ts"]) == [start, start + pd.Timedelta(minutes=7)]
    assert list(bars["close"]) == [6, 13]


def test_structural_reconstruction_rejects_partial_quote_volume_bucket() -> None:
    start = pd.Timestamp("2026-01-01T00:00:00Z")
    frame = pd.DataFrame(
        {
            "ts": pd.date_range(start, periods=10, freq="1min"),
            "symbol": "BTCUSDT",
            "close": range(10),
            "quote_volume": [1_000_000.0] * 4
            + [float("nan")]
            + [1_000_000.0] * 5,
        }
    )

    bars = complete_timeframe_bars(frame, "5m")

    assert list(bars["ts"]) == [start + pd.Timedelta(minutes=5)]
    assert list(bars["quote_volume"]) == [5_000_000.0]


def test_structural_reconstruction_rejects_negative_quote_volume_bucket() -> None:
    start = pd.Timestamp("2026-01-01T00:00:00Z")
    frame = pd.DataFrame(
        {
            "ts": pd.date_range(start, periods=10, freq="1min"),
            "symbol": "BTCUSDT",
            "close": range(10),
            "quote_volume": [-1.0] + [1_000_000.0] * 9,
        }
    )

    bars = complete_timeframe_bars(frame, "5m")

    assert list(bars["ts"]) == [start + pd.Timedelta(minutes=5)]
    assert list(bars["quote_volume"]) == [5_000_000.0]


def test_durable_bundle_and_native_memory_are_idempotent(tmp_path: Path) -> None:
    import json

    source = tmp_path / "source"
    source.mkdir()
    bundle = {"bundle_digest": "9" * 64, "manifest_digest": "8" * 64}
    (source / "run_bundle_manifest.json").write_text(
        json.dumps({"manifest_digest": bundle["manifest_digest"]}),
        encoding="utf-8",
    )
    destination = retain_bundle(source, tmp_path / "bundles", bundle)
    assert retain_bundle(source, tmp_path / "bundles", bundle) == destination

    first = record_alpha_memory(
        tmp_path / "memory.sqlite", assignment=assignment(), bundle=bundle
    )
    second = record_alpha_memory(
        tmp_path / "memory.sqlite", assignment=assignment(), bundle=bundle
    )
    assert first["disposition"] == "created"
    assert second["disposition"] == "existing"
    assert first["memory_database_digest"] == second["memory_database_digest"]
