from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

from bt.core.types import Bar
from bt.governance.alpha_strategy_pipeline import (
    confirm_card,
    draft_research_card,
    qualify_card,
)
from bt.contracts.research_specs_v2 import canonical_hash
from bt.hypotheses.contract import HypothesisContract
from bt.strategy.sol_to_eth_2h_tail_return import (
    EXPECTED_REPRESENTATION_PLAN_DIGEST,
    FROZEN_GRID,
    OUTPUT_FIELDS,
    QUESTION,
    SolToEth2hTailReturnStrategy,
    compile_decision_rows,
    sol_to_eth_tail_grid_evaluation,
)
import bt.strategy.sol_to_eth_2h_tail_return as strategy
import scripts.run_alpha_research_assignment as assignment_runner
from bt.validation.strategy_admission import validate_hypothesis_admission
from scripts.run_alpha_research_assignment import (
    BridgeError,
    materialize_causal_feature_frame,
    verify_trusted_dataset_bindings,
)

ROOT = Path(__file__).parents[2]
YAML_PATH = ROOT / "research/hypotheses/alpha_003_sol_to_eth_2h_tail_return.yaml"
RAW = yaml.safe_load(YAML_PATH.read_text())
PLAN_DIGEST = RAW["execution_semantics"]["adaptive_representation_plan_digest"]


def _minute_panels(days: int = 8) -> dict[str, pd.DataFrame]:
    timestamps = pd.date_range(
        "2023-01-01T00:00:00Z", periods=days * 24 * 60, freq="1min"
    )
    panels = {}
    for symbol, base, phase in (
        ("ETHUSDT", 1_200.0, 0.7),
        ("SOLUSDT", 12.0, 0.0),
    ):
        minute = np.arange(len(timestamps), dtype=float)
        close = base * np.exp(
            0.00001 * minute + 0.003 * np.sin(minute / 120.0 + phase)
        )
        panels[symbol] = pd.DataFrame(
            {
                "ts": timestamps,
                "symbol": symbol,
                "open": close,
                "high": close * 1.0001,
                "low": close * 0.9999,
                "close": close,
                "volume": 1_000.0,
                "quote_volume": 2_000_000.0,
            }
        )
    return panels


def _overlap_receipt(assignment: dict) -> dict:
    document = {
        "schema_version": "alpha-basket-overlap-admission-v1.0.0",
        "authority": "DATA-002/003",
        "dataset_bindings": [
            {
                key: item[key]
                for key in ("instrument", "dataset_build_id", "dataset_digest")
            }
            for item in assignment["dataset_bindings"]
        ],
        "instruments": sorted(assignment["instruments"]),
        "minimum_contiguous_days": 365,
        "admitted_start": assignment["window_start"],
        "admitted_end": assignment["window_end"],
    }
    return {**document, "record_digest": assignment_runner.digest(document)}


def _attached_frame(decisions: int = 4120) -> pd.DataFrame:
    start = pd.Timestamp("2023-01-01T00:00:00Z")
    # The native contract now rejects partial raw windows. Keep the argument for
    # existing call sites, but always build the exact non-leap frozen year.
    _ = decisions
    minutes = 365 * 24 * 60
    timestamps = pd.date_range(start, periods=minutes, freq="1min")
    minute = np.arange(minutes, dtype=float)
    members = []
    for symbol, base, phase in (
        ("ETHUSDT", 1_200.0, 0.7),
        ("SOLUSDT", 12.0, 0.0),
    ):
        close = base * np.exp(
            0.000001 * minute + 0.003 * np.sin(minute / 120.0 + phase)
        )
        members.append(
            pd.DataFrame(
                {
                    "ts": timestamps,
                    "symbol": symbol,
                    "open": close,
                    "high": close * 1.0001,
                    "low": close * 0.9999,
                    "close": close,
                    "volume": 1_000.0,
                    "quote_volume": 20_000.0,
                }
            )
        )
    frame = pd.concat(members, ignore_index=True)
    for symbol, identity in strategy.EXPECTED_DATASET_IDENTITIES.items():
        mask = frame.symbol.eq(symbol)
        for field, value in identity.items():
            frame.loc[mask, field] = value
    expected = strategy._recompute_representation(frame)
    encoded = json.dumps(list(OUTPUT_FIELDS), separators=(",", ":"))
    frame = frame.merge(expected, left_on="ts", right_on="decision_ts", how="left")
    is_decision = frame.decision_ts.notna()
    frame["prior_only_volatility_source_end_ts"] = None
    frame.loc[is_decision, "prior_only_volatility_source_end_ts"] = (
        frame.loc[is_decision, "ts"] - pd.Timedelta(hours=2)
    ).map(lambda value: value.isoformat())
    frame["representation_plan_digest"] = None
    frame.loc[is_decision, "representation_plan_digest"] = PLAN_DIGEST
    frame["representation_output_fields"] = None
    frame.loc[is_decision, "representation_output_fields"] = encoded
    frame["representation_decision_ts"] = None
    frame.loc[is_decision, "representation_decision_ts"] = frame.loc[
        is_decision, "ts"
    ].map(lambda value: value.isoformat())
    return frame.drop(columns=["decision_ts"])


def test_exact_contract_discovery_digest_and_admission() -> None:
    assignment = {"question": QUESTION, "question_digest": RAW["immutable_contract"]["question_digest"],
                  "dataset_build_id": RAW["immutable_contract"]["dataset_bindings"][0]["dataset_build_id"],
                  "dataset_digest": RAW["immutable_contract"]["dataset_bindings"][0]["dataset_digest"],
                  "venue": "bybit", "instrument": "ETHUSDT", "timeframe": "1m",
                  "window_start": "2023-01-01T00:00:00Z", "window_end": "2024-01-01T00:00:00Z"}
    assert draft_research_card(assignment, repository_root=str(ROOT))["research_question"] == QUESTION
    assert len(HypothesisContract.from_yaml(YAML_PATH).to_run_specs()) == 2
    assert validate_hypothesis_admission(YAML_PATH).status == "PASS"
    assert RAW["immutable_contract"]["required_fields"] == [
        "ts", "open", "high", "low", "close", "volume", "quote_volume"
    ]
    assert [item["producer_receipt_digest"] for item in RAW["immutable_contract"]["dataset_bindings"]] == [
        "f40db3e88a6ab082091269f99b55d2885ca52c938d25a919783f9b5675aefad6",
        "29c440c17effbb3261262299a23f65a99afabf914b2048c92c64a557b9a1245b",
    ]


def test_exact_card_binds_the_final_admitted_representation_plan() -> None:
    assignment = {
        "question": QUESTION,
        "question_digest": RAW["immutable_contract"]["question_digest"],
        "dataset_build_id": RAW["immutable_contract"]["dataset_bindings"][0][
            "dataset_build_id"
        ],
        "dataset_digest": RAW["immutable_contract"]["dataset_bindings"][0][
            "dataset_digest"
        ],
        "venue": "bybit",
        "instrument": "ETHUSDT",
        "timeframe": "1m",
        "window_start": "2023-01-01T00:00:00Z",
        "window_end": "2024-01-01T00:00:00Z",
        "representation_plan": RAW["representation_plan"],
    }

    card = draft_research_card(assignment, repository_root=str(ROOT))

    assert card["execution_semantics"]["adaptive_representation_plan_digest"] == (
        canonical_hash(RAW["representation_plan"])
    )
    assert PLAN_DIGEST == EXPECTED_REPRESENTATION_PLAN_DIGEST


def test_prior_only_materialization_and_native_causality_gate() -> None:
    class Materialized:
        receipt = {"output_fields": list(OUTPUT_FIELDS), "plan_digest": PLAN_DIGEST}
        frame = pd.DataFrame({"decision_at": pd.date_range("2023-01-01", periods=3, freq="2h", tz="UTC"),
                              **{name: [1.0, 2.0, 3.0] for name in OUTPUT_FIELDS}})
    causal = materialize_causal_feature_frame(Materialized())
    assert pd.isna(causal.iloc[0].ethusdt_realized_volatility_24h)
    assert causal.iloc[1].prior_only_volatility_source_end_ts == "2023-01-01T00:00:00+00:00"
    ts = pd.Timestamp("2023-01-01T04:00:00Z")
    encoded = json.dumps(list(OUTPUT_FIELDS), separators=(",", ":"))
    extra = {name: 1.0 for name in OUTPUT_FIELDS} | {
        "prior_only_volatility_source_end_ts": (ts - pd.Timedelta(hours=2)).isoformat(),
        "representation_plan_digest": PLAN_DIGEST, "representation_output_fields": encoded,
        "representation_decision_ts": ts.isoformat()}
    bars = {symbol: Bar(ts=ts, symbol=symbol, open=1, high=1, low=1, close=1, volume=1, extra=extra)
            for symbol in ("ETHUSDT", "SOLUSDT")}
    native = SolToEth2hTailReturnStrategy(adaptive_representation_plan_digest=PLAN_DIGEST)
    assert native.on_bars(ts, bars, set(bars), {})[0].metadata["native_payload_outcome"] == "consumed"
    bad = dict(extra, prior_only_volatility_source_end_ts=ts.isoformat())
    bars["ETHUSDT"] = Bar(ts=ts, symbol="ETHUSDT", open=1, high=1, low=1, close=1, volume=1, extra=bad)
    assert native.on_bars(ts, bars, set(bars), {})[0].metadata["native_payload_outcome"] == "invalid"

    mismatched = dict(extra, solusdt_log_return_2h=2.0)
    bars["ETHUSDT"] = Bar(
        ts=ts, symbol="ETHUSDT", open=1, high=1, low=1, close=1, volume=1,
        extra=extra,
    )
    bars["SOLUSDT"] = Bar(
        ts=ts, symbol="SOLUSDT", open=1, high=1, low=1, close=1, volume=1,
        extra=mismatched,
    )
    assert (
        native.on_bars(ts, bars, set(bars), {})[0].metadata[
            "native_payload_outcome"
        ]
        == "invalid"
    )


def test_evaluator_uses_attached_outputs_and_rejects_provenance_mutation() -> None:
    frame = _attached_frame()
    rows = compile_decision_rows(frame, plan_digest=PLAN_DIGEST)
    assert not rows.empty
    decision_ts = rows.iloc[100].decision_ts
    mutated = frame.copy()
    mutated.loc[mutated.ts.eq(decision_ts), "solusdt_log_return_2h"] = 9.0
    assert compile_decision_rows(mutated, plan_digest=PLAN_DIGEST).empty
    mutated = frame.copy()
    mutated.loc[mutated.ts.eq(decision_ts), "representation_plan_digest"] = "0" * 64
    assert compile_decision_rows(mutated, plan_digest=PLAN_DIGEST).empty


def test_representation_does_not_bridge_missing_clock_buckets() -> None:
    panels = _minute_panels(days=4)
    gap_start = pd.Timestamp("2023-01-02T00:00:00Z")
    gap_end = gap_start + pd.Timedelta(hours=2)
    frame = pd.concat(panels.values(), ignore_index=True)
    frame = frame.loc[
        ~(
            frame.symbol.eq("SOLUSDT")
            & frame.ts.ge(gap_start)
            & frame.ts.lt(gap_end)
        )
    ]

    represented = strategy._recompute_representation(frame).set_index("decision_ts")
    first_after_gap = gap_end + pd.Timedelta(hours=2)

    assert pd.isna(represented.loc[gap_end, "solusdt_log_return_2h"])
    assert pd.isna(represented.loc[first_after_gap, "solusdt_log_return_2h"])


def test_representation_requires_contiguous_prior_24h_volatility() -> None:
    panels = _minute_panels(days=4)
    gap_start = pd.Timestamp("2023-01-02T00:00:00Z")
    gap_end = gap_start + pd.Timedelta(hours=2)
    frame = pd.concat(panels.values(), ignore_index=True)
    frame = frame.loc[
        ~(
            frame.symbol.eq("ETHUSDT")
            & frame.ts.ge(gap_start)
            & frame.ts.lt(gap_end)
        )
    ]

    represented = strategy._recompute_representation(frame).set_index("decision_ts")
    first_valid_after_gap = gap_end + pd.Timedelta(hours=28)

    assert represented.loc[
        gap_end:first_valid_after_gap - pd.Timedelta(hours=2),
        "ethusdt_realized_volatility_24h",
    ].isna().all()
    assert pd.notna(
        represented.loc[first_valid_after_gap, "ethusdt_realized_volatility_24h"]
    )


def test_evaluator_rejects_unpinned_irregular_and_missing_decision_rows() -> None:
    frame = _attached_frame(decisions=40)
    assert compile_decision_rows(frame, plan_digest="0" * 64).empty

    decisions = frame.loc[frame.representation_plan_digest.notna(), "ts"].drop_duplicates()
    irregular = frame.loc[~frame.ts.eq(decisions.iloc[10])].copy()
    assert compile_decision_rows(irregular, plan_digest=PLAN_DIGEST).empty

    misaligned = frame.copy()
    source_ts = decisions.iloc[10]
    mask = misaligned.ts.eq(source_ts)
    misaligned.loc[mask, "ts"] = source_ts + pd.Timedelta(minutes=1)
    misaligned.loc[mask, "representation_decision_ts"] = source_ts + pd.Timedelta(
        minutes=1
    )
    assert compile_decision_rows(misaligned, plan_digest=PLAN_DIGEST).empty


def test_grid_retains_positive_negative_invalid_and_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    frame = _attached_frame()
    # Validation selection requires support only; held-out profitability gates must
    # not be applied while choosing between the two frozen variants.
    outcomes = iter(["negative", "negative", "negative"])
    def fake_score(rows, train, params, partition, plan_digest):
        outcome = next(outcomes)
        return strategy._result(outcome, outcome, params, records=[{"record_kind": "x"}],
                                plan_digest=plan_digest, mean_net_signed_residual_return=1.0,
                                selection_eligible=partition == "validation")
    monkeypatch.setattr(strategy, "_score", fake_score)
    result = sol_to_eth_tail_grid_evaluation(frame, parameter_grid=FROZEN_GRID,
        evaluation_start="2023-01-01T00:00:00Z", evaluation_end="2024-01-01T00:00:00Z",
        representation_plan_digest=PLAN_DIGEST)
    assert result["outcome"] == "negative" and result["test_open_count"] == 1
    audit = result["selection_bias_audit"]
    assert audit["preregistered_variant_count"] == 2
    assert audit["evaluated_variant_count"] == 2
    assert audit["selected_index"] == 0
    assert audit["test_open_count"] == 1
    assert audit["held_out_used_for_selection"] is False
    assert len(audit["candidates"]) == 2
    invalid = sol_to_eth_tail_grid_evaluation(frame, parameter_grid={"wrong": [1]},
        evaluation_start="2023-01-01T00:00:00Z", evaluation_end="2024-01-01T00:00:00Z",
        representation_plan_digest=PLAN_DIGEST)
    assert invalid["outcome"] == "invalid"
    assert invalid["selection_bias_audit"]["test_open_count"] == 0
    assert invalid["selection_bias_audit"]["selected_index"] is None
    monkeypatch.setattr(strategy, "_score", lambda *a, **k: (_ for _ in ()).throw(RuntimeError()))
    failed = sol_to_eth_tail_grid_evaluation(frame, parameter_grid=FROZEN_GRID,
        evaluation_start="2023-01-01T00:00:00Z", evaluation_end="2024-01-01T00:00:00Z",
        representation_plan_digest=PLAN_DIGEST)
    assert failed["outcome"] == "failed" and failed["test_open_count"] == 0
    assert failed["selection_bias_audit"]["evaluated_variant_count"] == 2
    assert failed["selection_bias_audit"]["held_out_used_for_selection"] is False


def test_evaluator_rejects_unbound_or_wrong_dataset_identity() -> None:
    frame = _attached_frame()
    assert not compile_decision_rows(frame, plan_digest=PLAN_DIGEST).empty

    unbound = frame.drop(columns=["source_dataset_digest"])
    assert compile_decision_rows(unbound, plan_digest=PLAN_DIGEST).empty

    incomplete_schema = frame.drop(columns=["open"])
    assert compile_decision_rows(incomplete_schema, plan_digest=PLAN_DIGEST).empty

    wrong = frame.copy()
    wrong.loc[wrong.symbol.eq("SOLUSDT"), "source_dataset_digest"] = "0" * 64
    assert compile_decision_rows(wrong, plan_digest=PLAN_DIGEST).empty

    partly_null = frame.copy()
    partly_null.loc[partly_null.index[0], "source_dataset_digest"] = None
    assert compile_decision_rows(partly_null, plan_digest=PLAN_DIGEST).empty

    for field in (
        "source_catalog_digest",
        "source_manifest_digest",
        "source_producer_receipt_digest",
        "source_lake_governance_digest",
        "source_partition_digest",
    ):
        missing = frame.drop(columns=[field])
        assert compile_decision_rows(missing, plan_digest=PLAN_DIGEST).empty
        altered = frame.copy()
        altered.loc[altered.symbol.eq("ETHUSDT"), field] = "0" * 64
        assert compile_decision_rows(altered, plan_digest=PLAN_DIGEST).empty


def test_evaluator_rejects_non_utc_and_out_of_window_raw_rows() -> None:
    frame = _attached_frame()
    non_utc = frame.copy()
    non_utc["ts"] = non_utc.ts.dt.tz_convert("Europe/Stockholm")
    assert compile_decision_rows(non_utc, plan_digest=PLAN_DIGEST).empty

    out_of_window = frame.copy()
    extra = frame.loc[frame.ts.eq(frame.ts.min())].copy()
    extra["ts"] = extra.ts - pd.Timedelta(minutes=1)
    out_of_window = pd.concat([extra, out_of_window], ignore_index=True)
    assert compile_decision_rows(out_of_window, plan_digest=PLAN_DIGEST).empty


def test_evaluator_rejects_window_outside_frozen_contract() -> None:
    result = sol_to_eth_tail_grid_evaluation(
        _attached_frame(decisions=40),
        parameter_grid=FROZEN_GRID,
        evaluation_start="2023-02-01T00:00:00Z",
        evaluation_end="2024-01-01T00:00:00Z",
        representation_plan_digest=PLAN_DIGEST,
    )
    assert result["outcome"] == "invalid"
    assert result["reason"] == "evaluation_window_differs_from_frozen_contract"
    assert result["test_open_count"] == 0


def test_real_split_preserves_declared_training_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(strategy, "MINIMUM_HISTORY", 12)
    frame = _attached_frame(decisions=90)
    seen_train_sizes: list[int] = []

    def score(rows, train, params, partition, plan_digest):
        seen_train_sizes.append(len(train))
        return strategy._result(
            "negative",
            "falsification_gate_failed",
            params,
            records=[{"record_kind": "scientific_observation"}],
            plan_digest=plan_digest,
            mean_net_signed_residual_return=-0.001,
        )

    monkeypatch.setattr(strategy, "_score", score)
    result = sol_to_eth_tail_grid_evaluation(
        frame,
        parameter_grid=FROZEN_GRID,
        evaluation_start="2023-01-01T00:00:00Z",
        evaluation_end="2024-01-01T00:00:00Z",
        representation_plan_digest=PLAN_DIGEST,
    )

    assert result["outcome"] == "negative"
    assert seen_train_sizes
    assert min(seen_train_sizes) >= strategy.MINIMUM_HISTORY


def test_liquidity_filtered_history_costs_and_same_timestamp_lag_rival() -> None:
    base = {
        "solusdt_log_return_2h": 0.001,
        "ethusdt_log_return_2h": 0.0,
        "ethusdt_realized_volatility_24h": 0.01,
        "solusdt_quote_volume_2h": 2_000_000.0,
        "ethusdt_quote_volume_2h": 2_000_000.0,
        "eth_target_4h": 0.002,
        "sol_lag_return": -0.001,
    }
    train = pd.DataFrame([base | {"solusdt_log_return_2h": 0.001 + i * 1e-9}
                          for i in range(4000)])
    # An illiquid extreme must neither satisfy history nor move the train-only tail.
    train = pd.concat([train, pd.DataFrame([base | {
        "solusdt_log_return_2h": 99.0, "solusdt_quote_volume_2h": 1.0
    }])], ignore_index=True)
    ts = pd.Timestamp("2023-12-01T00:00:00Z")
    rows = pd.DataFrame([base | {"decision_ts": ts, "target_exit_ts": ts + pd.Timedelta(hours=4),
                                 "solusdt_log_return_2h": 0.01, "eth_target_4h": 0.01}])
    result = strategy._score(rows, train, {
        "sol_return_tail_percentile": 0.90, "response_direction": "same",
        "volatility_lookback": 12}, "test", PLAN_DIGEST)
    assert result["support"] == 1
    assert result["outcome"] == "negative"
    assert result["gates"]["minimum_support"] is False
    assert result["gates"]["minimum_support_required"] == 30
    assert result["doubled_cost_mean_net_signed_residual_return"] == pytest.approx(
        result["mean_net_signed_residual_return"] - 0.0009
    )
    assert result["observation_records"][0]["decision_trace"]["same_timestamp_lag_rival"]
    assert result["lag_rival_mean_net_signed_residual_return"] < result["mean_net_signed_residual_return"]
    opposite = rows.copy()
    opposite["eth_target_4h"] = -0.01
    opposite_result = strategy._score(
        opposite,
        train,
        {
            "sol_return_tail_percentile": 0.90,
            "response_direction": "same",
            "volatility_lookback": 12,
        },
        "test",
        PLAN_DIGEST,
    )
    assert opposite_result["mean_net_signed_residual_return"] < 0


def test_trusted_bindings_and_registered_execution_retain_variant_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths: dict[str, Path] = {}
    bindings: list[dict] = []
    for original, (symbol, frame) in zip(
        RAW["immutable_contract"]["dataset_bindings"],
        _minute_panels().items(),
        strict=True,
    ):
        path = tmp_path / f"{symbol}.parquet"
        frame.to_parquet(path, index=False)
        actual_digest = assignment_runner.file_digest(path)
        paths[symbol] = path
        bindings.append(
            original
            | {
                "dataset_digest": actual_digest,
                "partition_digest": actual_digest,
                "dataset_path": str(path),
                "dataset_key": symbol.lower(),
            }
        )

    verify_trusted_dataset_bindings(
        {"dataset_bindings": bindings},
        [{key: item[key] for key in assignment_runner.TRUSTED_BINDING_FIELDS}
         | {"instrument": item["instrument"]} for item in bindings],
    )
    original_bytes = paths["SOLUSDT"].read_bytes()
    paths["SOLUSDT"].write_bytes(original_bytes + b"tampered")
    with pytest.raises(BridgeError, match="materialized dataset content mismatch"):
        verify_trusted_dataset_bindings(
            {"dataset_bindings": bindings},
            [{key: item[key] for key in assignment_runner.TRUSTED_BINDING_FIELDS}
             | {"instrument": item["instrument"]} for item in bindings],
        )
    paths["SOLUSDT"].write_bytes(original_bytes)

    assignment = {
        "base_ref": "a" * 40,
        "campaign_id": "11111111-1111-4111-8111-111111111111",
        "campaign_digest": "b" * 64,
        "source_candidate_id": "22222222-2222-4222-8222-222222222222",
        "source_candidate_digest": "c" * 64,
        "question": QUESTION,
        "question_digest": RAW["immutable_contract"]["question_digest"],
        "domain_key": "cross-asset-micro-alpha",
        "dataset_build_id": bindings[0]["dataset_build_id"],
        "dataset_digest": bindings[0]["dataset_digest"],
        "dataset_path": bindings[0]["dataset_path"],
        "dataset_key": bindings[0]["dataset_key"],
        "dataset_bindings": bindings,
        "instrument": "ETHUSDT",
        "instruments": list(strategy.INSTRUMENTS),
        "venue": "bybit",
        "timeframe": "1m",
        "research_timeframe": "2h",
        "window_start": "2023-01-01T00:00:00Z",
        "window_end": "2023-01-09T00:00:00Z",
        "tier": "Tier2B",
        "max_variants": 2,
        "representation_plan": RAW["representation_plan"],
        "bundle_root": str(tmp_path / "retained"),
        "memory_database": str(tmp_path / "memory.sqlite3"),
        "research_context": {
            "corpus_digest": "d" * 64,
            "abstained": True,
            "citations": [],
        },
        "execution_class": "commissioning",
    }
    assignment["overlap_admission_receipt"] = _overlap_receipt(assignment)
    reviewed_binding = RAW["immutable_contract"]["dataset_bindings"][0]
    review_assignment = assignment | {
        "dataset_build_id": reviewed_binding["dataset_build_id"],
        "dataset_digest": reviewed_binding["dataset_digest"],
        "window_start": RAW["immutable_contract"]["window"]["start"],
        "window_end": RAW["immutable_contract"]["window"]["end"],
    }
    card = confirm_card(
        draft_research_card(review_assignment, repository_root=str(ROOT)),
        actor="founder-operator",
        confirmed_at="2026-09-28T00:00:00Z",
    )
    qualification = qualify_card(card, repository_root=str(ROOT))
    qualification["artifact_bundle"]["engine_hypothesis_yaml"][
        "immutable_contract"
    ]["dataset_bindings"] = [
        {key: item[key] for key in (*assignment_runner.TRUSTED_BINDING_FIELDS, "instrument")}
        for item in bindings
    ]
    qualification["qualified"] = True
    qualification["review"]["gates"]["independent_review_complete"] = True
    assignment["qualification"] = qualification
    monkeypatch.setattr(strategy, "MINIMUM_HISTORY", 12)
    monkeypatch.setattr(assignment_runner, "governed_review_verified", lambda *_: True)
    monkeypatch.setattr(
        assignment_runner, "complete_independent_review", lambda _assignment, item: item
    )

    result = assignment_runner.execute_registered(
        assignment, ROOT, tmp_path / "output", max_workers=1
    )

    assert result["disposition"] == "commissioning_complete"
    assert result["publication_envelope"]["trial"]["hypothesis_evaluation"][
        "outcome"
    ] in {
        "positive", "negative", "invalid", "failed"
    }
    manifests = sorted(
        (tmp_path / "output").glob(
            "run-bundles-*/bundles/*/artifacts/downstream_reuse_manifest.json"
        )
    )
    assert len(manifests) == 2
    expected_parameters = {
        tuple(sorted(spec["params"].items()))
        for spec in HypothesisContract.from_dict(
            qualification["artifact_bundle"]["engine_hypothesis_yaml"]
        ).to_run_specs()
    }
    observed_parameters = set()
    for index, manifest_path in enumerate(manifests):
        artifact_dir = manifest_path.parent
        manifest = json.loads(manifest_path.read_text())
        evaluation_path = artifact_dir / "sol_to_eth_2h_tail_return_validation_evaluation.json"
        evaluation = json.loads(evaluation_path.read_text())
        observed_parameters.add(tuple(sorted(evaluation["parameters"].items())))
        assert manifest["variant_index"] == index
        assert manifest["representation_contract_digest"]
        assert manifest["artifacts"][evaluation_path.name] == assignment_runner.file_digest(
            evaluation_path
        )
        assert evaluation["outcome"] in {"positive", "negative", "invalid", "failed"}
        assert evaluation["record_digest"]
    assert observed_parameters == expected_parameters

    monkeypatch.setattr(strategy, "MINIMUM_HISTORY", 4_000)
    invalid_assignment = assignment | {
        "bundle_root": str(tmp_path / "retained-invalid"),
        "memory_database": str(tmp_path / "memory-invalid.sqlite3"),
    }
    invalid_result = assignment_runner.execute_registered(
        invalid_assignment, ROOT, tmp_path / "invalid-output", max_workers=1
    )
    assert invalid_result["disposition"] == "commissioning_complete"
    assert invalid_result["publication_envelope"]["trial"]["hypothesis_evaluation"][
        "outcome"
    ] == "invalid"
    invalid_gates = invalid_result["commissioning_receipt"]["gate_report"]
    assert invalid_gates["out_of_sample_evaluated"] is False
    assert invalid_gates["cost_stress_evaluated"] is False
    assert len(
        list(
            (tmp_path / "invalid-output").glob(
                "run-bundles-*/bundles/*/artifacts/"
                "sol_to_eth_2h_tail_return_validation_evaluation.json"
            )
        )
    ) == 2

    without_overlap_receipt = dict(invalid_assignment)
    without_overlap_receipt.pop("overlap_admission_receipt")
    with pytest.raises(BridgeError, match="basket overlap admission failed"):
        assignment_runner.execute_registered(
            without_overlap_receipt,
            ROOT,
            tmp_path / "missing-overlap-output",
            max_workers=1,
        )
