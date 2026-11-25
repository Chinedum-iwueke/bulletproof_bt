from __future__ import annotations

from pathlib import Path
import hashlib
import json
import math

import numpy as np
import pandas as pd
import pytest
import yaml

from bt.core.types import Bar
from bt.experiments.hypothesis_runner import run_hypothesis_contract
from bt.experiments.adaptive_representation import materialize_adaptive_representation
from bt.evaluation.alpha_research import required_observation_logging_evaluation
from bt.hypotheses.contract import HypothesisContract
from bt.governance.alpha_strategy_pipeline import (
    canonical_hash,
    confirm_card,
    draft_research_card,
    qualify_card,
)
from bt.strategy import make_strategy
from bt.strategy.btc_seasonal_quote_volume_volatility_2h import (
    CALIBRATION_INDICATORS,
    CALIBRATION_SCENARIOS,
    FIELDS,
    FROZEN_GRID,
    INNER_REPLICATES,
    PLAN_DIGESTS,
    PROPOSAL_DIGEST,
    RAW_OHLC,
    VARIANT_IDS,
    _dispatch_receipt_valid,
    _fit_variant,
    _prepare,
    _representation_receipt_valid,
    _runner_eligibility_valid,
    _seasonal,
    admit_point_in_time_eligibility,
    calibration_contract,
    calibration_work,
    evaluation_frame_digest,
    validate_synthetic_calibration_receipt,
)
import bt.strategy.btc_seasonal_quote_volume_volatility_2h as native
import scripts.run_alpha_research_assignment as assignment_runner
from scripts.run_alpha_research_assignment import (
    _execute_registered_synthetic_fixture,
    execute_registered,
    resolve_strategy_intent_evaluator,
    validate_overlap_admission_receipt,
)

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "research/hypotheses/aep_007c_btc_seasonal_quote_volume_volatility_2h.yaml"


def _bind_governed_review(assignment: dict, qualification: dict) -> dict:
    producer = {"agent_id": "10000000-0000-4000-8000-000000000001",
                "package_digest": "e" * 64, "context_group": "producer",
                "profile_digest": "f" * 64, "machine": "vm1",
                "provider": "deterministic", "model_family": "none",
                "runtime": "python"}
    subject = {"campaign_digest": assignment["campaign_digest"],
               "question_digest": assignment["question_digest"],
               "source_commit": assignment["base_ref"],
               "card_digest": canonical_hash(qualification["card"]),
               "artifact_bundle_digest": canonical_hash(qualification["artifact_bundle"]),
               "qualification_task_id": "synthetic-reviewed-task",
               "producer_agent_ids": [producer["agent_id"]],
               "producer_identities": [producer], "qualifier_identity": producer}
    assertion = {"schema_version": "evaluation-independence-assertion-v1.0.0",
                 "route_digest": "d" * 64,
                 "subject_digest": canonical_hash(subject), "assignments": [],
                 "producer": producer,
                 "policy": {"required_review_kinds": ["strategy_spec", "causality_leakage"],
                            "max_pairwise_shared_dimensions": 4}}
    for index, kind in enumerate(assertion["policy"]["required_review_kinds"]):
        review = {"subject_digest": canonical_hash(subject), "verdict": "approve",
                  "blockers": [], "checks": ["causality"],
                  "rationale": "Synthetic fixture contract review."}
        assertion["assignments"].append({
            "assignment_digest": str(index + 2) * 64, "review_kind": kind,
            "review_digest": canonical_hash(review), "alpha_strategy_review": review,
            "correlation_report": {},
            "evaluator_identity": {"agent_id": f"{index + 2}0000000-0000-4000-8000-000000000001",
                "package_digest": str(index + 3) * 64, "context_group": f"review-{index}",
                "profile_digest": str(index + 5) * 64, "machine": "vm1",
                "provider": "openai", "model_family": "codex", "runtime": "codex-cli"}})
    qualification["governed_review"] = {"subject": subject, "assertion": assertion,
        "receipt_digest": canonical_hash(assertion),
        "route_id": "40000000-0000-4000-8000-000000000001",
        "verdict": "independence_demonstrated"}
    qualification["qualified"] = True
    qualification["review"]["gates"]["independent_review_complete"] = True
    qualification["review"]["independent_of_drafter"] = True
    return qualification


def _frame(n: int = 220) -> pd.DataFrame:
    ts = pd.date_range("2025-01-01", periods=n, freq="2h", tz="UTC")
    ret = 0.002 * np.sin(np.arange(n) / 5) + 0.0001
    quote = 2_000_000 * np.exp(0.2 * np.sin(np.arange(n) / 7))
    close = np.full(n, 50_000.0)
    high = close * 1.001
    low = close / 1.001
    return pd.DataFrame({"ts": ts, FIELDS[0]: np.log1p(quote), FIELDS[1]: ret,
        FIELDS[2]: 0.01 + 0.002 * np.cos(np.arange(n) / 9),
        FIELDS[3]: (high - low) / close,
        FIELDS[4]: quote, FIELDS[5]: quote / close,
        RAW_OHLC[0]: close, RAW_OHLC[1]: high,
        RAW_OHLC[2]: low, RAW_OHLC[3]: close})


def _source_panel(frame: pd.DataFrame) -> pd.DataFrame:
    starts = pd.date_range(frame.ts.iloc[0], periods=len(frame) * 120,
                           freq="1min", tz="UTC")
    bar_index = np.arange(len(starts)) // 120
    close = frame[RAW_OHLC[3]].to_numpy()[bar_index]
    return pd.DataFrame({"ts": starts, "symbol": "BTCUSDT",
                          "open": close, "high": close * 1.001,
                          "low": close / 1.001, "close": close,
                          "volume": np.maximum(frame[FIELDS[5]].to_numpy()[bar_index] / 120, 1),
                          "quote_volume": frame[FIELDS[4]].to_numpy()[bar_index] / 120})


def _representation(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, object]]:
    """Use the native producer so the evaluator receives a complete-bar witness."""
    document = yaml.safe_load(CONTRACT.read_text())
    panel = _source_panel(frame)
    materialized = materialize_adaptive_representation(
        document["representation_plan"], {"BTCUSDT": panel}
    )
    return materialized.frame, materialized.receipt


def _dispatch_receipt(frame: pd.DataFrame, producer: dict[str, object]) -> dict[str, object]:
    body = {"schema_version": "adaptive-frame-dispatch-receipt-v1",
            "producer_receipt_digest": producer["receipt_digest"],
            "frame_value_digest": evaluation_frame_digest(frame)}
    return {**body, "receipt_digest": hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()}


def _calibration_receipt(outer_replicates: int = 104) -> dict[str, object]:
    contract = calibration_contract()
    scenarios = []
    for scenario_id, definition in CALIBRATION_SCENARIOS.items():
        target = definition["target_primary"]
        scenarios.append({
            "id": scenario_id,
            "dgp_contract_digest": contract["dgp_contract_digest"],
            "solve": {"philox_key": [521010, "solve", scenario_id],
                      "histories": 8190, "updates": 4,
                      "functional_evaluations": 77, "jacobian_rank": 8,
                      "coefficient_dimension": 9, "max_target_residual": 0.001,
                      "method": "fixed_minimum_norm_newton",
                      "initial_coefficients": [0.0] * 9,
                      "damping": [1.0, 0.5, 0.25, 0.125],
                      "functionals_per_update": 19,
                      "final_candidate_functionals": 1,
                      "returned_iterate": "b_4_evaluated_explicitly",
                      "central_step": 1e-5, "relative_svd_threshold": 1e-8},
            "verification": {"philox_key": [521010, "verify", scenario_id],
                             "histories": 8190,
                             "variants": [{"id": variant, "D_P": target,
                                           "D_R": 0.0, "A": target,
                                           "identity_error": 0.0, "sigma": 1.0,
                                           "max_mc_se": 0.001}
                                          for variant in VARIANT_IDS]},
            "outer": {"philox_key": [521010, "outer", scenario_id],
                      "attempted": outer_replicates,
                      "invalid_attempts": 0, "failure_reasons": {},
                      "indicators": {indicator: outer_replicates
                                     for indicator in CALIBRATION_INDICATORS}},
        })
    return {"contract_digest": contract["contract_digest"],
            "dgp_contract_digest": contract["dgp_contract_digest"],
            "outer_replicates": outer_replicates,
            "work": calibration_work(outer_replicates), "scenarios": scenarios}


def _scientific_frame(effect: float) -> pd.DataFrame:
    ts = pd.date_range("2024-11-19T16:00:00Z", "2026-05-18T20:00:00Z",
                       freq="2h", inclusive="left")
    rng = np.random.Generator(np.random.Philox(521002))
    innovation = rng.normal(size=len(ts))
    heldout = ts >= pd.Timestamp("2025-12-27T18:00:00Z")
    innovation[heldout] *= 1.5
    signs = np.where(rng.random(len(ts)) > 0.5, 1.0, -1.0)
    return_noise = rng.normal(size=len(ts))
    bucket = ts.hour.to_numpy() // 2
    log_quote = 15.2 + 0.18 * bucket + 0.22 * innovation
    magnitude = 0.001 + 0.0003 * np.abs(return_noise) + effect * np.maximum(
        np.r_[0.0, innovation[:-1]] - 1.15, 0.0
    )
    returns = signs * magnitude
    close = 50_000.0 * np.exp(np.cumsum(returns))
    open_ = np.r_[50_000.0, close[:-1]]
    high = np.maximum(open_, close) * 1.001
    low = np.minimum(open_, close) / 1.001
    quote = np.expm1(log_quote)
    return pd.DataFrame({
        "ts": ts, FIELDS[0]: log_quote, FIELDS[1]: returns,
        FIELDS[2]: 0.01, FIELDS[3]: (high - low) / close,
        FIELDS[4]: quote, FIELDS[5]: quote / close,
        RAW_OHLC[0]: open_, RAW_OHLC[1]: high,
        RAW_OHLC[2]: low, RAW_OHLC[3]: close,
    })


def _evaluate_scientific(frame: pd.DataFrame) -> dict[str, object]:
    document = yaml.safe_load(CONTRACT.read_text())
    scientific = document["immutable_contract"]["validation_plans"]["scientific"]
    native_frame, receipt = _representation(frame)
    return native.evaluate_alpha_intent(
        frame=pd.DataFrame(), decision_frame=None, adaptive_frame=native_frame,
        parameter_grid=document["parameter_grid"],
        validation_start=scientific["partitions"]["validation"]["start"],
        validation_end=scientific["partitions"]["validation"]["end"],
        test_start=scientific["partitions"]["test"]["start"],
        test_end=scientific["partitions"]["test"]["end"],
        representation_plan_digest=document["execution_semantics"]["adaptive_representation_plan_digest"],
        adaptive_representation_receipt=receipt,
        adaptive_frame_digest=evaluation_frame_digest(native_frame),
        adaptive_dispatch_receipt=_dispatch_receipt(native_frame, receipt),
        point_in_time_eligibility=None,
        immutable_contract=document["immutable_contract"], costs=document["costs"])


def _execute_synthetic_registered(frame: pd.DataFrame, output: Path) -> dict[str, object]:
    document = yaml.safe_load(CONTRACT.read_text())
    panel = _source_panel(frame)
    source_path = output.parent / f"{output.name}-source.parquet"
    panel.to_parquet(source_path, index=False)
    source_digest = assignment_runner.file_digest(source_path)
    original_binding = document["immutable_contract"]["dataset_bindings"][0]
    assignment = {
        "base_ref": document["immutable_contract"]["source_commit"],
        "campaign_id": "11111111-1111-4111-8111-111111111111",
        "campaign_digest": "b" * 64,
        "source_candidate_id": "22222222-2222-4222-8222-222222222222",
        "source_candidate_digest": "c" * 64,
        "question": document["immutable_contract"]["question"],
        "hypothesis_id": document["hypothesis_id"],
        "question_digest": canonical_hash({"question": " ".join(
            document["immutable_contract"]["question"].split())}),
        "domain_key": "market-microstructure",
        "dataset_build_id": original_binding["dataset_build_id"],
        "dataset_digest": original_binding["dataset_digest"],
        "dataset_path": str(source_path), "dataset_key": "btcusdt",
        "dataset_bindings": [{**original_binding, "dataset_path": str(source_path)}],
        "instrument": "BTCUSDT", "instruments": ["BTCUSDT"],
        "venue": "bybit", "timeframe": "1m", "research_timeframe": "2h",
        "window_start": document["immutable_contract"]["validation_plans"]["scientific"]["partitions"]["training"]["start"],
        "window_end": document["immutable_contract"]["validation_plans"]["scientific"]["partitions"]["target_tail"]["end"],
        "tier": "Tier2B", "max_variants": 4,
        "representation_plan": document["representation_plan"],
        "bundle_root": str(output.parent / "retained"),
        "memory_database": str(output.parent / "memory.sqlite3"),
        "research_context": {"corpus_digest": "d" * 64, "abstained": True,
                             "citations": []},
        "reusable_strategy": {
            "bounded_weekly_reuse_eligible": True,
            "input_mode": "single_instrument", "maximum_instruments": 1,
            "contract_path": str(CONTRACT.relative_to(ROOT)),
            "contract_digest": hashlib.sha256(CONTRACT.read_bytes()).hexdigest(),
            "hypothesis_id": document["hypothesis_id"],
            "strategy": document["entry"]["strategy"], "variant_count": 4,
            "title": document["title"], "description": document["description"],
            "logging_requirements": document["logging"]["required_fields"],
        },
    }
    card = confirm_card(draft_research_card(assignment, repository_root=str(ROOT)),
                        actor="founder-operator", confirmed_at="2026-11-24T00:00:00Z")
    qualification = qualify_card(card, repository_root=str(ROOT))
    synthetic_binding = {**original_binding, "dataset_digest": source_digest,
                         "partition_digest": source_digest,
                         "partition_digests": [source_digest]}
    contract_document = qualification["artifact_bundle"]["engine_hypothesis_yaml"]
    contract_document["immutable_contract"]["dataset_bindings"] = [synthetic_binding]
    membership_body = {"schema_version": "point-in-time-membership-receipt-v1",
        "venue": "bybit", "instrument": "BTCUSDT", "listing_verified": True,
        "membership_verified": True, "valid_from": "2023-01-01T00:00:00Z",
        "valid_through": "2027-01-01T00:00:00Z", "evidence_digest": source_digest,
        "dataset_bindings_digest": hashlib.sha256(json.dumps(
            [synthetic_binding], sort_keys=True, separators=(",", ":")
        ).encode("ascii")).hexdigest(),
        "authority": "admitted_point_in_time_source_receipt"}
    membership_receipt = {**membership_body, "receipt_digest": hashlib.sha256(json.dumps(
        membership_body, sort_keys=True, separators=(",", ":")
    ).encode("ascii")).hexdigest()}
    contract_document["immutable_contract"]["point_in_time_membership_receipt_digest"] = membership_receipt["receipt_digest"]
    assignment["dataset_digest"] = source_digest
    assignment["dataset_bindings"] = [{**synthetic_binding, "dataset_path": str(source_path)}]
    assignment["point_in_time_membership_receipt"] = membership_receipt
    assignment["qualification"] = _bind_governed_review(assignment, qualification)
    assignment["execution_class"] = "qualification"
    original_grid = assignment_runner.execute_variant_grid
    def bounded_grid(jobs, max_workers):
        bounded = []
        for index, job in enumerate(jobs):
            narrowed = dict(job)
            source = pd.read_parquet(job["data_path"]).head(3_000)
            path = output.parent / f"{output.name}-engine-{index}.parquet"
            source.to_parquet(path, index=False)
            narrowed["data_path"] = str(path)
            bounded.append(narrowed)
        return original_grid(bounded, max_workers)
    assignment_runner.execute_variant_grid = bounded_grid
    try:
        result = execute_registered(assignment, ROOT, output)
    finally:
        assignment_runner.execute_variant_grid = original_grid
    assert result["disposition"] in {"qualification_complete", "native_execution_complete"}
    artifacts = sorted(output.glob(
        "run-bundles-*/bundles/*/artifacts/strategy_intent_validation_evaluation.json"
    ))
    assert len(artifacts) == 4
    for path in artifacts:
        artifact = json.loads(path.read_text())
        assert artifact["method_proposal_digest"] == PROPOSAL_DIGEST
        if artifact["outcome"] in {"positive", "negative"}:
            assert len(artifact["metrics"]["design_artifact"]["columns"]) == 17
            assert set(artifact["metrics"]["thresholds"]) == {"primary", "rival"}
            assert set(artifact["metrics"]["support"]) == {"training", "validation"}
            assert set(artifact["metrics"]["validation"]["inference"]) == {"12", "24", "48"}
            assert set(artifact["held_out_evaluation"]["metrics"]["inference"]) == {"12", "24", "48"}
    trial = result["publication_envelope"]["trial"]
    return {"selection_candidates": trial["selection_bias_audit"]["validation_results"],
            "held_out_evaluation": trial["hypothesis_evaluation"]}


@pytest.fixture(scope="module")
def scientific_evaluations(tmp_path_factory: pytest.TempPathFactory) -> dict[str, dict[str, object]]:
    return {
        "positive": _execute_synthetic_registered(
            _scientific_frame(0.03), tmp_path_factory.mktemp("runner-positive") / "run"
        ),
        "negative": _execute_synthetic_registered(
            _scientific_frame(0.0), tmp_path_factory.mktemp("runner-negative") / "run"
        ),
    }


def test_contract_preserves_frozen_method_and_plans() -> None:
    doc = yaml.safe_load(CONTRACT.read_text())
    assert doc["immutable_contract"]["accepted_method_proposal_digest"] == PROPOSAL_DIGEST
    assert doc["immutable_contract"]["accepted_method_handoff_digest"] == \
        "ac80159f59f89c4555c34c41acf4456b851e01fe312eff310a4456150c2ad9bd"
    assert doc["immutable_contract"]["accepted_evidence_sha256"] == \
        "57d90ff8554b226ff8685928d2738b7690af4a931bfc548b3cc534c18dae99b4"
    assert doc["immutable_contract"]["source_commit"] == \
        "d232ee753f967760ce3d753abff607ee2232ce62"
    assert doc["immutable_contract"]["dataset_bindings"][0]["producer_receipt_digest"] == \
        "1d08a9002cf71a44905dd6f9d666504b9b05cdb562b4773b40229b0d6dae7266"
    assert {k: v["plan_digest"] for k, v in doc["immutable_contract"]["validation_plans"].items()} == PLAN_DIGESTS
    assert doc["parameter_grid"] == {k: list(v) for k, v in FROZEN_GRID.items()}
    assert doc["evaluation"]["uncertainty"]["replicates"] == INNER_REPLICATES
    assert doc["evaluation"]["calibration"]["three_outer_replicates_qualify"] is False
    assert doc["authority"]["calibration_execution"] is False
    calibration = calibration_contract()
    assert calibration["solver"] == {
        "histories_per_scenario": 8190, "coefficient_dimension": 9,
        "method": "fixed_minimum_norm_newton",
        "initial_coefficients": [0.0] * 9, "updates": 4,
        "damping": [1.0, 0.5, 0.25, 0.125], "central_step": 1e-5,
        "functionals_per_update": 19, "final_candidate_functionals": 1,
        "maximum_functionals_per_scenario": 77,
        "required_jacobian_rank": 8, "relative_svd_threshold": 1e-8,
        "maximum_target_residual": 0.005,
        "returned_iterate": "b_4_evaluated_explicitly",
        "infeasible_when": ["rank_below_8", "target_residual_above_0.005",
            "conditioning_probability_zero", "sigma_nonfinite_or_at_most_1e-8"],
    }
    assert calibration["verification"]["histories_per_scenario"] == 8190
    assert calibration["work"] == {
        "complete_history": 65936, "full_functional_evaluation": 308,
        "ols_fit": 20445568, "statistic_evaluation": 266240000,
    }
    assert calibration["caps"]["complete_history"] == 65936
    assert calibration["innovation"]["distribution"] == "iid_over_t_multivariate_normal"
    assert calibration["generative_method"]["eligibility_and_support"]["eligible_rows"] == {
        "training": 2500, "validation": 850, "test": 850}
    assert calibration["generative_method"]["derived_ohlcv"]["volume"] == \
        "volume[t]=quote_volume[t]/close[t]"


def test_eligibility_is_issued_only_for_verified_contract_bound_source() -> None:
    immutable = yaml.safe_load(CONTRACT.read_text())["immutable_contract"]
    fabricated = {"schema_version": "point-in-time-membership-receipt-v1",
                  "venue": "bybit", "instrument": "BTCUSDT",
                  "listing_verified": True, "membership_verified": True,
                  "authority": "admitted_point_in_time_source_receipt",
                  "dataset_bindings_digest": "1" * 64,
                  "valid_from": "2023-01-01T00:00:00Z",
                  "valid_through": "2027-01-01T00:00:00Z",
                  "evidence_digest": "1" * 64, "receipt_digest": "1" * 64}
    assert admit_point_in_time_eligibility(
        fabricated, immutable, verified_dataset_bindings_digest="1" * 64
    ) is None


def test_peer_receipt_boundaries_reject_malformed_nested_json_without_raising() -> None:
    document = yaml.safe_load(CONTRACT.read_text())
    immutable = document["immutable_contract"]
    frame, representation = _representation(_frame())
    plan_digest = document["execution_semantics"]["adaptive_representation_plan_digest"]
    assert _representation_receipt_valid(frame, representation, plan_digest)
    malformed_representation = dict(representation)
    malformed_representation["nested"] = {"bad": object()}
    assert not _representation_receipt_valid(
        frame, malformed_representation, plan_digest
    )

    dispatch = _dispatch_receipt(frame, representation)
    assert _dispatch_receipt_valid(frame, dispatch, representation)
    malformed_dispatch = dict(dispatch)
    malformed_dispatch["nested"] = [object()]
    assert not _dispatch_receipt_valid(frame, malformed_dispatch, representation)
    malformed_producer = dict(representation)
    malformed_producer["nested"] = {"bad": math.inf}
    assert not _dispatch_receipt_valid(frame, dispatch, malformed_producer)

    bindings = immutable["dataset_bindings"]
    binding_digest = hashlib.sha256(json.dumps(
        bindings, sort_keys=True, separators=(",", ":")
    ).encode("ascii")).hexdigest()
    membership_body = {
        "schema_version": "point-in-time-membership-receipt-v1",
        "venue": "bybit", "instrument": "BTCUSDT",
        "listing_verified": True, "membership_verified": True,
        "dataset_bindings_digest": binding_digest,
        "authority": "admitted_point_in_time_source_receipt",
        "valid_from": "2023-01-01T00:00:00Z",
        "valid_through": "2027-01-01T00:00:00Z",
        "evidence_digest": bindings[0]["dataset_digest"],
    }
    membership = {**membership_body, "receipt_digest": hashlib.sha256(json.dumps(
        membership_body, sort_keys=True, separators=(",", ":")
    ).encode("ascii")).hexdigest()}
    bound_contract = {**immutable,
                      "point_in_time_membership_receipt_digest": membership["receipt_digest"]}
    evidence = admit_point_in_time_eligibility(
        membership, bound_contract, verified_dataset_bindings_digest=binding_digest
    )
    assert evidence is not None
    assert _runner_eligibility_valid(evidence, bound_contract)

    malformed_memberships = []
    for field, value in (("nested", {"bad": object()}),
                         ("valid_from", "not-a-timestamp"),
                         ("valid_through", None)):
        malformed = dict(membership)
        malformed[field] = value
        malformed_memberships.append(malformed)
    missing_time = dict(membership)
    missing_time.pop("valid_from")
    malformed_memberships.append(missing_time)
    for malformed in malformed_memberships:
        assert admit_point_in_time_eligibility(
            malformed, bound_contract,
            verified_dataset_bindings_digest=binding_digest,
        ) is None

    malformed_contracts = []
    for malformed_bindings in (
        [{"nested": object()}],
        ["not-a-binding"],
        {"bad": 1},
        [{**bindings[0], "dataset_digest": ["unhashable"]}],
        [{**bindings[0], "dataset_digest": {"unhashable": True}}],
        [{**bindings[0], "dataset_digest": None}],
        [bindings[0], bindings[0]],
        [{key: value for key, value in bindings[0].items()
          if key != "dataset_build_id"}],
        [{key: value for key, value in bindings[0].items()
          if key != "producer_receipt_digest"}],
    ):
        malformed_contracts.append({**bound_contract,
                                    "dataset_bindings": malformed_bindings})
    for malformed_contract in malformed_contracts:
        assert admit_point_in_time_eligibility(
            membership, malformed_contract,
            verified_dataset_bindings_digest=binding_digest,
        ) is None
        assert not _runner_eligibility_valid(evidence, malformed_contract)


def test_held_out_evaluation_reuses_frozen_training_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    document = yaml.safe_load(CONTRACT.read_text())
    partitions = document["immutable_contract"]["validation_plans"]["scientific"]["partitions"]
    windows = {name: (partitions[name]["start"], partitions[name]["end"])
               for name in ("training", "validation", "test")}
    data = _prepare(_scientific_frame(0.03).rename(columns={"decision_at": "ts"}))
    params = {key: values[0] for key, values in FROZEN_GRID.items()}
    monkeypatch.setattr(native, "_bootstrap", lambda *args, block_length, **kwargs: {
        "valid": True, "undefined_replicates": 0, "block_length": block_length,
        "D_P": 1.0, "A": 1.0, "D_P_interval_95": [0.5, 1.5],
        "A_interval_95": [0.5, 1.5], "D_P_centered_p": 0.001,
        "A_centered_p": 0.001,
    })
    validation = _fit_variant(data, params, windows, evaluate_partitions=("validation",))
    assert validation["outcome"] == "test_open"
    monkeypatch.setattr(native.np.linalg, "lstsq", lambda *args, **kwargs: (
        (_ for _ in ()).throw(AssertionError("held-out evaluation refit training OLS"))))
    held_out = _fit_variant(
        data, params, windows, evaluate_partitions=("test",),
        frozen_state=validation["metrics"],
    )
    assert held_out["outcome"] == "test_open"
    assert held_out["metrics"]["training_state_reused"] is True
    assert held_out["metrics"]["design_artifact"] == \
        validation["metrics"]["design_artifact"]


def test_overlap_receipt_boundary_is_total_strict_utc_and_exact_members() -> None:
    bindings = [
        {"instrument": "BTCUSDT", "dataset_build_id": "btc-build",
         "dataset_digest": "a" * 64},
        {"instrument": "ETHUSDT", "dataset_build_id": "eth-build",
         "dataset_digest": "b" * 64},
    ]
    body = {
        "schema_version": "alpha-basket-overlap-admission-v1.0.0",
        "authority": "DATA-002/003",
        "dataset_bindings": bindings,
        "instruments": ["BTCUSDT", "ETHUSDT"],
        "minimum_contiguous_days": 365,
        "admitted_start": "2025-01-01T00:00:00+00:00",
        "admitted_end": "2026-01-01T00:00:00+00:00",
    }

    def signed(document: dict[str, object]) -> dict[str, object]:
        return {**document, "record_digest": assignment_runner.digest(document)}

    valid = signed(body)
    validate_overlap_admission_receipt(
        valid, bindings=bindings, instruments=["BTCUSDT", "ETHUSDT"],
        window_start="2025-01-01T00:00:00Z", window_end="2026-01-01T00:00:00Z",
    )
    malformed = []
    missing = dict(valid)
    missing.pop("authority")
    malformed.append(missing)
    malformed.append({**valid, "nested": {"bad": object()}})
    for field, value in (
        ("admitted_start", "not-a-timestamp"),
        ("admitted_end", "2026-01-01T01:00:00+01:00"),
        ("instruments", ["BTCUSDT", "BTCUSDT"]),
        ("dataset_bindings", [bindings[0], bindings[0]]),
        ("minimum_contiguous_days", {"unhashable": 365}),
    ):
        changed = {**body, field: value}
        malformed.append(signed(changed))
    for receipt in malformed:
        with pytest.raises(assignment_runner.BridgeError):
            validate_overlap_admission_receipt(
                receipt, bindings=bindings, instruments=["BTCUSDT", "ETHUSDT"],
                window_start="2025-01-01T00:00:00Z",
                window_end="2026-01-01T00:00:00Z",
            )


def test_prepare_target_and_seasonal_state_are_causal() -> None:
    frame = _frame(220)
    rows = _prepare(frame)
    expected = np.sqrt(frame.loc[1, FIELDS[1]] ** 2 + frame.loc[2, FIELDS[1]] ** 2)
    assert rows.loc[0, "y"] == expected
    assert rows.tail(2)["target_complete"].tolist() == [False, False]
    before = _seasonal(rows, 10)
    changed = frame.copy()
    changed.loc[121:, FIELDS[0]] += 20
    after = _seasonal(_prepare(changed), 10)
    pd.testing.assert_series_equal(before.iloc[:121], after.iloc[:121])
    gap = frame.drop(index=60).reset_index(drop=True)
    gap_state = _seasonal(_prepare(gap), 10)
    # The next occurrence of the same UTC bucket cannot bridge the missing day.
    assert np.isnan(gap_state.iloc[71])
    # Future liquidity cannot censor a target; only physical bar validity can.
    low_future_volume = frame.copy()
    low_future_volume.loc[1:2, FIELDS[4]] = 10.0
    low_future_volume.loc[1:2, FIELDS[0]] = np.log1p(10.0)
    assert bool(_prepare(low_future_volume).loc[0, "target_complete"]) is True


def test_native_strategy_tail_and_repeat_state() -> None:
    ts = pd.Timestamp("2026-05-18T14:00:00Z")
    payload = {field: 1.0 for field in FIELDS}
    payload[FIELDS[0]] = math.log1p(2_000_000.0)
    payload[FIELDS[3]] = 0.0
    payload[FIELDS[4]] = 2_000_000.0
    bar = Bar(ts, "BTCUSDT", 1, 1, 1, 1, 1, payload)
    first = make_strategy("btc_seasonal_quote_volume_volatility_2h")
    second = make_strategy("btc_seasonal_quote_volume_volatility_2h")
    assert len(first.on_bars(ts, {"BTCUSDT": bar}, {"BTCUSDT"}, {})) == 1
    assert second.records == []
    tail_ts = pd.Timestamp("2026-05-18T16:00:00Z")
    tail = Bar(tail_ts, "BTCUSDT", 1, 1, 1, 1, 1, payload)
    assert first.on_bars(tail_ts, {"BTCUSDT": tail}, {"BTCUSDT"}, {}) == []
    assert first.records[-1]["outcome"] == "target_only"

    invalid = Bar(ts, "BTCUSDT", 1, 1, 1, 1, 1, {})
    assert first.on_bars(ts, {"BTCUSDT": invalid}, {"BTCUSDT"}, {}) == []
    assert first.records[-1]["outcome"] == "invalid"
    inconsistent = payload | {FIELDS[3]: 0.5}
    malformed = Bar(ts, "BTCUSDT", 1, 1, 1, 1, 1, inconsistent)
    assert first.on_bars(ts, {"BTCUSDT": malformed}, {"BTCUSDT"}, {}) == []


def test_registered_synthetic_fixture_uses_eligibility_and_exact_variant_ids(
    tmp_path: Path,
) -> None:
    document = yaml.safe_load(CONTRACT.read_text())
    panel = _source_panel(_frame())
    source_digest = hashlib.sha256(
        pd.util.hash_pandas_object(panel, index=True).to_numpy(
            dtype="uint64"
        ).tobytes()
    ).hexdigest()
    result = _execute_registered_synthetic_fixture({
        "question": document["immutable_contract"]["question"],
        "hypothesis_id": document["hypothesis_id"],
        "execution_authority": False,
        "scientific_certification": False,
        "synthetic_source_panels": {"BTCUSDT": panel},
        "synthetic_source_digest": source_digest,
    }, ROOT, tmp_path / "synthetic-runner")
    assert result["disposition"] == "synthetic_registered_runner_complete"
    assert result["terminal_outcome"] == "failed"
    assert result["variant_artifact_count"] == 4
    registered = json.loads((tmp_path / "synthetic-runner" /
        "registered-evaluation.json").read_text())
    assert "error" not in registered
    assert len(registered["selection_candidates"]) == 4
    expected = {
        "q90_L90": (0.90, 90), "q90_L180": (0.90, 180),
        "q95_L90": (0.95, 90), "q95_L180": (0.95, 180),
    }
    for variant_id, (quantile, days) in expected.items():
        artifact = json.loads((tmp_path / "synthetic-runner" / "runs" /
            variant_id / "strategy_intent_validation_evaluation.json").read_text())
        assert artifact["variant_id"] == variant_id
        assert artifact["outcome"] == "invalid"
        assert artifact["parameters"]["volume_surprise_quantile"] == quantile
        assert artifact["parameters"]["seasonal_reference_days"] == days


def test_registered_synthetic_fixture_propagates_interface_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    document = yaml.safe_load(CONTRACT.read_text())
    panel = _source_panel(_frame())
    source_digest = hashlib.sha256(
        pd.util.hash_pandas_object(panel, index=True).to_numpy(
            dtype="uint64"
        ).tobytes()
    ).hexdigest()

    def incompatible_evaluator(**kwargs):
        del kwargs
        raise TypeError("synthetic_interface_mismatch")

    incompatible_evaluator.__module__ = native.__name__
    monkeypatch.setattr(
        assignment_runner, "resolve_strategy_intent_evaluator",
        lambda contract, repository: incompatible_evaluator,
    )
    with pytest.raises(TypeError, match="synthetic_interface_mismatch"):
        _execute_registered_synthetic_fixture({
            "question": document["immutable_contract"]["question"],
            "hypothesis_id": document["hypothesis_id"],
            "execution_authority": False,
            "scientific_certification": False,
            "synthetic_source_panels": {"BTCUSDT": panel},
            "synthetic_source_digest": source_digest,
        }, ROOT, tmp_path / "synthetic-runner-error")
def test_prepare_rejects_misaligned_and_invalid_ohlc_rows() -> None:
    misaligned = _frame()
    misaligned["ts"] += pd.Timedelta(hours=1)
    assert not _prepare(misaligned)["bar_valid"].any()
    broken = _frame()
    broken.loc[20, RAW_OHLC[2]] = broken.loc[20, RAW_OHLC[3]] * 1.1
    assert bool(_prepare(broken).loc[20, "bar_valid"]) is False


def test_terminal_outcome_classes_are_distinct() -> None:
    data = _prepare(_frame())
    params = {"volume_surprise_quantile": .9, "seasonal_reference_days": 90,
              "prior_volatility_bars": 12, "uncertainty_block_length": 12}
    windows = {name: (data.ts.min(), data.ts.max() + pd.Timedelta(hours=2))
               for name in ("training", "validation", "test")}
    invalid = _fit_variant(data, params, windows)
    assert invalid["outcome"] == "invalid"


def test_native_contract_runner_executes_strategy_and_evaluator_repeatably() -> None:
    document = yaml.safe_load(CONTRACT.read_text())
    evaluator = resolve_strategy_intent_evaluator(document, ROOT)
    assert evaluator is not None
    assert evaluator.__module__.endswith("btc_seasonal_quote_volume_volatility_2h")
    scientific = document["immutable_contract"]["validation_plans"]["scientific"]
    contract = HypothesisContract.from_yaml(CONTRACT)

    witness_frame, witness_receipt = _representation(_frame())
    witness_digest = evaluation_frame_digest(witness_frame)
    mutated = witness_frame.copy()
    mutated.loc[10, FIELDS[1]] = 123.0
    with pytest.raises(ValueError, match="adaptive_frame_value_digest_mismatch"):
        evaluator(
            frame=pd.DataFrame(), decision_frame=mutated, adaptive_frame=mutated,
            parameter_grid=document["parameter_grid"],
            validation_start=scientific["partitions"]["validation"]["start"],
            validation_end=scientific["partitions"]["validation"]["end"],
            test_start=scientific["partitions"]["test"]["start"],
            test_end=scientific["partitions"]["test"]["end"],
            representation_plan_digest=document["execution_semantics"]["adaptive_representation_plan_digest"],
            adaptive_representation_receipt=witness_receipt,
            adaptive_frame_digest=witness_digest,
            adaptive_dispatch_receipt=_dispatch_receipt(witness_frame, witness_receipt),
            point_in_time_eligibility=None,
            immutable_contract=document["immutable_contract"], costs=document["costs"],
        )
    with pytest.raises(ValueError, match="authoritative_adaptive_dispatch_receipt_required"):
        evaluator(
            frame=pd.DataFrame(), decision_frame=mutated, adaptive_frame=mutated,
            parameter_grid=document["parameter_grid"],
            validation_start=scientific["partitions"]["validation"]["start"],
            validation_end=scientific["partitions"]["validation"]["end"],
            test_start=scientific["partitions"]["test"]["start"],
            test_end=scientific["partitions"]["test"]["end"],
            representation_plan_digest=document["execution_semantics"]["adaptive_representation_plan_digest"],
            adaptive_representation_receipt=witness_receipt,
            adaptive_frame_digest=evaluation_frame_digest(mutated),
            adaptive_dispatch_receipt=_dispatch_receipt(witness_frame, witness_receipt),
            point_in_time_eligibility=None,
            immutable_contract=document["immutable_contract"], costs=document["costs"],
        )

    invalid_membership = evaluator(
        frame=pd.DataFrame(), decision_frame=witness_frame, adaptive_frame=witness_frame,
        parameter_grid=document["parameter_grid"],
        validation_start=scientific["partitions"]["validation"]["start"],
        validation_end=scientific["partitions"]["validation"]["end"],
        test_start=scientific["partitions"]["test"]["start"],
        test_end=scientific["partitions"]["test"]["end"],
        representation_plan_digest=document["execution_semantics"]["adaptive_representation_plan_digest"],
        adaptive_representation_receipt=witness_receipt,
        adaptive_frame_digest=witness_digest,
        adaptive_dispatch_receipt=_dispatch_receipt(witness_frame, witness_receipt),
        point_in_time_eligibility=None,
        immutable_contract=document["immutable_contract"], costs=document["costs"],
    )
    assert invalid_membership["held_out_evaluation"]["outcome"] == "invalid"

    def executor(spec: dict[str, object], tier: str) -> dict[str, object]:
        strategy = make_strategy(document["entry"]["strategy"], **spec["params"])
        frame, receipt = _representation(_frame())
        row = frame.iloc[13]
        ts = pd.Timestamp(row.decision_at)
        payload = {field: float(row[field]) for field in FIELDS}
        bar = Bar(ts, "BTCUSDT", float(row[RAW_OHLC[0]]),
                  float(row[RAW_OHLC[1]]), float(row[RAW_OHLC[2]]),
                  float(row[RAW_OHLC[3]]), float(row[FIELDS[5]]), payload)
        signals = strategy.on_bars(ts, {"BTCUSDT": bar}, {"BTCUSDT"}, {})
        tail_ts = pd.Timestamp("2026-05-18T16:00:00Z")
        tail = Bar(tail_ts, "BTCUSDT", bar.open, bar.high, bar.low,
                   bar.close, bar.volume, payload)
        assert strategy.on_bars(tail_ts, {"BTCUSDT": tail}, {"BTCUSDT"}, {}) == []
        evaluation = evaluator(
            frame=pd.DataFrame(), decision_frame=frame,
            adaptive_frame=frame, parameter_grid=document["parameter_grid"],
            validation_start=scientific["partitions"]["validation"]["start"],
            validation_end=scientific["partitions"]["validation"]["end"],
            test_start=scientific["partitions"]["test"]["start"],
            test_end=scientific["partitions"]["test"]["end"],
            representation_plan_digest=document["execution_semantics"]["adaptive_representation_plan_digest"],
            adaptive_representation_receipt=receipt,
            adaptive_frame_digest=evaluation_frame_digest(frame),
            adaptive_dispatch_receipt=_dispatch_receipt(frame, receipt),
            point_in_time_eligibility=None,
            immutable_contract=document["immutable_contract"], costs=document["costs"])
        logging_reports = [required_observation_logging_evaluation(
            item, document["logging"]["required_fields"]
        ) for item in evaluation["selection_candidates"]]
        heldout_logging = required_observation_logging_evaluation(
            evaluation["held_out_evaluation"], document["logging"]["required_fields"]
        )
        return {"num_trades": 0, "ev_r_gross": None, "ev_r_net": None,
                "native_signal_count": len(signals),
                "native_record_count": len(strategy.records),
                "variant_artifacts": len(evaluation["selection_candidates"]),
                "logging_complete": all(item["passed"] for item in logging_reports)
                and heldout_logging["passed"],
                "terminal_outcome": evaluation["held_out_evaluation"]["outcome"],
                "tier_seen": tier}

    kwargs = dict(contract=contract, executor=executor, symbol="BTCUSDT",
                  timeframe="2h", start_ts="2025-01-01", end_ts="2026-05-18",
                  available_tiers={"Tier2"}, phase="tier2")
    first = run_hypothesis_contract(**kwargs)
    second = run_hypothesis_contract(**kwargs)
    stable = ("grid_id", "native_signal_count", "native_record_count",
              "variant_artifacts", "logging_complete", "terminal_outcome",
              "tier_seen")
    assert [{key: row[key] for key in stable} for row in first] == [
        {key: row[key] for key in stable} for row in second]
    assert len(first) == 4
    assert all(row["native_signal_count"] == 1 for row in first)
    assert all(row["native_record_count"] == 2 for row in first)
    assert all(row["variant_artifacts"] == 4 for row in first)
    assert all(row["logging_complete"] is True for row in first)
    assert all(row["terminal_outcome"] == "invalid" for row in first)


def test_execute_registered_persists_invalid_failed_and_repeat_isolation(tmp_path: Path) -> None:
    first_dir, second_dir = tmp_path / "invalid-a", tmp_path / "invalid-b"
    invalid_frame = _scientific_frame(0.0)
    invalid_frame[FIELDS[4]] = 2_000_000.0
    invalid_frame[FIELDS[0]] = math.log1p(2_000_000.0)
    first = _execute_synthetic_registered(invalid_frame, first_dir)
    second = _execute_synthetic_registered(invalid_frame, second_dir)
    assert first["held_out_evaluation"]["outcome"] == "failed"
    assert {item["outcome"] for item in first["selection_candidates"]} == {"invalid"}
    assert second == first
    mixed = _scientific_frame(0.0)
    cutoff = pd.Timestamp("2025-02-17T20:00:00Z")
    damaged = mixed["ts"] < cutoff
    mixed.loc[damaged, [FIELDS[0], FIELDS[4]]] = np.nan
    inconclusive = _execute_synthetic_registered(mixed, tmp_path / "inconclusive")
    assert inconclusive["held_out_evaluation"]["outcome"] == "inconclusive"
    assert "inconclusive" in {
        item["outcome"] for item in inconclusive["selection_candidates"]
    }


@pytest.mark.parametrize("expected", ["positive", "negative"])
def test_real_estimator_bootstrap_and_holm_terminal_paths(
    scientific_evaluations: dict[str, dict[str, object]], expected: str
) -> None:
    result = scientific_evaluations[expected]
    candidates = result["selection_candidates"]
    assert len(candidates) == 4
    assert {item["outcome"] for item in candidates} == {expected}
    assert result["held_out_evaluation"]["outcome"] == expected
    for item in candidates:
        assert item["scientific_certification"] is False
        assert set(item["metrics"]["support"]) == {"training", "validation"}
        assert "test" not in item["metrics"]
        assert set(item["metrics"]["validation"]["inference"]) == {"12", "24", "48"}
        assert all(
            inference["undefined_replicates"] == 0
            for inference in item["metrics"]["validation"]["inference"].values()
        )
        report = required_observation_logging_evaluation(
            item, yaml.safe_load(CONTRACT.read_text())["logging"]["required_fields"]
        )
        assert report["passed"] is True
    held_variants = result["held_out_evaluation"]["variant_evaluations"]
    assert len(held_variants) == 4
    assert {item["outcome"] for item in held_variants} == {expected}
    assert all(set(item["metrics"]["inference"]) == {"12", "24", "48"}
               for item in held_variants)


def test_synthetic_calibration_contract_terminal_states_and_caps() -> None:
    accepted = validate_synthetic_calibration_receipt(_calibration_receipt())
    assert accepted["status"] == "success"
    assert accepted["scientific_certification"] is False
    assert accepted["exceeded"] == {}
    limited = validate_synthetic_calibration_receipt(_calibration_receipt(3))
    assert limited["status"] == "inconclusive"
    assert limited["three_outer_replicates_qualify"] is False
    inconclusive = validate_synthetic_calibration_receipt(_calibration_receipt(0))
    assert inconclusive["status"] == "inconclusive"
    failed_receipt = _calibration_receipt()
    failed_receipt["scenarios"][0]["solve"]["jacobian_rank"] = 7
    failed = validate_synthetic_calibration_receipt(failed_receipt)
    assert failed["status"] == "failed"
    assert any("solver_contract_failed" in reason for reason in failed["reasons"])
    wrong_dgp = _calibration_receipt()
    wrong_dgp["scenarios"][0]["dgp_contract_digest"] = "0" * 64
    wrong_dgp_result = validate_synthetic_calibration_receipt(wrong_dgp)
    assert wrong_dgp_result["status"] == "failed"
    assert any("dgp_contract_digest_mismatch" in reason
               for reason in wrong_dgp_result["reasons"])
    nonfinite_receipt = _calibration_receipt()
    nonfinite_receipt["scenarios"][0]["solve"]["max_target_residual"] = np.nan
    nonfinite_receipt["scenarios"][0]["verification"]["variants"][0]["D_P"] = np.nan
    assert validate_synthetic_calibration_receipt(nonfinite_receipt)["status"] == "failed"
    malformed_receipts = []
    for field in ("max_target_residual",):
        malformed = _calibration_receipt()
        malformed["scenarios"][0]["solve"][field] = "not-a-number"
        malformed_receipts.append(malformed)
    for field in ("D_P", "D_R", "A", "sigma", "max_mc_se"):
        malformed = _calibration_receipt()
        malformed["scenarios"][0]["verification"]["variants"][0][field] = object()
        malformed_receipts.append(malformed)
    malformed = _calibration_receipt()
    malformed["scenarios"][0]["outer"]["failure_reasons"] = {"rank": "one"}
    malformed_receipts.append(malformed)
    malformed = _calibration_receipt()
    malformed["scenarios"][0]["outer"]["indicators"][CALIBRATION_INDICATORS[0]] = math.inf
    malformed_receipts.append(malformed)
    malformed = _calibration_receipt()
    malformed["outer_replicates"] = []
    malformed_receipts.append(malformed)
    for malformed in malformed_receipts:
        terminal = validate_synthetic_calibration_receipt(malformed)
        assert terminal["status"] == "failed"
        assert terminal["scientific_certification"] is False
        assert terminal["execution_authority"] is False
    over = _calibration_receipt(105)
    blocked = validate_synthetic_calibration_receipt(over)
    assert blocked["status"] == "capacity_blocked"
    assert blocked["exceeded"]["complete_history"] == 65_940
    assert blocked["exceeded"]["statistic_evaluation"] == 268_800_000


@pytest.mark.parametrize("collection", ["scenarios", "variants"])
@pytest.mark.parametrize("defect", ["non_mapping", "missing", "extra", "duplicate", "typed_id"])
def test_calibration_registered_collections_fail_explicitly(
    collection: str, defect: str,
) -> None:
    receipt = _calibration_receipt()
    members = (receipt["scenarios"] if collection == "scenarios" else
               receipt["scenarios"][0]["verification"]["variants"])
    if defect == "non_mapping":
        members.append("malformed")
    elif defect == "missing":
        members.pop()
    elif defect == "extra":
        extra = dict(members[0])
        extra["id"] = "unregistered"
        members.append(extra)
    elif defect == "duplicate":
        members.append(dict(members[0]))
    else:
        members[0] = {**members[0], "id": 90}
    terminal = validate_synthetic_calibration_receipt(receipt)
    assert terminal["status"] == "failed"
    assert terminal["scientific_certification"] is False
    assert terminal["execution_authority"] is False
    assert any("scenarios" in reason if collection == "scenarios"
               else "verification_variants" in reason
               for reason in terminal["reasons"])


def test_calibration_nested_receipt_collections_are_total_and_typed() -> None:
    malformed_receipts = []
    for field, value in (("solve", []), ("verification", "bad"), ("outer", 1)):
        malformed = _calibration_receipt()
        malformed["scenarios"][0][field] = value
        malformed_receipts.append(malformed)
    malformed = _calibration_receipt()
    malformed["scenarios"][0]["verification"]["variants"] = {"id": VARIANT_IDS[0]}
    malformed_receipts.append(malformed)
    malformed = _calibration_receipt()
    malformed["scenarios"][0]["outer"]["failure_reasons"] = [("rank", 1)]
    malformed_receipts.append(malformed)
    malformed = _calibration_receipt()
    malformed["scenarios"][0]["outer"]["failure_reasons"] = {1: 0}
    malformed_receipts.append(malformed)
    malformed = _calibration_receipt()
    malformed["scenarios"][0]["outer"]["indicators"] = []
    malformed_receipts.append(malformed)
    malformed = _calibration_receipt()
    malformed["scenarios"][0]["verification"]["variants"][0]["identity_error"] = "zero"
    malformed_receipts.append(malformed)
    for malformed in malformed_receipts:
        terminal = validate_synthetic_calibration_receipt(malformed)
        assert terminal["status"] == "failed"
        assert terminal["scientific_certification"] is False
        assert terminal["execution_authority"] is False


def test_mixed_test_validity_is_invalid_not_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    document = yaml.safe_load(CONTRACT.read_text())
    scientific = document["immutable_contract"]["validation_plans"]["scientific"]
    calls = 0
    def mixed_fit(*args, **kwargs):
        nonlocal calls
        del args, kwargs
        calls += 1
        inf = {str(length): {"D_P": 1., "A": .5, "D_P_interval_95": [.2, 1.8],
               "D_P_centered_p": .001, "A_centered_p": .001} for length in (12, 24, 48)}
        test = {"terminal_outcome": "invalid", "reason": "support"} if calls == 5 else {"inference": {k: v.copy() for k, v in inf.items()}}
        return {"outcome": "test_open", "passed": False, "metrics": {
            "validation": {"inference": {k: v.copy() for k, v in inf.items()}, "D_P": 1., "D_R": .5, "A": .5},
            "test": test}}
    monkeypatch.setattr(native, "_fit_variant", mixed_fit)
    frame, receipt = _representation(_frame())
    result = native.evaluate_alpha_intent(frame=pd.DataFrame(), decision_frame=None,
        adaptive_frame=frame, parameter_grid=document["parameter_grid"],
        validation_start=scientific["partitions"]["validation"]["start"], validation_end=scientific["partitions"]["validation"]["end"],
        test_start=scientific["partitions"]["test"]["start"], test_end=scientific["partitions"]["test"]["end"],
        representation_plan_digest=document["execution_semantics"]["adaptive_representation_plan_digest"],
        adaptive_representation_receipt=receipt,
        adaptive_frame_digest=evaluation_frame_digest(frame),
        adaptive_dispatch_receipt=_dispatch_receipt(frame, receipt),
        point_in_time_eligibility=None,
        immutable_contract=document["immutable_contract"], costs=document["costs"])
    assert result["held_out_evaluation"]["outcome"] == "invalid"
