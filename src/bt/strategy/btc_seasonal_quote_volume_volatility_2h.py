"""Native BTC seasonal quote-volume surprise / future-volatility study.

This module has no order authority.  Predictor state is prior-only; the final
two bars may complete targets but are never decision rows.
"""
from __future__ import annotations

from itertools import product
import hashlib
import json
import math
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from bt.core.types import Bar, Signal
from bt.strategy import register_strategy
from bt.strategy.base import Strategy

QUESTION = ("Does a high point-in-time BTCUSDT completed-2h quote-volume surprise, "
            "standardized against prior observations from the same UTC two-hour bucket, "
            "predict higher BTCUSDT realized volatility over the next two contiguous completed 2h bars?")
PLAN_DIGESTS = {
    "smoke": "57d9a71d4ea536c845952a82755c377768b8195c839c193a045d123be7685a97",
    "scientific": "abce5cb33cd22d5fb55e64b80deacfe13838695161f3986eadd157d562899dea",
}
PROPOSAL_DIGEST = "532cf1f5331e39dff121e4224b9188d66b4b37229ad8aadaa1522cceef87aecb"
ACCEPTED_REPRESENTATION_DIGEST = "06334f51b04604847ff509e259bdbea335f0b54a78926cc30b1aee98c7333146"
REPRESENTATION_DIGEST = "206555a30e397a1549f13c7b234c8ba9ed10b36bc9cd7439f1585edc65d850bd"
FIELDS = ("btcusdt_log1p_quote_volume_2h", "btcusdt_log_return_2h",
          "btcusdt_prior_24h_realized_volatility", "btcusdt_range_over_close_2h",
          "btcusdt_quote_volume_2h", "btcusdt_volume_2h")
FROZEN_GRID = {"volume_surprise_quantile": (0.90, 0.95),
               "seasonal_reference_days": (90, 180),
               "prior_volatility_bars": (12,), "uncertainty_block_length": (12,)}
MIN_ROWS = {"training": 2500, "validation": 850, "test": 850}
MIN_EVENTS = {"training": 80, "validation": 40, "test": 40}
INNER_REPLICATES = 10_000
TARGET_TAIL_START = pd.Timestamp("2026-05-18T16:00:00Z")
TARGET_TAIL_END = pd.Timestamp("2026-05-18T20:00:00Z")
CALIBRATION_CAPS = {"complete_history": 65_936, "full_functional_evaluation": 320,
                    "ols_fit": 21_233_760, "statistic_evaluation": 266_240_000}
RAW_OHLC = ("BTCUSDT__open", "BTCUSDT__high", "BTCUSDT__low", "BTCUSDT__close")
REPRESENTATION_OUTPUT_FIELDS = (
    FIELDS[0], FIELDS[1], FIELDS[2], "btcusdt_high_low_spread_2h",
    FIELDS[3], FIELDS[4], FIELDS[5],
)
VARIANT_IDS = ("q90_L90", "q90_L180", "q95_L90", "q95_L180")
DATASET_BINDING_FIELDS = frozenset({
    "dataset_build_id", "dataset_digest", "catalog_digest", "manifest_digest",
    "producer_receipt_digest", "lake_governance_digest", "partition_digests",
    "venue", "instrument",
})
CALIBRATION_SCENARIOS = {
    "null_serial_deff_2_25": {"phi": 5 / 13, "target_primary": 0.0},
    "null_serial_deff_4": {"phi": 3 / 5, "target_primary": 0.0},
    "effect_0_75_serial_deff_2_25": {"phi": 5 / 13, "target_primary": 0.75},
    "effect_0_75_serial_deff_4": {"phi": 3 / 5, "target_primary": 0.75},
}
CALIBRATION_INDICATORS = (
    *(f"primary_{variant}" for variant in VARIANT_IDS),
    *(f"paired_{variant}" for variant in VARIANT_IDS),
    "primary_family", "paired_family", "joint_passage",
)


def registered_variant_parameters(
    parameter_grid: Mapping[str, Sequence[Any]],
) -> list[dict[str, Any]]:
    """Materialize the accepted variant identity order for every caller."""
    normalized = {key: tuple(parameter_grid.get(key, ())) for key in FROZEN_GRID}
    if normalized != FROZEN_GRID:
        raise ValueError("parameter_grid_differs_from_frozen_four_variants")
    by_id = {
        f"q{int(float(q) * 100)}_L{int(days)}": {
            "volume_surprise_quantile": float(q),
            "seasonal_reference_days": int(days),
            "prior_volatility_bars": 12,
            "uncertainty_block_length": 12,
        }
        for q, days in product(
            FROZEN_GRID["volume_surprise_quantile"],
            FROZEN_GRID["seasonal_reference_days"],
        )
    }
    if tuple(by_id) != VARIANT_IDS:
        raise ValueError("registered_variant_identity_mismatch")
    return [by_id[variant_id] for variant_id in VARIANT_IDS]


def calibration_work(outer_replicates: int) -> dict[str, int]:
    """Return exact unlike-unit work for the frozen four-scenario design."""
    return {
        "complete_history": 4 * 8190 * 2 + 4 * outer_replicates,
        "full_functional_evaluation": 4 * (4 * 19 + 1),
        "ols_fit": 4 * 8190 * 4 * 19 * 8 + 4 * 8190 * 8 * 2
        + 4 * outer_replicates * 8,
        "statistic_evaluation": 4 * outer_replicates * 2 * 4 * 2 * 4 * 10_000,
    }


def calibration_contract() -> dict[str, Any]:
    """Machine-readable accepted calibration interface; grants no execution."""
    contract = {
        "schema_version": "btc-seasonal-volume-calibration-contract-v1",
        "seed": 521010,
        "scenarios": CALIBRATION_SCENARIOS,
        "variants": list(VARIANT_IDS),
        "innovation": {
            "vector": ["u_r", "u_q", "u_w", "u_e"],
            "distribution": "iid_over_t_multivariate_normal",
            "dimension": 4, "mean": [0.0, 0.0, 0.0, 0.0],
            "marginal_variances": [1.0, 1.0, 1.0, 1.0],
            "off_diagonal_covariance": 0.25,
            "panel_independence": "solve_verify_and_each_outer_path_disjoint",
            "philox_key_template": [521010, "panel", "dgp_id",
                                    "path_id", "process", "time"],
        },
        "generative_method": {
            "initialization": {
                "close_0": 1.0, "h_q_0": 0.0, "h_w_0": 0.0,
                "pre_burn_in_returns_controls_events": 0.0,
                "burn_in_bars": 2160,
                "history": "continuous_burn_in_partitions_boundary_exclusions_two_target_tail_bars",
            },
            "state_transitions": {
                "phi_by_scenario": [5 / 13, 3 / 5],
                "h_q": "h_q[t]=phi*h_q[t-1]+sqrt(1-phi^2)*u_q[t]",
                "h_w": "h_w[t]=phi*h_w[t-1]+sqrt(1-phi^2)*u_w[t]",
                "quote_volume": "quote_volume[t]=exp(20+h_q[t])",
                "log_quote_volume": "l[t]=log1p(quote_volume[t])",
                "wick_driver": "w[t]=exp(-4+h_w[t])",
            },
            "controls": {
                "absolute_return": "a[t]=abs(r[t])",
                "prior_24h_realized_volatility": "v[t]=sqrt(sum(r[t-j]^2,j=1..12))",
                "range_over_close": "g[t]=(high[t]-low[t])/close[t]",
                "log_quote_volume": "l[t]=log1p(quote_volume[t])",
                "utc_buckets": "11 two-hour indicators; 00:00-02:00 omitted",
                "standardization": "training_only",
                "wick_driver_is_not_a_control": True,
            },
            "return": "r[t]=theta_0+0.20*a[t-1]+0.25*v[t-1]+0.15*g[t-1]+0.10*l[t-1]+sum(gamma[j]*W[t-1,j],j=1..8)+u_r[t]",
            "derived_ohlcv": {
                "open": "open[t]=close[t-1]",
                "close": "close[t]=close[t-1]*exp(r[t])",
                "high": "high[t]=max(open[t],close[t])*(1+w[t]/2)",
                "low": "low[t]=min(open[t],close[t])/(1+w[t]/2)",
                "volume": "volume[t]=quote_volume[t]/close[t]",
            },
            "seasonal_surprise": {
                "reference_days": [90, 180],
                "reference": "strictly_prior_eligible_same_UTC_two_hour_bucket",
                "scale": "sample_standard_deviation",
                "invalid_when": ["missing_history", "zero_scale", "nonfinite"],
            },
            "events": {
                "variant_order": list(VARIANT_IDS),
                "primary": "I_P[t,k]=1{s[t,L_k]>training_linear_quantile(q_k)}",
                "rival": "I_R[t,k]=1{z_training(l[t])>training_linear_quantile(q_k)}",
                "strict_threshold": True,
                "event_vector": "W[1:4]=I_P; W[5:8]=I_R",
                "thresholds_frozen_in_validation_and_test": True,
            },
            "outcome": "y[t]=sqrt(r[t+1]^2+r[t+2]^2) on contiguous completed 2h bars",
            "eligibility_and_support": {
                "eligible_rows": {"training": 2500, "validation": 850, "test": 850},
                "primary_events": {"training": 80, "validation": 40, "test": 40},
                "rival_events": {"training": 80, "validation": 40, "test": 40},
                "post_generation_gates": ["continuity", "finite_values", "liquidity",
                    "seasonal_history", "design_rank_17", "primary_support", "rival_support",
                    "listing_and_point_in_time_membership"],
                "conditioning": "complete_generated_experiment_satisfying_every_frozen_gate",
            },
        },
        "generative_equation": {
            "return": "r[t]=theta_0+0.20*a[t-1]+0.25*v[t-1]+0.15*g[t-1]+0.10*l[t-1]+sum(gamma[j]*W[t-1,j],j=1..8)+u_r[t]",
            "coefficient_roles": ["theta_0_intercept",
                "gamma_1_primary_q90_L90", "gamma_2_primary_q90_L180",
                "gamma_3_primary_q95_L90", "gamma_4_primary_q95_L180",
                "gamma_5_rival_q90_L90", "gamma_6_rival_q90_L180",
                "gamma_7_rival_q95_L90", "gamma_8_rival_q95_L180"],
            "shared_outcome_across_variants": True,
        },
        "solver": {"histories_per_scenario": 8190, "coefficient_dimension": 9,
                   "method": "fixed_minimum_norm_newton",
                   "initial_coefficients": [0.0] * 9,
                   "updates": 4, "damping": [1.0, 0.5, 0.25, 0.125],
                   "central_step": 1e-5,
                   "functionals_per_update": 19,
                   "final_candidate_functionals": 1,
                   "maximum_functionals_per_scenario": 77,
                   "required_jacobian_rank": 8, "relative_svd_threshold": 1e-8,
                   "maximum_target_residual": 0.005,
                   "returned_iterate": "b_4_evaluated_explicitly",
                   "infeasible_when": ["rank_below_8",
                       "target_residual_above_0.005",
                       "conditioning_probability_zero", "sigma_nonfinite_or_at_most_1e-8"]},
        "verification": {"histories_per_scenario": 8190,
                         "maximum_contrast_error": 0.01,
                         "maximum_identity_error": 1e-12,
                         "minimum_sigma": 1e-8, "maximum_mc_se": 0.005},
        "outer": {"replicates_per_scenario": 104, "inner_replicates": 10_000,
                  "indicators": list(CALIBRATION_INDICATORS),
                  "maximum_clopper_pearson_half_width": 0.10},
        "work": calibration_work(104), "caps": dict(CALIBRATION_CAPS),
        "execution_authority": False, "scientific_certification": False,
    }
    contract["dgp_contract_digest"] = _calibration_dgp_digest(contract)
    contract["contract_digest"] = hashlib.sha256(
        json.dumps(contract, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()
    return contract


def _calibration_dgp_digest(contract: Mapping[str, Any]) -> str:
    bound = {
        key: contract[key]
        for key in ("innovation", "generative_method", "generative_equation", "scenarios", "variants")
    }
    return hashlib.sha256(
        json.dumps(bound, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()


def _clopper_pearson(successes: int, attempts: int) -> tuple[float, float]:
    from scipy.stats import beta

    low = 0.0 if successes == 0 else float(beta.ppf(0.025, successes, attempts - successes + 1))
    high = 1.0 if successes == attempts else float(beta.ppf(0.975, successes + 1, attempts - successes))
    return low, high


def _structured_int(value: Any) -> int | None:
    """Return an exact structured integer, or ``None`` for malformed input."""
    if isinstance(value, bool) or not isinstance(value, (int, np.integer)):
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _structured_float(value: Any) -> float | None:
    """Return a finite structured number without leaking conversion errors."""
    if isinstance(value, bool) or not isinstance(value, (int, float, np.number)):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _exact_registered_members(
    value: Any, registered_ids: Sequence[str], reason_prefix: str,
) -> tuple[list[Mapping[str, Any]], list[str]]:
    """Validate a JSON collection before any member access or iteration.

    Registered identities are strings and must occur exactly once.  Keeping
    this boundary separate prevents malformed extras from being ignored by a
    set comprehension and subsequently reaching hashing or member access.
    """
    if not isinstance(value, list):
        return [], [f"{reason_prefix}_not_list"]
    if any(not isinstance(item, Mapping) for item in value):
        return [], [f"{reason_prefix}_member_not_mapping"]
    members = list(value)
    identities = [item.get("id") for item in members]
    if any(not isinstance(identity, str) for identity in identities):
        return [], [f"{reason_prefix}_id_not_string"]
    expected = list(registered_ids)
    reasons: list[str] = []
    duplicates = sorted({identity for identity in identities if identities.count(identity) > 1})
    if duplicates:
        reasons.append(f"{reason_prefix}_duplicate_id")
    if set(identities) - set(expected):
        reasons.append(f"{reason_prefix}_extra_id")
    if set(expected) - set(identities):
        reasons.append(f"{reason_prefix}_missing_id")
    if len(identities) != len(expected) and not reasons:
        reasons.append(f"{reason_prefix}_cardinality_mismatch")
    return (members if not reasons else []), reasons


def validate_synthetic_calibration_receipt(receipt: Mapping[str, Any]) -> dict[str, Any]:
    """Validate synthetic lifecycle evidence without generating histories."""
    contract = calibration_contract()
    reasons: list[str] = []
    if not isinstance(receipt, Mapping):
        receipt = {}
        reasons.append("receipt_not_mapping")
    attempts_value = _structured_int(receipt.get("outer_replicates"))
    if attempts_value is None or attempts_value < 0:
        attempts = -1
        reasons.append("outer_replicates_invalid")
    else:
        attempts = attempts_value
    work = receipt.get("work")
    expected_work = calibration_work(attempts) if attempts >= 0 else {}
    if receipt.get("contract_digest") != contract["contract_digest"]:
        reasons.append("contract_digest_mismatch")
    if receipt.get("dgp_contract_digest") != _calibration_dgp_digest(contract):
        reasons.append("dgp_contract_digest_mismatch")
    if work != expected_work:
        reasons.append("typed_work_mismatch")
    exceeded = {
        key: int(expected_work.get(key, 0))
        for key, cap in CALIBRATION_CAPS.items()
        if int(expected_work.get(key, 0)) > cap
    }
    scenario_receipts, collection_reasons = _exact_registered_members(
        receipt.get("scenarios"), tuple(CALIBRATION_SCENARIOS), "scenarios"
    )
    reasons.extend(collection_reasons)
    precision_ok = attempts == 104
    precision: dict[str, dict[str, list[float]]] = {}
    for scenario in scenario_receipts:
        scenario_id = scenario["id"]
        target = CALIBRATION_SCENARIOS[scenario_id]["target_primary"]
        solve = scenario.get("solve", {})
        verification = scenario.get("verification", {})
        outer = scenario.get("outer", {})
        if not isinstance(solve, Mapping):
            solve = {}
        if not isinstance(verification, Mapping):
            verification = {}
        if not isinstance(outer, Mapping):
            outer = {}
        solve_residual = _structured_float(solve.get("max_target_residual"))
        if scenario.get("dgp_contract_digest") != _calibration_dgp_digest(contract):
            reasons.append(f"{scenario_id}:dgp_contract_digest_mismatch")
        if solve.get("philox_key") != [521010, "solve", scenario_id] or any(
            solve.get(key) != value for key, value in {
                "histories": 8190, "updates": 4,
                "functional_evaluations": 77,
                "jacobian_rank": 8, "coefficient_dimension": 9,
            }.items()
        ) or solve.get("method") != "fixed_minimum_norm_newton" \
                or solve.get("initial_coefficients") != [0.0] * 9 \
                or solve.get("damping") != [1.0, 0.5, 0.25, 0.125] \
                or solve.get("functionals_per_update") != 19 \
                or solve.get("final_candidate_functionals") != 1 \
                or solve.get("returned_iterate") != "b_4_evaluated_explicitly" \
                or solve.get("central_step") != 1e-5 \
                or solve.get("relative_svd_threshold") != 1e-8 \
                or solve_residual is None or solve_residual > 0.005:
            reasons.append(f"{scenario_id}:solver_contract_failed")
        if verification.get("philox_key") != [521010, "verify", scenario_id] or verification.get("histories") != 8190:
            reasons.append(f"{scenario_id}:verification_binding_failed")
        variants, variant_reasons = _exact_registered_members(
            verification.get("variants"), VARIANT_IDS,
            f"{scenario_id}:verification_variants",
        )
        reasons.extend(variant_reasons)
        for item in variants:
            dp = _structured_float(item.get("D_P"))
            dr = _structured_float(item.get("D_R"))
            paired = _structured_float(item.get("A"))
            sigma = _structured_float(item.get("sigma"))
            mc_se = _structured_float(item.get("max_mc_se"))
            identity_error = _structured_float(item.get("identity_error"))
            if (None in (dp, dr, paired, sigma, mc_se, identity_error)
                    or abs(dp - target) > 0.01 or abs(dr) > 0.01
                    or abs(paired - target) > 0.01
                    or abs(paired - (dp - dr)) > 1e-12
                    or identity_error > 1e-12
                    or sigma <= 1e-8 or mc_se > 0.005):
                reasons.append(f"{scenario_id}:{item.get('id')}:verification_failed")
        if outer.get("philox_key") != [521010, "outer", scenario_id] or outer.get("attempted") != attempts:
            reasons.append(f"{scenario_id}:outer_binding_failed")
        invalid_attempts = outer.get("invalid_attempts")
        failure_reasons = outer.get("failure_reasons")
        invalid_count = _structured_int(invalid_attempts)
        failure_counts = ([_structured_int(value) for value in failure_reasons.values()]
                          if isinstance(failure_reasons, Mapping)
                          and all(isinstance(key, str) for key in failure_reasons)
                          else [None])
        if (invalid_count is None or not 0 <= invalid_count <= attempts
                or any(value is None or value < 0 for value in failure_counts)
                or sum(value for value in failure_counts if value is not None) != invalid_count):
            reasons.append(f"{scenario_id}:invalid_outer_accounting")
        indicators = outer.get("indicators")
        if not isinstance(indicators, Mapping) or set(indicators) != set(CALIBRATION_INDICATORS):
            reasons.append(f"{scenario_id}:outer_indicators_missing")
            indicators = {}
        for indicator, successes in indicators.items():
            count = _structured_int(successes)
            if count is None or not 0 <= count <= attempts:
                reasons.append(f"{scenario_id}:{indicator}:invalid_success_count")
                continue
            low, high = _clopper_pearson(count, attempts)
            precision.setdefault(scenario_id, {})[indicator] = [low, high]
            if (high - low) / 2 > 0.10:
                precision_ok = False
    if reasons:
        status = "failed"
    elif exceeded:
        status = "capacity_blocked"
    elif not precision_ok:
        status = "inconclusive"
    else:
        status = "success"
    return {"schema_version": "btc-seasonal-volume-calibration-terminal-v1",
            "status": status, "synthetic_validation_only": True,
            "scientific_certification": False, "execution_authority": False,
            "outer_replicates": attempts, "three_outer_replicates_qualify": False,
            "work": expected_work, "caps": dict(CALIBRATION_CAPS),
            "exceeded": exceeded, "reasons": reasons,
            "clopper_pearson_95": precision,
            "contract_digest": contract["contract_digest"]}


def _ts(value: Any) -> pd.Timestamp:
    result = pd.Timestamp(value)
    if result.tz is None:
        raise ValueError("strict_utc_required")
    return result.tz_convert("UTC")


def _json_value_valid(value: Any, active: set[int] | None = None) -> bool:
    """Return whether ``value`` is finite, acyclic, native JSON data."""
    if value is None or isinstance(value, (str, bool)):
        return True
    if isinstance(value, int) and not isinstance(value, bool):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    if not isinstance(value, (Mapping, list)):
        return False
    active = set() if active is None else active
    identity = id(value)
    if identity in active:
        return False
    active.add(identity)
    try:
        if isinstance(value, Mapping):
            return all(
                isinstance(key, str) and _json_value_valid(member, active)
                for key, member in value.items()
            )
        return all(_json_value_valid(member, active) for member in value)
    finally:
        active.remove(identity)


def _json_digest(value: Any) -> str | None:
    """Hash only a value already proven to have a valid JSON shape."""
    if not _json_value_valid(value):
        return None
    try:
        payload = json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    except (TypeError, ValueError, OverflowError):
        return None
    return hashlib.sha256(payload).hexdigest()


def _representation_receipt_valid(
    frame: pd.DataFrame, receipt: Mapping[str, Any] | None, plan_digest: str,
) -> bool:
    """Bind evaluator eligibility to the native complete-bar producer receipt."""
    if not isinstance(receipt, Mapping) or not _json_value_valid(receipt):
        return False
    bound = dict(receipt)
    claimed_digest = bound.pop("receipt_digest", None)
    receipt_digest = _json_digest(bound)
    if claimed_digest != receipt_digest:
        return False
    output_fields = list(REPRESENTATION_OUTPUT_FIELDS)
    if (receipt.get("schema_version") != "adaptive-representation-receipt-v1.0.0"
            or receipt.get("plan_digest") != plan_digest
            or receipt.get("basket") != ["BTCUSDT"]
            or receipt.get("research_timeframe") != "2h"
            or receipt.get("output_fields") != output_fields
            or receipt.get("aligned_rows") != len(frame)
            or receipt.get("complete_rows") != {"BTCUSDT": len(frame)}
            or receipt.get("future_fill_used") is not False
            or receipt.get("outcome_data_consulted") is not False
            or frame.empty):
        return False
    digest_payload = {
        "columns": list(frame.columns), "rows": len(frame),
        "first_decision_at": pd.Timestamp(frame["decision_at"].iloc[0]).isoformat(),
        "last_decision_at": pd.Timestamp(frame["decision_at"].iloc[-1]).isoformat(),
        "null_counts": {field: int(frame[field].isna().sum())
                        for field in output_fields},
    }
    frame_digest = hashlib.sha256(
        json.dumps(digest_payload, sort_keys=True, separators=(",", ":"),
                   ensure_ascii=True).encode("ascii")
    ).hexdigest()
    return receipt.get("frame_digest") == frame_digest


def evaluation_frame_digest(frame: pd.DataFrame) -> str:
    """Bind the exact adaptive values dispatched by the native assignment runner."""
    value_hash = pd.util.hash_pandas_object(frame, index=True).to_numpy(
        dtype=np.uint64
    ).tobytes()
    header = json.dumps(list(frame.columns), separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(header + b"\0" + value_hash).hexdigest()


def _dispatch_receipt_valid(
    frame: pd.DataFrame, receipt: Mapping[str, Any] | None,
    representation_receipt: Mapping[str, Any] | None,
) -> bool:
    """Verify the runner's pre-dispatch value witness, not a caller assertion."""
    if (not isinstance(receipt, Mapping)
            or not isinstance(representation_receipt, Mapping)
            or not _json_value_valid(receipt)
            or not _json_value_valid(representation_receipt)):
        return False
    body = dict(receipt)
    claimed = body.pop("receipt_digest", None)
    actual = _json_digest(body)
    return bool(
        claimed == actual
        and body.get("schema_version") == "adaptive-frame-dispatch-receipt-v1"
        and body.get("producer_receipt_digest") == representation_receipt.get("receipt_digest")
        and body.get("frame_value_digest") == evaluation_frame_digest(frame)
    )


_ELIGIBILITY_CAPABILITY = object()


@dataclass(frozen=True)
class RunnerEligibilityEvidence:
    """Opaque evidence capability issued only after native source admission."""
    receipt_digest: str
    dataset_bindings_digest: str
    evidence_digest: str
    _capability: object


def admit_point_in_time_eligibility(
    receipt: Mapping[str, Any] | None,
    immutable_contract: Mapping[str, Any],
    *,
    verified_dataset_bindings_digest: str,
) -> RunnerEligibilityEvidence | None:
    """Resolve a contract-bound receipt against already verified source bytes."""
    if not isinstance(immutable_contract, Mapping):
        return None
    dataset_bindings = immutable_contract.get("dataset_bindings", [])
    if (not isinstance(receipt, Mapping)
            or not isinstance(dataset_bindings, list)
            or len(dataset_bindings) != 1
            or not _json_value_valid(receipt)
            or not _json_value_valid(dataset_bindings)
            or any(not isinstance(binding, Mapping)
                   or not DATASET_BINDING_FIELDS.issubset(binding)
                   for binding in dataset_bindings)):
        return None
    body = dict(receipt)
    claimed = body.pop("receipt_digest", None)
    actual = _json_digest(body)
    binding_digest = _json_digest(dataset_bindings)
    if actual is None or binding_digest is None:
        return None
    source_digests = [binding.get("dataset_digest") for binding in dataset_bindings]
    binding_identities = [
        (binding.get("dataset_build_id"), binding.get("dataset_digest"),
         binding.get("instrument"))
        for binding in dataset_bindings
    ]
    if any(
        not isinstance(source_digest, str) or len(source_digest) != 64
        for source_digest in source_digests
    ) or any(
        not all(isinstance(value, str) and value for value in identity)
        for identity in binding_identities
    ) or len(set(binding_identities)) != len(binding_identities):
        return None
    admitted_source_digests = {
        source_digest for source_digest in source_digests
    }
    try:
        valid_from = _ts(body.get("valid_from"))
        valid_through = _ts(body.get("valid_through"))
    except (TypeError, ValueError, OverflowError):
        return None
    valid = bool(
        claimed == actual
        and claimed == immutable_contract.get("point_in_time_membership_receipt_digest")
        and body.get("schema_version") == "point-in-time-membership-receipt-v1"
        and body.get("venue") == "bybit"
        and body.get("instrument") == "BTCUSDT"
        and body.get("listing_verified") is True
        and body.get("membership_verified") is True
        and body.get("dataset_bindings_digest") == binding_digest
        and body.get("authority") == "admitted_point_in_time_source_receipt"
        and binding_digest == verified_dataset_bindings_digest
        and body.get("evidence_digest") in admitted_source_digests
        and valid_from <= pd.Timestamp("2024-11-19T20:00:00Z")
        and valid_through >= pd.Timestamp("2026-05-18T20:00:00Z")
        and isinstance(body.get("evidence_digest"), str)
        and len(body["evidence_digest"]) == 64
    )
    if not valid:
        return None
    return RunnerEligibilityEvidence(
        receipt_digest=str(claimed), dataset_bindings_digest=binding_digest,
        evidence_digest=str(body["evidence_digest"]),
        _capability=_ELIGIBILITY_CAPABILITY,
    )


def _runner_eligibility_valid(
    evidence: RunnerEligibilityEvidence | None,
    immutable_contract: Mapping[str, Any],
) -> bool:
    if (not isinstance(evidence, RunnerEligibilityEvidence)
            or not isinstance(immutable_contract, Mapping)):
        return False
    bindings = immutable_contract.get("dataset_bindings", [])
    if (not isinstance(bindings, list)
            or not _json_value_valid(bindings)
            or any(not isinstance(binding, Mapping) for binding in bindings)):
        return False
    binding_digest = _json_digest(bindings)
    if binding_digest is None:
        return False
    return bool(
        evidence._capability is _ELIGIBILITY_CAPABILITY
        and evidence.receipt_digest
        == immutable_contract.get("point_in_time_membership_receipt_digest")
        and evidence.dataset_bindings_digest == binding_digest
    )


def _linear_quantile(values: np.ndarray, q: float) -> float:
    return float(np.quantile(values, q, method="linear"))


def _prepare(frame: pd.DataFrame) -> pd.DataFrame:
    missing = {"ts", *FIELDS, *RAW_OHLC} - set(frame)
    if missing:
        raise ValueError("missing_representation_fields:" + ",".join(sorted(missing)))
    data = frame.loc[:, ["ts", *FIELDS, *RAW_OHLC]].copy()
    data["ts"] = [_ts(value) for value in data["ts"]]
    data = data.sort_values("ts", kind="stable").reset_index(drop=True)
    if data.ts.duplicated().any():
        raise ValueError("duplicate_timestamp")
    for field in (*FIELDS, *RAW_OHLC):
        data[field] = pd.to_numeric(data[field], errors="coerce")
    aligned = (data.ts.dt.minute.eq(0) & data.ts.dt.second.eq(0)
               & data.ts.dt.microsecond.eq(0) & data.ts.dt.hour.mod(2).eq(0))
    open_, high, low, close = (data[field] for field in RAW_OHLC)
    valid_ohlc = (
        np.isfinite(data[list(RAW_OHLC)].to_numpy(float)).all(axis=1)
        & (open_ > 0) & (high > 0) & (low > 0) & (close > 0)
        & (low <= np.minimum(open_, close))
        & (high >= np.maximum(open_, close)) & (high >= low)
    )
    accepted_inputs = [FIELDS[0], FIELDS[1], FIELDS[3], FIELDS[4], FIELDS[5]]
    finite = np.isfinite(data[accepted_inputs].to_numpy(float)).all(axis=1)
    physical_valid = (aligned & valid_ohlc & finite & (data[FIELDS[5]] >= 0)
             & np.isclose(data[FIELDS[3]], (high - low) / close,
                          rtol=1e-10, atol=1e-15))
    # Liquidity is a decision-time eligibility gate.  It must not be reused for
    # either future bar because that would remove outcomes using future volume.
    valid = physical_valid & (data[FIELDS[4]] >= 1_000_000)
    data["bar_valid"] = valid
    data["physical_bar_valid"] = physical_valid
    data["bucket"] = data.ts.dt.hour // 2
    data["a"] = data[FIELDS[1]].abs()
    # The accepted estimator is root-sum-of-squares, not demeaned rolling std.
    # Reconstruct it from exact-grid completed returns instead of substituting
    # the producer's diagnostic realized-volatility field.
    returns = data[FIELDS[1]].to_numpy(float)
    valid_array = data["bar_valid"].to_numpy(bool)
    physical_valid_array = data["physical_bar_valid"].to_numpy(bool)
    timestamp_ns = data["ts"].astype("int64").to_numpy()
    exact_step = np.diff(timestamp_ns) == pd.Timedelta(hours=2).value
    squared = pd.Series(returns * returns)
    prior_sum = squared.rolling(12, min_periods=12).sum().shift(1).to_numpy()
    prior_valid = pd.Series(valid_array.astype(int)).rolling(12, min_periods=12).sum().shift(1).to_numpy() == 12
    prior_contiguous = np.zeros(len(data), dtype=bool)
    if len(data) > 12:
        prior_contiguous[12:] = (
            pd.Series(exact_step.astype(int)).rolling(12, min_periods=12).sum()
            .to_numpy()[11:] == 12
        )
    data["v"] = np.where(prior_valid & prior_contiguous, np.sqrt(prior_sum), np.nan)
    data["g"] = data[FIELDS[3]]
    data["l"] = data[FIELDS[0]]
    target_complete = np.zeros(len(data), dtype=bool)
    target = np.full(len(data), np.nan)
    if len(data) > 2:
        complete = (physical_valid_array[1:-1] & physical_valid_array[2:]
                    & exact_step[:-1] & exact_step[1:])
        target_complete[:-2] = complete
        target[:-2] = np.where(
            complete, np.sqrt(returns[1:-1] ** 2 + returns[2:] ** 2), np.nan
        )
    data["y"] = target
    data["target_complete"] = target_complete
    return data


def _seasonal(data: pd.DataFrame, days: int) -> pd.Series:
    output = np.full(len(data), np.nan, dtype=float)
    count = days  # one observation per UTC 2h bucket per day
    timestamps = data["ts"].astype("int64").to_numpy()
    values = data["l"].to_numpy(float)
    valid = data["bar_valid"].to_numpy(bool)
    one_day_ns = pd.Timedelta(days=1).value
    for bucket in range(12):
        indexes = np.flatnonzero(data["bucket"].to_numpy() == bucket)
        for position in range(count, len(indexes)):
            i = int(indexes[position])
            prior_idx = indexes[position - count:position]
            prior = values[prior_idx]
            # Exact-grid continuity: the same bucket must occur on every prior day.
            expected = timestamps[i] - np.arange(count, 0, -1) * one_day_ns
            if not np.array_equal(timestamps[prior_idx], expected):
                continue
            scale = prior.std(ddof=1)
            if (np.isfinite(prior).all() and valid[prior_idx].all()
                    and scale > 0 and valid[i]):
                output[i] = (values[i] - prior.mean()) / scale
    return pd.Series(output, index=data.index, dtype=float)


def _partition(data: pd.DataFrame, start: Any, end: Any) -> pd.DataFrame:
    start, end = _ts(start), _ts(end)
    return data.loc[(data.ts >= start) & (data.ts < end)].copy()


def _design(rows: pd.DataFrame, event: np.ndarray, means: np.ndarray, scales: np.ndarray) -> np.ndarray:
    controls = (rows[["a", "v", "g", "l"]].to_numpy(float) - means) / scales
    buckets = np.column_stack([(rows.bucket.to_numpy() == bucket).astype(float) for bucket in range(1, 12)])
    return np.column_stack([np.ones(len(rows)), event.astype(float), controls, buckets])


def _contrast(residual: np.ndarray, event: np.ndarray) -> float:
    return float(residual[event].mean() - residual[~event].mean())


def _terminal_evidence(outcome: str, reason: str, decision_ts: Any) -> list[dict[str, Any]]:
    timestamp = _ts(decision_ts).isoformat()
    return [{"record_kind": "terminal_no_observation", "outcome": outcome,
             "reason": reason, "decision_ts": timestamp,
             "representation_plan_digest": REPRESENTATION_DIGEST,
             "representation_output_fields": json.dumps(
                 REPRESENTATION_OUTPUT_FIELDS, separators=(",", ":")
             ),
             "representation_decision_ts": timestamp,
             "decision_trace": {"scientific_observation": False,
                                "reason": reason}}]


def _bootstrap(rp: np.ndarray, rr: np.ndarray, ep: np.ndarray, er: np.ndarray,
               *, block_length: int, seed: int) -> dict[str, Any]:
    """Registered circular moving-block inference without refitting."""
    n = len(rp)
    rng = np.random.Generator(np.random.Philox(seed))
    blocks = math.ceil(n / block_length)
    dp0, dr0 = _contrast(rp, ep), _contrast(rr, er)
    a0 = dp0 - dr0
    dp = np.full(INNER_REPLICATES, np.nan)
    aa = np.full(INNER_REPLICATES, np.nan)
    undefined = 0
    offsets = np.arange(block_length)
    batch_size = 128
    for first in range(0, INNER_REPLICATES, batch_size):
        size = min(batch_size, INNER_REPLICATES - first)
        starts = rng.integers(0, n, size=(size, blocks))
        indexes = ((starts[:, :, None] + offsets) % n).reshape(size, -1)[:, :n]
        bep, ber = ep[indexes], er[indexes]
        count_p, count_r = bep.sum(axis=1), ber.sum(axis=1)
        defined = ((count_p > 0) & (count_p < n)
                   & (count_r > 0) & (count_r < n))
        undefined += int((~defined).sum())
        if not defined.any():
            continue
        selected = indexes[defined]
        selected_p, selected_r = bep[defined], ber[defined]
        selected_count_p, selected_count_r = count_p[defined], count_r[defined]
        primary_values, rival_values = rp[selected], rr[selected]
        primary = ((primary_values * selected_p).sum(axis=1) / selected_count_p
                   - (primary_values * ~selected_p).sum(axis=1)
                   / (n - selected_count_p))
        rival = ((rival_values * selected_r).sum(axis=1) / selected_count_r
                 - (rival_values * ~selected_r).sum(axis=1)
                 / (n - selected_count_r))
        targets = np.flatnonzero(defined) + first
        dp[targets] = primary
        aa[targets] = primary - rival
    if undefined:
        return {"valid": False, "undefined_replicates": undefined,
                "reason": "bootstrap_event_group_absent_no_redraw"}
    return {"valid": True, "undefined_replicates": 0, "block_length": block_length,
            "D_P": dp0, "A": a0,
            "D_P_interval_95": np.quantile(dp, [.025, .975], method="linear").tolist(),
            "A_interval_95": np.quantile(aa, [.025, .975], method="linear").tolist(),
            "D_P_centered_p": float((1 + np.sum(dp - dp0 >= dp0)) / (INNER_REPLICATES + 1)),
            "A_centered_p": float((1 + np.sum(aa - a0 >= a0)) / (INNER_REPLICATES + 1))}


def _fit_variant(
    data: pd.DataFrame,
    params: Mapping[str, Any],
    windows: Mapping[str, tuple[Any, Any]],
    *,
    evaluate_partitions: tuple[str, ...] = ("validation",),
    frozen_state: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    q, days = float(params["volume_surprise_quantile"]), int(params["seasonal_reference_days"])
    data = data.copy()
    data["s"] = _seasonal(data, days)
    parts = {name: _partition(data, *windows[name]) for name in ("training", "validation", "test")}
    base_cols = ["a", "v", "g", "l", "s", "y"]
    if frozen_state is None:
        train = parts["training"].dropna(subset=base_cols)
        means = train[["a", "v", "g", "l"]].mean().to_numpy(float)
        scales = train[["a", "v", "g", "l"]].std(ddof=0).to_numpy(float)
        if len(train) < MIN_ROWS["training"] or not np.isfinite(scales).all() or (scales <= 0).any():
            reason = "insufficient_eligible_training_rows_or_scale"
            return {"outcome": "invalid", "reason": reason, "passed": False,
                    "metrics": {}, "decision_records": [],
                    "terminal_evidence_records": _terminal_evidence(
                        "invalid", reason, windows["training"][0])}
        primary_threshold = _linear_quantile(train.s.to_numpy(float), q)
        rival_z_train = (train.l.to_numpy(float) - means[3]) / scales[3]
        rival_threshold = _linear_quantile(rival_z_train, q)
        ip = train.s.to_numpy(float) > primary_threshold
        ir = rival_z_train > rival_threshold
        support = {"training": {"primary": int(ip.sum()), "rival": int(ir.sum())}}
        if min(support["training"].values()) < MIN_EVENTS["training"]:
            reason = "insufficient_training_event_support"
            return {"outcome": "invalid", "reason": reason, "passed": False,
                    "metrics": {"support": support}, "decision_records": [],
                    "terminal_evidence_records": _terminal_evidence(
                        "invalid", reason, windows["training"][0])}
        xp, xr = _design(train, ip, means, scales), _design(train, ir, means, scales)
        if np.linalg.matrix_rank(xp) != 17 or np.linalg.matrix_rank(xr) != 17:
            reason = "deficient_training_design_rank"
            return {"outcome": "invalid", "reason": reason, "passed": False,
                    "metrics": {"support": support}, "decision_records": [],
                    "terminal_evidence_records": _terminal_evidence(
                        "invalid", reason, windows["training"][0])}
        bp = np.linalg.lstsq(xp, train.y.to_numpy(float), rcond=None)[0]
        br = np.linalg.lstsq(xr, train.y.to_numpy(float), rcond=None)[0]
        design_artifact = {
            "columns": ["intercept", "event", "z_abs_return", "z_prior_rv12",
                        "z_range_over_close", "z_log1p_quote_volume",
                        *[f"utc_bucket_{bucket}" for bucket in range(1, 12)]],
            "primary_rank": int(np.linalg.matrix_rank(xp)),
            "rival_rank": int(np.linalg.matrix_rank(xr)),
            "training_rows": int(len(train)),
            "standardizer_means": means.tolist(),
            "standardizer_scales": scales.tolist(),
            "primary_coefficients": bp.tolist(),
            "rival_coefficients": br.tolist(),
            "target": "sqrt(r[t+1]^2+r[t+2]^2)",
        }
    else:
        try:
            artifact = frozen_state["design_artifact"]
            thresholds = frozen_state["thresholds"]
            training_support = frozen_state["support"]["training"]
            means = np.asarray(artifact["standardizer_means"], dtype=float)
            scales = np.asarray(artifact["standardizer_scales"], dtype=float)
            bp = np.asarray(artifact["primary_coefficients"], dtype=float)
            br = np.asarray(artifact["rival_coefficients"], dtype=float)
            primary_threshold = float(thresholds["primary"])
            rival_threshold = float(thresholds["rival"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("invalid_frozen_training_state") from exc
        if (means.shape != (4,) or scales.shape != (4,) or bp.shape != (17,)
                or br.shape != (17,) or not np.isfinite(np.r_[means, scales, bp, br,
                                                              primary_threshold,
                                                              rival_threshold]).all()
                or (scales <= 0).any()):
            raise ValueError("invalid_frozen_training_state")
        support = {"training": dict(training_support)}
        design_artifact = dict(artifact)
    metrics: dict[str, Any] = {
        "support": support,
        "thresholds": {"primary": primary_threshold, "rival": rival_threshold},
        "design_artifact": design_artifact,
        "training_state_reused": frozen_state is not None,
    }
    for name in evaluate_partitions:
        rows = parts[name].dropna(subset=base_cols)
        if len(rows) < MIN_ROWS[name]:
            if name == "test":
                metrics[name] = {"terminal_outcome": "invalid", "reason": "insufficient_eligible_test_rows"}
                continue
            reason = f"insufficient_eligible_{name}_rows"
            return {"outcome": "invalid", "reason": reason, "passed": False,
                    "metrics": metrics, "decision_records": [],
                    "terminal_evidence_records": _terminal_evidence(
                        "invalid", reason, windows[name][0])}
        ep = rows.s.to_numpy(float) > primary_threshold
        er = ((rows.l.to_numpy(float) - means[3]) / scales[3]) > rival_threshold
        support[name] = {"primary": int(ep.sum()), "rival": int(er.sum())}
        if min(support[name].values()) < MIN_EVENTS[name]:
            if name == "test":
                metrics[name] = {"terminal_outcome": "invalid", "reason": "insufficient_test_event_support", "support": support[name]}
                continue
            reason = f"insufficient_{name}_event_support"
            return {"outcome": "invalid", "reason": reason, "passed": False,
                    "metrics": metrics, "decision_records": [],
                    "terminal_evidence_records": _terminal_evidence(
                        "invalid", reason, windows[name][0])}
        rp = rows.y.to_numpy(float) - (_design(rows, ep, means, scales) @ bp - bp[1] * ep)
        rr = rows.y.to_numpy(float) - (_design(rows, er, means, scales) @ br - br[1] * er)
        dp, dr = _contrast(rp, ep), _contrast(rr, er)
        inference = {}
        for length in (12, 24, 48):
            result = _bootstrap(rp, rr, ep, er, block_length=length,
                                seed=521010 + days * 100 + int(q * 100) + length + (0 if name == "validation" else 10_000))
            if not result["valid"]:
                if name == "test":
                    metrics[name] = {"terminal_outcome": "invalid", "reason": result["reason"], "undefined_replicates": result["undefined_replicates"]}
                    break
                reason = result["reason"]
                return {"outcome": "invalid", "reason": reason,
                        "passed": False, "metrics": metrics,
                        "decision_records": [],
                        "terminal_evidence_records": _terminal_evidence(
                            "invalid", reason, windows[name][0])}
            inference[str(length)] = result
        if name == "test" and "terminal_outcome" in metrics.get(name, {}):
            continue
        records = [
            {"decision_ts": row.ts.isoformat(),
             "seasonal_surprise": float(row.s),
             "primary_event": bool(primary), "rival_event": bool(rival),
             "future_realized_volatility_4h": float(row.y),
             "D_P": dp, "D_R": dr, "A": dp - dr,
             "terminal_outcome": "scientific_observation"}
            for row, primary, rival in zip(
                rows.itertuples(index=False), ep, er, strict=True
            )
        ]
        metrics[name] = {"D_P": dp, "D_R": dr, "A": dp - dr,
                         "rows": len(rows), "inference": inference,
                         "decision_records": records}
    # Familywise decisions are attached after all four variants are available.
    return {"outcome": "test_open", "reason": None, "passed": False, "metrics": metrics,
            "uncertainty": {"method": "circular_moving_block_centered_bootstrap_Holm", "inner_replicates": INNER_REPLICATES,
                            "primary_block_lengths": [12, 24], "sensitivity_block_lengths": [24, 48], "executed": True},
            "scientific_certification": False,
            "decision_records": metrics.get("validation", {}).get("decision_records", []),
            "terminal_evidence_records": []}


def _holm(pvalues: list[float]) -> list[bool]:
    rejected = [False] * len(pvalues)
    for rank, index in enumerate(sorted(range(len(pvalues)), key=pvalues.__getitem__)):
        if pvalues[index] <= .05 / (len(pvalues) - rank):
            rejected[index] = True
        else:
            break
    return rejected


def evaluate_alpha_intent(*, frame: pd.DataFrame, decision_frame: pd.DataFrame | None,
                          adaptive_frame: pd.DataFrame | None, parameter_grid: Mapping[str, Sequence[Any]],
                          validation_start: Any, validation_end: Any, test_start: Any, test_end: Any,
                          representation_plan_digest: str | None, immutable_contract: Mapping[str, Any],
                          costs: Mapping[str, Any],
                          adaptive_representation_receipt: Mapping[str, Any] | None = None,
                          adaptive_frame_digest: str | None = None,
                          adaptive_dispatch_receipt: Mapping[str, Any] | None = None,
                          point_in_time_eligibility: RunnerEligibilityEvidence | None = None) -> dict[str, Any]:
    """Evaluate all exact variants through the governed native-runner protocol."""
    del frame, decision_frame, costs
    if adaptive_frame is None:
        raise ValueError("adaptive_representation_required")
    normalized = {key: tuple(parameter_grid.get(key, ())) for key in FROZEN_GRID}
    if normalized != FROZEN_GRID:
        raise ValueError("parameter_grid_differs_from_frozen_four_variants")
    if immutable_contract.get("accepted_method_proposal_digest") != PROPOSAL_DIGEST:
        raise ValueError("accepted_method_binding_mismatch")
    if representation_plan_digest != REPRESENTATION_DIGEST:
        raise ValueError("accepted_representation_plan_binding_mismatch")
    if not _representation_receipt_valid(
        adaptive_frame, adaptive_representation_receipt, representation_plan_digest
    ):
        raise ValueError("complete_bar_representation_receipt_required")
    if adaptive_frame_digest != evaluation_frame_digest(adaptive_frame):
        raise ValueError("adaptive_frame_value_digest_mismatch")
    if not _dispatch_receipt_valid(
        adaptive_frame, adaptive_dispatch_receipt, adaptive_representation_receipt
    ):
        raise ValueError("authoritative_adaptive_dispatch_receipt_required")
    if not _runner_eligibility_valid(point_in_time_eligibility, immutable_contract):
        reason = "point_in_time_listing_membership_unverified"
        variants = []
        for params in registered_variant_parameters(parameter_grid):
            variants.append({"parameters": params, "outcome": "invalid",
                             "reason": reason, "passed": False, "metrics": {},
                             "decision_records": [],
                             "terminal_evidence_records": _terminal_evidence(
                                 "invalid", reason, validation_start)})
        return {"selection_candidates": variants, "selected_parameters": None,
                "selection_metric": "validation_frozen_OLS_residual_D_P",
                "held_out_evaluation": {"outcome": "invalid", "passed": False,
                    "held_out_evaluated": False, "metrics": {},
                    "failed_gates": [reason], "scientific_certification": False,
                    "decision_records": [], "terminal_evidence_records":
                    _terminal_evidence("invalid", reason, validation_start)},
                "decision_records": [],
                "representation_plan_digest": representation_plan_digest,
                "calibration_status": "not_executed", "scientific_certification": False}
    data = _prepare(adaptive_frame.rename(columns={"decision_at": "ts"}))
    train_window = immutable_contract["validation_plans"]["scientific"]["partitions"]["training"]
    windows = {"training": (train_window["start"], train_window["end"]),
               "validation": (validation_start, validation_end), "test": (test_start, test_end)}
    variants = []
    for params in registered_variant_parameters(parameter_grid):
        result = _fit_variant(data, params, windows, evaluate_partitions=("validation",))
        variants.append({"parameters": params, **result})
    valid_indexes = [i for i, x in enumerate(variants) if x["outcome"] == "test_open"]
    if len(valid_indexes) == 4:
        for partition in ("validation",):
            for length in (12, 24, 48):
                hp = _holm([variants[i]["metrics"][partition]["inference"][str(length)]["D_P_centered_p"] for i in valid_indexes])
                ha = _holm([variants[i]["metrics"][partition]["inference"][str(length)]["A_centered_p"] for i in valid_indexes])
                for position, i in enumerate(valid_indexes):
                    inference = variants[i]["metrics"][partition]["inference"][str(length)]
                    inference["holm_primary_rejected"] = hp[position]
                    inference["holm_paired_rejected"] = ha[position]
        for i in valid_indexes:
            def partition_passes(partition: str) -> bool:
                metric = variants[i]["metrics"][partition]
                return all(
                    metric["inference"][str(length)]["D_P_interval_95"][0] > 0
                    and metric["inference"][str(length)]["D_P"] > 0
                    and metric["inference"][str(length)]["A"] > 0
                    and metric["inference"][str(length)]["holm_primary_rejected"]
                    and metric["inference"][str(length)]["holm_paired_rejected"]
                    for length in (12, 24, 48)
                )
            validation_passed = partition_passes("validation")
            variants[i]["outcome"] = "positive" if validation_passed else "negative"
            variants[i]["passed"] = validation_passed
    else:
        for i in valid_indexes:
            variants[i]["outcome"] = "inconclusive"
            variants[i]["passed"] = False
    eligible_indexes = [i for i, item in enumerate(variants)
                        if item["outcome"] in {"positive", "negative"}]
    if len(eligible_indexes) == 4:
        # Validation does not select or replace a method.  Once the complete
        # validation family is frozen, each of the four registered variants is
        # evaluated exactly once on held-out test data.
        test_results = [
            _fit_variant(data, variants[i]["parameters"], windows,
                         evaluate_partitions=("test",),
                         frozen_state=variants[i]["metrics"])
            for i in eligible_indexes
        ]
        test_family_valid = all(
            "inference" in result["metrics"].get("test", {})
            for result in test_results
        )
        if test_family_valid:
            for length in (12, 24, 48):
                hp = _holm([result["metrics"]["test"]["inference"][str(length)]["D_P_centered_p"]
                            for result in test_results])
                ha = _holm([result["metrics"]["test"]["inference"][str(length)]["A_centered_p"]
                            for result in test_results])
                for position, result in enumerate(test_results):
                    inference = result["metrics"]["test"]["inference"][str(length)]
                    inference["holm_primary_rejected"] = hp[position]
                    inference["holm_paired_rejected"] = ha[position]
        held_variants = []
        for position, result in enumerate(test_results):
            metrics = result["metrics"].get("test", {})
            if test_family_valid:
                passed = all(
                    metrics["inference"][str(length)]["D_P_interval_95"][0] > 0
                    and metrics["inference"][str(length)]["D_P"] > 0
                    and metrics["inference"][str(length)]["A"] > 0
                    and metrics["inference"][str(length)]["holm_primary_rejected"]
                    and metrics["inference"][str(length)]["holm_paired_rejected"]
                    for length in (12, 24, 48)
                )
                outcome = "positive" if passed else "negative"
            else:
                passed, outcome = False, "invalid"
            variants[eligible_indexes[position]]["test_outcome"] = outcome
            held_variants.append({"parameters": variants[eligible_indexes[position]]["parameters"],
                                  "outcome": outcome, "passed": passed,
                                  "metrics": metrics,
                                  "scientific_certification": False,
                                  "decision_records": metrics.get("decision_records", []),
                                  "terminal_evidence_records": [] if metrics.get("decision_records")
                                  else _terminal_evidence("invalid", metrics.get("reason", "held_out_family_invalid"), windows["test"][0])})
        aggregate_outcome = ("invalid" if not test_family_valid else
                             "positive" if any(item["passed"] for item in held_variants)
                             else "negative")
        held = {"outcome": aggregate_outcome,
                "passed": aggregate_outcome == "positive",
                "held_out_evaluated": True, "variant_evaluations": held_variants,
                "metrics": {"variant_count": 4},
                "failed_gates": [] if aggregate_outcome == "positive"
                else ["held_out_family_invalid" if aggregate_outcome == "invalid"
                      else "held_out_inference_failed"],
                "scientific_certification": False,
                "decision_records": [record for item in held_variants
                                     for record in item["decision_records"]],
                "terminal_evidence_records": [record for item in held_variants
                                               for record in item["terminal_evidence_records"]]}
    else:
        terminal = "inconclusive" if any(
            item["outcome"] == "inconclusive" for item in variants
        ) else "failed"
        held = {"outcome": "failed", "passed": False,
                "held_out_evaluated": False, "metrics": {},
                "failed_gates": ["no_valid_variant"],
                "scientific_certification": False, "decision_records": [],
                "terminal_evidence_records": _terminal_evidence(
                    terminal, "incomplete_registered_validation_family",
                    windows["validation"][0])}
        held["outcome"] = terminal
        held["failed_gates"] = ["incomplete_registered_validation_family"]
    return {"selection_candidates": variants, "selected_parameters": None,
            "selection_metric": "none_all_four_registered_variants", "held_out_evaluation": held,
            "decision_records": [], "representation_plan_digest": representation_plan_digest,
            "calibration_status": "not_executed", "scientific_certification": False}


@register_strategy("btc_seasonal_quote_volume_volatility_2h")
class BtcSeasonalQuoteVolumeVolatility2hStrategy(Strategy):
    """Observation-only classic-engine adapter with instance-local state."""
    def __init__(self, **_: Any) -> None:
        self.records: list[dict[str, Any]] = []

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: Mapping[str, Any]) -> list[Signal]:
        del ctx
        bar = bars_by_symbol.get("BTCUSDT")
        if bar is None or "BTCUSDT" not in tradeable:
            return []
        extra = bar.extra if isinstance(bar.extra, Mapping) else {}
        decision_ts = _ts(ts)
        tail = (TARGET_TAIL_START <= decision_ts < TARGET_TAIL_END
                or bool(extra.get("target_only_tail", False)))
        valid = all(field in extra and np.isfinite(float(extra[field])) for field in FIELDS)
        valid = bool(
            valid and decision_ts.minute == 0 and decision_ts.second == 0
            and decision_ts.microsecond == 0 and decision_ts.hour % 2 == 0
            and bar.open > 0 and bar.high > 0 and bar.low > 0 and bar.close > 0
            and bar.low <= min(bar.open, bar.close)
            and bar.high >= max(bar.open, bar.close) and bar.high >= bar.low
            and float(extra[FIELDS[4]]) >= 1_000_000
            and float(extra[FIELDS[5]]) >= 0
            and math.isclose(float(extra[FIELDS[0]]),
                             math.log1p(float(extra[FIELDS[4]])),
                             rel_tol=1e-10, abs_tol=1e-12)
            and math.isclose(float(extra[FIELDS[3]]),
                             (bar.high - bar.low) / bar.close,
                             rel_tol=1e-10, abs_tol=1e-15)
        )
        outcome = "target_only" if tail else "consumed" if valid else "invalid"
        self.records.append({"ts": _ts(ts).isoformat(), "outcome": outcome})
        if tail or not valid:
            return []
        return [Signal(ts=ts, symbol="BTCUSDT", side=None, signal_type="btc_seasonal_volume_observation",
                       confidence=0.0, metadata={"strategy": "btc_seasonal_quote_volume_volatility_2h",
                       "research_observation_only": True, "no_order_authority": True,
                       "native_payload_outcome": outcome, "accepted_method_proposal_digest": PROPOSAL_DIGEST})]
