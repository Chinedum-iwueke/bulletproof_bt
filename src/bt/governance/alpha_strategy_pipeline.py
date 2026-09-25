"""Typed research-card drafting and pre-execution qualification for ALPHA-003."""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
import hashlib
import json
from math import prod
import os
from pathlib import Path
import re
import stat
from typing import Any

import yaml

from bt.contracts.research_specs_v2 import (
    EXACT_TRUTH,
    build_artifact_bundle,
    canonical_hash,
    validate_hypothesis_card,
)


def draft_research_card(
    assignment: dict[str, Any], *, repository_root: str
) -> dict[str, Any]:
    """Discover reviewed-source cards by exact question, never by topic similarity."""
    question = " ".join(assignment["question"].split())
    question_digest = canonical_hash({"question": question})
    if assignment["question_digest"] != question_digest:
        raise ValueError("question_digest_mismatch")
    if assignment.get("reusable_strategy") is not None:
        return draft_registered_strategy_card(
            assignment, repository_root=repository_root
        )
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    descriptor = os.open(Path(repository_root), flags | os.O_DIRECTORY)
    try:
        for part in ("research", "hypotheses", "cards"):
            try:
                child = os.open(part, flags | os.O_DIRECTORY, dir_fd=descriptor)
            except FileNotFoundError:
                raise ValueError("exact_engineered_strategy_card_missing") from None
            os.close(descriptor)
            descriptor = child
        try:
            card_fd = os.open(f"{question_digest}.json", flags, dir_fd=descriptor)
        except FileNotFoundError:
            raise ValueError("exact_engineered_strategy_card_missing") from None
        with os.fdopen(card_fd, "rb") as stream:
            before = os.fstat(stream.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_size > 1_000_000:
                raise ValueError("invalid_engineered_card_file")
            payload = stream.read(1_000_001)
            after = os.fstat(stream.fileno())
            changed = any(
                getattr(before, field) != getattr(after, field)
                for field in (
                    "st_dev",
                    "st_ino",
                    "st_size",
                    "st_mtime_ns",
                    "st_ctime_ns",
                )
            )
            if changed or len(payload) > 1_000_000:
                raise ValueError("engineered_card_changed_during_read")
        card = json.loads(payload)
    finally:
        os.close(descriptor)
    if not isinstance(card, dict):
        raise ValueError("engineered_card_must_be_object")
    if card.get("research_question") != question:
        raise ValueError("engineered_card_question_mismatch")
    if card.get("status") != "draft" or any(
        key in card for key in ("confirmed_by", "confirmed_at")
    ):
        raise ValueError("engineered_card_cannot_self_approve")
    expected_dataset = {
        "dataset_build_id": assignment["dataset_build_id"],
        "dataset_digest": assignment["dataset_digest"],
        "venue": assignment.get("venue", "bybit"),
        "instrument": assignment["instrument"],
        "timeframe": assignment["timeframe"],
    }
    if card.get("dataset_binding") != expected_dataset:
        raise ValueError("engineered_card_dataset_mismatch")
    if card.get("execution_window") != {
        "start": assignment["window_start"],
        "end": assignment["window_end"],
    }:
        raise ValueError("engineered_card_window_mismatch")
    errors = validate_hypothesis_card(card, require_confirmed=False)
    if errors:
        raise ValueError("invalid_engineered_card:" + ",".join(errors))
    representation_plan = assignment.get("representation_plan")
    if representation_plan is not None:
        card = deepcopy(card)
        if (
            card.get("execution_semantics", {}).get(
                "adaptive_representation_plan_digest"
            )
            == "assignment_bound_exact_plan"
        ):
            card["execution_semantics"]["adaptive_representation_plan_digest"] = (
                canonical_hash(representation_plan)
            )
        expected_fields = [
            item["output_field"] for item in representation_plan["transformations"]
        ]
        required_fields = [
            *expected_fields,
            "representation_plan_digest",
            "representation_output_fields",
            "representation_decision_ts",
        ]
        semantics = card.get("execution_semantics", {})
        declared_required = semantics.get("required_extra_columns")
        provenance_bound = bool(
            isinstance(declared_required, list)
            and declared_required[: len(expected_fields)] == expected_fields
            and all(
                field in declared_required
                for field in required_fields[len(expected_fields) :]
            )
        )
        if (
            semantics.get("adaptive_representation_plan_digest")
            != canonical_hash(representation_plan)
            or semantics.get("adaptive_representation_fields") != expected_fields
            or not (
                declared_required == expected_fields or provenance_bound
            )
        ):
            raise ValueError("engineered_card_adaptive_representation_mismatch")
    count = parameter_variant_count(card)
    if not 1 <= count <= min(8, assignment.get("max_variants", 8)):
        raise ValueError("engineered_card_parameter_budget_exceeded")
    return card


def parameter_variant_count(card: dict[str, Any]) -> int:
    grid = card["parameters"]
    if (
        not isinstance(grid, dict)
        or not grid
        or any(not isinstance(values, list) or not values for values in grid.values())
    ):
        raise ValueError("invalid_parameter_grid")
    return prod(len(values) for values in grid.values())


def draft_registered_strategy_card(
    assignment: dict[str, Any], *, repository_root: str
) -> dict[str, Any]:
    """Bind a new question to an exact, already-reviewed native engine contract."""
    capability = assignment["reusable_strategy"]
    if capability.get("bounded_weekly_reuse_eligible") is not True:
        raise ValueError("registered_strategy_not_weekly_eligible")
    if (
        capability.get("input_mode") != "single_instrument"
        or capability.get("maximum_instruments") != 1
        or len(assignment.get("instruments", [assignment["instrument"]])) != 1
    ):
        raise ValueError("registered_strategy_input_cardinality_mismatch")
    root = Path(repository_root).resolve(strict=True)
    allowed = (root / "research" / "hypotheses").resolve(strict=True)
    path = (root / str(capability["contract_path"])).resolve(strict=True)
    if allowed not in path.parents or path.suffix != ".yaml":
        raise ValueError("registered_strategy_path_invalid")
    payload_bytes = path.read_bytes()
    if hashlib.sha256(payload_bytes).hexdigest() != capability["contract_digest"]:
        raise ValueError("registered_strategy_digest_mismatch")
    payload = yaml.safe_load(payload_bytes)
    if not isinstance(payload, dict):
        raise ValueError("registered_strategy_contract_invalid")
    entry = payload.get("entry")
    parameters = payload.get("parameter_grid")
    semantics = {
        **payload.get("execution_semantics", {}),
        **{key: payload.get("truth_contract", {}).get(key) for key in EXACT_TRUTH},
    }
    contract_logging = payload.get("logging", {}).get("required_fields", [])
    logging = capability.get("logging_requirements", [])
    if (
        payload.get("hypothesis_id") != capability["hypothesis_id"]
        or not isinstance(entry, dict)
        or entry.get("strategy") != capability["strategy"]
        or not isinstance(parameters, dict)
        or prod(len(values) for values in parameters.values())
        != capability["variant_count"]
        or not isinstance(semantics, dict)
        or any(semantics.get(key) != expected for key, expected in EXACT_TRUTH.items())
        or not set(contract_logging).issubset(set(logging))
    ):
        raise ValueError("registered_strategy_capability_mismatch")
    if capability["variant_count"] > min(8, assignment.get("max_variants", 8)):
        raise ValueError("registered_strategy_parameter_budget_exceeded")
    signal_timeframe = str(
        entry.get("signal_timeframe", semantics.get("signal_timeframe", "1m"))
    )
    if assignment.get("research_timeframe", "1m") != signal_timeframe:
        raise ValueError("registered_strategy_timeframe_mismatch")
    question = " ".join(assignment["question"].split())
    citations = [
        {
            "object_id": item["object_id"],
            "content_digest": item["content_digest"],
            "coordinates": item["coordinates"],
        }
        for item in assignment.get("research_context", {}).get("citations", [])
    ]
    card = {
        "schema_version": "hypothesis_card_v1",
        "card_id": (
            f"reuse-{capability['hypothesis_id'].lower()}-"
            f"{assignment['question_digest'][:12]}"
        ),
        "program_id": f"alpha-campaign-{assignment['campaign_id'][:8]}",
        "version": 1,
        "status": "draft",
        "title": capability["title"],
        "claim": question,
        "intuition": capability["description"],
        "market_mechanism": capability["description"],
        "representation_mode": "registered_engine_contract",
        "engine_strategy_name": capability["strategy"],
        "engine_hypothesis_template": capability["contract_path"],
        "engine_hypothesis_template_digest": capability["contract_digest"],
        "features": [],
        "gates": [],
        "entry": {
            "type": "registered_engine_contract",
            "contract_path": capability["contract_path"],
        },
        "exit": {
            "type": "registered_engine_contract",
            "contract_path": capability["contract_path"],
        },
        "sizing": {
            "mode": "engine_contract",
            "stop_required": True,
        },
        "risk_controls": {
            "authority": "engine",
            "no_capital_research": True,
        },
        "parameters": parameters,
        "data_requirements": ["research_panel"],
        "logging_requirements": logging,
        "evaluation": payload.get("evaluation", {"required_tiers": ["Tier2", "Tier3"]}),
        "falsification_criteria": [
            "Reject when preregistered held-out or cost-stress gates fail.",
            "Reject when the exact native contract cannot replay deterministically.",
        ],
        "expected_failure_modes": [
            "The proposed mechanism does not survive held-out costs.",
            "The frozen native implementation is not an exact test of the question.",
        ],
        "execution_semantics": semantics,
        "source_citations": citations,
        "field_provenance": {
            field: {
                "state": "recommended",
                "confidence": 1.0,
                "basis": (
                    "Exact registered Bulletproof contract "
                    f"{capability['contract_digest']}"
                ),
            }
            for field in ("claim", "entry", "exit")
        },
        "dataset_binding": {
            "dataset_build_id": assignment["dataset_build_id"],
            "dataset_digest": assignment["dataset_digest"],
            "venue": assignment.get("venue", "bybit"),
            "instrument": assignment["instrument"],
            "timeframe": assignment["timeframe"],
        },
        "execution_window": {
            "start": assignment["window_start"],
            "end": assignment["window_end"],
        },
        "research_question": question,
    }
    errors = validate_hypothesis_card(card, require_confirmed=False)
    if errors:
        raise ValueError("invalid_registered_draft:" + ",".join(errors))
    return card


def governed_review_verified(
    assignment: dict[str, Any], qualification: dict[str, Any] | None
) -> bool:
    """Replay review evidence from an authenticated immutable control-plane assignment."""
    if not isinstance(qualification, dict):
        return False
    packet = qualification.get("governed_review")
    if not isinstance(packet, dict):
        return False
    subject, assertion = packet.get("subject"), packet.get("assertion")
    if not isinstance(subject, dict) or not isinstance(assertion, dict):
        return False
    if not isinstance(assertion.get("assignments"), list):
        return False
    producer, policy = assertion.get("producer"), assertion.get("policy")
    excluded = subject.get("producer_agent_ids")
    producer_identities = subject.get("producer_identities")
    if (
        packet.get("verdict") != "independence_demonstrated"
        or not re.fullmatch(r"[0-9a-f-]{36}", str(packet.get("route_id", "")))
        or assertion.get("schema_version") != "evaluation-independence-assertion-v1.0.0"
        or not re.fullmatch(r"[0-9a-f]{64}", str(assertion.get("route_digest", "")))
        or not isinstance(producer, dict)
        or not isinstance(policy, dict)
        or not isinstance(excluded, list)
        or not excluded
        or not all(isinstance(actor, str) and actor for actor in excluded)
        or not isinstance(producer_identities, list)
        or not producer_identities
        or not all(isinstance(identity, dict) for identity in producer_identities)
        or producer.get("agent_id") not in excluded
    ):
        return False
    for identity in producer_identities:
        if (
            not all(
                isinstance(identity.get(key), str) and identity[key]
                for key in (
                    "agent_id",
                    "package_digest",
                    "context_group",
                    "profile_digest",
                    "machine",
                    "provider",
                    "model_family",
                    "runtime",
                )
            )
            or not re.fullmatch(r"[0-9a-f]{64}", identity["package_digest"])
            or not re.fullmatch(r"[0-9a-f]{64}", identity["profile_digest"])
        ):
            return False
    if {identity.get("agent_id") for identity in producer_identities} != set(excluded):
        return False
    primary = subject.get("qualifier_identity")
    if (
        not isinstance(primary, dict)
        or primary not in producer_identities
        or any(
            primary.get(key) != producer.get(key)
            for key in (
                "agent_id",
                "package_digest",
                "context_group",
                "machine",
                "provider",
                "model_family",
                "runtime",
            )
        )
    ):
        return False
    required = policy.get("required_review_kinds")
    ceiling = policy.get("max_pairwise_shared_dimensions")
    if (
        not isinstance(required, list)
        or not all(isinstance(kind, str) and kind for kind in required)
        or not {"strategy_spec", "causality_leakage"}.issubset(set(required))
        or not isinstance(ceiling, int)
        or not 0 <= ceiling <= 4
    ):
        return False
    reviewers = []
    kinds = []
    for item in assertion["assignments"]:
        if not isinstance(item, dict):
            return False
        identity, review = (
            item.get("evaluator_identity"),
            item.get("alpha_strategy_review"),
        )
        if not isinstance(identity, dict) or not isinstance(review, dict):
            return False
        if (
            not isinstance(item.get("review_kind"), str)
            or not item["review_kind"]
            or identity.get("agent_id") in excluded
            or not all(
                isinstance(identity.get(key), str) and identity[key]
                for key in (
                    "agent_id",
                    "package_digest",
                    "context_group",
                    "profile_digest",
                    "machine",
                    "provider",
                    "model_family",
                    "runtime",
                )
            )
            or not re.fullmatch(r"[0-9a-f]{64}", identity["package_digest"])
            or not re.fullmatch(r"[0-9a-f]{64}", identity["profile_digest"])
            or not re.fullmatch(r"[0-9a-f]{64}", str(item.get("assignment_digest", "")))
            or item.get("review_digest") != canonical_hash(review)
            or review.get("subject_digest") != canonical_hash(subject)
            or review.get("verdict") != "approve"
            or review.get("blockers") != []
            or not isinstance(review.get("checks"), list)
            or not review["checks"]
            or not isinstance(review.get("rationale"), str)
            or len(review["rationale"]) < 20
            or not isinstance(item.get("correlation_report"), dict)
        ):
            return False
        for other in (producer, *producer_identities, *reviewers):
            if any(
                identity.get(key) == other.get(key)
                for key in (
                    "agent_id",
                    "package_digest",
                    "context_group",
                )
            ):
                return False
            if (
                sum(
                    identity.get(key) == other.get(key)
                    for key in (
                        "machine",
                        "provider",
                        "model_family",
                        "runtime",
                    )
                )
                > ceiling
            ):
                return False
        reviewers.append(identity)
        kinds.append(item.get("review_kind"))
    if len(kinds) != len(set(kinds)) or set(kinds) != set(required):
        return False
    expected = {
        "campaign_digest": assignment["campaign_digest"],
        "question_digest": assignment["question_digest"],
        "source_commit": assignment["base_ref"],
        "card_digest": canonical_hash(qualification.get("card")),
        "artifact_bundle_digest": canonical_hash(qualification.get("artifact_bundle")),
    }
    return (
        all(subject.get(key) == value for key, value in expected.items())
        and assertion.get("subject_digest") == canonical_hash(subject)
        and packet.get("receipt_digest") == canonical_hash(assertion)
    )


def draft_weekend_momentum_card(assignment: dict[str, Any]) -> dict[str, Any]:
    question = " ".join(assignment["question"].split())
    lowered = question.casefold()
    if (
        not all(term in lowered for term in ("weekend", "momentum"))
        or assignment["instrument"] != "BTCUSDT"
        or not re.search(r"\bbtc\b", lowered)
    ):
        raise ValueError("question_requires_bounded_strategy_engineering")
    citations = [
        {
            "object_id": item["object_id"],
            "content_digest": item["content_digest"],
            "coordinates": item["coordinates"],
        }
        for item in assignment.get("research_context", {}).get("citations", [])
    ]
    card = {
        "schema_version": "hypothesis_card_v1",
        "card_id": f"alpha-weekend-momentum-{assignment['question_digest'][:12]}",
        "program_id": f"alpha-campaign-{assignment['campaign_id'][:8]}",
        "version": 1,
        "status": "draft",
        "title": "BTC Weekend Lagged-Return Momentum",
        "claim": "During UTC weekends, the sign of BTC perpetual trailing 60-minute return predicts the same sign of the next tradable return after realistic costs.",
        "intuition": "Lower weekend liquidity may increase short-horizon continuation after directional order-flow shocks.",
        "market_mechanism": "Reduced weekend participation can lower displayed depth and slow replenishment, allowing recent directional displacement to persist briefly.",
        "engine_strategy_name": "alpha_weekend_momentum",
        "engine_hypothesis_template": "research/hypotheses/alpha_weekend_momentum.yaml",
        "features": [
            {
                "id": "day_of_week",
                "source": "ohlcv",
                "transform": "calendar_day",
                "lag": 0,
            },
            {
                "id": "momentum_60m",
                "source": "ohlcv",
                "source_field": "close",
                "transform": "return",
                "window": 60,
                "lag": 1,
            },
            {
                "id": "momentum_abs",
                "source": "derived",
                "transform": "abs",
                "inputs": ["momentum_60m"],
                "lag": 0,
            },
            {
                "id": "atr_60",
                "source": "ohlcv",
                "transform": "atr",
                "window": 60,
                "lag": 1,
            },
        ],
        "gates": [
            {"left": "day_of_week", "op": ">=", "right": 5},
            {"left": "momentum_abs", "op": ">=", "right_param": "momentum_threshold"},
        ],
        "entry": {
            "direction": "feature_sign",
            "direction_feature": "momentum_60m",
            "timing": "bar_close_submit_next_bar_execution",
            "pyramiding": False,
            "flip": False,
        },
        "exit": {
            "type": "fixed_stop_time_exit",
            "stop_param": "stop_atr_multiple",
            "max_hold_param": "max_hold_bars",
        },
        "sizing": {
            "mode": "constant_r",
            "risk_parameter": "r_per_trade",
            "stop_required": True,
        },
        "risk_controls": {
            "r_per_trade": 0.005,
            "max_positions": 1,
            "max_notional_pct_equity": 0.25,
            "max_gross_notional_pct_equity": 0.25,
            "max_leverage": 1.0,
            "forbid_pyramiding": True,
        },
        "parameters": {
            "momentum_lookback_bars": [60],
            "momentum_threshold": [0.0025, 0.005],
            "stop_atr_multiple": [2.0, 3.0],
            "max_hold_bars": [30, 60],
            "atr_window": [60],
            "r_per_trade": [0.005],
        },
        "data_requirements": ["research_panel"],
        "logging_requirements": [
            "decision_trace",
            "stop_price",
            "momentum_60m",
            "day_of_week",
            "entry_state_liquidity_regime",
        ],
        "evaluation": {
            "tiers": ["Tier2B"],
            "metrics": [
                "oos_trade_count",
                "oos_mean_net_r",
                "double_cost_oos_mean_net_r",
                "maximum_drawdown",
                "selection_bias_audit",
            ],
        },
        "falsification_criteria": [
            "Held-out mean net R is not positive",
            "Double-cost held-out mean net R is not positive",
            "Fewer than 50 held-out trades",
            "The effect is not distinct from weekday behavior in the comparison receipt",
        ],
        "expected_failure_modes": [
            "Weekend is only a volatility proxy",
            "The result is fee-sensitive",
            "UTC weekend boundaries do not match liquidity conditions",
            "A small grid overstates robustness",
        ],
        "execution_semantics": {
            **EXACT_TRUTH,
            "signal_timeframe": "1m",
            "base_execution_timeframe": "1m",
            "base_data_frequency_expected": "1m",
            "exit_monitoring_timeframe": "1m",
            "signal_bar_policy": "closed_bar_only",
            "stop_model": "atr_multiple",
            "stop_update_policy": "fixed_at_entry",
            "tp_update_policy": "none",
            "hold_time_unit": "signal_bars",
            "atr_source_timeframe": "1m",
        },
        "source_citations": citations,
        "field_provenance": {
            "claim": {
                "state": "recommended",
                "confidence": 0.8,
                "basis": "selected Research Intelligence question narrowed to an executable causal claim",
            },
            "entry": {
                "state": "recommended",
                "confidence": 0.8,
                "basis": "lagged 60-minute return and next-bar engine execution",
            },
            "exit": {
                "state": "recommended",
                "confidence": 0.7,
                "basis": "bounded fixed-stop and time-exit contract",
            },
        },
        "dataset_binding": {
            "dataset_build_id": assignment["dataset_build_id"],
            "dataset_digest": assignment["dataset_digest"],
            "venue": assignment.get("venue", "bybit"),
            "instrument": assignment["instrument"],
            "timeframe": assignment["timeframe"],
        },
        "execution_window": {
            "start": assignment["window_start"],
            "end": assignment["window_end"],
        },
        "research_question": question,
    }
    errors = validate_hypothesis_card(card, require_confirmed=False)
    if errors:
        raise ValueError("invalid_draft:" + ",".join(errors))
    return card


def confirm_card(
    card: dict[str, Any], *, actor: str, confirmed_at: str
) -> dict[str, Any]:
    result = deepcopy(card)
    result["status"] = "confirmed"
    result["confirmed_by"] = actor
    result["confirmed_at"] = confirmed_at
    for field in ("claim", "entry", "exit"):
        result["field_provenance"][field]["state"] = "confirmed"
    errors = validate_hypothesis_card(result)
    if errors:
        raise ValueError("invalid_confirmation:" + ",".join(errors))
    return result


def qualify_card(card: dict[str, Any], *, repository_root: str) -> dict[str, Any]:
    bundle = build_artifact_bundle(
        card, repo_root=repository_root, available_datasets={"research_panel"}
    )
    readiness = bundle["compile_readiness"]
    gates = {
        "schema_valid": True,
        "causality_valid": all(
            int(item.get("lag", 0)) >= 0 for item in card["features"]
        ),
        "auxiliary_joins_backward": all(
            item.get("join", "backward") == "backward" for item in card["features"]
        ),
        "leakage_review_passed": card["execution_semantics"]["aux_join_direction"]
        == "backward"
        and card["execution_semantics"]["missing_bars"] == "no_decision",
        "strategy_compilable": readiness["status"]
        in {"registry_ready", "graph_compilable"},
        "independent_review_complete": False,
    }
    review = {
        "schema_version": "alpha-deterministic-spec-check-v1.0.0",
        "reviewer": "bt.spec_compiler",
        "reviewed_at": datetime.now(UTC).isoformat(),
        "card_digest": canonical_hash(card),
        "gates": gates,
        "blockers": readiness["blockers"],
        "independent_of_drafter": False,
        "execution_authority": False,
    }
    review["review_digest"] = canonical_hash(review)
    return {
        "schema_version": "alpha-strategy-qualification-v1.0.0",
        "card": card,
        "card_digest": canonical_hash(card),
        "artifact_bundle": bundle,
        "review": review,
        "qualified": (
            all(gates.values())
            if card.get("independent_review_required", False)
            else all(
                value for key, value in gates.items()
                if key != "independent_review_complete"
            )
        ),
        "qualification_scope": "deterministic_compilation_only",
        "tier": "Tier2B",
        "dataset": card["dataset_binding"],
        "window": card["execution_window"],
        "parameter_grid": card["parameters"],
        "variant_count": parameter_variant_count(card),
        "authority": {
            "capital": False,
            "orders": False,
            "promotion": False,
            "self_approval": False,
        },
    }


def complete_independent_review(
    assignment: dict[str, Any], qualification: dict[str, Any]
) -> dict[str, Any]:
    """Promote compilation to qualification only from bound external review evidence."""
    if not governed_review_verified(assignment, qualification):
        raise ValueError("independent_specification_review_unverified")
    result = deepcopy(qualification)
    result["review"]["gates"]["independent_review_complete"] = True
    result["review"]["independent_of_drafter"] = True
    result["review"]["governed_review_receipt_digest"] = canonical_hash(
        assignment["governed_review"]
        if "governed_review" in assignment
        else qualification.get("governed_review")
    )
    result["review"]["review_digest"] = canonical_hash(
        {key: value for key, value in result["review"].items() if key != "review_digest"}
    )
    result["qualified"] = all(result["review"]["gates"].values())
    result["qualification_scope"] = "independently_reviewed_execution"
    return result
