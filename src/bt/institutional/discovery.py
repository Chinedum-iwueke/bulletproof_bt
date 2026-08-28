"""DISC-002..005 and DISC-007 authoritative scientific producers."""

from __future__ import annotations

import hashlib
import itertools
import math
from collections.abc import Iterable
from typing import Any

import numpy as np

from .receipt import ProducerReceipt, build_receipt, digest


class DiscoveryProducerError(ValueError):
    """A discovery computation violates its preregistered contract."""


_BINARY = {"add", "subtract", "multiply", "divide", "greater", "less"}
_UNARY = {"abs", "negate", "log"}


def _compile(
    node: dict[str, Any], fields: dict[str, dict[str, Any]], depth: int = 0
) -> tuple[dict[str, Any], str]:
    if depth > 32 or set(node) != {"op", "args"}:
        raise DiscoveryProducerError("invalid or excessive factor expression")
    op, args = node["op"], node["args"]
    if op == "field":
        if len(args) != 2 or args[0] not in fields or int(args[1]) < 0:
            raise DiscoveryProducerError(
                "field requires known name and nonnegative lag"
            )
        lag = int(args[1]) + int(fields[args[0]].get("availability_lag", 0))
        if lag < 1:
            raise DiscoveryProducerError("factor reads information before availability")
        return {"op": "field", "field": args[0], "effective_lag": lag}, fields[args[0]][
            "unit"
        ]
    if op == "constant":
        value = float(args[0])
        if len(args) != 1 or not math.isfinite(value):
            raise DiscoveryProducerError("constant must be finite")
        return {"op": "constant", "value": value}, "dimensionless"
    if op in _UNARY:
        child, unit = _compile(args[0], fields, depth + 1)
        if len(args) != 1 or (op == "log" and unit != "dimensionless"):
            raise DiscoveryProducerError("invalid unary expression")
        return {"op": op, "args": [child]}, unit
    if op in _BINARY:
        if len(args) != 2:
            raise DiscoveryProducerError("binary operator requires two arguments")
        left, left_unit = _compile(args[0], fields, depth + 1)
        right, right_unit = _compile(args[1], fields, depth + 1)
        if op in {"add", "subtract", "greater", "less"} and left_unit != right_unit:
            raise DiscoveryProducerError("unit mismatch")
        unit = "boolean" if op in {"greater", "less"} else left_unit
        if op == "multiply" and left_unit != "dimensionless":
            unit = (
                right_unit
                if right_unit == "dimensionless"
                else f"({left_unit}*{right_unit})"
            )
        if op == "divide":
            unit = (
                "dimensionless"
                if left_unit == right_unit
                else f"({left_unit}/{right_unit})"
            )
        return {"op": op, "args": [left, right]}, unit
    raise DiscoveryProducerError(f"unsupported operator {op}")


def factor_program_receipt(
    *, specification: dict[str, Any], dataset_digest: str, source_commit: str
) -> ProducerReceipt:
    fields = specification["fields"]
    factors: dict[str, Any] = {}
    for name, factor in sorted(specification["factors"].items()):
        expression, unit = _compile(factor["expression"], fields)
        if unit != factor["output_unit"]:
            raise DiscoveryProducerError(
                "declared factor unit differs from inferred unit"
            )
        factors[name] = {"expression": expression, "unit": unit}
    parameters = specification.get("parameters", {})
    combinations = (
        list(itertools.product(*(parameters[name] for name in sorted(parameters))))
        if parameters
        else [()]
    )
    if len(combinations) > int(specification["maximum_trials"]):
        raise DiscoveryProducerError("parameter grid exceeds maximum_trials")
    trials = []
    for ordinal, values in enumerate(combinations, start=1):
        assignment = dict(zip(sorted(parameters), values, strict=True))
        trials.append(
            {
                "ordinal": ordinal,
                "parameters": assignment,
                "trial_digest": digest(
                    {"specification": specification, "parameters": assignment}
                ),
            }
        )
    result = {
        "schema_version": "disc003-factor-program-v1.0.0",
        "factors": factors,
        "trials": trials,
        "trial_count": len(trials),
    }
    return build_receipt(
        milestone="DISC-003",
        producer="bt.institutional.discovery.factor_program_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs=specification,
        dataset_digest=dataset_digest,
        configuration={"maximum_trials": specification["maximum_trials"]},
        artifacts=result,
        result=result,
    )


def search_proposal_receipt(
    *,
    program_receipt: ProducerReceipt,
    observations: Iterable[dict[str, Any]],
    method: str,
    seed: int,
    budget: int,
    source_commit: str,
) -> ProducerReceipt:
    if program_receipt.milestone != "DISC-003":
        raise DiscoveryProducerError("search requires DISC-003 receipt")
    trials = program_receipt.result["trials"]
    if (
        budget < 1
        or budget > len(trials)
        or method not in {"exhaustive", "random", "structured"}
    ):
        raise DiscoveryProducerError("invalid search method or budget")
    history = list(observations)
    observed = {item["trial_digest"]: item for item in history}
    unknown = set(observed) - {item["trial_digest"] for item in trials}
    if unknown:
        raise DiscoveryProducerError("observation outside preregistered universe")

    def score(trial: dict[str, Any]) -> tuple[float, str]:
        raw = hashlib.sha256(f"{seed}:{trial['trial_digest']}".encode()).digest()[:8]
        noise = int.from_bytes(raw, "big") / (2**64 - 1)
        if method == "exhaustive":
            return (-float(trial["ordinal"]), trial["trial_digest"])
        if method == "structured":
            return (
                sum(abs(float(value) - 0.5) for value in trial["parameters"].values())
                + noise * 1e-12,
                trial["trial_digest"],
            )
        return (noise, trial["trial_digest"])

    remaining = [trial for trial in trials if trial["trial_digest"] not in observed]
    proposals = sorted(remaining, key=score, reverse=True)[
        : max(0, budget - len(observed))
    ]
    result = {
        "schema_version": "disc004-search-proposal-v1.0.0",
        "program_receipt_digest": program_receipt.receipt_digest,
        "method": method,
        "seed": seed,
        "budget": budget,
        "history_digest": digest(sorted(observed.items())),
        "proposals": proposals,
    }
    return build_receipt(
        milestone="DISC-004",
        producer="bt.institutional.discovery.search_proposal_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={"program": program_receipt.receipt_digest, "observations": history},
        dataset_digest=program_receipt.dataset_digest,
        configuration={"method": method, "seed": seed, "budget": budget},
        artifacts=proposals,
        result=result,
    )


def symbolic_candidate_receipt(
    *,
    base_program: ProducerReceipt,
    candidates: Iterable[dict[str, Any]],
    allowed_operators: set[str],
    maximum_nodes: int,
    source_commit: str,
) -> ProducerReceipt:
    if base_program.milestone != "DISC-003":
        raise DiscoveryProducerError("symbolic search requires DISC-003 receipt")
    candidate_list = list(candidates)
    accepted, rejected, seen = [], [], set()
    for candidate in candidate_list:
        operators = list(candidate.get("operators", []))
        candidate_digest = digest(candidate)
        reason = None
        if len(operators) > maximum_nodes:
            reason = "complexity_budget"
        elif not set(operators).issubset(allowed_operators):
            reason = "operator_not_allowed"
        elif candidate_digest in seen:
            reason = "semantic_duplicate"
        seen.add(candidate_digest)
        item = {
            "candidate_digest": candidate_digest,
            "candidate": candidate,
            "reason": reason,
        }
        (rejected if reason else accepted).append(item)
    result = {
        "schema_version": "disc005-symbolic-candidates-v1.0.0",
        "base_program_receipt_digest": base_program.receipt_digest,
        "accepted": accepted,
        "rejected": rejected,
    }
    return build_receipt(
        milestone="DISC-005",
        producer="bt.institutional.discovery.symbolic_candidate_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs=candidate_list,
        dataset_digest=base_program.dataset_digest,
        configuration={
            "operators": sorted(allowed_operators),
            "maximum_nodes": maximum_nodes,
        },
        artifacts=result,
        result=result,
    )


def selection_audit_receipt(
    *,
    trials: Iterable[dict[str, Any]],
    alpha: float,
    source_commit: str,
    dataset_digest: str,
) -> ProducerReceipt:
    family = list(trials)
    if not family or not 0 < alpha <= 0.05:
        raise DiscoveryProducerError(
            "selection audit requires a nonempty family and alpha <= 0.05"
        )
    pvalues = np.asarray([float(item["p_value"]) for item in family], dtype=np.float64)
    if np.any((pvalues < 0) | (pvalues > 1)):
        raise DiscoveryProducerError("p-values must be in [0, 1]")
    order = np.argsort(pvalues, kind="stable")
    discoveries: list[str] = []
    for rank, index in enumerate(order, start=1):
        if pvalues[index] <= alpha * rank / len(family):
            discoveries.append(str(family[int(index)]["trial_digest"]))
    winner = min(
        family, key=lambda item: (float(item["p_value"]), str(item["trial_digest"]))
    )
    ranks = [float(item.get("validation_rank", len(family))) for item in family]
    pbo = float(np.mean(np.asarray(ranks) > (len(family) + 1) / 2))
    result = {
        "schema_version": "disc007-selection-audit-v1.0.0",
        "family_size": len(family),
        "winner_digest": winner["trial_digest"],
        "nominal_p_value": float(winner["p_value"]),
        "bonferroni_p_value": min(1.0, float(winner["p_value"]) * len(family)),
        "bh_discoveries": sorted(discoveries),
        "pbo": round(pbo, 12),
        "selection_risk_detected": min(1.0, float(winner["p_value"]) * len(family))
        > alpha
        or pbo > 0.5,
    }
    return build_receipt(
        milestone="DISC-007",
        producer="bt.institutional.discovery.selection_audit_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs=family,
        dataset_digest=dataset_digest,
        configuration={"alpha": alpha},
        artifacts=result,
        result=result,
    )


def opportunity_map_receipt(
    *,
    baseline: np.ndarray,
    candidate: np.ndarray,
    costs: np.ndarray,
    dataset_digest: str,
    source_commit: str,
) -> ProducerReceipt:
    if (
        not (baseline.shape == candidate.shape == costs.shape)
        or baseline.ndim != 1
        or len(baseline) < 20
    ):
        raise DiscoveryProducerError(
            "aligned one-dimensional samples with at least 20 rows are required"
        )
    if not np.isfinite(np.concatenate([baseline, candidate, costs])).all():
        raise DiscoveryProducerError("samples must be finite")
    incremental = candidate - baseline - costs
    mean = float(np.mean(incremental))
    stderr = float(np.std(incremental, ddof=1) / math.sqrt(len(incremental)))
    result = {
        "schema_version": "disc002-opportunity-map-v1.0.0",
        "observations": len(incremental),
        "baseline_mean": float(np.mean(baseline)),
        "candidate_mean": float(np.mean(candidate)),
        "cost_mean": float(np.mean(costs)),
        "incremental_net_effect": mean,
        "standard_error": stderr,
        "lower_95": mean - 1.96 * stderr,
        "upper_95": mean + 1.96 * stderr,
        "qualified": mean - 1.96 * stderr > 0,
    }
    return build_receipt(
        milestone="DISC-002",
        producer="bt.institutional.discovery.opportunity_map_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={
            "baseline": baseline.tolist(),
            "candidate": candidate.tolist(),
            "costs": costs.tolist(),
        },
        dataset_digest=dataset_digest,
        configuration={"confidence": 0.95},
        artifacts=result,
        result=result,
    )
