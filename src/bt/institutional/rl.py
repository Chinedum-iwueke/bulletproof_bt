"""RL-001..002 offline dataset and conservative evaluation producers."""

from __future__ import annotations

import math
from collections.abc import Iterable
from typing import Any

import numpy as np

from .receipt import ProducerReceipt, build_receipt, digest


class RLProducerError(ValueError):
    """Offline-RL evidence violates support or causal evaluation constraints."""


def offline_dataset_receipt(
    *,
    transitions: Iterable[dict[str, Any]],
    allowed_actions: set[str],
    dataset_digest: str,
    shadow_receipt_digest: str,
    source_commit: str,
) -> ProducerReceipt:
    rows = list(transitions)
    if not rows or not allowed_actions:
        raise RLProducerError("transitions and a bounded action space are required")
    episodes, action_counts = set(), {action: 0 for action in allowed_actions}
    normalized = []
    for ordinal, row in enumerate(rows):
        action = str(row["action"])
        propensity = float(row["behavior_propensity"])
        if action not in allowed_actions or not 0 < propensity <= 1:
            raise RLProducerError("unknown action or invalid behavior propensity")
        state = np.asarray(row["state"], dtype=np.float64)
        next_state = np.asarray(row["next_state"], dtype=np.float64)
        reward = float(row["reward"])
        if (
            state.shape != next_state.shape
            or not np.isfinite(np.concatenate([state, next_state, [reward]])).all()
        ):
            raise RLProducerError("transition state/reward is malformed")
        if int(row["step"]) < 0 or (
            ordinal
            and row["episode_id"] == rows[ordinal - 1]["episode_id"]
            and int(row["step"]) != int(rows[ordinal - 1]["step"]) + 1
        ):
            raise RLProducerError("episode steps are not contiguous")
        episodes.add(str(row["episode_id"]))
        action_counts[action] += 1
        normalized.append(
            {
                **row,
                "state": state.tolist(),
                "next_state": next_state.tolist(),
                "reward": reward,
                "behavior_propensity": propensity,
            }
        )
    unsupported = sorted(
        action for action, count in action_counts.items() if count == 0
    )
    minimum_support = min(action_counts.values()) / len(rows)
    result = {
        "schema_version": "rl001-offline-dataset-v1.0.0",
        "shadow_receipt_digest": shadow_receipt_digest,
        "transition_count": len(rows),
        "episode_count": len(episodes),
        "action_counts": action_counts,
        "unsupported_actions": unsupported,
        "minimum_action_support": minimum_support,
        "transition_digest": digest(normalized),
        "qualified": not unsupported and minimum_support >= 0.01,
    }
    return build_receipt(
        milestone="RL-001",
        producer="bt.institutional.rl.offline_dataset_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs=normalized,
        dataset_digest=dataset_digest,
        configuration={"allowed_actions": sorted(allowed_actions)},
        artifacts=result,
        result=result,
    )


def off_policy_evaluation_receipt(
    *,
    dataset_receipt: ProducerReceipt,
    rewards: np.ndarray,
    behavior_probabilities: np.ndarray,
    target_probabilities: np.ndarray,
    direct_values: np.ndarray,
    confidence_z: float,
    maximum_weight: float,
    minimum_effective_sample_size: float,
    source_commit: str,
) -> ProducerReceipt:
    if dataset_receipt.milestone != "RL-001" or not dataset_receipt.result["qualified"]:
        raise RLProducerError("RL-002 requires a qualified RL-001 receipt")
    arrays = [
        np.asarray(item, dtype=np.float64)
        for item in (
            rewards,
            behavior_probabilities,
            target_probabilities,
            direct_values,
        )
    ]
    if (
        len({item.shape for item in arrays}) != 1
        or arrays[0].ndim != 1
        or not np.isfinite(np.concatenate(arrays)).all()
    ):
        raise RLProducerError("off-policy arrays must be finite and aligned")
    rewards, behavior, target, direct = arrays
    if np.any(behavior <= 0) or np.any(target < 0):
        raise RLProducerError("invalid policy probabilities")
    weights = target / behavior
    clipped = np.minimum(weights, maximum_weight)
    weight_sum = float(np.sum(clipped))
    if weight_sum <= 0:
        raise RLProducerError("target policy has no observed support")
    ips = float(np.mean(clipped * rewards))
    snips = float(np.sum(clipped * rewards) / weight_sum)
    direct_mean = float(np.mean(direct))
    dr = float(np.mean(direct + clipped * (rewards - direct)))
    estimates = {"ips": ips, "snips": snips, "direct": direct_mean, "doubly_robust": dr}
    effective_sample_size = weight_sum**2 / float(np.sum(np.square(clipped)))
    standard_error = float(
        np.std(direct + clipped * (rewards - direct), ddof=1) / math.sqrt(len(rewards))
    )
    lower_bounds = {
        name: value - confidence_z * standard_error for name, value in estimates.items()
    }
    failures = []
    if effective_sample_size < minimum_effective_sample_size:
        failures.append("weak_effective_sample_size")
    if float(np.max(weights)) > maximum_weight:
        failures.append("unsafe_importance_weight")
    disagreement_limit = max(2 * confidence_z * standard_error, 0.01)
    if max(estimates.values()) - min(estimates.values()) > disagreement_limit:
        failures.append("estimator_disagreement")
    result = {
        "schema_version": "rl002-off-policy-evaluation-v1.0.0",
        "dataset_receipt_digest": dataset_receipt.receipt_digest,
        "estimates": estimates,
        "lower_bounds": lower_bounds,
        "effective_sample_size": effective_sample_size,
        "maximum_importance_weight": float(np.max(weights)),
        "standard_error": standard_error,
        "estimator_disagreement_limit": disagreement_limit,
        "failures": failures,
        "shadow_eligible": not failures,
    }
    return build_receipt(
        milestone="RL-002",
        producer="bt.institutional.rl.off_policy_evaluation_receipt",
        producer_version="1.0.0",
        source_commit=source_commit,
        inputs={
            "rewards": rewards.tolist(),
            "behavior": behavior.tolist(),
            "target": target.tolist(),
            "direct": direct.tolist(),
        },
        dataset_digest=dataset_receipt.dataset_digest,
        configuration={
            "confidence_z": confidence_z,
            "maximum_weight": maximum_weight,
            "minimum_effective_sample_size": minimum_effective_sample_size,
        },
        artifacts=result,
        result=result,
    )
