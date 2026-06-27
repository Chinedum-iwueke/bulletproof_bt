from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


Assessment = Literal["supportive", "neutral", "caution", "block", "insufficient_evidence"]
Mode = Literal["shadow", "enforced"]


@dataclass(frozen=True)
class MemoryPolicy:
    mode: Mode
    minimum_support: int = 20
    maximum_drift_ratio: float = 1.5
    block_assessments: tuple[Assessment, ...] = ("block",)
    block_on_insufficient_evidence: bool = False
    approved: bool = False


@dataclass(frozen=True)
class MemoryPolicyInput:
    assessment: Assessment
    support_count: int
    drift_ratio: float | None
    calibration_status: str
    requested_quantity: float
    source_is_trade_episode_only: bool


@dataclass(frozen=True)
class MemoryPolicyDecision:
    would_block: bool
    applied_block: bool
    reason_codes: tuple[str, ...]
    effective_quantity: float
    risk_increase_forbidden: bool = True


def evaluate_memory_policy(*, policy: MemoryPolicy, value: MemoryPolicyInput) -> MemoryPolicyDecision:
    reasons: list[str] = []
    if not value.source_is_trade_episode_only:
        reasons.append("non_trade_evidence_cannot_authorize")
    if value.support_count < policy.minimum_support:
        reasons.append("support_below_policy_minimum")
    if value.drift_ratio is not None and value.drift_ratio > policy.maximum_drift_ratio:
        reasons.append("state_drift_above_policy_maximum")
    if value.assessment in policy.block_assessments:
        reasons.append("assessment_blocked")
    if value.assessment == "insufficient_evidence" and policy.block_on_insufficient_evidence:
        reasons.append("insufficient_evidence_blocked")
    if value.calibration_status not in {"calibrated", "provisional"}:
        reasons.append("calibration_unavailable")
    would_block = bool(reasons)
    applied = would_block and policy.mode == "enforced" and policy.approved
    return MemoryPolicyDecision(
        would_block=would_block,
        applied_block=applied,
        reason_codes=tuple(reasons),
        effective_quantity=0.0 if applied else value.requested_quantity,
    )

