from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ReconciliationPolicy(str, Enum):
    LOG_ONLY = "log_only"
    WARN = "warn"
    FREEZE_ON_MATERIAL = "freeze_on_material"
    AUTO_ACCEPT_SIMULATED_SAFE_DIFFERENCE = "auto_accept_simulated_safe_difference"


class ReconciliationAction(str, Enum):
    LOG = "log"
    WARN = "warn"
    FREEZE = "freeze"
    ACCEPT = "accept"


@dataclass(frozen=True)
class ReconciliationDecision:
    policy: ReconciliationPolicy
    action: ReconciliationAction
    reason: str


def decide_policy_action(*, policy: ReconciliationPolicy, has_material_mismatch: bool, simulated_safe_only: bool) -> ReconciliationDecision:
    if policy == ReconciliationPolicy.LOG_ONLY:
        return ReconciliationDecision(policy=policy, action=ReconciliationAction.LOG, reason="policy_log_only")
    if policy == ReconciliationPolicy.WARN:
        return ReconciliationDecision(policy=policy, action=ReconciliationAction.WARN, reason="policy_warn")
    if policy == ReconciliationPolicy.FREEZE_ON_MATERIAL:
        action = ReconciliationAction.FREEZE if has_material_mismatch else ReconciliationAction.LOG
        return ReconciliationDecision(policy=policy, action=action, reason="material_mismatch" if has_material_mismatch else "no_material_mismatch")

    if simulated_safe_only and not has_material_mismatch:
        return ReconciliationDecision(
            policy=policy,
            action=ReconciliationAction.ACCEPT,
            reason="simulated_safe_difference",
        )
    return ReconciliationDecision(policy=policy, action=ReconciliationAction.WARN, reason="fallback_warn")
