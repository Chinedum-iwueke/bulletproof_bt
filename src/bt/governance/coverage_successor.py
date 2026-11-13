"""Outcome-blind successor proposals. These never mutate or authorize a task."""

from copy import deepcopy
from datetime import timedelta

from bt.governance.data_requirements import (
    RequirementsError,
    fingerprint,
    instant,
    plan_validation,
)


def _verified(document: dict, digest_key: str) -> None:
    if document.get(digest_key) != fingerprint(
        {k: v for k, v in document.items() if k != digest_key}
    ):
        raise RequirementsError(f"invalid {digest_key}")


def plan_coverage_successor(
    *,
    frozen_inputs: dict,
    parent_plan: dict,
    mandate_digest: str,
    policy: dict,
    prior_successors: list[dict],
    exposure_ledger: dict,
) -> dict:
    """Choose the latest common interval within explicit historical authority.

    The caller must authenticate the mandate and supply the complete durable
    lineage/exposure ledger. Digests prove consistency, not authority. Exposure
    intervals are excluded conservatively from the entire proposed experiment.
    No model, outcome, filesystem, network or paid execution is involved.
    """
    _verified(parent_plan, "plan_digest")
    _verified(exposure_ledger, "ledger_digest")
    if len(mandate_digest) != 64 or any(
        c not in "0123456789abcdef" for c in mandate_digest
    ):
        raise RequirementsError("mandate digest is required")
    if set(policy) != {
        "envelope_start",
        "envelope_end",
        "maximum_successors",
        "selection_policy",
        "outcome_search_allowed",
    }:
        raise RequirementsError("explicit bounded coverage policy required")
    limit = policy["maximum_successors"]
    if (
        type(limit) is not int
        or not 1 <= limit <= 3
        or policy["selection_policy"] != "latest_common_eligible_interval"
        or policy["outcome_search_allowed"] is not False
    ):
        raise RequirementsError("invalid coverage successor policy")
    left, right = instant(policy["envelope_start"]), instant(policy["envelope_end"])
    if not timedelta(days=365) <= right - left <= timedelta(days=1095):
        raise RequirementsError("coverage envelope must span 365 to 1095 days")
    original = frozen_inputs["approved_window"]
    if not left <= instant(original["start"]) < instant(original["end"]) <= right:
        raise RequirementsError("frozen window lies outside historical authority")
    # Replaying the original plan binds every scientific requirement and budget.
    if plan_validation(**frozen_inputs, mode="scientific") != parent_plan:
        raise RequirementsError("parent plan differs from frozen contract")
    lineage = fingerprint({k: v for k, v in frozen_inputs.items() if k != "coverage"})
    if (
        exposure_ledger.get("lineage_digest") != lineage
        or exposure_ledger.get("complete") is not True
    ):
        raise RequirementsError("complete lineage-bound exposure ledger required")
    intervals = exposure_ledger.get("intervals")
    if not isinstance(intervals, list):
        raise RequirementsError(
            "exposure intervals must be explicit, including when empty"
        )
    for interval in intervals:
        if set(interval) != {"start", "end"} or instant(interval["start"]) >= instant(
            interval["end"]
        ):
            raise RequirementsError("malformed exposure interval")
    previous = parent_plan["plan_digest"]
    for ordinal, receipt in enumerate(prior_successors, 1):
        _verified(receipt, "successor_digest")
        if (
            receipt.get("lineage_digest") != lineage
            or receipt.get("ordinal") != ordinal
            or receipt.get("parent_digest") != previous
            or receipt.get("mandate_digest") != mandate_digest
            or receipt.get("policy_digest") != fingerprint(policy)
        ):
            raise RequirementsError("successor lineage is stale, incomplete or forked")
        previous = receipt["successor_digest"]
    reasons = []
    if len(prior_successors) >= limit:
        reasons.append("coverage_successor_budget_exhausted")
    if parent_plan["reasons"] != ["insufficient_contiguous_eligible_basket_history"]:
        reasons.append("failure_not_coverage_only")
    if any(
        item.get("coverage_digest") == fingerprint(frozen_inputs["coverage"])
        and item.get("exposure_ledger_digest") == exposure_ledger["ledger_digest"]
        for item in prior_successors
    ):
        reasons.append("unchanged_coverage_and_exposure_already_planned")
    args = deepcopy(frozen_inputs)
    args["approved_window"] = {
        "start": policy["envelope_start"],
        "end": policy["envelope_end"],
    }
    # Exclusion is conservative: no re-use of observed validation/test outcomes,
    # even as training data in a successor. Round cuts inward to the bar clock.
    step = timedelta(minutes=frozen_inputs["requirements"]["research_bar_minutes"])
    for receipt in args["coverage"]:
        pieces = [
            (instant(i["start"]), instant(i["end"])) for i in receipt["intervals"]
        ]
        for excluded in intervals:
            a, b = instant(excluded["start"]), instant(excluded["end"])
            remaining = []
            for start, end in pieces:
                if b <= start or a >= end:
                    remaining.append((start, end))
                    continue
                cut_left = start + ((a - start) // step) * step
                cut_right = start + (-((start - b) // step)) * step
                if start < cut_left:
                    remaining.append((start, min(end, cut_left)))
                if cut_right < end:
                    remaining.append((max(start, cut_right), end))
            pieces = remaining
        receipt["intervals"] = [
            {"start": a.isoformat(), "end": b.isoformat()} for a, b in pieces
        ]
        receipt["eligibility_evidence_digest"] = fingerprint(
            {
                "original_receipt_digest": receipt["receipt_digest"],
                "exposure_ledger_digest": exposure_ledger["ledger_digest"],
            }
        )
        receipt["receipt_digest"] = fingerprint(
            {k: v for k, v in receipt.items() if k != "receipt_digest"}
        )
    plan = None if reasons else plan_validation(**args, mode="scientific")
    if plan is not None:
        reasons.extend(plan["reasons"])
    result = {
        "schema_version": "coverage-aware-successor-proposal-v1",
        "status": "deferred" if reasons else "proposed",
        "reasons": reasons,
        "ordinal": len(prior_successors) + 1,
        "parent_digest": previous,
        "lineage_digest": lineage,
        "mandate_digest": mandate_digest,
        "policy_digest": fingerprint(policy),
        "coverage_digest": fingerprint(frozen_inputs["coverage"]),
        "exposure_ledger_digest": exposure_ledger["ledger_digest"],
        "scientific_plan": plan,
        "planning_inputs": args if plan is not None and not reasons else None,
        "requires_independent_review": True,
        "execution_authority": False,
        "scientific_certification": False,
    }
    return result | {"successor_digest": fingerprint(result)}
