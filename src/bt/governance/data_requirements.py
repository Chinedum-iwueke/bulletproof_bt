"""Deterministic planning from reviewed requirements and bound coverage receipts.

No file discovery, model calls, approval issuance or execution occurs here.
Intervals are half-open completed, eligible research-bar decision intervals.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import Any


class RequirementsError(ValueError):
    pass


def fingerprint(value: Any) -> str:
    return sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    ).hexdigest()


def instant(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise RequirementsError(
            "coverage and windows require timezone-aware timestamps"
        )
    return result.astimezone(timezone.utc)


@dataclass(frozen=True)
class DataRequirements:
    research_bar_minutes: int
    feature_warmup_bars: int
    minimum_training_bars: int
    minimum_validation_bars: int
    minimum_test_bars: int
    target_horizon_bars: int
    purge_bars: int
    embargo_bars: int
    minimum_scientific_window_days: int
    smoke_bars: int
    minimum_event_support: dict[str, int]
    rationale: str
    basis_digest: str

    @classmethod
    def parse(cls, value: dict[str, Any]) -> DataRequirements:
        try:
            result = cls(**value)
        except (TypeError, ValueError) as exc:
            raise RequirementsError(
                "explicit typed data requirements are missing or malformed"
            ) from exc
        zero_allowed = {"feature_warmup_bars", "purge_bars", "embargo_bars"}
        for name, item in asdict(result).items():
            if name in {"minimum_event_support", "rationale", "basis_digest"}:
                continue
            if type(item) is not int or item < (0 if name in zero_allowed else 1):
                raise RequirementsError(f"invalid data requirement: {name}")
        if not isinstance(result.minimum_event_support, dict) or any(
            not isinstance(key, str) or not key or type(count) is not int or count < 1
            for key, count in result.minimum_event_support.items()
        ):
            raise RequirementsError(
                "event support requirements must be explicit positive counts"
            )
        if (
            not isinstance(result.rationale, str)
            or not result.rationale.strip()
            or not isinstance(result.basis_digest, str)
            or len(result.basis_digest) != 64
            or any(c not in "0123456789abcdef" for c in result.basis_digest)
        ):
            raise RequirementsError(
                "requirements need rationale and a reviewed basis digest"
            )
        return result


def coverage_receipt(
    *,
    instrument: str,
    binding: dict[str, Any],
    bar_minutes: int,
    eligible_decisions: list[str],
    eligibility_evidence_digest: str,
) -> dict[str, Any]:
    """Compact already quality-checked decisions, not raw row counts or date extents.

    The producer owns value/membership/complete-source-bar eligibility checks.
    Its immutable evidence digest is mandatory; this helper checks clock integrity.
    """
    if type(bar_minutes) is not int or bar_minutes < 1:
        raise RequirementsError("research bars must be at least one minute")
    if (
        not isinstance(eligibility_evidence_digest, str)
        or len(eligibility_evidence_digest) != 64
        or any(c not in "0123456789abcdef" for c in eligibility_evidence_digest)
    ):
        raise RequirementsError("coverage needs prior eligibility evidence")
    times = [instant(value) for value in eligible_decisions]
    step = timedelta(minutes=bar_minutes)
    if times != sorted(set(times)):
        raise RequirementsError("eligible decisions must be sorted and unique")
    if times and any((time - times[0]) % step for time in times):
        raise RequirementsError("eligible decisions must share a research-bar clock")
    intervals = []
    for time in times:
        if intervals and instant(intervals[-1]["end"]) == time:
            intervals[-1]["end"] = (time + step).isoformat()
        else:
            intervals.append(
                {"start": time.isoformat(), "end": (time + step).isoformat()}
            )
    document = {
        "schema_version": "eligible-research-coverage-v1",
        "instrument": instrument,
        "binding_digest": fingerprint(binding),
        "research_bar_minutes": bar_minutes,
        "eligibility_evidence_digest": eligibility_evidence_digest,
        "intervals": intervals,
    }
    return document | {"receipt_digest": fingerprint(document)}


def plan_validation(
    *,
    requirements: dict[str, Any],
    coverage: list[dict[str, Any]],
    dataset_bindings: list[dict[str, Any]],
    instruments: list[str],
    source_commit: str,
    approved_window: dict[str, str],
    maximum_history_days: int,
    maximum_bar_variant_work: int,
    variants: int,
    mode: str,
) -> dict[str, Any]:
    """Plan once without searching outcomes, widening approval or weakening support.

    Work units are conservative loaded bars x assets x variants, not claimed CPU
    seconds or RAM. Runtime resource admission remains a separate mandatory gate.
    """
    req = DataRequirements.parse(requirements)
    if mode not in {"smoke", "scientific"}:
        raise RequirementsError("validation mode must be smoke or scientific")
    if (
        any(
            type(x) is not int or x < 1
            for x in (maximum_history_days, maximum_bar_variant_work, variants)
        )
        or variants > 8
    ):
        raise RequirementsError("invalid bounded execution budget")
    if (
        len(source_commit) != 40
        or any(c not in "0123456789abcdef" for c in source_commit)
        or not instruments
        or len(set(instruments)) != len(instruments)
    ):
        raise RequirementsError(
            "an immutable source commit and unique basket are required"
        )
    start, end = instant(approved_window["start"]), instant(approved_window["end"])
    if start >= end:
        raise RequirementsError("approved window is empty")
    step = timedelta(minutes=req.research_bar_minutes)
    bound = {}
    for binding in dataset_bindings:
        names = binding.get("instruments", [binding.get("instrument")])
        if (
            len(names) != 1
            or names[0] not in instruments
            or names[0] in bound
            or binding.get("instrument", names[0]) != names[0]
        ):
            raise RequirementsError("coverage requires exact single-asset bindings")
        bound[names[0]] = fingerprint(binding)
    if set(bound) != set(instruments) or len(coverage) != len(instruments):
        raise RequirementsError("coverage must cover the exact basket")
    common = [(start, end)]
    seen = set()
    clock = None
    for receipt in coverage:
        symbol = receipt.get("instrument")
        if (
            symbol not in bound
            or symbol in seen
            or receipt.get("schema_version") != "eligible-research-coverage-v1"
            or receipt.get("binding_digest") != bound[symbol]
            or receipt.get("research_bar_minutes") != req.research_bar_minutes
            or not receipt.get("eligibility_evidence_digest")
            or receipt.get("receipt_digest")
            != fingerprint(
                {
                    key: value
                    for key, value in receipt.items()
                    if key != "receipt_digest"
                }
            )
        ):
            raise RequirementsError("coverage receipt identity or digest mismatch")
        seen.add(symbol)
        intervals = []
        previous = None
        for interval in receipt["intervals"]:
            left, right = instant(interval["start"]), instant(interval["end"])
            if left >= right or (right - left) % step or (previous and left < previous):
                raise RequirementsError("coverage intervals are invalid or overlapping")
            if clock is None:
                clock = left
            if (left - clock) % step:
                raise RequirementsError("basket research-bar clocks disagree")
            previous = right
            intervals.append((left, right))
        common = [
            (max(a, c), min(b, d))
            for a, b in common
            for c, d in intervals
            if max(a, c) < min(b, d)
        ]
    # Clip approval boundaries inward to whole eligible decision bars.
    if clock is not None:
        common = [
            (
                clock + math.ceil((a - clock) / step) * step,
                clock + math.floor((b - clock) / step) * step,
            )
            for a, b in common
        ]
    warmup = req.feature_warmup_bars
    gap = max(req.purge_bars, req.target_horizon_bars) + req.embargo_bars
    scientific_min = max(
        req.minimum_training_bars
        + req.minimum_validation_bars
        + req.minimum_test_bars
        + 2 * gap
        + req.target_horizon_bars,
        math.ceil(req.minimum_scientific_window_days * 1440 / req.research_bar_minutes),
    )
    evaluation = req.smoke_bars if mode == "smoke" else scientific_min
    # Smoke decisions need future observations too; these are not extra decisions.
    required = warmup + evaluation + (req.target_horizon_bars if mode == "smoke" else 0)
    work = required * len(instruments) * variants
    duration = required * step
    reasons = []
    if duration > timedelta(days=maximum_history_days):
        reasons.append("history_budget_insufficient")
    if mode == "smoke" and evaluation * step > timedelta(days=31):
        reasons.append("smoke_exceeds_31_day_bound")
    if work > maximum_bar_variant_work:
        reasons.append("compute_budget_insufficient")
    fits = [(a, b) for a, b in common if b - a >= duration]
    available = max((max(0, int((b - a) / step)) for a, b in common), default=0)
    if not fits:
        reasons.append("insufficient_contiguous_eligible_basket_history")
    plan = {
        "schema_version": "requirements-aware-validation-plan-v1",
        "mode": mode,
        "status": "deferred" if reasons else "ready",
        "reasons": reasons,
        "source_commit": source_commit,
        "requirements_digest": fingerprint(requirements),
        "dataset_bindings_digest": fingerprint(dataset_bindings),
        "coverage_receipt_digests": sorted(x["receipt_digest"] for x in coverage),
        "approved_window": approved_window,
        "instruments": instruments,
        "variants": variants,
        "required_bars_per_asset": required,
        "available_contiguous_bars_per_asset": available,
        "bar_shortfall": max(0, required - available),
        "bar_variant_work": work,
        "budgets": {
            "maximum_history_days": maximum_history_days,
            "maximum_bar_variant_work": maximum_bar_variant_work,
        },
        "partitions": {},
        "event_support": {
            "status": "not_yet_observed",
            "required": req.minimum_event_support,
        },
        "scientific_certification": False,
        "execution_authority": False,
        "resource_admission_required": True,
    }
    if not reasons:
        _, finish = max(fits, key=lambda interval: interval[1])
        begin = finish - duration
        evaluation_start = begin + warmup * step

        def interval(a, b):
            return {"start": a.isoformat(), "end": b.isoformat()}

        partitions = {"feature_warmup": interval(begin, evaluation_start)}
        if mode == "smoke":
            smoke_end = finish - req.target_horizon_bars * step
            partitions["smoke"] = interval(evaluation_start, smoke_end)
            partitions["target_tail"] = interval(smoke_end, finish)
        else:
            test_end = finish - req.target_horizon_bars * step
            test_start = test_end - req.minimum_test_bars * step
            validation_end = test_start - gap * step
            validation_start = validation_end - req.minimum_validation_bars * step
            train_end = validation_start - gap * step
            partitions.update(
                training=interval(evaluation_start, train_end),
                validation=interval(validation_start, validation_end),
                test=interval(test_start, test_end),
                target_tail=interval(test_end, finish),
            )
        plan["partitions"] = partitions
    return plan | {"plan_digest": fingerprint(plan)}


def preflight_evidence(
    evidence: dict[str, Any], *, source_commit: str
) -> dict[str, Any]:
    """New authoring contracts must supply reviewed requirements, never prose guesses."""
    package = evidence.get("validation_requirements")
    if not isinstance(package, dict):
        raise RequirementsError(
            "data_requirements_missing: require reviewed requirements and eligible coverage"
        )
    candidate = evidence["discovery_candidate"]
    req = DataRequirements.parse(package["requirements"])
    # Preflight runs in the lean worker interpreter, without pandas/numpy.
    timeframe = re.fullmatch(
        r"([1-9][0-9]*)([mhd])", candidate["data"]["research_timeframe"]
    )
    minutes = (
        int(timeframe[1]) * {"m": 1, "h": 60, "d": 1440}[timeframe[2]]
        if timeframe
        else None
    )
    if minutes != req.research_bar_minutes:
        raise RequirementsError("requirements timeframe differs from representation")
    return {
        mode: plan_validation(
            requirements=package["requirements"],
            coverage=package["coverage"],
            dataset_bindings=evidence["dataset_bindings"],
            instruments=candidate["data"]["instruments"],
            source_commit=source_commit,
            approved_window=evidence["window"],
            variants=candidate["parameter_budget"]["maximum_variants"],
            maximum_history_days=package["maximum_history_days"],
            maximum_bar_variant_work=package["maximum_bar_variant_work"],
            mode=mode,
        )
        for mode in ("smoke", "scientific")
    }


def verify_execution_plan(
    assignment: dict[str, Any], immutable: dict[str, Any]
) -> dict[str, Any] | None:
    """Replay new contracts; historical contracts are not silently rewritten."""
    package = immutable.get("validation_requirements")
    if package is None:
        return None
    mode = (
        "smoke"
        if assignment.get("execution_class") == "commissioning"
        else "scientific"
    )
    plan = plan_validation(
        requirements=package["requirements"],
        coverage=package["coverage"],
        dataset_bindings=immutable["dataset_bindings"],
        instruments=immutable["instruments"],
        source_commit=assignment["base_ref"],
        approved_window=immutable["window"],
        maximum_history_days=package["maximum_history_days"],
        maximum_bar_variant_work=package["maximum_bar_variant_work"],
        variants=immutable["maximum_variants"],
        mode=mode,
    )
    if plan["status"] != "ready" or assignment.get("validation_plan") != plan:
        raise RequirementsError(
            "execution validation plan is missing, stale or infeasible"
        )
    parts = plan["partitions"]
    begin = parts["smoke" if mode == "smoke" else "training"]["start"]
    end = parts["target_tail"]["end"]
    if instant(assignment["window_start"]) != instant(begin) or instant(
        assignment["window_end"]
    ) != instant(end):
        raise RequirementsError("execution window differs from frozen validation plan")
    return plan


def assess_event_support(
    plan: dict[str, Any], observed: dict[str, int]
) -> dict[str, Any]:
    """Observed support cannot be guaranteed by availability or profitable outcomes."""
    if any(type(value) is not int or value < 0 for value in observed.values()):
        raise RequirementsError("observed support must be nonnegative integer counts")
    required = plan["event_support"]["required"]
    shortfalls = {
        key: value - observed.get(key, 0)
        for key, value in required.items()
        if observed.get(key, 0) < value
    }
    return {
        "plan_digest": plan["plan_digest"],
        "observed": observed,
        "status": "insufficient_support" if shortfalls else "support_met",
        "shortfalls": shortfalls,
        "scientific_certification": False,
    }
