"""Outcome-blind native planning before independent scientific review."""

from copy import deepcopy

from bt.experiments.adaptive_representation import feature_history
from bt.governance.data_requirements import fingerprint, preflight_evidence
from bt.governance.coverage_successor import plan_coverage_successor
from bt.governance.event_support_planning import assess_event_support


def plan_requirements(evidence, *, source_commit):
    history = feature_history(evidence["representation_plan"])
    requirements = evidence["validation_requirements"]["requirements"]
    plans = preflight_evidence(evidence, source_commit=source_commit)
    reasons = []
    minimum = max(history.values(), default=0)
    if requirements["feature_warmup_bars"] < minimum:
        reasons.append("composed_feature_history_exceeds_declared_warmup")
    for mode, plan in plans.items():
        reasons.extend(f"{mode}:{reason}" for reason in plan["reasons"])
    proposal = None
    context = evidence.get("coverage_successor_context")
    if (context is not None and minimum <= requirements["feature_warmup_bars"]
            and plans["scientific"]["reasons"] == ["insufficient_contiguous_eligible_basket_history"]
            and all(reason == "smoke:insufficient_contiguous_eligible_basket_history"
                    or reason == "scientific:insufficient_contiguous_eligible_basket_history"
                    for reason in reasons)):
        package = evidence["validation_requirements"]
        proposal = plan_coverage_successor(
            frozen_inputs={
                "requirements": requirements, "coverage": package["coverage"],
                "dataset_bindings": evidence["dataset_bindings"],
                "instruments": evidence["discovery_candidate"]["data"]["instruments"],
                "source_commit": source_commit, "approved_window": evidence["window"],
                "variants": evidence["discovery_candidate"]["parameter_budget"]["maximum_variants"],
                "maximum_history_days": package["maximum_history_days"],
                "maximum_bar_variant_work": package["maximum_bar_variant_work"],
            },
            parent_plan=plans["scientific"], **context,
        )
    successor_plans = None
    if proposal is not None and proposal["status"] == "proposed":
        successor = deepcopy(evidence)
        parts = proposal["scientific_plan"]["partitions"]
        successor["window"] = {"start": parts["feature_warmup"]["start"], "end": parts["target_tail"]["end"]}
        successor["validation_requirements"]["coverage"] = proposal["planning_inputs"]["coverage"]
        successor_plans = preflight_evidence(successor, source_commit=source_commit)
    support = assess_event_support(
        evidence["discovery_candidate"].get("scientific_specification", {}),
        requirements, (successor_plans or plans)["scientific"],
    )
    reasons.extend(support["blockers"])
    diagnostic = None
    if "diagnostic_request" in evidence:
        from bt.governance.diagnostic_tools import run_diagnostic
        request = evidence["diagnostic_request"]
        if request["subject_digest"] != fingerprint(evidence["discovery_candidate"]):
            raise ValueError("diagnostic subject differs from candidate")
        diagnostic = run_diagnostic(request, runtime_source_commit=source_commit,
            evidence_catalog=evidence.get("diagnostic_evidence_catalog", {}))
        if diagnostic["result"].get("status") == "await_engineering":
            reasons.append("diagnostic:await_engineering")
    report = {
        "schema_version": "native-requirements-feasibility-v1",
        "input_digest": fingerprint(evidence),
        "source_commit": source_commit,
        "status": "deferred" if reasons else "ready",
        "reasons": reasons,
        "feature_history": {
            "outputs": history, "minimum_prior_bars": minimum,
            "declared_warmup_bars": requirements["feature_warmup_bars"],
            "scope": "structural_lower_bound_not_observed_feature_availability",
        },
        "validation_plans": plans,
        "coverage_successor_proposal": proposal,
        "successor_validation_plans": successor_plans,
        "event_support": "must_be_measured_after_execution_not_inferred_from_coverage",
        "event_support_planning": support,
        "scientific_certification": False,
        "execution_authority": False,
    }
    if diagnostic is not None:
        report["native_diagnostic"] = diagnostic
    return report | {"report_digest": fingerprint(report)}
