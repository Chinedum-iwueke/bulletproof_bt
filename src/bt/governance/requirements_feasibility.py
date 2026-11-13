"""Outcome-blind native planning before independent scientific review."""

from bt.experiments.adaptive_representation import feature_history
from bt.governance.data_requirements import fingerprint, preflight_evidence


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
        "event_support": "must_be_measured_after_execution_not_inferred_from_coverage",
        "scientific_certification": False,
        "execution_authority": False,
    }
    return report | {"report_digest": fingerprint(report)}
