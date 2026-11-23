"""Outcome-blind design-contract checks; completeness is not scientific validity."""
import math
import re

from bt.governance.data_requirements import RequirementsError, fingerprint


def assess_design_adequacy(contract, *, subject_digest, source_commit, evidence_catalog=None):
    """Validate a bounded proposed calibration contract without running it."""
    if not isinstance(subject_digest, str) or not re.fullmatch(r"[0-9a-f]{64}", subject_digest):
        raise RequirementsError("exact subject digest required")
    if not isinstance(source_commit, str) or not re.fullmatch(r"[0-9a-f]{40}", source_commit):
        raise RequirementsError("exact source commit required")
    fields = {"schema_version", "estimand", "estimator_id", "meaningful_effect",
              "dependence_assumptions", "multiplicity", "eligibility_policy",
              "undefined_replicate_policy", "calibration"}
    structured = isinstance(contract, dict) and 'experiment' in contract
    assured = isinstance(contract, dict) and contract.get('schema_version') == 'design-adequacy-contract-v2'
    if assured:
        fields.add('assurance')
    if structured:
        fields |= {'experiment', 'calibration_design'}
    if not isinstance(contract, dict) or set(contract) != fields:
        raise RequirementsError("design adequacy fields differ")
    if contract["schema_version"] not in {"design-adequacy-contract-v1", "design-adequacy-contract-v2"}:
        raise RequirementsError("unsupported design adequacy version")
    if assured and not structured:
        raise RequirementsError('assured design requires a frozen experiment')
    for name in fields - {"schema_version", "meaningful_effect", "calibration", 'experiment', 'calibration_design', 'assurance'}:
        if not isinstance(contract[name], str) or not 1 <= len(contract[name].strip()) <= 4000:
            raise RequirementsError("missing or oversized design statement: " + name)
    effect = contract["meaningful_effect"]
    if not isinstance(effect, dict) or set(effect) != {"value", "units", "justification", "evidence_digests"}:
        raise RequirementsError("explicit effect value, units and justification required")
    if type(effect["value"]) not in {int, float} or not math.isfinite(effect["value"]) or effect["value"] <= 0:
        raise RequirementsError("effect must be finite and positive")
    for name in ("units", "justification"):
        if not isinstance(effect[name], str) or not 1 <= len(effect[name].strip()) <= 4000:
            raise RequirementsError("effect units and justification required")
    refs = effect["evidence_digests"]
    if not isinstance(refs, list) or not 1 <= len(refs) <= 16 or any(
            not isinstance(ref, str) or not re.fullmatch(r"[0-9a-f]{64}", ref) for ref in refs):
        raise RequirementsError("bounded effect justification references required")
    calibration = contract["calibration"]
    if not isinstance(calibration, dict) or set(calibration) != {
            "seed", "outer_replicates", "inner_replicates", "max_work_units", "dgp_ids"}:
        raise RequirementsError("explicit calibration budget required")
    for name, maximum in (("seed", 2**32-1), ("outer_replicates", 10000),
                          ("inner_replicates", 10000), ("max_work_units", 10_000_000)):
        value = calibration[name]
        if type(value) is not int or not (0 if name == "seed" else 1) <= value <= maximum:
            raise RequirementsError("invalid calibration bound: " + name)
    dgps = calibration["dgp_ids"]
    if not isinstance(dgps, list) or not 1 <= len(dgps) <= 16 or any(
            not isinstance(name, str) or not re.fullmatch(r"[a-z0-9_-]{1,80}", name) for name in dgps):
        raise RequirementsError("bounded explicit DGP identifiers required")
    if len(set(dgps)) != len(dgps):
        raise RequirementsError("duplicate DGP identifiers")
    work = len(dgps)*calibration["outer_replicates"]*calibration["inner_replicates"]
    if work > calibration["max_work_units"]:
        raise RequirementsError("calibration exceeds declared work budget")
    result = {
        "schema_version": "design-adequacy-assessment-v1", "subject_digest": subject_digest,
        "source_commit": source_commit, "contract_digest": fingerprint(contract),
        "status": "requires_independent_review_and_native_calibration",
        "calibration_work_units": work,
        "unverified_obligations": ["effect_justification_and_evidence_custody",
            "dependence_and_eligibility_assumptions", "estimator_and_dgp_implementation_binding",
            "actual_compute_cost_per_replicate", "null_size_interval_coverage_and_power"],
        "market_data_used": False, "scientific_certification": False, "execution_authority": False,
    }
    if structured:
        from bt.governance.calibration_consistency import assess_consistency
        findings = assess_consistency(contract['experiment'], contract['calibration_design'], calibration)
        if assured:
            from bt.governance.method_assurance import assess_assurance
            assurance_findings, totals = assess_assurance(contract['assurance'], evidence_catalog or {})
            findings += assurance_findings
            assurance = contract['assurance']
            ledger = assurance.get('workload', {}) if isinstance(assurance, dict) else {}
            caps = ledger.get('caps', {}) if isinstance(ledger, dict) else {}
            if not isinstance(caps, dict) or caps.get('statistic_evaluation') != calibration['max_work_units']:
                findings.append(dict(code='work_budget_binding', path='assurance.workload.caps',
                    message='statistic_evaluation cap must equal the declared calibration max_work_units.',
                    owner='method_designer', disposition='repair'))
            result['declared_work_by_unit'] = totals
            result['unverified_obligations'] += ['definition_semantics_and_dependency_completeness',
                'work_factor_completeness_and_runtime_enforcement']
        result['consistency'] = {'status': 'repair_required' if findings else 'structurally_consistent',
                                 'finding_count': len(findings), 'findings': findings[:64],
                                 'scientific_validity': 'unverified'}
        if findings:
            result['status'] = 'requires_method_correction'
        result['unverified_obligations'] += ['declared_dgp_execution_parity', 'joint_distribution_scientific_validity']
    else:
        result['consistency'] = {'status': 'unverified_legacy_prose', 'scientific_validity': 'unverified'}
    return result | {"report_digest": fingerprint(result)}
