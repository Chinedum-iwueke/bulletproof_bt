"""Allowlisted native diagnostics. No arbitrary commands, lake access or approval."""
import hashlib
import json
import math
from pathlib import Path
import random
import re

from bt.governance.data_requirements import RequirementsError, fingerprint
from bt.governance.design_adequacy import assess_design_adequacy
from bt.governance.event_support_planning import assess_event_support
from bt.governance.residual_bootstrap import paired_bootstrap

TOOLS = {
    "design_adequacy_v1": ["design_adequacy.py", "data_requirements.py"],
    "event_support_v1": ["event_support_planning.py", "data_requirements.py"],
    "requirements_feasibility_v1": ["requirements_feasibility.py", "data_requirements.py", "event_support_planning.py"],
    "residual_calibration_v1": ["residual_bootstrap.py", "design_adequacy.py", "data_requirements.py"],
}
DGPS = {"iid_null_v1": (0., False), "ar_null_v1": (.6, False), "iid_shift_v1": (0., True)}


def _fields(value, fields):
    if not isinstance(value, dict) or set(value) != set(fields):
        raise RequirementsError("diagnostic fields differ")


def _rate(count, total):
    if not total:
        return {"count": count, "denominator": total, "rate": None, "wilson95": None}
    p, z = count/total, 1.959963984540054
    scale = 1+z*z/total
    center = (p+z*z/(2*total))/scale
    half = z*math.sqrt(p*(1-p)/total+z*z/(4*total*total))/scale
    return {"count": count, "denominator": total, "rate": p,
            "wilson95": [max(0., center-half), min(1., center+half)]}


def _calibrate(payload, subject, source):
    _fields(payload, ("design", "sample_size", "block_length"))
    design = payload["design"]
    assessment = assess_design_adequacy(design, subject_digest=subject, source_commit=source)
    if (design["estimator_id"] != "frozen_residual_contrast_v1" or any(
            d not in DGPS for d in design["calibration"]["dgp_ids"])
            or design["multiplicity"] != "single_primary_one_sided_0.05"
            or design["eligibility_policy"] != "all_synthetic_rows"
            or design["undefined_replicate_policy"] != "invalidate_experiment"):
        return {"status": "await_engineering", "reason": "unregistered_estimator_or_dgp",
                "requires_engineering_approval": True, "assessment": assessment}
    n, length = payload["sample_size"], payload["block_length"]
    cal = design["calibration"]
    if type(n) is not int or not 20 <= n <= 2000 or type(length) is not int or not 1 <= length <= n:
        raise RequirementsError("invalid calibration sample or block size")
    if cal["inner_replicates"] < 99:
        raise RequirementsError("at least 99 bootstrap replicates required")
    work = len(cal["dgp_ids"])*cal["outer_replicates"]*(n*length + cal["inner_replicates"]*math.ceil(n/length))
    if work > min(cal["max_work_units"], 10_000_000):
        raise RequirementsError("calibration exceeds admitted work budget")
    if design["meaningful_effect"]["units"] != "residual SD" or design["meaningful_effect"]["value"] > 100:
        raise RequirementsError("registered DGP requires residual SD units")
    results = []
    for dgp in cal["dgp_ids"]:
        rho, shifted = DGPS[dgp]
        counts = dict(valid=0, invalid=0, rejected=0, covered=0)
        for trial in range(cal["outer_replicates"]):
            # DGP/seed binding is independent of requested DGP ordering.
            seed = int(fingerprint([cal["seed"], dgp, trial])[:16], 16)
            rng = random.Random(seed)
            primary_events = [rng.random() < .1 for _ in range(n)]
            rival_events = [rng.random() < .1 for _ in range(n)]
            state, y = rng.gauss(0, 1), []
            delta = design["meaningful_effect"]["value"] if shifted else 0.
            for event in primary_events:
                state = rho*state + math.sqrt(1-rho*rho)*rng.gauss(0, 1)
                y.append(state + delta*event)
            result = paired_bootstrap(y, y, primary_events, rival_events,
                block_length=length, replicates=cal["inner_replicates"], seed=seed)
            if result["status"] != "ok":
                counts["invalid"] += 1
                continue
            counts["valid"] += 1
            # The primary contrast has known population delta. Paired inference
            # is exercised, but no paired population truth is asserted here.
            primary = result["statistics"][0]
            counts["rejected"] += int(primary["observed"] > 0 and primary["centered_p"] <= .05)
            counts["covered"] += int(primary["interval95"][0] <= delta <= primary["interval95"][1])
        results.append({"dgp_id": dgp, "truth_primary": delta,
            "valid_experiments": counts["valid"], "invalid_experiments": counts["invalid"],
            "rejection_all_experiments": _rate(counts["rejected"], cal["outer_replicates"]),
            "rejection_given_valid": _rate(counts["rejected"], counts["valid"]),
            "coverage_given_valid": _rate(counts["covered"], counts["valid"])})
    return {"status": "diagnostic_only", "assessment": assessment, "work_units": work,
        "seed": cal["seed"], "results": results,
        "scope": "frozen residual core; primary marginal .05 test; not fitted strategy, paired power or joint Holm acceptance",
        "assumptions": "unit Gaussian innovations; IID .1 event indicators; no eligibility loss or nuisance fitting",
        "invalid_draw_policy": "invalidate entire experiment; preserve invalid denominator"}


def run_diagnostic(request, *, runtime_source_commit, evidence_catalog, disabled_tools=()):
    """Caller supplies custody-verified evidence; this verifies its content hashes."""
    _fields(request, ("schema_version", "tool", "source_commit", "subject_digest", "payload"))
    if request["schema_version"] != "native-diagnostic-request-v1":
        raise RequirementsError("unsupported diagnostic request")
    if not isinstance(runtime_source_commit, str) or not re.fullmatch(r"[a-f0-9]{40}", runtime_source_commit):
        raise RequirementsError("invalid runtime source")
    if request["source_commit"] != runtime_source_commit:
        raise RequirementsError("diagnostic source mismatch")
    if not isinstance(request["subject_digest"], str) or not re.fullmatch(r"[a-f0-9]{64}", request["subject_digest"]):
        raise RequirementsError("invalid subject binding")
    try:
        size = len(json.dumps([request, evidence_catalog], allow_nan=False).encode())
    except (ValueError, TypeError, RecursionError) as exc:
        raise RequirementsError("invalid diagnostic JSON") from exc
    if size > 262144:
        raise RequirementsError("diagnostic input exceeds bound")
    if not isinstance(evidence_catalog, dict) or len(evidence_catalog) > 16:
        raise RequirementsError("invalid evidence catalog")
    for digest, document in evidence_catalog.items():
        if fingerprint(document) != digest:
            raise RequirementsError("evidence content digest mismatch")
    tool, payload = request["tool"], request["payload"]
    if not isinstance(tool, str) or not 1 <= len(tool) <= 100:
        raise RequirementsError("invalid tool identifier")
    if tool not in TOOLS or tool in disabled_tools:
        result = {"status": "await_engineering", "reason": "tool_unavailable",
                  "requires_engineering_approval": True}
        files = []
    else:
        files = TOOLS[tool]
        if tool in {"design_adequacy_v1", "residual_calibration_v1"}:
            design = payload if tool == "design_adequacy_v1" else payload.get("design") if isinstance(payload, dict) else None
            assess_design_adequacy(design, subject_digest=request["subject_digest"], source_commit=runtime_source_commit)
            if any(ref not in evidence_catalog for ref in design["meaningful_effect"]["evidence_digests"]):
                raise RequirementsError("missing justification evidence")
            result = (assess_design_adequacy(design, subject_digest=request["subject_digest"], source_commit=runtime_source_commit)
                      if tool == "design_adequacy_v1" else _calibrate(payload, request["subject_digest"], runtime_source_commit))
        elif tool == "event_support_v1":
            _fields(payload, ("specification", "requirements", "scientific_plan"))
            result = assess_event_support(**payload)
        else:
            from bt.governance.requirements_feasibility import plan_requirements
            if "diagnostic_request" in payload:
                raise RequirementsError("recursive diagnostic dispatch prohibited")
            result = plan_requirements(payload, source_commit=runtime_source_commit)
    implementation = {name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
                      for name in ["diagnostic_tools.py", *files]}
    receipt = {"schema_version": "native-diagnostic-receipt-v1", "tool": tool,
        "source_commit": runtime_source_commit, "subject_digest": request["subject_digest"],
        "request_digest": fingerprint(request), "evidence_catalog_digest": fingerprint(evidence_catalog),
        "implementation_digests": implementation, "result": result,
        "evidence_kind": "synthetic_calibration" if tool == "residual_calibration_v1" and result["status"] == "diagnostic_only" else "design_diagnostic",
        "market_data_used": False, "scientific_certification": False, "execution_authority": False}
    return receipt | {"receipt_digest": fingerprint(receipt)}
