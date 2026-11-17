from copy import deepcopy

import pytest

from bt.governance.data_requirements import RequirementsError, fingerprint
from bt.governance.diagnostic_tools import run_diagnostic
from bt.governance.requirements_feasibility import plan_requirements
from tests.governance.test_design_adequacy import contract
from tests.governance.test_requirements_feasibility import evidence

SOURCE = "c"*40


def setup(tool="design_adequacy_v1"):
    document = {"claim": "Synthetic test assumption, not verified market evidence"}
    catalog = {fingerprint(document): document}
    design = contract()
    design["meaningful_effect"]["evidence_digests"] = list(catalog)
    request = dict(schema_version="native-diagnostic-request-v1", tool=tool,
        source_commit=SOURCE, subject_digest="b"*64, payload=design)
    return request, catalog


def run(request, catalog, **kwargs):
    return run_diagnostic(request, runtime_source_commit=SOURCE, evidence_catalog=catalog, **kwargs)


def test_receipt_reproducibility_and_evidence_binding():
    request, catalog = setup()
    receipt = run(request, catalog)
    assert receipt == run(request, catalog)
    assert receipt['execution_authority'] is receipt['scientific_certification'] is False
    assert receipt['receipt_digest'] == fingerprint({k:v for k,v in receipt.items() if k!='receipt_digest'})
    assert receipt['implementation_digests']
    assert receipt['result']['status'] == 'requires_independent_review_and_native_calibration'


@pytest.mark.parametrize('change', ['source', 'evidence', 'missing', 'oversize'])
def test_binding_or_input_failure_precedes_calculation(change):
    request, catalog = setup()
    if change == 'source': request['source_commit'] = 'a'*40
    if change == 'evidence': catalog[next(iter(catalog))]['claim'] = 'changed'
    if change == 'missing': catalog = {}
    if change == 'oversize': request['payload']['estimand'] = 'x'*262145
    with pytest.raises(RequirementsError): run(request, catalog)


def test_unknown_and_disabled_tools_defer_not_import():
    request, catalog = setup('__import__("os")')
    assert run(request, catalog)['result']['status'] == 'await_engineering'
    request['tool'] = 'design_adequacy_v1'
    assert run(request, catalog, disabled_tools=('design_adequacy_v1',))['result']['status'] == 'await_engineering'


def calibration():
    request, catalog = setup('residual_calibration_v1')
    design = request['payload']
    design['estimator_id'] = 'frozen_residual_contrast_v1'
    design['multiplicity'] = 'single_primary_one_sided_0.05'
    design['eligibility_policy'] = 'all_synthetic_rows'
    design['undefined_replicate_policy'] = 'invalidate_experiment'
    design['calibration'].update(outer_replicates=3, inner_replicates=99,
        max_work_units=100000, dgp_ids=['iid_null_v1','ar_null_v1','iid_shift_v1'])
    request['payload'] = dict(design=design, sample_size=80, block_length=12)
    return request, catalog


def test_registered_calibration_has_scoped_metrics_and_known_truth():
    request, catalog = calibration()
    report = run(request, catalog)
    assert report == run(request, catalog)
    assert report['evidence_kind'] == 'synthetic_calibration'
    for item in report['result']['results']:
        assert item['valid_experiments'] + item['invalid_experiments'] == 3
        assert item['rejection_all_experiments']['denominator'] == 3
    assert report['result']['results'][0]['truth_primary'] == 0
    assert report['result']['results'][2]['truth_primary'] == .25


def test_unsupported_estimator_and_excessive_work_never_execute():
    request, catalog = calibration()
    request['payload']['design']['estimator_id'] = 'full_candidate_ols'
    assert run(request, catalog)['result']['status'] == 'await_engineering'
    request, catalog = calibration()
    request['payload']['design']['calibration']['max_work_units'] = 1000
    with pytest.raises(RequirementsError): run(request, catalog)


def test_native_planning_carries_diagnostic_and_defers_unavailable_tool():
    e = evidence()
    source = 'a'*40
    request, catalog = setup('not_registered')
    request['source_commit'] = source
    request['subject_digest'] = fingerprint(e['discovery_candidate'])
    e.update(diagnostic_request=request, diagnostic_evidence_catalog=catalog)
    report = plan_requirements(e, source_commit=source)
    assert report['status'] == 'deferred'
    assert 'diagnostic:await_engineering' in report['reasons']
    assert report['native_diagnostic']['request_digest'] == fingerprint(request)


def test_recursive_dispatch_rejected():
    request, catalog = setup('requirements_feasibility_v1')
    request['payload'] = {'diagnostic_request': {}}
    with pytest.raises(RequirementsError, match='recursive'): run(request, catalog)


@pytest.mark.parametrize('field,value', [
    ('multiplicity', 'joint_holm'), ('eligibility_policy', 'candidate_filter'),
    ('undefined_replicate_policy', 'drop_draws'),
])
def test_unsupported_policy_cannot_be_approximated(field, value):
    request, catalog = calibration()
    request['payload']['design'][field] = value
    report = run(request, catalog)
    assert report['result']['status'] == 'await_engineering'
    assert report['evidence_kind'] == 'design_diagnostic'


def test_dgp_order_does_not_change_random_streams():
    request, catalog = calibration()
    first = run(request, catalog)['result']['results']
    request['payload']['design']['calibration']['dgp_ids'].reverse()
    assert first == list(reversed(run(request, catalog)['result']['results']))
