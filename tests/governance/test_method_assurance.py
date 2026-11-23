from copy import deepcopy

import pytest

from bt.governance.data_requirements import fingerprint
from bt.governance.method_assurance import assess_assurance


def fixture():
    document = {'primitive': 'Initialized return innovation with an explicit assumed law.',
                'derived': 'Absolute return is the absolute value of the return input.'}
    digest = fingerprint(document)
    assurance = dict(required_outputs=['absolute_return'], definitions=[
        dict(id='return_input', kind='primitive', dependencies=[], statement=None, evidence_digest=digest, pointer='/primitive'),
        dict(id='absolute_return', kind='derived', dependencies=['return_input'], statement=None, evidence_digest=digest, pointer='/derived')],
        workload=dict(caps={'statistic_evaluation': 1000}, stages=[
            dict(id=p, phase=p, unit='statistic_evaluation', factors={'calls': 10}, not_applicable_reason=None)
            for p in ['generation', 'fit', 'solve', 'verification', 'bootstrap']]))
    return assurance, {digest: document}


def codes(a, e):
    return {f['code'] for f in assess_assurance(a, e)[0]}


def test_resolved_contract_returns_dimensioned_totals_not_authority():
    a, e = fixture()
    assert assess_assurance(a, e) == ([], {'statistic_evaluation': 50})
    assert a == fixture()[0]


@pytest.mark.parametrize('mutation,expected', [
    (lambda a: a['definitions'][1].update(pointer='/absent'), 'unresolved_definition'),
    (lambda a: a['definitions'][1].update(pointer='/bad~2'), 'invalid_pointer'),
    (lambda a: a['definitions'][1].update(dependencies=['absent']), 'missing_dependency'),
    (lambda a: a['definitions'][1].update(dependencies=['absolute_return']), 'definition_cycle'),
    (lambda a: a['definitions'].append(deepcopy(a['definitions'][1])), 'duplicate_definition'),
    (lambda a: a.update(required_outputs=['missing']), 'missing_output_definition'),
    (lambda a: a['workload']['stages'].pop(), 'missing_work_phase'),
    (lambda a: a['workload']['stages'][0].update(factors={'calls': True}), 'invalid_work_factors'),
    (lambda a: a['workload']['stages'][0].update(factors={'calls': 1001}), 'work_cap_exceeded'),
    (lambda a: a['workload']['stages'][0].update(unit='history'), 'invalid_work_factors'),
    (lambda a: a['workload']['stages'].append(deepcopy(a['workload']['stages'][0])), 'duplicate_work_stage'),
])
def test_structural_defects_return_actionable_findings(mutation, expected):
    a, e = fixture()
    mutation(a)
    assert expected in codes(a, e)


def test_tampered_evidence_and_missing_catalog_fail_closed():
    a, e = fixture()
    assert 'unresolved_definition' in codes(a, {})
    next(iter(e.values()))['derived'] = 'changed'
    assert 'unresolved_definition' in codes(a, e)


def test_new_inline_definition_is_possible_without_fabricating_a_prior_receipt():
    a, e = fixture()
    a['definitions'][1].update(statement='absolute_return = abs(return_input)', evidence_digest=None, pointer=None)
    assert not codes(a, e)
    a['definitions'][1]['pointer'] = '/derived'
    assert 'ambiguous_definition' in codes(a, e)


def test_separate_units_are_not_summed_as_if_equivalent():
    a, e = fixture()
    a['workload']['caps']['history'] = 2
    a['workload']['stages'][0].update(unit='history', factors={'histories': 2})
    assert assess_assurance(a, e) == ([], {'statistic_evaluation': 40, 'history': 2})


def test_solver_factor_product_cannot_hide_behind_a_panel_count():
    a, e = fixture()
    a['workload']['stages'][2]['factors'] = {'starts': 19, 'iterations': 500, 'jacobian_calls': 18, 'histories': 8192}
    assert 'work_cap_exceeded' in codes(a, e)


def test_non_applicable_phases_require_reasons():
    a, e = fixture()
    a['workload']['stages'][2].update(unit=None, factors={}, not_applicable_reason='No optimization is used by this analytical method.')
    assert not codes(a, e)
    a['workload']['stages'][2]['not_applicable_reason'] = 'skip'
    assert 'invalid_work_exemption' in codes(a, e)


def test_v2_native_integration_requires_resolved_assurance():
    from test_design_adequacy import contract
    from test_calibration_consistency import experiment_fixture
    from bt.governance.design_adequacy import assess_design_adequacy
    experiment, design, calibration = experiment_fixture()
    assurance, evidence = fixture()
    assurance['workload']['caps']['statistic_evaluation'] = calibration['max_work_units']
    value = contract() | dict(schema_version='design-adequacy-contract-v2', experiment=experiment,
        calibration_design=design, calibration=calibration, assurance=assurance)
    result = assess_design_adequacy(value, subject_digest='b'*64, source_commit='c'*40, evidence_catalog=evidence)
    assert result['consistency']['status'] == 'structurally_consistent'
    assert result['scientific_certification'] is result['execution_authority'] is False
    blocked = assess_design_adequacy(value, subject_digest='b'*64, source_commit='c'*40)
    assert blocked['status'] == 'requires_method_correction'
    value['assurance']['workload']['caps']['statistic_evaluation'] += 1
    mismatch = assess_design_adequacy(value, subject_digest='b'*64, source_commit='c'*40, evidence_catalog=evidence)
    assert 'work_budget_binding' in {f['code'] for f in mismatch['consistency']['findings']}
