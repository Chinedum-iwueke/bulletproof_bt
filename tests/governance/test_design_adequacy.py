from copy import deepcopy

import pytest

from bt.governance.data_requirements import RequirementsError
from bt.governance.design_adequacy import assess_design_adequacy


def contract():
    return dict(schema_version="design-adequacy-contract-v1", estimand="residual contrast",
        estimator_id="proposed_estimator_not_yet_registered", meaningful_effect={
            "value": .25, "units": "residual SD", "justification": "Synthetic fixture, not a scientific justification",
            "evidence_digests": ["a"*64]}, dependence_assumptions="AR scenarios proposed",
        multiplicity="two four-hypothesis Holm families", eligibility_policy="fail closed on support",
        undefined_replicate_policy="invalidate; never discard silently",
        calibration={"seed": 42, "outer_replicates": 100, "inner_replicates": 199,
                     "max_work_units": 100000, "dgp_ids": ["null_iid", "null_ar"]})


def assess(c):
    return assess_design_adequacy(c, subject_digest="b"*64, source_commit="c"*40)


def test_complete_contract_is_not_scientific_approval():
    result = assess(contract())
    assert result == assess(contract())
    assert result['calibration_work_units'] == 39800
    assert result['execution_authority'] is result['scientific_certification'] is False
    assert result['status'] == 'requires_independent_review_and_native_calibration'
    assert result['consistency']['status'] == 'unverified_legacy_prose'
    changed = contract()
    changed['calibration']['seed'] += 1
    assert result['report_digest'] != assess(changed)['report_digest']


def test_structured_native_diagnostic_returns_repair_then_readiness_not_approval():
    from test_calibration_consistency import experiment_fixture
    experiment, design, calibration = experiment_fixture()
    value = contract() | dict(experiment=experiment, calibration_design={}, calibration=calibration)
    failed = assess(value)
    assert failed['status'] == 'requires_method_correction'
    assert failed['consistency']['findings'][0]['code'] == 'missing_design'
    value['calibration_design'] = design
    ready = assess(value)
    assert ready['consistency']['status'] == 'structurally_consistent'
    assert ready['scientific_certification'] is False
    assert ready['execution_authority'] is False


@pytest.mark.parametrize('field', list(contract()))
def test_missing_obligation_rejected(field):
    value = contract()
    del value[field]
    with pytest.raises(RequirementsError):
        assess(value)


@pytest.mark.parametrize('value', [float('nan'), float('inf'), 0, True])
def test_invalid_effect_rejected(value):
    c = contract()
    c['meaningful_effect']['value'] = value
    with pytest.raises(RequirementsError):
        assess(c)


def test_compute_and_reference_guards():
    for change in ('budget', 'duplicate', 'reference'):
        c = deepcopy(contract())
        if change == 'budget':
            c['calibration']['max_work_units'] = 10
        elif change == 'duplicate':
            c['calibration']['dgp_ids'] = ['same', 'same']
        else:
            c['meaningful_effect']['evidence_digests'] = ['not-a-receipt']
        with pytest.raises(RequirementsError):
            assess(c)


@pytest.mark.parametrize('budget', [266_240_000, 10**15])
def test_large_prospective_budget_is_not_execution_authority(budget):
    value = contract()
    value['calibration']['max_work_units'] = budget
    result = assess(value)
    assert result['execution_authority'] is False
    assert result['scientific_certification'] is False
    assert result['status'] == 'requires_independent_review_and_native_calibration'


@pytest.mark.parametrize('budget', [10**15 + 1, True, 1.5, float('inf')])
def test_prospective_budget_remains_bounded_integer(budget):
    value = contract()
    value['calibration']['max_work_units'] = budget
    with pytest.raises(RequirementsError, match='max_work_units'):
        assess(value)
