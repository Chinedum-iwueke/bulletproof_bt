from copy import deepcopy

import pytest

from bt.governance.calibration_consistency import assess_consistency


def experiment_fixture(count=4, replicates=10000, purpose='scientific'):
    variants = [{'id': 'v'+str(i), 'event_probability': .1 if i % 2 == 0 else .05}
                for i in range(count)]
    experiment = dict(variants=variants, families=[
        {'id': 'primary', 'variant_ids': [v['id'] for v in variants]},
        {'id': 'comparison', 'variant_ids': [v['id'] for v in variants]}],
        partitions=[{'id': 'train', 'rows': 2500}, {'id': 'test', 'rows': 850}],
        purpose=purpose, inner_replicates=replicates, statistic_id='registered-statistic')
    scenario = dict(id='null_joint', families=deepcopy(experiment['families']),
        cells=[dict(partition_id=p['id'], variant_id=v['id'], event_probability=v['event_probability'],
                    expected_events=p['rows']*v['event_probability'],
                    sampling_rule='Synthetic Bernoulli marginal assumption, shared latent process.')
               for p in experiment['partitions'] for v in variants],
        joint_generation='Synthetic test declaration only; executable parity is not established.',
        null_or_effect='Synthetic null with zero registered effects for every variant.',
        estimator_application='Apply the registered statistic to all variants on shared draws.',
        seed_schedule='Derive independent streams from outer index and registered seed.',
        outputs='Retain each statistic, joint family decisions and undefined outcomes.',
        event_rate_mode='registered', event_rate_assumption='Nominal probabilities only, not observed support or fitted-quantile validity.')
    design = dict(purpose=purpose, statistic_id=experiment['statistic_id'], scenarios=[scenario])
    calibration = dict(seed=42, outer_replicates=10, inner_replicates=replicates,
                       max_work_units=10000000, dgp_ids=['null_joint'])
    return experiment, design, calibration


@pytest.mark.parametrize('count,replicates,purpose', [(1,199,'diagnostic'), (4,10000,'scientific'), (6,999,'scientific')])
def test_different_experiment_shapes_are_not_btc_or_bootstrap_constants(count, replicates, purpose):
    experiment, design, calibration = experiment_fixture(count, replicates, purpose)
    before = deepcopy((experiment, design, calibration))
    assert assess_consistency(experiment, design, calibration) == []
    assert (experiment, design, calibration) == before


@pytest.mark.parametrize('defect,code', [
    ('family','family_mismatch'), ('missing_cell','missing_cells'),
    ('duplicate_cell','cell_binding'), ('arithmetic','event_arithmetic'),
    ('rate','event_rate_mismatch'), ('replicates','replicate_mismatch'),
    ('purpose','procedure_mismatch'), ('statistic','procedure_mismatch'),
    ('scenario','scenario_mismatch'), ('label_only','missing_generation_rule'),
    ('nan','event_arithmetic'), ('boolean','event_arithmetic')])
def test_typed_findings_generalize(defect, code):
    experiment, design, calibration = experiment_fixture()
    scenario = design['scenarios'][0]
    if defect == 'family': scenario['families'][0]['variant_ids'] = ['v0']
    elif defect == 'missing_cell': scenario['cells'].pop()
    elif defect == 'duplicate_cell': scenario['cells'].append(deepcopy(scenario['cells'][0]))
    elif defect == 'arithmetic': scenario['cells'][0]['expected_events'] = 85
    elif defect == 'rate': scenario['cells'][1].update(event_probability=.1, expected_events=250)
    elif defect == 'replicates': calibration['inner_replicates'] = 1000
    elif defect == 'purpose': design['purpose'] = 'diagnostic'
    elif defect == 'statistic': design['statistic_id'] = 'different'
    elif defect == 'scenario': scenario['id'] = 'unregistered'
    elif defect == 'label_only': scenario['joint_generation'] = 'AR1'
    elif defect == 'nan': scenario['cells'][0]['event_probability'] = float('nan')
    else: scenario['cells'][0]['event_probability'] = True
    findings = assess_consistency(experiment, design, calibration)
    assert code in {f['code'] for f in findings}
    assert all(f['disposition'] == 'repair' for f in findings)


def test_r4_failure_pattern_and_session_style_correction():
    experiment, corrected, calibration = experiment_fixture()
    broken = deepcopy(corrected)
    broken['scenarios'][0]['families'] = [{'id': 'primary', 'variant_ids': ['v0']}]
    for cell in broken['scenarios'][0]['cells']:
        cell.update(event_probability=.1, expected_events=85)
    wrong_budget = calibration | {'inner_replicates': 1000}
    codes = {f['code'] for f in assess_consistency(experiment, broken, wrong_budget)}
    assert {'family_mismatch','event_arithmetic','event_rate_mismatch','replicate_mismatch'} <= codes
    assert assess_consistency(experiment, corrected, calibration) == []


def test_explicit_sensitivity_is_not_forbidden_or_called_empirical_support():
    experiment, design, calibration = experiment_fixture()
    scenario = design['scenarios'][0]
    scenario['event_rate_mode'] = 'sensitivity'
    scenario['cells'][0].update(event_probability=.08, expected_events=200)
    assert assess_consistency(experiment, design, calibration) == []


def test_invalid_frozen_experiment_is_not_author_repair():
    experiment, design, calibration = experiment_fixture()
    experiment['partitions'][0]['rows'] = 0
    findings = assess_consistency(experiment, design, calibration)
    assert all(f['owner'] == 'experiment_producer' and f['disposition'] == 'requires_new_contract' for f in findings)


def test_continuous_statistic_need_not_invent_event_groups():
    experiment, design, calibration = experiment_fixture(6, 999)
    for variant in experiment['variants']:
        variant['event_probability'] = None
    for cell in design['scenarios'][0]['cells']:
        cell.update(event_probability=None, expected_events=None,
                    sampling_rule='Continuous statistic; no event/control classification applies.')
    assert assess_consistency(experiment, design, calibration) == []
