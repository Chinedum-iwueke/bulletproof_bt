from copy import deepcopy
import hashlib
from pathlib import Path

import pytest
import yaml

from bt.governance.data_requirements import fingerprint, plan_validation, verify_execution_plan
from bt.governance.released_execution import prepare_released_execution_contract
from test_data_requirements import inputs


@pytest.fixture
def preparation(monkeypatch):
    import bt.governance.released_execution as module
    import bt.governance.alpha_strategy_pipeline as pipeline

    root = Path(__file__).resolve().parents[2]
    path = root / 'research/hypotheses/aep_007c_btc_seasonal_quote_volume_volatility_2h.yaml'
    template = yaml.safe_load(path.read_bytes())
    plan = deepcopy(template['representation_plan'])
    projected = deepcopy(plan)
    projected.pop('transformation_rationale')
    projected.pop('rejected_alternatives')
    for member in projected['basket_members']: member.pop('selection_rationale')
    for transform in projected['transformations']: transform.pop('rationale')
    # Boundary fixture: contract/compilation internals are tested separately.
    template['immutable_contract']['accepted_representation_plan_digest'] = fingerprint(plan)
    monkeypatch.setattr(module.yaml, 'safe_load', lambda raw: deepcopy(template))
    transition = dict(schema_version='released-representation-transition-v1',
        accepted_representation_digest=fingerprint(plan), released_representation_digest=fingerprint(plan),
        release_source_commit='a'*40, engineering_bundle_digest='b'*64,
        operational_projection_digest=fingerprint(projected), narrative_changes=[],
        disposition='requires_independent_execution_handoff_review',
        operational_projection_equal=True, scientific_equivalence_verified=False,
        execution_authority=False, historical_documents_mutated=False)
    transition['evidence_digest'] = fingerprint(transition)
    assignment = dict(base_ref='a'*40, reusable_strategy=dict(
        contract_path=str(path.relative_to(root)), contract_digest=hashlib.sha256(path.read_bytes()).hexdigest()),
        released_execution_preparation=dict(schema_version='released-execution-preparation-v1',
            expected_template_digest=fingerprint(template), expected_method_digest='c'*64,
            engineering_bundle_digest='b'*64, accepted_representation=plan,
            representation_transition=transition, native_dataset_bindings=[],
            validation_requirements={}, approved_window={}, expected_plans={}))
    monkeypatch.setattr(module, 'prepare_released_execution_contract',
        lambda *args, **kwargs: dict(contract=deepcopy(template), lineage={'execution_authority':False}))
    card = {'status':'draft', 'identity':'prospective-card'}
    monkeypatch.setattr(pipeline, 'draft_research_card', lambda *args, **kwargs: deepcopy(card))
    def unexpected(*args, **kwargs):
        pytest.fail('Unapproved preparation must not confirm or qualify a card')
    monkeypatch.setattr(pipeline, 'confirm_card', unexpected)
    monkeypatch.setattr(pipeline, 'qualify_card', unexpected)
    return module, root, assignment, card


def test_preparation_stops_before_confirmation_without_exact_approval(preparation):
    module, root, assignment, card = preparation
    before = deepcopy(assignment)
    result = module.prepare_released_qualification(assignment, repository_root=root)
    assert result['disposition'] == 'released_execution_card_prepared'
    assert result['card_digest'] == fingerprint(card)
    assert result['execution_authority'] is False
    assert 'qualification' not in result
    assert assignment == before


def test_preparation_rejects_approval_for_another_card(preparation):
    module, root, assignment, _ = preparation
    assignment['card_approval'] = dict(card_digest='f'*64, actor='reviewer', approved_at='2026-11-25T00:00:00Z')
    with pytest.raises(ValueError, match='Exact prospective card'):
        module.prepare_released_qualification(assignment, repository_root=root)


@pytest.mark.parametrize('field', ['execution_authority', 'historical_documents_mutated'])
def test_rehashed_transition_cannot_claim_authority_or_rewrite_history(preparation, field):
    module, root, assignment, _ = preparation
    transition = assignment['released_execution_preparation']['representation_transition']
    transition[field] = True
    transition['evidence_digest'] = fingerprint({k:v for k,v in transition.items() if k != 'evidence_digest'})
    with pytest.raises(ValueError, match='transition evidence'):
        module.prepare_released_qualification(assignment, repository_root=root)


def fixture(instruments=('TRBUSDT',), minutes=20):
    args = inputs(instruments=instruments, minutes=minutes)
    package = {k:deepcopy(args[k]) for k in ('requirements','coverage','maximum_history_days','maximum_bar_variant_work')}
    plans = {mode:plan_validation(**args,mode=mode) for mode in ('smoke','scientific')}
    template = dict(parameter_grid={'threshold':[1,2,3,4]},
        execution_semantics={'warmup_observations':args['requirements']['feature_warmup_bars']},
        immutable_contract=dict(source_commit='b'*40, accepted_method_proposal_digest='c'*64,
            question='Retained scientific question, unchanged by release.',
            instruments=list(instruments), dataset_bindings=deepcopy(args['dataset_bindings']),
            validation_plans=deepcopy(plans)))
    kwargs = dict(expected_template_digest=fingerprint(template), expected_method_digest='c'*64,
        source_commit=args['source_commit'], dataset_bindings=args['dataset_bindings'],
        validation_requirements=package, approved_window=args['approved_window'], expected_plans=plans)
    return template, kwargs


@pytest.mark.parametrize('instruments,minutes', [(('TRBUSDT',),20),(('ETHUSDT','SOLUSDT'),30)])
def test_source_binding_preserves_science_and_replays_native_plans(instruments, minutes):
    template,kwargs = fixture(instruments,minutes)
    before = deepcopy((template,kwargs))
    result = prepare_released_execution_contract(template,**kwargs)
    assert (template,kwargs) == before
    assert result == prepare_released_execution_contract(template,**kwargs)
    assert result['contract']['immutable_contract']['question'] == template['immutable_contract']['question']
    assert not result['lineage']['execution_authority']
    assert result['lineage']['requires_independent_execution_review']
    for mode in ('smoke','scientific'):
        plan = kwargs['expected_plans'][mode]
        parts = plan['partitions']
        assignment = dict(base_ref=kwargs['source_commit'],validation_plan=plan,
            execution_class='commissioning' if mode=='smoke' else 'qualification',
            window_start=parts['smoke' if mode=='smoke' else 'training']['start'],
            window_end=parts['target_tail']['end'])
        assert verify_execution_plan(assignment,result['contract']['immutable_contract']) == plan


@pytest.mark.parametrize('field,value', [('expected_template_digest','f'*64),('expected_method_digest','f'*64),
                                      ('source_commit','main')])
def test_changed_release_identity_is_rejected(field,value):
    template,kwargs = fixture();kwargs[field]=value
    with pytest.raises(ValueError): prepare_released_execution_contract(template,**kwargs)


def test_changed_data_or_partitions_cannot_be_hidden_by_rehashing():
    template,kwargs = fixture();kwargs['dataset_bindings'][0]['dataset_digest']='f'*64
    with pytest.raises(ValueError):prepare_released_execution_contract(template,**kwargs)
    template,kwargs = fixture();template['immutable_contract']['validation_plans']['smoke']['partitions']['smoke']['start']='2025-01-01T00:00:00+00:00'
    kwargs['expected_template_digest']=fingerprint(template)
    with pytest.raises(ValueError,match='partitions'):prepare_released_execution_contract(template,**kwargs)


def test_tampered_plan_is_not_promoted_into_a_new_identity():
    template,kwargs = fixture();kwargs['expected_plans']['smoke']['plan_digest']='f'*64
    with pytest.raises(ValueError,match='planning'):prepare_released_execution_contract(template,**kwargs)


def test_warmup_mismatch_requires_correction_not_silent_normalization():
    template,kwargs=fixture();template['execution_semantics']['warmup_observations']=1
    kwargs['expected_template_digest']=fingerprint(template)
    with pytest.raises(ValueError,match='warmup'):prepare_released_execution_contract(template,**kwargs)


def test_equivalent_utc_notation_preserves_original_documents_and_hashes():
    template,kwargs=fixture()
    for plan in template['immutable_contract']['validation_plans'].values():
        for interval in plan['partitions'].values():
            for key in interval: interval[key]=interval[key].replace('+00:00','Z')
    kwargs['expected_template_digest']=fingerprint(template)
    before=deepcopy(template)
    result=prepare_released_execution_contract(template,**kwargs)
    assert template == before
    assert result['lineage']['historical_validation_plans_digest']==fingerprint(before['immutable_contract']['validation_plans'])


def test_naive_partition_timestamps_are_rejected():
    template,kwargs=fixture()
    template['immutable_contract']['validation_plans']['smoke']['partitions']['smoke']['end']='2025-02-01T00:00:00'
    kwargs['expected_template_digest']=fingerprint(template)
    with pytest.raises(ValueError):prepare_released_execution_contract(template,**kwargs)
