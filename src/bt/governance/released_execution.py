"""Prepare a prospective execution artifact without changing a released template.

The caller must establish custody of release, method, admission and planning
evidence. This module replays identities and plans; it grants no authority and
does not turn compilation or method acceptance into execution review.
"""
from copy import deepcopy
import hashlib
from math import prod
from pathlib import Path
import re

import yaml

from bt.governance.data_requirements import fingerprint, instant, plan_validation
from bt.governance.dataset_bindings import verify_dataset_binding_identities


def _partition_instants(partitions):
    if not isinstance(partitions, dict) or not partitions:
        raise ValueError('Frozen experiment partitions are missing')
    result = {}
    for name, interval in partitions.items():
        if not isinstance(interval, dict) or set(interval) != {'start', 'end'}:
            raise ValueError('Frozen experiment partition fields are invalid')
        result[name] = (instant(interval['start']), instant(interval['end']))
        if result[name][0] >= result[name][1]:
            raise ValueError('Frozen experiment partition interval is invalid')
    return result


def prepare_released_execution_contract(
    template, *, expected_template_digest, expected_method_digest, source_commit,
    dataset_bindings, validation_requirements, approved_window, expected_plans,
):
    if fingerprint(template) != expected_template_digest:
        raise ValueError('Released template digest mismatch')
    if not isinstance(source_commit, str) or not re.fullmatch(r'[0-9a-f]{40}', source_commit):
        raise ValueError('Released source commit is invalid')
    original = template.get('immutable_contract', {})
    if (not isinstance(expected_method_digest, str)
            or not re.fullmatch(r'[0-9a-f]{64}', expected_method_digest)
            or original.get('accepted_method_proposal_digest') != expected_method_digest):
        raise ValueError('Accepted method identity mismatch')
    prior_bindings = original.get('dataset_bindings')
    if not isinstance(prior_bindings, list) or not prior_bindings:
        raise ValueError('Released template lacks frozen dataset bindings')
    # Re-attestation may change its producer receipt, never the underlying bytes
    # or other declared identities. These checks do not verify receipt custody.
    preserved = [{k:v for k,v in item.items() if k != 'producer_receipt_digest'}
                 for item in prior_bindings]
    normalized = verify_dataset_binding_identities(dataset_bindings, preserved)
    for old in prior_bindings:
        symbol = old.get('instrument', (old.get('instruments') or [None])[0])
        if 'producer_receipt_digest' in old:
            receipt = normalized[symbol].get('producer_receipt_digest')
            if not isinstance(receipt, str) or not re.fullmatch(r'[0-9a-f]{64}', receipt):
                raise ValueError('New producer receipt identity missing')
    if set(expected_plans) != {'smoke', 'scientific'}:
        raise ValueError('Both source-bound native plans are required')
    grid = template.get('parameter_grid')
    if not isinstance(grid, dict) or not grid or not all(isinstance(v, list) and v for v in grid.values()):
        raise ValueError('Released variant grid is invalid')
    variants = prod(len(v) for v in grid.values())
    package = deepcopy(validation_requirements)
    requirements = package['requirements']
    semantics = template['execution_semantics']
    if semantics.get('warmup_observations') != requirements['feature_warmup_bars']:
        raise ValueError('Frozen warmup differs from validated requirements')
    plans = {}
    for mode in ('smoke', 'scientific'):
        plan = plan_validation(requirements=requirements, coverage=package['coverage'],
            dataset_bindings=dataset_bindings, instruments=original['instruments'],
            source_commit=source_commit, approved_window=approved_window,
            maximum_history_days=package['maximum_history_days'],
            maximum_bar_variant_work=package['maximum_bar_variant_work'], variants=variants, mode=mode)
        if (plan['status'] != 'ready' or plan['reasons'] or plan != expected_plans[mode]):
            raise ValueError('Current native planning evidence mismatch or infeasible')
        if _partition_instants(plan['partitions']) != _partition_instants(original['validation_plans'][mode]['partitions']):
            raise ValueError('Frozen experiment partitions changed')
        plans[mode] = plan
    result = deepcopy(template)
    immutable = result['immutable_contract']
    immutable.update(source_commit=source_commit, dataset_bindings=deepcopy(dataset_bindings),
        validation_requirements=package, window=deepcopy(approved_window), maximum_variants=variants,
        validation_plans=plans)
    lineage = dict(schema_version='released-execution-contract-binding-v1',
        released_template_digest=expected_template_digest,
        historical_immutable_contract_digest=fingerprint(original),
        accepted_method_proposal_digest=expected_method_digest,
        prior_source_commit=original.get('source_commit'), source_commit=source_commit,
        prior_dataset_bindings_digest=fingerprint(prior_bindings),
        historical_validation_plans_digest=fingerprint(original['validation_plans']),
        dataset_bindings_digest=fingerprint(dataset_bindings),
        native_plan_digests={mode:plan['plan_digest'] for mode,plan in plans.items()},
        prepared_contract_digest=fingerprint(result),
        requires_independent_execution_review=True, execution_authority=False,
        scientific_certification=False)
    return dict(contract=result, lineage=lineage | {'binding_digest':fingerprint(lineage)})


def prepare_released_qualification(assignment, *, repository_root):
    """Compile the prospective artifact; existing execution review stays mandatory."""
    from bt.experiments.adaptive_representation import validate_plan
    from bt.governance.alpha_strategy_pipeline import confirm_card, draft_research_card, qualify_card
    from bt.hypotheses.contract import HypothesisContract

    request = assignment['released_execution_preparation']
    required = {'schema_version', 'expected_template_digest', 'expected_method_digest',
                'engineering_bundle_digest', 'accepted_representation', 'representation_transition',
                'native_dataset_bindings', 'validation_requirements', 'approved_window', 'expected_plans'}
    if set(request) != required or request['schema_version'] != 'released-execution-preparation-v1':
        raise ValueError('Released execution preparation fields are not exact')
    root = Path(repository_root).resolve(strict=True)
    capability = assignment['reusable_strategy']
    path = (root / capability['contract_path']).resolve(strict=True)
    if not path.is_relative_to(root / 'research/hypotheses') or path.suffix != '.yaml':
        raise ValueError('Released template path is invalid')
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != capability['contract_digest']:
        raise ValueError('Released template file digest mismatch')
    template = yaml.safe_load(raw)
    accepted = request['accepted_representation']
    released = template['representation_plan']
    transition = request['representation_transition']
    body = {k:v for k,v in transition.items() if k != 'evidence_digest'}
    if (fingerprint(body) != transition.get('evidence_digest')
            or transition.get('schema_version') != 'released-representation-transition-v1'
            or transition.get('release_source_commit') != assignment['base_ref']
            or transition.get('engineering_bundle_digest') != request['engineering_bundle_digest']
            or transition.get('execution_authority') is not False
            or transition.get('historical_documents_mutated') is not False
            or transition.get('operational_projection_equal') is not True
            or transition.get('scientific_equivalence_verified') is not False
            or transition.get('disposition') != 'requires_independent_execution_handoff_review'):
        raise ValueError('Representation transition evidence is invalid')
    if (fingerprint(accepted) != transition.get('accepted_representation_digest')
            or fingerprint(released) != transition.get('released_representation_digest')
            or fingerprint(accepted) != template['immutable_contract']['accepted_representation_plan_digest']
            or fingerprint(released) != template['execution_semantics']['adaptive_representation_plan_digest']):
        raise ValueError('Representation transition full-document identity mismatch')
    projected = []
    changes = []
    for document in (accepted, released):
        validate_plan(document)
        value = deepcopy(document)
        value.pop('transformation_rationale'); value.pop('rejected_alternatives')
        for member in value['basket_members']: member.pop('selection_rationale')
        for transform in value['transformations']: transform.pop('rationale')
        projected.append(value)
    if (fingerprint(projected[0]) != fingerprint(projected[1])
            or fingerprint(projected[0]) != transition.get('operational_projection_digest')):
        raise ValueError('Representation operational transition mismatch')
    for key in ('transformation_rationale', 'rejected_alternatives'):
        if accepted[key] != released[key]:
            changes.append(dict(path='/'+key, accepted=accepted[key], released=released[key]))
    for collection, field in (('basket_members','selection_rationale'),('transformations','rationale')):
        for index,(a,b) in enumerate(zip(accepted[collection],released[collection],strict=True)):
            if a[field] != b[field]:
                changes.append(dict(path=f'/{collection}/{index}/{field}',accepted=a[field],released=b[field]))
    if changes != transition.get('narrative_changes'):
        raise ValueError('Representation narrative evidence is incomplete')
    binding = prepare_released_execution_contract(template,
        expected_template_digest=request['expected_template_digest'],
        expected_method_digest=request['expected_method_digest'], source_commit=assignment['base_ref'],
        dataset_bindings=request['native_dataset_bindings'],
        validation_requirements=request['validation_requirements'], approved_window=request['approved_window'],
        expected_plans=request['expected_plans'])
    runtime_assignment = deepcopy(assignment)
    runtime_assignment['representation_plan'] = deepcopy(released)
    card = draft_research_card(runtime_assignment, repository_root=str(root))
    HypothesisContract.from_dict(binding['contract'])
    approval = assignment.get('card_approval')
    if approval is None:
        return dict(disposition='released_execution_card_prepared', hypothesis_card=card,
                    card_digest=fingerprint(card), prepared_binding=binding,
                    representation_transition=deepcopy(transition), execution_authority=False)
    if (approval.get('card_digest') != fingerprint(card)
            or not isinstance(approval.get('actor'), str) or not approval['actor'].strip()
            or not isinstance(approval.get('approved_at'), str)):
        raise ValueError('Exact prospective card approval is required')
    instant(approval['approved_at'])
    card = confirm_card(card, actor=approval['actor'], confirmed_at=approval['approved_at'])
    qualification = qualify_card(card, repository_root=str(root))
    qualification['artifact_bundle']['engine_hypothesis_yaml'] = binding['contract']
    qualification['artifact_bundle']['released_execution_binding'] = binding['lineage']
    qualification['artifact_bundle']['representation_transition'] = deepcopy(transition)
    qualification['qualification_scope'] = 'released_execution_compilation_only'
    qualification['authority'] = dict(capital=False, orders=False, promotion=False, self_approval=False)
    return dict(disposition='released_execution_prepared', qualification=qualification,
                representation_plan=deepcopy(released), execution_authority=False)
