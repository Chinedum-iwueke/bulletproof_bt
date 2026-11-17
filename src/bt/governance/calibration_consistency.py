"""Outcome-blind structural checks; declared DGP prose is not executable proof."""
import math


def experiment_errors(experiment):
    errors = []
    if not isinstance(experiment, dict) or set(experiment) != {
            'variants', 'families', 'partitions', 'purpose', 'inner_replicates', 'statistic_id'}:
        return ['experiment must declare variants, families, partitions, purpose and procedure']
    for name in ('variants', 'families', 'partitions'):
        rows = experiment[name]
        if (not isinstance(rows, list) or not 1 <= len(rows) <= 64
                or any(not isinstance(row, dict) or not isinstance(row.get('id'), str)
                       or not 1 <= len(row['id']) <= 80 for row in rows)
                or len({row['id'] for row in rows}) != len(rows)):
            errors.append(name + ' requires bounded unique identifiers')
    if errors:
        return errors
    variants = {v['id'] for v in experiment['variants']}
    if any(set(v) != {'id', 'event_probability'} or
           (v.get('event_probability') is not None and
            (type(v['event_probability']) not in (int, float) or
             not math.isfinite(v['event_probability']) or not 0 <= v['event_probability'] <= 1))
           for v in experiment['variants']):
        errors.append('variants require identifiers and explicit nominal event probability or null')
    if any(set(p) != {'id', 'rows'} or type(p.get('rows')) is not int
           or not 1 <= p['rows'] <= 10000000 for p in experiment['partitions']):
        errors.append('partitions require positive bounded row counts')
    for family in experiment['families']:
        members = family.get('variant_ids')
        if (set(family) != {'id', 'variant_ids'} or not isinstance(members, list)
                or not members or any(not isinstance(v, str) or v not in variants for v in members)
                or len(set(members)) != len(members)):
            errors.append('family membership must reference unique registered variants')
    if experiment['purpose'] not in ('scientific', 'diagnostic'):
        errors.append('explicit scientific or diagnostic purpose required')
    if type(experiment['inner_replicates']) is not int or not 1 <= experiment['inner_replicates'] <= 10000:
        errors.append('invalid registered replicate count')
    if not isinstance(experiment['statistic_id'], str) or not 1 <= len(experiment['statistic_id']) <= 200:
        errors.append('explicit statistic identifier required')
    return errors


def assess_consistency(experiment, design, calibration):
    """Return actionable findings, never invent experiment facts from prose."""
    errors = experiment_errors(experiment)
    if errors:
        return [{'code': 'invalid_frozen_experiment', 'path': 'experiment',
                 'message': message, 'owner': 'experiment_producer', 'disposition': 'requires_new_contract'}
                for message in errors]
    findings = []

    def fail(code, path, message):
        findings.append(dict(code=code, path=path, message=message,
                             owner='method_designer', disposition='repair'))

    if not isinstance(design, dict) or set(design) != {'purpose', 'statistic_id', 'scenarios'}:
        fail('missing_design', 'calibration_design', 'Declare purpose, statistic_id and complete scenarios.')
        return findings
    for field in ('purpose', 'statistic_id'):
        if design[field] != experiment[field]:
            fail('procedure_mismatch', field, 'Calibration must bind the frozen experiment '+field+'.')
    if calibration['inner_replicates'] != experiment['inner_replicates']:
        fail('replicate_mismatch', 'calibration.inner_replicates',
             'Use the registered replicate count; a cheaper diagnostic needs a separate frozen contract.')
    scenarios = design['scenarios']
    if (not isinstance(scenarios, list) or not 1 <= len(scenarios) <= 16
            or any(not isinstance(s, dict) or not isinstance(s.get('id'), str) for s in scenarios)):
        fail('invalid_scenarios', 'scenarios', 'Declare bounded, identified DGP scenarios.')
        return findings
    if (len({s['id'] for s in scenarios}) != len(scenarios)
            or {s['id'] for s in scenarios} != set(calibration['dgp_ids'])):
        fail('scenario_mismatch', 'scenarios', 'Define each registered DGP exactly once.')
    variants = {v['id'] for v in experiment['variants']}
    probabilities = {v['id']: v['event_probability'] for v in experiment['variants']}
    partitions = {p['id']: p['rows'] for p in experiment['partitions']}
    families = {f['id']: set(f['variant_ids']) for f in experiment['families']}
    for scenario in scenarios:
        path = 'scenarios.'+scenario['id']
        if set(scenario) != {'id', 'families', 'cells', 'joint_generation', 'null_or_effect',
                             'estimator_application', 'seed_schedule', 'outputs',
                             'event_rate_mode', 'event_rate_assumption'}:
            fail('incomplete_scenario', path, 'Supply families, cells, joint law, parameterization, estimator, seeds and outputs.')
            continue
        for field in ('joint_generation', 'null_or_effect', 'estimator_application', 'seed_schedule', 'outputs', 'event_rate_assumption'):
            if not isinstance(scenario[field], str) or not 20 <= len(scenario[field].strip()) <= 4000:
                fail('missing_generation_rule', path+'.'+field, 'Specify a bounded reproducible rule, not just a scenario label.')
        if scenario['event_rate_mode'] not in ('registered', 'sensitivity'):
            fail('event_rate_mode', path, 'Declare registered event rates or an explicit sensitivity assumption.')
        actual = scenario['families']
        if (not isinstance(actual, list) or len(actual) != len(families)
                or any(not isinstance(f, dict) or set(f) != {'id', 'variant_ids'}
                       or not isinstance(f['id'], str) or not isinstance(f['variant_ids'], list)
                       or any(not isinstance(v, str) for v in f['variant_ids']) for f in actual)):
            fail('family_mismatch', path+'.families', 'Bind every registered family and its variants jointly.')
        elif (len({f['id'] for f in actual}) != len(actual)
              or {f['id']: set(f['variant_ids']) for f in actual} != families
              or any(len(set(f['variant_ids'])) != len(f['variant_ids']) for f in actual)):
            fail('family_mismatch', path+'.families', 'Calibration family membership differs from the frozen experiment.')
        cells = scenario['cells']
        expected = {(p, v) for p in partitions for v in variants}
        seen = set()
        if not isinstance(cells, list) or len(cells) > 4096:
            fail('invalid_cells', path+'.cells', 'Declare bounded variant-by-partition event assumptions.')
            continue
        for cell in cells:
            if (not isinstance(cell, dict) or set(cell) != {'partition_id', 'variant_id', 'event_probability',
                    'expected_events', 'sampling_rule'} or not isinstance(cell['partition_id'], str)
                    or not isinstance(cell['variant_id'], str)):
                fail('invalid_cell', path+'.cells', 'Use exact identified event-assumption cells.')
                continue
            key = (cell['partition_id'], cell['variant_id'])
            if key not in expected or key in seen:
                fail('cell_binding', path+'.cells', 'Each registered variant/partition must occur exactly once.')
                continue
            seen.add(key)
            p, count = cell['event_probability'], cell['expected_events']
            nominal = probabilities[key[1]]
            if scenario['event_rate_mode'] == 'registered' and nominal is not None and p != nominal:
                fail('event_rate_mismatch', path+'.cells.'+'.'.join(key),
                     'Use this variant\'s registered nominal event probability or explicitly declare a sensitivity scenario.')
            no_events = nominal is None and p is None and count is None
            if not no_events and (type(p) not in (int, float) or not math.isfinite(p) or not 0 <= p <= 1
                    or type(count) not in (int, float) or not math.isfinite(count)
                    or not math.isclose(count, partitions[key[0]]*p, rel_tol=1e-9, abs_tol=1e-9)):
                fail('event_arithmetic', path+'.cells.'+'.'.join(key),
                     'Expected events must equal registered partition rows times declared event probability; this is not observed support.')
            if not isinstance(cell['sampling_rule'], str) or not 20 <= len(cell['sampling_rule'].strip()) <= 2000:
                fail('event_generation_missing', path+'.cells', 'State event generation and any conditioning explicitly.')
        if seen != expected:
            fail('missing_cells', path+'.cells', 'Cover every registered variant and partition, including distinct event assumptions.')
    return findings
