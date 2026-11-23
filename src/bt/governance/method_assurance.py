"""Bounded definition resolution and dimensioned work accounting, never execution."""
import math
import re

from bt.governance.data_requirements import fingerprint


def assess_assurance(assurance, evidence):
    findings = []

    def fail(code, path, message):
        findings.append(dict(code=code, path=path, message=message,
                             owner='method_designer', disposition=(
                                 'await_authority_or_capacity' if code == 'work_cap_exceeded' else 'repair')))

    if not isinstance(assurance, dict) or set(assurance) != {'definitions', 'required_outputs', 'workload'}:
        fail('missing_assurance', 'assurance', 'Declare definitions, required_outputs and workload.')
        return findings, {}
    definitions = assurance['definitions']
    if not isinstance(definitions, list) or not 1 <= len(definitions) <= 128:
        fail('invalid_definitions', 'assurance.definitions', 'Supply 1..128 explicit definitions.')
        return findings, {}
    nodes = {}
    for i, node in enumerate(definitions):
        path = f'assurance.definitions.{i}'
        if (not isinstance(node, dict) or set(node) != {'id', 'kind', 'dependencies', 'statement', 'evidence_digest', 'pointer'}
                or not isinstance(node['id'], str) or not re.fullmatch('[a-zA-Z][a-zA-Z0-9_]{0,79}', node['id'])
                or node['kind'] not in ('primitive', 'derived')):
            fail('invalid_definition', path, 'Use an identified primitive or derived definition and exact evidence reference.')
            continue
        if node['id'] in nodes:
            fail('duplicate_definition', path, 'A symbol must have one authoritative definition.')
            continue
        nodes[node['id']] = node
        deps = node['dependencies']
        if (not isinstance(deps, list) or len(deps) > 128
                or any(not isinstance(d, str) for d in deps)
                or len(set(deps)) != len(deps)
                or (node['kind'] == 'primitive' and deps)
                or (node['kind'] == 'derived' and not deps)):
            fail('invalid_dependencies', path, 'Primitives have no dependencies; derived definitions require unique dependencies.')
        digest, pointer = node['evidence_digest'], node['pointer']
        if node['statement'] is not None:
            if (not isinstance(node['statement'], str) or not 1 <= len(node['statement'].strip()) <= 4000
                    or digest is not None or pointer is not None):
                fail('ambiguous_definition', path, 'Supply one inline definition OR one evidence reference, never both.')
            continue
        if not isinstance(digest, str) or digest not in evidence or fingerprint(evidence[digest]) != digest:
            fail('unresolved_definition', path, 'Definition evidence must exist with its exact content digest.')
            continue
        if (not isinstance(pointer, str) or not pointer.startswith('/') or len(pointer) > 1000
                or re.search(r'~(?![01])', pointer)):
            fail('invalid_pointer', path, 'Use an explicit RFC 6901 JSON pointer to a definition statement.')
            continue
        value = evidence[digest]
        try:
            for part in pointer[1:].split('/'):
                key = part.replace('~1', '/').replace('~0', '~')
                if isinstance(value, list):
                    if not re.fullmatch('0|[1-9][0-9]*', key):
                        raise ValueError('invalid array index')
                    value = value[int(key)]
                else:
                    value = value[key]
            if not isinstance(value, str) or not 1 <= len(value.strip()) <= 4000:
                raise ValueError('not a bounded definition statement')
        except (KeyError, IndexError, TypeError, ValueError):
            fail('unresolved_definition', path, 'Reference must resolve to a nonempty bounded definition statement.')
    roots = assurance['required_outputs']
    if (not isinstance(roots, list) or not 1 <= len(roots) <= 128
            or any(not isinstance(r, str) or r not in nodes for r in roots)
            or len(set(roots)) != len(roots)):
        fail('missing_output_definition', 'assurance.required_outputs', 'Every required output must resolve to a unique definition.')
    visiting, done = set(), set()

    def visit(name):
        if name in visiting:
            fail('definition_cycle', 'assurance.definitions.' + name,
                 'Unroll lagged state into initialized inputs; unresolved dependency cycles are not executable definitions.')
            return
        if name in done:
            return
        visiting.add(name)
        deps = nodes[name]['dependencies']
        for dep in deps if isinstance(deps, list) else []:
            if not isinstance(dep, str) or dep not in nodes:
                fail('missing_dependency', 'assurance.definitions.' + name, 'Every dependency must have a declared definition.')
            else:
                visit(dep)
        visiting.remove(name)
        done.add(name)

    for name in nodes:
        visit(name)
    totals = assess_workload(assurance['workload'], fail)
    return findings, totals


def assess_workload(workload, fail):
    """Sum unlike units separately; factors are declared upper bounds, not observations."""
    phases = {'generation', 'fit', 'solve', 'verification', 'bootstrap'}
    if not isinstance(workload, dict) or set(workload) != {'caps', 'stages'}:
        fail('missing_workload', 'assurance.workload', 'Declare unit caps and all workload stages.')
        return {}
    caps, stages = workload['caps'], workload['stages']
    if (not isinstance(caps, dict) or not 1 <= len(caps) <= 16
            or any(not isinstance(k, str) or not re.fullmatch('[a-z][a-z0-9_]{0,79}', k)
                   or type(v) is not int or not 1 <= v <= 10**15 for k, v in caps.items())):
        fail('invalid_work_caps', 'assurance.workload.caps', 'Use explicit positive integer caps per named unit.')
        return {}
    if not isinstance(stages, list) or not 1 <= len(stages) <= 128:
        fail('invalid_work_stages', 'assurance.workload.stages', 'Declare a bounded workload ledger.')
        return {}
    totals, seen, covered = dict.fromkeys(caps, 0), set(), set()
    for i, stage in enumerate(stages):
        path = f'assurance.workload.stages.{i}'
        if (not isinstance(stage, dict) or set(stage) != {'id', 'phase', 'unit', 'factors', 'not_applicable_reason'}
                or not isinstance(stage['id'], str) or not 1 <= len(stage['id']) <= 80
                or not isinstance(stage['phase'], str) or stage['phase'] not in phases):
            fail('invalid_work_stage', path, 'Identify each stage, phase, unit and multiplicative upper-bound factors.')
            continue
        if stage['id'] in seen:
            fail('duplicate_work_stage', path, 'Work stage identifiers must be unique.')
            continue
        seen.add(stage['id'])
        covered.add(stage['phase'])
        reason, factors, unit = stage['not_applicable_reason'], stage['factors'], stage['unit']
        if reason is not None:
            if (not isinstance(reason, str) or not 20 <= len(reason.strip()) <= 2000
                    or factors != {} or unit is not None):
                fail('invalid_work_exemption', path, 'Omitted work needs an explicit reason and no unit or factors.')
            continue
        if (not isinstance(unit, str) or unit not in caps or not isinstance(factors, dict)
                or not 1 <= len(factors) <= 16
                or any(not isinstance(k, str) or not 1 <= len(k) <= 80 or type(v) is not int
                       or not 1 <= v <= 10**9 for k, v in factors.items())):
            fail('invalid_work_factors', path, 'Every counted stage requires a capped unit and positive bounded integer factors.')
            continue
        totals[unit] += math.prod(factors.values())
    for phase in sorted(phases - covered):
        fail('missing_work_phase', 'assurance.workload', f'Account for {phase} or explicitly justify its absence.')
    for unit, total in totals.items():
        if total > caps[unit]:
            fail('work_cap_exceeded', 'assurance.workload.caps.' + unit,
                 f'Declared upper-bound work {total} exceeds cap {caps[unit]}; do not reduce scientific requirements silently.')
    return totals
