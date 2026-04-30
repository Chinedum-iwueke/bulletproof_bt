from __future__ import annotations
import argparse, ast, csv, json
from pathlib import Path
from typing import Any
import yaml

STRATEGY_ROOT = Path('src/bt/strategy')


def load_strategy_registry() -> dict[str, Path]:
    out: dict[str, Path] = {}
    for py in STRATEGY_ROOT.glob('*.py'):
        text = py.read_text(encoding='utf-8')
        for m in __import__('re').finditer(r'@register_strategy\("([^"]+)"\)', text):
            out[m.group(1)] = py
    return out


def find_strategy_name(doc: dict[str, Any]) -> str | None:
    for key in ('strategy', 'strategy_name', 'strategy_id', 'registry_key'):
        if isinstance(doc.get(key), str):
            return doc[key]
    entry = doc.get('entry') or {}
    for key in ('strategy', 'strategy_name', 'strategy_id', 'registry_key'):
        if isinstance(entry.get(key), str):
            return entry[key]
    meta = doc.get('metadata') or {}
    for key in ('strategy', 'strategy_name', 'strategy_id', 'registry_key'):
        if isinstance(meta.get(key), str):
            return meta[key]
    return None


def audit_strategy_file(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {k: False for k in (
            'has_decision_trace','has_state_snapshot','has_reason_code','has_setup_class','has_conditions_bool_map','has_blockers_bool_map','has_gate_margins','has_parameter_trace','has_signal_metadata'
        )}
    t = path.read_text(encoding='utf-8')
    return {
        'has_decision_trace': ('decision_trace' in t),
        'has_state_snapshot': ('entry_state_' in t or 'state_snapshot' in t),
        'has_reason_code': ('reason_code' in t),
        'has_setup_class': ('setup_class' in t),
        'has_conditions_bool_map': ('conditions_bool_map' in t),
        'has_blockers_bool_map': ('blockers_bool_map' in t),
        'has_gate_margins': ('gate_margins' in t),
        'has_parameter_trace': ('parameter_combination' in t or 'parameter_set_id' in t),
        'has_signal_metadata': ('metadata=' in t),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--hypotheses-dir', default='research/hypotheses')
    ap.add_argument('--output-dir', default='research/audits')
    ap.add_argument('--strict', action='store_true')
    ap.add_argument('--smoke-run', action='store_true')
    ap.add_argument('--max-hypotheses', type=int)
    args = ap.parse_args()

    hyp_files = sorted(Path(args.hypotheses_dir).glob('*.yaml'))
    if args.max_hypotheses:
        hyp_files = hyp_files[:args.max_hypotheses]
    registry = load_strategy_registry()

    rows: list[dict[str, Any]] = []
    critical = 0
    for hyp in hyp_files:
        doc = yaml.safe_load(hyp.read_text(encoding='utf-8')) or {}
        sname = find_strategy_name(doc)
        sfile = registry.get(sname or '')
        flags = audit_strategy_file(sfile)
        missing: list[str] = []
        if not flags['has_decision_trace']:
            missing.append('strategy_entry_decision_trace_metadata')
        if not flags['has_reason_code']:
            missing.append('reason_code')
        if not flags['has_setup_class']:
            missing.append('setup_class')
        if not flags['has_conditions_bool_map']:
            missing.append('conditions_bool_map')
        if not flags['has_blockers_bool_map']:
            missing.append('blockers_bool_map')
        if not flags['has_gate_margins']:
            missing.append('gate_margins')
        if not flags['has_parameter_trace']:
            missing.append('parameter_combination')
        status = 'compliant' if not missing else ('partial_engine_fallback' if sfile else 'missing_strategy')
        if args.strict and missing and status != "missing_strategy":
            critical += 1
        rows.append({
            'hypothesis_id': doc.get('hypothesis_id'),
            'yaml_path': str(hyp),
            'strategy_name': sname,
            'strategy_file': str(sfile) if sfile else '',
            'status': status,
            **flags,
            'expected_structural_bucket_fields': 'entry_state_csi_pctile,entry_state_vol_pctile,entry_state_spread_proxy_pctile,entry_state_tr_over_atr,entry_decision_setup_class',
            'missing_requirements': ';'.join(missing),
            'patched': False,
            'notes': 'Engine-level metadata fallback may provide decision trace/state snapshot.'
        })
    outdir = Path(args.output_dir); outdir.mkdir(parents=True, exist_ok=True)
    csv_path = outdir / 'hypothesis_strategy_logging_audit.csv'
    md_path = outdir / 'hypothesis_strategy_logging_audit.md'
    js_path = outdir / 'hypothesis_strategy_logging_audit.json'
    if rows:
        with csv_path.open('w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
    js_path.write_text(json.dumps(rows, indent=2), encoding='utf-8')
    lines=["# Hypothesis Strategy Logging Audit","",f"Scanned hypotheses: {len(rows)}","", "| hypothesis_id | strategy_name | status | missing_requirements | strategy_file |","|---|---|---|---|---|"]
    for r in rows:
        lines.append(f"| {r['hypothesis_id']} | {r['strategy_name']} | {r['status']} | {r['missing_requirements'] or 'none'} | {r['strategy_file'] or 'N/A'} |")
    md_path.write_text('\n'.join(lines)+'\n', encoding='utf-8')
    if args.smoke_run:
        print('Smoke run is not implemented in this validator; use existing run pipeline for tiny experiment checks.')
    print(f"Wrote audit: {csv_path}, {md_path}, {js_path}")
    return 1 if (args.strict and critical>0) else 0


if __name__ == '__main__':
    raise SystemExit(main())
