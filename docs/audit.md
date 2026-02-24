# Stability Audit Harness

Enable with config:

```yaml
audit:
  enabled: true
  level: full      # basic|full
  max_events_per_file: 5000
  determinism_check: true
  required_layers: []
```

When enabled, the runner writes `run_dir/audit/` artifacts:

- `data_audit.json`
- `resample_audit.jsonl`
- `signal_audit.jsonl`
- `order_audit.jsonl`
- `fill_audit.jsonl`
- `position_audit.jsonl`
- `portfolio_audit.jsonl`
- `alignment_audit.jsonl`
- `determinism_report.json` (when determinism check enabled)
- `stability_report.json`
- `coverage.json`

`coverage.json` reports expected/required/executed/skipped layers, per-layer event counts,
per-layer violations, and an overall pass/fail status. Layers that emit only on violations
still register execution heartbeats so coverage is explicit even when violations are zero.

## Stability Gate CLI

After a run, evaluate audit stability with:

```bash
python -m bt.audit.gate --run-dir outputs/runs/<run_id> [--required L1,L2] [--strict|--no-strict] [--json]
```

Exit codes:

- `0`: pass (no violations and required coverage complete)
- `2`: violations found
- `3`: missing required layer execution
- `4`: `coverage.json` missing/unreadable (or fallback source missing)

Use `--no-strict` to fall back to `stability_report.json` when `coverage.json` is unavailable.

Overhead is near-zero when `audit.enabled: false` because hooks short-circuit before writing.
