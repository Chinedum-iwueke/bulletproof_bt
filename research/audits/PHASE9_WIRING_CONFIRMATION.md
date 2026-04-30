# PHASE9 Wiring Confirmation

## Snapshot
- Hypotheses scanned: 32
- Unique strategies referenced: 20
- Already strategy-level compliant (explicit decision trace in strategy file): 0
- Patched strategies in this change: 0
- Remaining non-compliant at strategy-file level: 32 hypothesis mappings (31 mapped strategies rely on engine fallback, 1 hypothesis missing explicit strategy key).

## State feature capture path
- Entry state snapshots are attached at engine level in `Engine._enrich_signal_metadata` using `self._state_layer.snapshot(symbol=...)`, then copied as `entry_state_*` metadata keys when absent.
- This preserves no-lookahead semantics because state is read from the current causal online state layer at signal time.

## Decision trace capture path
- If a strategy does not provide `metadata["decision_trace"]`, engine fallback injects a default decision trace payload (`reason_code`, setup placeholders, maps, gate placeholders, parameter placeholder).
- Trade logging flattens the decision trace via `flatten_decision_trace` into `entry_decision_*` and `entry_gate_*` fields.

## Dataset preservation
- Trade logging passes through enriched metadata keys with prefixes:
  - `identity_*`, `entry_state_*`, `entry_gate_*`, `entry_decision_*`, `execution_*`, `risk_*`, `path_*`, `exit_*`, `counterfactual_*`, `label_*`.
- `trade_enrichment` adds path/counterfactual/label derived columns while preserving pre-existing metadata fields.

## Structural outputs
- `ev_by_bucket.py` consumes state and setup fields for CSI/vol/liquidity/displacement/setup buckets and emits missing-field diagnostics when unavailable.
- `post_run_analysis.py` can consume enriched datasets and emit structural bucket summaries when the required state/decision columns are present.

## Audit artifacts generated
- `research/audits/hypothesis_strategy_logging_audit.csv`
- `research/audits/hypothesis_strategy_logging_audit.md`
- `research/audits/hypothesis_strategy_logging_audit.json`

## Pre-tmux validation command
```bash
python orchestrator/validate_hypothesis_logging.py \
  --hypotheses-dir research/hypotheses \
  --output-dir research/audits \
  --strict
```

## Safety verdict
- **Not yet safe to claim full strict research-grade strategy-level compliance**: strict mode currently fails because strategies mostly rely on engine fallback instead of explicit per-strategy decision trace maps.
- **Safe for additive backward-compatible execution** with engine-level fallback and state snapshot wiring already active.
