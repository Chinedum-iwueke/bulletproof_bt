# Bulletproof_exec Phase 2 restart + recovery contract

## Persisted state in Phase 2

When `exec.persist_state: true`, runtime writes durable state into `state.path` SQLite:

- `session_state`: run/session status + liveness metadata.
- `order_lifecycle_events`: local order submit/ack/fill lifecycle records.
- `processed_events`: dedupe keys (e.g. broker fill ids) already applied.
- `checkpoints`: periodic/final runtime snapshots.
- `positions_snapshots`: latest known local position snapshots.
- `balance_snapshots`: latest known balance snapshots.

Runtime artifacts (`decisions.jsonl`, `orders.jsonl`, `fills.jsonl`, etc.) remain the execution audit stream. SQLite is the recovery boundary for restart safety.

## Restart policy semantics

### `restart_policy: fresh`

- Runtime starts a new session without loading prior checkpoint state.
- Prior SQLite/history is retained (no auto-destruction).

### `restart_policy: resume`

- Runtime inspects latest session for mode (`shadow` or `paper_simulated`).
- If a valid checkpoint exists, runtime restores:
  - last processed bar timestamp (bar skip on replay)
  - next client order sequence
  - processed event dedupe keys
  - latest order/position/balance checkpoint payloads
- If session exists but checkpoint missing/incomplete, runtime degrades to fresh with explicit recovery metadata.
- If checkpoint is corrupt, runtime degrades to fresh with explicit recovery metadata.

### `restart_policy: reconcile_only`

- True reconciliation engine is not available in Phase 2.
- Runtime explicitly degrades to conservative fresh-start behavior.

## Mode-specific resume meaning

### Shadow mode

- Decision path resumes with bar skip using checkpointed `last_bar_ts`.
- No order submission occurs, so resume safety centers on deterministic bar progression and liveness state.

### Paper simulated mode

- Runtime resumes bar progression from checkpointed `last_bar_ts`.
- Duplicate fill application is guarded via processed broker-event dedupe keys.
- Local portfolio effects are persisted in checkpoint snapshots for restart continuity.

## Deduplication guarantees (Phase 2)

- Fill dedupe is guaranteed only for events that carry stable dedupe keys and are persisted in `processed_events`.
- Simulated adapter lifecycle dedupe is deterministic within this Phase 2 sync runtime shape.

## Explicit non-guarantees (until later phases)

Phase 2 does **not** yet provide:

- real broker transport semantics,
- reconciliation-only replay correctness,
- cancel/amend lifecycle parity,
- cross-process distributed execution guarantees,
- full broker-local ledger convergence.

These are intentionally deferred to reconciliation and real-broker phases.
