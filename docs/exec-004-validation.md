# EXEC-004 Validation

## Ownership and boundary

`bt.institutional.oms` is the authoritative quantitative producer for deterministic
order-state and reconciliation evidence. Hermes may register its immutable schema and
verify exact receipts, but does not recreate OMS state or infer venue truth.

The producer grants no order, routing, allocation, promotion, or capital authority.
It is deliberately usable before EXEC-008 venue certification because its pilot is a
sealed no-network replay. Real venue submission remains disabled.

## Contract

- Client order IDs are deterministic from venue and idempotency identity, never a
  volatile process counter.
- Reusing an idempotency key with equivalent content suppresses the retry. Reusing it
  with changed economic content fails closed.
- Commands, lifecycle events, venue executions, journal state, and reconciliation
  decisions are digest bound.
- Lifecycle transitions are monotonic. Terminal orders cannot receive later events.
- Fill executions are exactly-once by venue execution ID; conflicting reuse fails.
- Partial-fill and cancel races preserve cumulative quantity and cannot overfill.
- An unknown submission result can only be cleared by a fresh independent venue
  snapshot matching the canonical client order ID.
- Unknown external orders, missing venue orders, stale snapshots, state/fill drift,
  and position or balance drift freeze further submission.
- Restart replay produces the same journal digest.

## Automated evidence

The focused suite covers duplicate commands and events, content conflicts, partial
fills, cancel races, unknown orders, overfills, terminal-state violations, disconnect
ambiguity, stale snapshots, position/balance breaks, restart replay, dependency
binding, and the no-authority receipt boundary.

```bash
PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_exec004_oms_reconciliation.py \
  tests/exec/test_idempotency_phase3.py \
  tests/exec/test_order_lifecycle_phase3.py \
  tests/exec/test_reconcile_engine_phase3.py \
  tests/exec/test_exec_restart_smoke_phase2.py \
  tests/exec/test_execution_router_phase5.py

.venv/bin/ruff check \
  src/bt/institutional/oms.py \
  scripts/exec004_pilot.py \
  tests/test_exec004_oms_reconciliation.py
```

## Rollback

Stop submissions, retain the append-only command/event journal and venue snapshot,
deactivate the Hermes schema, and reconcile independently before any recovery. Never
delete or rewrite an ambiguous command, event, fill, discrepancy, or receipt.
