# Execution Reconciliation Contract (Phase 3)

## Scope in Phase 3
Phase 3 reconciliation is an internal control-plane safety layer. It compares runtime local truth against adapter-reported truth and persisted state snapshots during simulated execution.

## Compared sources in simulated mode
- Local runtime open-order ledger (execution router lifecycle state).
- Adapter-reported open and completed orders.
- Local fill history vs adapter-reported recent fills.
- Local position snapshot vs adapter-reported positions snapshot.
- Local balance snapshot vs adapter-reported balance snapshot.

## Mismatch policy styles
- `log_only`: record differences only.
- `warn`: record with warning disposition.
- `freeze_on_material`: freeze trading loop when material mismatches are found.
- `auto_accept_simulated_safe_difference`: narrow simulated-only acceptance for non-material safe order-presence drift.

## Material mismatch in Phase 3
A mismatch is material when it violates configured tolerances:
- fill aggregate quantity drift beyond `material_fill_qty_tolerance`
- position quantity drift beyond `material_position_qty_tolerance`
- balance quantity drift beyond `material_balance_tolerance`
- missing/extra fill identities between local and adapter histories

## Guarantees in this phase
- Deterministic reconciliation tick cadence.
- Structured reconciliation artifacts in `reconciliation.jsonl`.
- Explicit policy-to-disposition handling.
- Local order lifecycle state transitions captured for reconciliation.

## Not guaranteed yet (pre-broker transport)
- Venue-native order/execution reconciliation fidelity.
- Exchange sequencing guarantees, transport retry semantics, or websocket gap recovery.
- Full cancel/amend lifecycle parity.

## Why this prepares Phase 4 broker integration
Phase 3 establishes stable reconciliation boundaries and typed artifacts so broker-specific adapters map to an existing control plane rather than inventing broker reconciliation semantics ad hoc.
