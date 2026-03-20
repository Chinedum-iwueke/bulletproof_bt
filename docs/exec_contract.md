# Bulletproof_exec Contract (Phase 0)

## Why `src/bt/exec` exists

`bt.exec` is the live/paper execution runtime boundary that lives beside the deterministic backtest engine in the same repository. Keeping execution under `src/bt/exec` preserves a single canonical trading domain model while allowing runtime-specific lifecycle, adapter, and durability contracts to evolve independently.

## Reuse from existing backtest code

Phase 0 reuses core domain types from `bt.core.types` directly:

- `Order`
- `Fill`
- `Position`

This enforces parity between backtest accounting semantics and execution-runtime event semantics.

## What remains backtest-owned

The following remain owned by backtest modules and are not moved or rewritten in this phase:

- bar-by-bar deterministic engine orchestration
- strategy computation internals
- risk policy internals
- portfolio accounting implementation

`bt.exec` only introduces contracts needed to integrate with those existing capabilities.

## Runtime contract roles

### Adapters

- `MarketDataAdapter`: start/stop, closed-bar subscription, event stream, health.
- `BrokerAdapter`: start/stop, event stream, order submit/cancel/amend, open orders/positions/balances/recent fills, health.

Adapters are intentionally broker-agnostic so the same contracts support:

- simulated execution adapter
- Bybit demo/live adapters
- future broker integrations

### Runtime events

Runtime-side events represent scheduler/control-plane signals:

- lifecycle (`startup`, `shutdown`)
- heartbeat/timer
- closed-bar trigger
- health/stale-data observations
- reconciliation tick trigger

### Broker events

Broker-side canonical events represent normalized trading/account activity:

- order acknowledged/rejected
- order partially filled/filled/cancelled
- position snapshot
- balance snapshot
- connection status

### State store

Execution state store is a durability interface for:

- order lifecycle records
- normalized broker event persistence
- checkpoints for restart/recovery
- processed event dedupe tracking
- query of open orders and latest position/balance snapshots

No concrete persistence backend is implemented in Phase 0.

### Reconciliation

Reconciliation is represented only as a runtime tick/event boundary in this phase. Logic implementation is deferred.

## Strategy parity rule

Bulletproof_exec strategy parity rule is strict:

- strategy decision evaluation happens on **closed bars only**
- runtime market data adapters must expose closed-bar semantics clearly

This rule keeps live/paper behavior aligned with backtest assumptions.
