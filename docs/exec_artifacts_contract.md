# Bulletproof_exec Run Artifacts Contract (Phase 0)

The execution runtime will emit a run folder that follows the existing Bulletproof_bt artifact philosophy: explicit, append-only JSONL streams plus compact run metadata.

## Required files

- `config_used.yaml`
  - Fully resolved execution config used for the run.

- `run_manifest.json`
  - Immutable run identity/metadata (run id, mode, timestamps, git/build fingerprints, key runtime settings).

- `run_status.json`
  - Mutable run lifecycle status (`starting`, `running`, `stopping`, `stopped`, `failed`) with last-update UTC timestamp and optional error summary.

- `decisions.jsonl`
  - Strategy decision records generated on closed-bar boundaries before broker submission.

- `orders.jsonl`
  - Local order lifecycle entries (intent accepted, submitted, acknowledged, amended, cancelled, rejected, filled terminal updates).

- `fills.jsonl`
  - Canonical execution fills with quantity/price/fee/slippage/accounting metadata.

- `positions_snapshots.jsonl`
  - Time-ordered position snapshots captured from local/broker reconciliation points.

- `balances_snapshots.jsonl`
  - Time-ordered account balance snapshots.

- `reconciliation.jsonl`
  - Reconciliation outcomes and divergences between local state and broker-observed state.

- `heartbeat.jsonl`
  - Runtime heartbeat/timer records for supervision and liveness analysis.

- `broker_events.jsonl`
  - Normalized broker events as received and mapped into canonical event envelopes.

## Schema expectations

For all JSON/JSONL records:

- include explicit UTC timestamps (`ts` or equivalent)
- keep records append-friendly and deterministic to parse
- include stable ids/dedupe keys where available (`run_id`, `order_id`, `broker_event_id`, `client_order_id`)
- preserve canonical field names aligned with core domain types (`symbol`, `side`, `qty`, `price`, etc.)

Phase 0 defines intent and naming only; concrete serializers/writers are deferred.
