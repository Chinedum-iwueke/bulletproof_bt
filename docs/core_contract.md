# Core Contract (T0 Guardrail)

This document defines the **core invariants** that must remain stable while adding FX/Traditional market support.

## Core invariants

- **No lookahead:** strategy/execution decisions may use only information available at the decision timestamp.
- **No interpolation:** missing bars stay missing; the engine must not synthesize bars to fill gaps.
- **Determinism:** same input data + same resolved config must produce the same outputs after canonical normalization:
  - floats rounded to fixed precision,
  - timestamps normalized to ISO-8601 UTC,
  - JSON compared with canonical key ordering.
- **Artifact contract:** each run directory must contain required Stage-F artifacts. Optional benchmark artifacts are present only when benchmark is enabled.
  - Required: `config_used.yaml`, `equity.csv`, `trades.csv`, `performance.json`, `fills.jsonl`, `decisions.jsonl`, `performance_by_bucket.csv`.
  - `run_status.json` may be produced by the orchestrating flow and is treated as conditionally required when enabled in that flow.

## Instrument-agnostic core surface

The following areas are core and must remain instrument-agnostic:

- Engine event loop and sequencing.
- Feed interface / iteration contract.
- Artifact writers and schema stability policy:
  - required artifact filenames stay stable,
  - field names/types for established artifacts remain stable for existing crypto behavior,
  - schema changes are additive/backward-compatible when possible,
  - breaking schema changes must be explicit and versioned.

## Where instrument differences belong

Instrument-specific behavior must be isolated outside the core loop, primarily in:

- Instrument specifications (contract size, lot size, tick rules).
- Execution pricing adapters.
- Fee/slippage adapters.
- Session/calendar rules.

This separation preserves crypto backtest behavior while enabling new market adapters.
