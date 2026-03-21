# Bybit Adapter Contract (Phase 4, Read-Only)

## Scope
Phase 4 introduces a **read-only** Bybit adapter for transport/auth/config/mapping correctness.

Supported now:
- REST auth/signing for private V5 endpoints.
- Read-only truth surfaces: balances, positions, open orders, completed/recent orders, recent executions/fills.
- Thin public/private websocket client lifecycle and health reporting.
- Instrument metadata cache lookup.
- Doctor CLI diagnostics for config + connectivity + truth-surface fetches.

Not supported yet:
- submit/cancel/amend mutation calls.
- live order routing.
- runtime-driven private websocket event processing for execution state transitions.

## Environment & Config
Use `broker.venue: bybit` and `broker.environment: demo|live`.
Default endpoint selection is environment-specific and can be overridden with `broker.endpoints.*`.
Auth is resolved from environment variables named by:
- `broker.auth.api_key_env`
- `broker.auth.api_secret_env`

## Canonical Surfaces
The adapter maps Bybit payloads into existing canonical models:
- `Order`
- `Fill`
- `Position`
- `BalanceSnapshot`

No raw Bybit payloads leave adapter boundaries in these surfaces.

## Doctor Workflow
Run:
- `python -m bt.exec.cli.doctor --config configs/exec/bybit_demo.yaml --override configs/exec.yaml`

Optional WS checks:
- add `--check-ws`

Doctor outputs a structured summary with pass/fail checks, health status, and fetched object counts.

## Phase-5 Preparation
This phase establishes:
- authenticated transport
- endpoint/config discipline
- payload normalization seams
- adapter health visibility

Phase 5 can build execution mutations on top of this without rewriting the adapter foundation.
