# 1. Executive Summary

Bulletproof_bt already has a deterministic, event-driven bar engine with explicit domain types (`Signal`, `OrderIntent`, `Order`, `Fill`, `Trade`), strong risk/execution contracts, and a mature artifact/logging pipeline. The fastest, safest path for Bulletproof_exec is **same-repo placement** under a new `src/bt/exec/` package that reuses existing core contracts and strategy adapters rather than introducing a parallel architecture.

The primary blockers before live/paper execution are not indicators/strategy logic—they are missing live lifecycle abstractions: durable order state machine, broker adapter interface, reconciliation loop, and restart-safe state persistence. Those should be added as thin layers around existing `core/types`, `strategy`, `risk`, `portfolio`, `logging`, and config-resolution conventions.

Recommendation in one line: build `bt.exec` as an event-runtime + broker adapter stack that consumes existing strategy/risk/order contracts, while extracting only minimal shared primitives where current code is backtest-loop-coupled.

# 2. Relevant Existing Repo Components

## Top-level and package layout

- Top-level architecture is already segmented cleanly: `configs/`, `docs/`, `scripts/`, `src/bt/*`, `tests/`.
- Runtime domains already present in `src/bt/`: `core`, `data`, `execution`, `portfolio`, `risk`, `strategy`, `logging`, `audit`, `experiments`, `metrics`, `benchmark`, `instruments`.

## Strategy architecture

- Strategy contract is explicit and simple: `Strategy.on_bars(ts, bars_by_symbol, tradeable, ctx) -> list[Signal]` (`src/bt/strategy/base.py`).
- Strategy context is intentionally read-only via `StrategyContextView` (`src/bt/strategy/context_view.py`) and adapters in `src/bt/strategy/htf_context.py`.
- Strategy instantiation uses registry + filtered constructor kwargs (`src/bt/strategy/__init__.py`).
- Parity-critical assumptions today:
  - strategies see **closed bars only**, per timestamp
  - `bars_by_symbol` may be sparse (gap-preserving)
  - `tradeable` set supplied by universe layer
  - `ctx` contains indicator snapshots / position context and must remain immutable.

## Engine/event architecture

- Core engine (`BacktestEngine`) is event-driven, bar-by-bar loop (`src/bt/core/engine.py`), not vectorized.
- Data ingress via feed abstraction (`HistoricalDataFeed`/`StreamingHistoricalDataFeed`) with `next()` returning current-bar bundle (`src/bt/data/feed.py`, `src/bt/data/stream_feed.py`, `src/bt/data/load_feed.py`).
- Decision flow in loop:
  1) feed bars
  2) universe update
  3) indicator update
  4) strategy emits `Signal`
  5) risk converts to `OrderIntent`
  6) engine materializes `Order`
  7) execution model returns fills
  8) portfolio applies fills and marks-to-market
  9) logs artifacts.
- Domain objects already present in `src/bt/core/types.py`: `Bar`, `Signal`, `OrderIntent`, `Order`, `Fill`, `Position`, `Trade`.

## Execution simulation/broker-like abstractions

- `ExecutionModel.process(ts, bars_by_symbol, open_orders)` processes open orders and returns `(updated_orders, fills)` (`src/bt/execution/execution_model.py`).
- Current support is market-order only; non-market raises `NotImplementedError`.
- Deterministic fill pipeline exists: intrabar -> spread -> slippage -> fee/commission.
- This is reusable for paper mode parity simulations; however, it is **bar-sim-centric**, not broker-event-centric.

## Portfolio/risk/sizing

- `RiskEngine.signal_to_order_intent` holds core approval/sizing rules; includes stop contract resolution, margin checks, and reject codes (`src/bt/risk/risk_engine.py`, `src/bt/risk/reject_codes.py`).
- `Portfolio` + `PositionBook` provide deterministic accounting and trade lifecycle tracking (`src/bt/portfolio/portfolio.py`, `src/bt/portfolio/position.py`).
- Rich risk metadata is propagated into trades and outputs.
- Reusable but currently assumes bar-scheduled mark-to-market and immediate local fill application.

## Config system

- Layering and deep-merge are mature (`src/bt/config.py`, `docs/config_layering_contract.md`).
- Canonicalization and guardrails are centralized in `resolve_config` (`src/bt/core/config_resolver.py`).
- Convention style:
  - YAML configs in `configs/`
  - nested namespaced keys (`data.*`, `execution.*`, `risk.*`, `outputs.*`, `audit.*`)
  - resolved config snapshot saved to run folder (`config_used.yaml`).

## Logging/output/run folders

- Run artifacts are formalized by contract (`docs/output_artifacts_contract.md`, `src/bt/logging/run_contract.py`).
- Structured outputs include JSONL decisions/fills, CSV trades/equity, JSON performance/cost/run status/manifests.
- Writers are deterministic and flush frequently (`src/bt/logging/jsonl.py`, `src/bt/logging/trades.py`, `src/bt/logging/run_manifest.py`).
- Audit subsystem already provides layer coverage/violation events (`src/bt/audit/audit_manager.py`).

## CLI/orchestration patterns

- CLI entrypoints are thin wrappers in `scripts/` (`run_backtest.py`, `run_experiment_grid.py`, etc.).
- Orchestration and core run logic live in `src/bt/api.py` and `src/bt/experiments/grid_runner.py`.
- Pattern to preserve: small CLI -> library call -> post-run artifact validation/writing.

## Testing/quality conventions

- Extensive pytest suite at `tests/`, including contracts, regression determinism, run artifacts, stop semantics, strategy behavior.
- `pytest.ini` defines `integration` and `smoke` markers.
- Tooling expectations in `pyproject.toml`: `pytest`, `ruff`, `mypy` as dev deps.
- Quality philosophy inferred: contract-first, deterministic outputs, additive schema evolution.

# 3. Reusable Components

High-confidence reusable for Bulletproof_exec:

1. **Core domain models**: `Bar`, `Signal`, `OrderIntent`, `Order`, `Fill`, `Trade` (`src/bt/core/types.py`).
2. **Strategy contract + adapters**: base strategy interface, read-only context, HTF and signal-conflict adapters (`src/bt/strategy/*`).
3. **Risk contract and sizing logic**: stop resolution, reject codes, instrument-aware sizing (`src/bt/risk/*`).
4. **Portfolio accounting primitives**: position/trade bookkeeping and PnL attribution (`src/bt/portfolio/*`).
5. **Instrument + execution profile resolution**: instrument registry/spec and execution profile resolver (`src/bt/instruments/*`, `src/bt/execution/profile.py`).
6. **Config resolution pipeline**: merge + normalize + snapshot approach (`src/bt/config.py`, `src/bt/core/config_resolver.py`).
7. **Logging and artifact formatting utilities**: JSON deterministic writers, JSONL canonicalization, run manifests, cost breakdown style (`src/bt/logging/*`).
8. **Audit layer scaffolding**: event recording, coverage summaries (`src/bt/audit/audit_manager.py`).

# 4. Components That Need Refactor Before Exec

1. **Engine loop disentanglement (high priority)**
   - `BacktestEngine` currently owns many responsibilities (signal pipeline, order staging, fill handling, forced liquidation, artifact writes) in one monolithic loop (`src/bt/core/engine.py`).
   - Refactor need: extract runtime-agnostic orchestration units (decision step, order normalization, fill application, post-fill invariants) into reusable services.

2. **Order lifecycle model extension**
   - Existing `OrderState`/`Order` are sufficient for backtest but do not yet encode broker IDs, partial fills, cancel/replace lifecycle, ack/reject transitions.
   - Need additive metadata/state conventions (without breaking backtest).

3. **Persistent state/restart boundary**
   - No canonical event journal/state checkpoint module exists today for restart-safe live runtime.
   - Need durable persistence around open orders, positions, last processed broker/data sequence, and idempotency keys.

4. **Execution abstraction split**
   - `ExecutionModel` is simulation-centric and bar-triggered (`src/bt/execution/execution_model.py`).
   - Need interface split: simulation execution adapter vs broker execution adapter under a common contract.

5. **Clock/runtime abstraction**
   - There is a `core/clock.py`, but current backtest flow is feed-driven loop; live runtime needs explicit scheduler + event multiplexer (market data, broker updates, reconciliation ticks).

6. **Run artifact contract for live mode**
   - Existing output contract is strong for backtests; live exec needs additive artifacts (orders ledger, broker events ledger, reconciliation reports, restart snapshots, heartbeat/status timeline).

# 5. Proposed Shared-Core Boundary

Recommended boundary (minimally invasive):

## Keep backtest-owned (`bt` backtest runtime only)

- `src/bt/core/engine.py` (backtest loop orchestration)
- backtest experiment runners (`src/bt/experiments/*`)
- benchmark-only modules (`src/bt/benchmark/*`)
- simulation-specific fill timing internals that assume bar delays.

## Shared/core (usable by bt + exec)

- `src/bt/core/types.py`
- selected enums/reason codes (`src/bt/core/enums.py`, `reason_codes.py` as applicable)
- strategy base + adapters (`src/bt/strategy/base.py`, `context_view.py`, conflict adapter semantics)
- risk and sizing contracts (`src/bt/risk/*`)
- portfolio/position accounting (`src/bt/portfolio/*`) with minimal extension points
- instrument specs/registry (`src/bt/instruments/*`)
- logging formatters and serialization utilities (`src/bt/logging/formatting.py`, `jsonl.py` patterns)
- config merge + resolver conventions (`src/bt/config.py`, `src/bt/core/config_resolver.py`).

## New shared abstractions needed

- `ExecutionAdapter` interface (simulate vs broker)
- `OrderStore` / `ExecutionStateStore` for durable runtime state
- `BrokerEvent` canonical schema
- `ReconciliationEngine` contract
- `RuntimeEvent` envelope for market/broker/timer events.

# 6. Proposed Bulletproof_exec Placement

**Recommendation: same repository, under `src/bt/exec/`.**

Why this fits current repo reality:

- Existing code is already a modular package (`src/bt/...`) with strong test/contracts and shared domain types.
- Parity goal is highest when strategy/risk/portfolio/config/logging reuse is direct import-level reuse.
- Splitting into separate repo/package now would create immediate duplication risk in contracts and schema versions.
- `scripts/` + `src/bt/api.py` patterns support adding new exec entrypoints without restructuring existing backtest flows.

Proposed immediate namespace:
- `src/bt/exec/` for runtime, broker adapters, reconciliation, persistence, config parsing specific to exec mode.

# 7. Proposed Folder Tree

```text
src/bt/
  exec/
    __init__.py
    runtime.py                # event loop orchestration (market/broker/timer)
    events.py                 # RuntimeEvent, BrokerEvent envelopes
    clocking.py               # live scheduler/timers (wrapping core clock where possible)
    state_store.py            # restart-safe order/position/runtime checkpoints
    reconcile.py              # broker vs local reconciliation logic
    order_lifecycle.py        # submit/cancel/replace/partial-fill lifecycle handling
    adapters/
      __init__.py
      base.py                 # ExecutionAdapter/BrokerAdapter contracts
      simulated.py            # wraps existing execution model for paper parity mode
      bybit.py                # bybit demo/live adapter
    logging/
      __init__.py
      exec_artifacts.py       # exec artifact writers (orders/reconcile/heartbeats)
    cli/
      __init__.py
      run_exec.py             # cli entrypoint consistent with scripts/ style
```

No move of existing backtest modules in Phase 1; only additive package + surgical extraction where needed.

# 8. Proposed Config Layout

Preserve current layered philosophy and naming style:

- Base: `configs/engine.yaml` remains backtest default.
- Add exec base: `configs/exec.yaml`.
- Add broker-specific overlays:
  - `configs/exec/bybit_demo.yaml`
  - `configs/exec/bybit_live.yaml`
- Keep same merge order semantics (base -> fees/slippage where applicable -> overrides -> resolve).

Proposed new namespaces (additive):

```yaml
exec:
  mode: paper | live
  state_dir: outputs/exec_state
  heartbeat_seconds: 5
  reconcile_interval_seconds: 30
  restart_policy: resume

broker:
  venue: bybit
  account: demo | live
  testnet: true
  symbols: [BTCUSDT]
  recv_window_ms: 5000

routing:
  order_type_default: market
  time_in_force_default: GTC
```

Keep existing keys reused directly where parity matters:
- `strategy.*`, `risk.*`, `instrument.*`, `execution.*` (for paper/simulation parameters), `outputs.*`, `audit.*`.

# 9. Proposed Run/Logging Layout

Preserve existing run-folder contract style while adding exec-specific logs:

```text
outputs/exec_runs/
  exec_<timestamp_or_runid>/
    config_used.yaml
    run_manifest.json
    run_status.json
    decisions.jsonl             # strategy/risk decisions (same semantics where possible)
    orders.jsonl                # submit/ack/reject/cancel/replace events
    fills.jsonl                 # broker-confirmed fills
    positions_snapshots.jsonl   # periodic local+broker position snapshots
    reconciliation.jsonl        # mismatch detection/resolution records
    heartbeat.jsonl             # liveness + loop latency
    session_summary.json
    audit/                      # keep audit manager style where compatible
```

Carry over:
- deterministic formatting helpers
- explicit schema versions
- canonical side/qty validation before persistence (`logging/jsonl.py` style)
- `run_manifest` + artifact manifest conventions.

# 10. Risks and Mitigations

1. **Backtest fill assumptions leak into live behavior**
   - Why: current execution pipeline is bar-based and deterministic, while live fills are asynchronous and partial.
   - Severity: **High**.
   - Mitigation: separate `ExecutionAdapter` contracts and never let broker adapter depend on bar-delay semantics.

2. **Strategy parity drift due to different context timing**
   - Why: live data streaming might trigger strategy before bar close unless enforced.
   - Severity: **High**.
   - Mitigation: enforce close-bar gating in exec runtime to match `on_bars` contract exactly.

3. **Order state insufficiency for real broker lifecycle**
   - Why: current order model lacks explicit partial/cancel-replace state richness.
   - Severity: **High**.
   - Mitigation: additive order lifecycle metadata and broker IDs; avoid breaking existing backtest states.

4. **No restart-safe canonical store**
   - Why: backtest run is single-process transient; live requires crash recovery and idempotency.
   - Severity: **High**.
   - Mitigation: implement append-only event journal + periodic checkpoints before live rollout.

5. **Portfolio/risk coupling to local immediate fills**
   - Why: `portfolio.apply_fills` assumes direct ingestion of finalized fills.
   - Severity: **Medium**.
   - Mitigation: introduce order-fill reconciliation layer that feeds only confirmed canonical fills into portfolio.

6. **Config fragmentation risk**
   - Why: adding broker/runtime knobs can sprawl if not namespaced.
   - Severity: **Medium**.
   - Mitigation: strict `exec.*`, `broker.*`, `routing.*` namespaces + extend `resolve_config` validations.

7. **Output/audit contract divergence**
   - Why: exec runtime may write ad hoc logs if not contract-driven.
   - Severity: **Medium**.
   - Mitigation: define `exec` artifact contract doc first; implement deterministic writers with schema versions.

8. **Reconciliation blind spots**
   - Why: current system has no broker-vs-local reconciliation primitive.
   - Severity: **High**.
   - Mitigation: mandatory periodic reconcile phase with explicit mismatch policies and artifacts.

# 11. Recommended Build Phases

## Phase 1 — Shared contract extraction hardening
- **Objective**: carve runtime-agnostic orchestration primitives from `BacktestEngine` without behavior change.
- **Dependencies**: `src/bt/core/engine.py`, `src/bt/core/types.py`, `src/bt/risk/*`, `src/bt/portfolio/*`, `src/bt/logging/jsonl.py`.
- **Likely files**: new helper modules under `src/bt/core/` or `src/bt/exec/` plus tiny backtest rewiring.
- **Why first**: enables safe reuse and prevents duplicated decision/fill logic.

## Phase 2 — Exec runtime skeleton (paper-first with simulated adapter)
- **Objective**: build `bt.exec` event loop using same strategy/risk interfaces and a simulated adapter wrapping existing execution logic.
- **Dependencies**: strategy adapters, risk engine, portfolio, execution model.
- **Likely files**: `src/bt/exec/runtime.py`, `events.py`, `adapters/base.py`, `adapters/simulated.py`, `scripts/run_exec.py`.
- **Why before broker integration**: proves parity path and artifact model before external API risk.

## Phase 3 — Durable state + restart safety
- **Objective**: persist runtime state and replay safely after restart.
- **Dependencies**: phase 2 runtime lifecycle points.
- **Likely files**: `src/bt/exec/state_store.py`, checkpoint/event journal utilities, tests for crash/restart idempotency.
- **Why before live broker**: avoids unsafe deployment with in-memory-only state.

## Phase 4 — Reconciliation subsystem
- **Objective**: reconcile local order/position/fill state with adapter-reported broker truth.
- **Dependencies**: state store + runtime + adapter interface.
- **Likely files**: `src/bt/exec/reconcile.py`, reconciliation artifacts writers, mismatch policy config validation.
- **Why before Bybit live**: ensures operational safety and auditable mismatch handling.

## Phase 5 — Bybit adapter (demo)
- **Objective**: implement broker adapter for demo environment under unified contract.
- **Dependencies**: runtime, state, reconciliation.
- **Likely files**: `src/bt/exec/adapters/bybit.py`, broker auth/config modules, adapter integration tests.
- **Why before live**: validate end-to-end with lower operational risk.

## Phase 6 — Bybit live hardening
- **Objective**: production controls (rate limits, retries/backoff, kill-switches, runbook artifacts).
- **Dependencies**: stable demo adapter and reconciliation metrics.
- **Likely files**: adapter hardening, config guardrails, operational docs.
- **Why last**: only after parity + restart + reconciliation are proven.

# 12. Open Questions / Unknowns

1. `core/types.OrderState` breadth for partial fills/cancel-replace is not fully audited here (requires explicit enum review + intended broker mappings).
2. No existing broker API client modules are present; bybit transport/client decisions are still open.
3. Existing `run_status.json` schema expectations for non-backtest runtimes are undefined.
4. Recovery semantics for in-flight orders across process restarts need explicit product decision (resume/cancel/reconcile-first behavior).
5. Whether live mode should reuse `trades.csv` exactly or introduce `executions.csv` + derived `trades.csv` requires contract decision.
6. Market data source contract for live bars (bar-close authoritative source, clock synchronization) is not yet defined in repo docs.

# 13. “Phase 0 Implementation Candidate”

## Exact goal
Create an **exec contract foundation only**: add `docs/exec_contract.md` plus minimal typed interfaces (`ExecutionAdapter`, `BrokerEvent`, `ExecutionStateStore`) without implementing broker calls or runtime logic.

## Why this is the safest next step
- Locks interface boundaries before code spread.
- Prevents accidental architecture drift from backtest conventions.
- Enables early review of parity, state, and reconciliation assumptions.

## Exact files likely to be created or touched

- New docs:
  - `docs/exec_contract.md`
- New minimal code stubs:
  - `src/bt/exec/__init__.py`
  - `src/bt/exec/adapters/base.py`
  - `src/bt/exec/events.py`
  - `src/bt/exec/state_store.py`
- Possible light-touch updates:
  - `src/bt/__init__.py` (export surface if desired)
  - `tests/` new contract tests for interface shape only.

## Exact non-goals

- No bybit API integration.
- No live order submission.
- No event loop implementation.
- No portfolio/risk behavior changes.
- No strategy logic changes.
- No replacement of current backtest engine.
