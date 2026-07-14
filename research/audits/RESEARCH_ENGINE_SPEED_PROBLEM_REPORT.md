# Research Engine Speed Problem Report

Date: 2026-07-02  
Repository: `bulletproof_bt`  
Primary question: how do we make the research system fast enough on modest compute without weakening backtest truth?

## First Principle

Backtest truth is the product. Speed is only useful after truth is preserved.

No speedup is more important than a reliable, auditable, deterministic backtest. A fast engine that changes entries, exits, fills, costs, margin, liquidation behavior, state availability, or artifact truth is not an optimization. It is a different simulator, and it must be rejected until it proves parity with the classic path.

The current classic path remains the reference implementation. Any accelerated path must be subordinate to it:

```text
candidate fast path -> parity gate -> classic-equivalent execution -> production enablement
```

If parity fails, the system must fail closed and use classic execution.

## Non-Negotiable Backtest Requirements

A safe and reliable backtest engine must preserve all of the following. These are requirements, not preferences.

### Market Data Truth

- Timestamps must be strict UTC.
- Bars must be monotonic per symbol.
- Duplicate timestamps must be rejected or deterministically deduplicated before use.
- Missing bars must remain missing; no silent interpolation, synthetic bars, or hidden forward fill of OHLCV.
- Funding and OI must be causal backward-asof joins.
- Mark/index candles must align to known candle timestamps.
- Source timestamps such as `funding_source_ts` and `oi_source_ts` must never be greater than the decision bar timestamp.
- Volatile universe membership must be historical and timestamp-gated; future membership must not be visible.

### Decision Truth

- Strategies must make decisions only from information available at the decision timestamp.
- HTF features must be based only on completed higher-timeframe bars.
- Rolling percentiles, z-scores, ATR, trend, displacement, funding/OI/basis state, and CSI must be causal.
- Start-window and warmup behavior must be explicit and deterministic.
- Missing features must cause missing/rejected decisions or clearly logged unavailable state, not hidden substitutes.

### Execution Truth

- Fees, slippage, spread, delays, stop behavior, and intrabar assumptions must remain explicit.
- Position sizing must obey the configured risk model.
- Notional caps, leverage, used margin, free margin, forced liquidation, and margin breach checks must be internally consistent.
- Open positions must continue to receive bars needed for exits, stops, mark-to-market, and liquidation checks.
- For volatile universes, leaving active membership must stop new entries, not erase existing position management.

### Artifact Truth

Every run must produce enough evidence to audit and learn from it:

- `config_used.yaml`
- `decisions.jsonl`
- `fills.jsonl`
- `trades.csv`
- `equity.csv`
- `performance.json`
- `run_status.json`
- timing/resource metadata where available
- post-run summaries, extracted datasets, state findings, verdict cards, terminal cards, and memory ingestion after truth gates pass

Artifacts must be complete, internally consistent, and reusable by the research memory/ML pipeline.

### Determinism And Resume Truth

- The same config, data snapshot, and code version should produce the same result.
- Completed runs must not be skipped if required artifacts are missing or stale.
- Failed/incomplete runs must be visible and recoverable.
- Cleanup must not delete source-of-truth logs before extraction and summaries are complete.

### Speed Is Conditional

Speedups are allowed only when they preserve the above requirements. The acceptance test for a fast path is not "it is faster." The acceptance test is:

- same metrics;
- same equity curve;
- same semantic trades;
- same causal state availability;
- same execution/accounting behavior;
- same required artifacts;
- documented parity certificate.

## Executive Summary

The current bottleneck is not just "Python is slow" or "we need more workers." The deeper problem is algorithmic work volume:

- each grid run repeatedly scans large multi-symbol, 1-minute research panels;
- stable runs span roughly 29 major symbols;
- volatile runs can involve hundreds of historically active instruments and dynamic membership;
- every parameter row currently performs much of the same feature/state/decision traversal again;
- the engine preserves bar-by-bar, no-lookahead, event-driven truth, which limits unsafe shortcuts;
- post-run artifacts are rich and valuable, but they add IO, serialization, and memory pressure.

We tried several speed paths. The only paths that are production-safe are the ones that preserve semantic parity with classic execution. Some unsafe static-column paths produced 2x to 4x speedups, but failed parity by generating different trades/equity/metrics. Those cannot be promoted until the feature-state contract exactly reproduces classic runtime state.

The direction now should shift from "make the current loop run faster" to "avoid doing redundant work." The core architectural target is an engine-wide compiled candidate/event pipeline:

1. Load market data once in compact columnar arrays.
2. Compute shared causal features once per symbol/timeframe/data snapshot.
3. Generate sparse candidate events per strategy family/parameter grid.
4. Let the classic accounting/execution engine remain the source of truth for fills, costs, margin, stops, liquidation, equity, logs, and artifacts.
5. Run a parity gate before any compiled path is allowed in daemon production.

This is attainable, but the implementation must be treated as a certified compiled research layer, not a casual cache.

## Current Compute Constraint

Observed / user-reported VM environment:

- 36 vCPU
- about 73 GiB RAM visible in Proxmox
- VM memory pressure frequently reported high by Proxmox because Linux page cache plus active workers can push host-visible usage toward 90-100%
- long-running daemon workloads compete with:
  - backtest workers
  - research-data download/build processes
  - parquet reads/writes
  - post-run analysis and extraction
  - possible background services

Practical consequence: simply increasing workers helps only until memory bandwidth, disk IO, panel duplication, and per-process RSS dominate. Above that point, more workers can slow the system or destabilize the VM.

## Current Architecture

### Classic Path: The Reference Architecture

The current working classic path is the source of truth for the research system. It is slower because it does the honest work explicitly:

```text
hypothesis YAML
  -> grid manifest
  -> per-row config resolution
  -> research panel loader
  -> causal state/feature availability
  -> bar-by-bar strategy decisions
  -> classic execution/accounting engine
  -> streamed decisions/fills/trades/equity artifacts
  -> performance + sanity checks
  -> post-run analysis
  -> dataset extraction
  -> state discovery
  -> verdict cards / terminal cards
  -> research memory ingestion
```

The classic path keeps the full event-driven semantics:

- one decision point at a time;
- no future data exposed to the strategy;
- no hidden interpolation;
- execution realism remains inside the engine, not inside strategy shortcuts;
- logs and artifacts are emitted as source-of-truth evidence.

This path is the benchmark every acceleration must match. The correct mental model is:

```text
classic = truth reference
fast path = candidate optimization
parity test = admission gate
daemon enablement = only after certificate
```

If the fast path cannot reproduce the classic path, the fast path is wrong for production even if it is much faster.

### Data Lake

The new `research_data/` subsystem stores canonical panels:

```text
research_data/canonical/<market>/<exchange>/<symbol>/timeframe=1m/research_panel.parquet
research_data/manifests/stable_universe.parquet
research_data/manifests/volatile_universe_membership.parquet
```

Panels may contain:

- OHLCV
- mark price
- index price
- funding
- open interest
- basis/premium
- liquidation columns when available
- derived state/kernel feature columns when built

The research panel loader supports stable and volatile universes. For volatile, timestamped membership is required so inactive symbols are not exposed to strategies.

### Stable Runs

Stable universe is a fixed set of major Binance USDT perps. Current Binance stable availability is 29 native symbols. Stable is easier to optimize because the symbol set is fixed and full panels can be loaded directly per symbol.

### Volatile Runs

Volatile is materially harder:

- membership is timestamped;
- instruments rotate historically;
- hundreds of symbols can appear;
- active universe at time `t` must use only information known at `t`;
- positions/orders may need continuation data after the symbol leaves active membership;
- treating all volatile symbols as always active is not allowed.

We built a materialized volatile active panel:

```text
research_data/canonical/perp/<exchange>/_volatile_active/timeframe=1m/research_panel.parquet
```

This reduces active-row loading, but does not eliminate the harder continuation/position-state issue.

### Runner / Daemon

The research daemon queues hypotheses, builds grids, launches parallel workers, runs post-run analysis, extracts datasets, generates verdicts/cards, and updates memory. The parallel runner has resource controls and run-status artifacts, but each run still largely behaves as an independent process with its own data load and execution loop.

## What We Have Tried

### 1. More Workers

Increasing workers improves throughput only while the workload remains CPU-bound and memory/IO remain under control. In practice:

- stable has tolerated higher worker counts better;
- volatile consumes more memory/IO and runs longer;
- Proxmox has reported high memory even when guest tools suggested some of it was reclaimable cache;
- VM interruptions have made it risky to push worker counts blindly.

Conclusion: more workers are useful but not the primary solution. They amplify duplicated work.

### 2. Resource Controls and Resume Safety

Implemented or tightened:

- RAM-aware worker controls;
- run timeout option;
- fail-fast vs no-fail-fast behavior;
- atomic `run_status.json`;
- strict resume checks;
- stale artifact detection;
- process failure isolation;
- daemon command logs and heartbeat metadata.

Result: better stability and less wasted recovery time, but not a large speed leap.

### 3. Rich Research Panel Loader

The loader now consumes canonical `research_data` panels instead of the old curated external folders. It preserves OHLCV as bar data and passes funding/OI/mark/index/basis/liquidation features through the extra/state path.

Result: better data quality and richer learning, but larger panels and more state columns increase load and processing work.

### 4. Volatile Materialized Active Feed

We built a materialized volatile active panel from timestamped membership. This prevents treating all volatile members as active all the time.

Result:

- correctness improved;
- memory footprint improved relative to loading all volatile symbols as always active;
- but volatile runs remain slow because dynamic membership, continuation state, and large candidate surface still require substantial processing.

### 5. Online Strategy-Family Event Adapter

For L7-H1, an online event adapter was tested. It keeps runtime semantics close to classic by computing/using state in a way that matches the classic engine.

Observed comparison examples:

- `l7_h1_stable_static_rebuilt_2d`: online fast vs classic passed metrics/equity parity.
- Speedup was modest, roughly 1.0x to 1.09x for the tested stable two-day window.

Conclusion: safe, but not enough.

### 6. Static Column Fast Path

We built L7-H1 static columns into panels using:

```bash
PYTHONPATH=src python3 -m bt.research_data.cli build-l7h1-kernel-features \
  --exchange binance \
  --universe stable \
  --timeframe 1m \
  --signal-timeframes 15m,1h
```

Coverage was rebuilt successfully. Sampled stable panels now had `l7h1_*` feature columns across the full available history.

However, parity failed:

- `l7_h1_stable_static_rebuilt_static_2d`
  - 15m static was about 2.1x faster but metrics/equity/trades differed.
  - 1h static was about 2.0x faster but metrics/equity/trades differed.
- `l7_h1_stable_static_rebuilt_static_2d_warmup3d`
  - 15m static was about 4.2x faster but still failed parity.
  - 1h static was about 3.7x faster but still failed parity.

Likely cause:

- static columns are computed from full-history panels;
- classic runtime state depends on the run window, warmup window, HTF readiness, strategy state initialization, and exact first eligible decision timestamp;
- static features can be "valid" earlier or differently than the classic runtime state would be;
- small candidate differences cascade into different positions, exits, PnL, and equity.

Conclusion: static columns are promising but not production-safe yet. They need a certification contract.

### 7. Volatile Static Stamping

We attempted volatile static paths by stamping columns onto active or individual member panels.

Observed examples:

- `l7_h1_2d_volatile_individual_stamped`: roughly 2x to 2.6x faster, but failed parity.
- warmup variants also failed.

Conclusion: volatile static is harder than stable static because active membership and post-entry continuation windows must both be represented exactly.

### 8. Sparse Context / Candidate Event Attempts

Some sparse context attempts for stable L7-H1 passed parity and produced larger speedups:

- `l7_h1_2d_sparse_context_stable_v2`
  - stable 15m: about 2.05x speedup, parity passed;
  - stable 1h: about 1.94x speedup, parity passed.

But full stable+volatile versions showed:

- stable benefited;
- volatile gained little or none;
- not all approaches generalized safely.

Conclusion: sparse/event methods are the most promising, but need to be engine-wide and designed around volatile membership from first principles.

### 9. Columnar Candidate Event Experiments

Examples:

- `columnar_candidate_event_l1h11b_stable_short`
  - stable 15m: about 1.08x speedup, parity passed;
  - stable 1h: about 1.11x speedup, parity passed.
- `columnar_candidate_event_l7h1_stable_short`
  - about 1.05x to 1.06x speedup; metrics/equity passed, but one semantic-trade comparison did not fully match due artifact/schema differences.

Conclusion: the current columnar candidate implementation is a framework step, not the final speed leap. It has not yet eliminated the dominant work.

## Current Bottlenecks

### 1. Repeated Full-Panel Traversal

Each grid row often traverses the same symbols and date range again. If a 24-row grid uses the same data and only changes thresholds/stops, repeatedly loading and scanning the same panel is wasteful.

### 2. Per-Worker Dataset Duplication

Parallel workers are separate processes. Each can load overlapping panels, duplicate memory, and fight for disk bandwidth.

### 3. Volatile Universe Explosion

Volatile runs combine:

- many instruments;
- dynamic active membership;
- candidate generation over large active surfaces;
- continuation requirements for open positions;
- expensive logging and state extraction.

The volatile problem is not just "more symbols." It is "more symbols plus time-varying eligibility plus causal continuation."

### 4. Rich State Feature Computation

Funding/OI/mark/index/basis state features are valuable. But if computed or merged repeatedly per run, they become costly. Causal precomputed panels help, but only when their semantics match runtime state exactly.

### 5. IO and Artifact Weight

Required truth artifacts include decisions, fills, trades, equity, performance, summaries, state findings, extracted datasets, cards, and memory updates. This is correct for research, but it means run time includes more than strategy execution.

### 6. Python Object Overhead

Bar-by-bar loops using Python objects, dictionaries, pandas rows, signal objects, and JSON serialization are expensive. This matters especially when the algorithm touches millions of bars.

### 7. Memory Bandwidth and Cache Locality

The "What Every Programmer Should Know About Memory" lesson is directly relevant:

- contiguous arrays beat pointer-heavy objects;
- fewer passes over memory beat repeated scans;
- columnar layout helps CPU cache;
- memory bandwidth can dominate CPU arithmetic;
- separate worker processes duplicate hot data unless carefully shared.

The current architecture still moves too much data through Python-level objects.

## Why Some Fast Paths Failed

### Static Features Are Not Automatically Equivalent

Precomputing a feature over full history changes its effective availability unless the runtime masks it exactly like classic execution. A feature column can be numerically causal but still not semantically equivalent if classic would not have initialized that state yet.

Required missing contract:

- feature version;
- source data snapshot;
- strategy family;
- signal timeframe;
- warmup requirement;
- valid-from timestamp per symbol;
- exact runtime mask;
- parity certificate.

### Volatile Active Rows Are Not Enough

If a symbol is active at entry but inactive later, the engine may still need continuation bars for exits, stops, mark-to-market, and risk. A materialized "active only" feed helps entries but can break exits if used naively.

### Candidate Shortcuts Can Skip Necessary State

A sparse candidate event kernel is safe only if:

- it emits every event that classic could act on;
- it preserves all bars needed for open positions/orders;
- it preserves HTF close timing;
- it preserves risk/execution state transitions;
- it lets classic accounting remain authoritative.

Missing any of those changes the strategy.

## Safe Architecture Already in Place

The following are useful foundations:

- canonical research panel lake;
- stable and volatile manifests;
- volatile active materialization;
- rich derivatives state feature preservation;
- causal funding/OI backward-asof joins;
- no-lookahead validation;
- runner resource controls;
- atomic run status;
- strict resume checks;
- parity comparison harness;
- strategy-family kernel flags;
- online adapter fallback;
- truth certification docs;
- prompt-generation rules for future hypotheses.

These are not wasted. They are the scaffold needed for a bigger compiled engine.

## Recommended Next Architecture

### Target: Certified Compiled Candidate Engine

Instead of optimizing each worker independently, build a two-layer system:

1. **Compiled research layer**
   - columnar, contiguous arrays;
   - integer symbol IDs;
   - shared causal feature computation;
   - sparse candidate event generation;
   - parameter-grid vectorization where possible;
   - memory-mapped read-only panels;
   - strict feature/candidate provenance.

2. **Classic truth layer**
   - receives candidate events and necessary continuation bars;
   - performs all account/risk/fill/fee/slippage/stop/equity logic;
   - writes the same truth artifacts;
   - remains the source of truth.

The compiled layer should never directly publish PnL. It should only reduce the event set the classic layer needs to process.

### Core Design

#### 1. Market Snapshot Object

Build a reusable snapshot per:

- data root;
- exchange;
- universe;
- date range;
- timeframe;
- membership version;
- panel file hashes.

It exposes:

- `ts_ns: int64[]`
- `symbol_id: int32[]`
- `open/high/low/close/volume: float64/float32[]`
- optional rich columns;
- active membership bitmap or interval table;
- per-symbol row offsets.

This avoids repeated parquet-to-pandas-to-object conversion.

#### 2. Feature Registry

Feature functions declare:

- required input columns;
- timeframe;
- lookback;
- warmup bars;
- output dtype;
- causality contract;
- hash/version.

Feature arrays are computed once per snapshot and stored/memmaped.

#### 3. Strategy Family Compiler

For each strategy family, compile only the admission logic:

- candidate entry timestamps;
- side;
- symbol;
- parameter row ID;
- required initial stop/metadata;
- decision trace fields.

It must not execute fills or finalize PnL.

#### 4. Parameter-Grid Vectorization

Many grids differ by thresholds:

- `d0 in {1.8, 2.2}`
- `theta in {0.7, 0.8}`
- stop/trail multipliers

Instead of scanning data once per parameter row, scan once and evaluate all thresholds against the same feature arrays. Emit candidates tagged with parameter row IDs.

This is likely the largest safe speed leap.

#### 5. Continuation Stream Builder

For each candidate set, build the minimal bar stream needed by classic execution:

- all candidate decision bars;
- all bars while position/order is live;
- all bars needed for trailing stops/time exits;
- all bars needed for HTF completeness;
- forced bars at session/end boundaries.

For volatile, continuation bars may include symbols after they leave active membership, but new entries remain membership-gated.

#### 6. Classic Execution Replay

The classic engine consumes sparse/continuation streams and produces exactly the normal artifacts. If parity fails, the daemon falls back to full classic.

#### 7. Certification Gate

Every compiled route must have a certificate:

```text
strategy_family
timeframe
universe
data_snapshot_hash
feature_version
compiler_version
parity_window
same_metrics=true
same_equity=true
same_semantic_trades=true
approved_at
```

No certificate, no production compiled path.

## What Not To Do

Do not:

- treat all volatile symbols as always active;
- use full-history static features without runtime readiness masking;
- publish compiled PnL directly;
- skip bars needed for open exits/stops;
- cache features across incompatible params without a hash;
- reuse stale artifacts as completed;
- reduce logs before extraction/memory;
- replace causal online percentiles with full-sample diagnostics;
- hide missing funding/OI/liquidation availability by filling zeros.

## Research Questions For The Next Design Pass

1. Can we scan one market snapshot once and evaluate all parameter rows for a family?
2. Can candidate generation be expressed as pure array operations for the top 5 strategy families?
3. What is the minimum continuation bar set needed to preserve exact exits?
4. Can the classic engine operate on sparse bar streams without assuming dense global time?
5. Can we build a run bundle where all parameter rows share one loaded memory map?
6. Which artifacts must be per-run vs can be written once per grid?
7. Can decisions logs be written sparsely for rejected non-candidate bars while preserving enough auditability?
8. Can post-run analysis consume normalized trade/equity artifacts without rereading large decisions logs?
9. Can volatile membership be represented as interval joins over integer symbol IDs instead of dataframe filters?
10. Can we build a golden parity suite that runs automatically for every compiled family?

## Near-Term Implementation Plan

### Phase 1: Measurement First

Add per-run timing breakdown that separates:

- parquet load;
- panel normalization;
- feature computation;
- strategy decision loop;
- execution/accounting;
- decision logging;
- fill/trade/equity writing;
- post-run analysis;
- extraction;
- memory/card generation.

Without this, speed work will keep chasing shadows.

### Phase 2: Shared Snapshot Loader

Implement a read-only market snapshot for one grid, not one run. First target stable because it is simpler. Use memory maps where safe.

### Phase 3: Grid-Level Feature Precompute

Compute common features once per snapshot/timeframe/family and share them across parameter rows.

### Phase 4: Vectorized Candidate Generation

For one strategy family, evaluate all parameter thresholds in one pass. L7-H1 is a good target because its admission logic is feature-heavy and its exits can remain classic.

### Phase 5: Sparse Classic Replay

Feed classic execution only candidate/continuation bars. The first version can be conservative and include extra bars. Correctness first, then shrink the stream.

### Phase 6: Certified Production Enablement

Only enable compiled routes in the daemon after parity passes on:

- stable short window;
- stable longer window;
- volatile short window;
- volatile membership edge cases;
- different start dates to catch warmup bugs.

## Expected Impact

Realistic expectations:

- resource controls alone: stability, not speed;
- online adapters: 1.0x to 1.1x typical;
- safe sparse stable paths: up to about 2x observed in some L7-H1 tests;
- unsafe static paths: 2x to 4x observed, but not usable until certified;
- true grid-level vectorized candidate generation: plausible 3x to 10x for parameter-heavy families if most work is duplicated admission scanning;
- hardware upgrade: linear-ish only until memory/IO bottlenecks dominate.

The biggest safe leap is not C++ by itself. It is reducing passes over data and reducing Python object traffic. C++/Numba/Rust can help after the algorithm is reorganized around columnar arrays and sparse candidate events.

## Final Diagnosis

We have been partly solving the wrong layer. Worker count, static columns, and local adapters are useful, but the research system is slow because each run repeats too much data traversal and feature/state work.

The institutional-style answer is not "buy infinite compute." It is:

- one canonical data snapshot;
- one causal feature pass;
- one candidate generation pass over all parameter rows;
- sparse continuation streams;
- classic execution for truth;
- automatic parity certification;
- fail-closed daemon enablement.

That is the architecture that can let modest compute compete intelligently: not by pretending to have more hardware, but by refusing to recompute the same facts 24, 48, or 64 times.
