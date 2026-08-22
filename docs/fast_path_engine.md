# Fast Path Engine

> Deprecated: production research runs now resolve to the classic event-driven
> engine. See `docs/fast_path_deprecation.md`.

Bulletproof_bt now has a conservative fast-path execution seam beside the
classic event-driven engine. The public workflow is unchanged: existing
orchestrator, daemon, queue, and `scripts/run_parallel_hypothesis_grid.py`
commands still call the same interfaces.

## Selection

Use:

```bash
BULLETPROOF_EXECUTION_ENGINE=classic
```

or set:

```yaml
execution_engine: classic   # classic | auto | fast_path
```

`auto` and `fast_path` remain accepted only for old configs; they are
downgraded to classic execution.

`classic` is the production default. `auto` and `fast_path` are accepted for
backward compatibility, but they are deprecated and downgraded to classic
execution. Runs write `fast_path_status.json` with
`mode=classic_deprecated_fallback`.

## What Exists Now

- `bt.engine.fast_path.data_session.DataSession`
  loads research panels once and exposes a reusable `MarketDataSnapshot` with
  integer symbol ids, contiguous OHLCV arrays, optional rich feature arrays,
  volatile active-membership masks, and candidate-readiness masks.
- `bt.engine.fast_path.candidate_events`
  defines the columnar candidate-event schedule contract: UTC timestamp arrays,
  integer symbol ids, contiguous boolean candidate masks, and deterministic
  skip counters.
- `FeatureBank`
  caches reusable EMA/ATR calculations.
- `signal_compiler.inspect_support`
  decides whether a run can safely use a fast adapter.
- `numba_kernels`
  provides an optional Numba facade without making Numba a hard dependency.
- `batch_runner.run_fast_path_if_supported`
  is the fallback-safe selector wired into `bt.api.run_backtest`.
- `run_timing.json`
  records timing for data load, support check, engine build/run, metrics, and
  artifact stages.
- `bt.engine.fast_path.l7_h1_kernel`
  provides the compiled causal feature kernel for the L7-H1 CSI-gated
  displacement trend family.

## Columnar Candidate-Event Scheduling

Research-panel runs can enable:

```yaml
data:
  candidate_event_mode: auto
  columnar_candidate_events: true
```

This is a scheduling optimization only. While the portfolio is flat and there
are no live orders, the research-panel feed suppresses timestamps that do not
carry a causal event marker such as `htf_<tf>_ready` or
`l7h1_<tf>_compiled_feature_ready`. Once an order or position exists, the feed
switches back to dense 1m bars so fills, stops, trailing exits, liquidations,
mark-to-market, and equity remain classic-engine truthful.

Every enabled run writes `candidate_event_summary.json` with emitted/skipped
timestamp and row counts. If no causal event markers are present, no timestamps
are skipped.

Skipped flat timestamps are replayed into `equity.csv` as constant-equity
heartbeat rows before the next emitted candidate event. This preserves
performance, drawdown-duration, Sharpe/Sortino, margin-utilization, and
artifact parity while avoiding the expensive strategy/risk/logging loop for
bars that cannot produce an entry or exit.

Volatile runs use this path only when the materialized volatile panel carries
causal candidate columns for the active/continuation stream. Online volatile
feature paths keep dense classic bars because their candidate state depends on
the exact emitted 1m stream.

## MarketDataSnapshot Contract

`DataSession.from_config(config)` is the system-wide read-once market data
foundation for future fast adapters. It understands the canonical
`research_data` layout, stable manifests, volatile membership manifests, and
materialized volatile panels.

The snapshot exposes:

- `symbols` and `symbol_to_id` for integer-keyed kernels;
- `SymbolArrays.ts/open/high/low/close/volume` as contiguous NumPy arrays;
- `SymbolArrays.extras` for optional rich columns such as funding, OI,
  mark/index/basis, HTF context, and family kernel columns;
- `SymbolArrays.active_mask` for stable or timestamp-gated volatile membership;
- `SymbolArrays.candidate_ready` for sparse candidate scheduling;
- `MarketDataSnapshot.active_symbols_at_ns(ts_ns)` for timestamp-gated
  membership inspection.

This layer is read-only and causal. It is not allowed to replace the classic
risk engine, execution engine, portfolio accounting, or artifact writers until a
specific strategy-family adapter has passed parity. Its job is to prevent every
run from repeatedly rebuilding the same DataFrame-heavy market view.

## L7-H1 Compiled Feature Kernel

L7-H1 now supports a safe hybrid mode:

- compiled kernel computes causal strategy-family features from research panels;
- L7-H1 strategy consumes those columns when
  `l7h1_<timeframe>_compiled_feature_ready` is true;
- classic engine still performs risk sizing, order delay, fills, stops,
  decisions, trades, equity, performance, and artifact writing.

Build L7-H1 feature columns for stable panels:

```bash
PYTHONPATH=src python3 -m bt.research_data.cli build-l7h1-kernel-features \
  --exchange binance \
  --universe stable \
  --timeframe 1m \
  --signal-timeframes 15m,1h
```

Build them for the materialized active volatile panel:

```bash
PYTHONPATH=src python3 -m bt.research_data.cli build-l7h1-kernel-features \
  --exchange binance \
  --universe volatile-active \
  --timeframe 1m \
  --signal-timeframes 15m,1h
```

Runs report this as:

```json
{
  "handled": false,
  "mode": "classic_with_compiled_l7h1_features"
}
```

`handled=false` is intentional: execution is still handled by the classic
engine. The fast component is the compiled feature path.

## Current Limitation

Most rich research hypotheses still run through the classic feature path because
their strategy-family kernels have not yet been parity-expanded. Do not enable a
full compiled execution adapter until it preserves:

- no lookahead
- bar-by-bar execution
- 1m exit monitoring
- strict HTF completeness
- existing fills/trades/equity/performance artifacts

New strategy families must follow
`docs/hypothesis_strategy_generation_prompt_instructions.md`.

## Validation

Run:

```bash
PYTHONPATH=src:. pytest -q tests/test_fast_path_engine.py
```

Historical parity tools may still run deterministic fixtures, but production
research should not rely on fast-path parity.

## Operational Use

For daemon retests:

```bash
BULLETPROOF_EXECUTION_ENGINE=classic PYTHONPATH=src:. python3 orchestrator/research_daemon.py \
  --db research_db/research.sqlite \
  --config orchestrator/daemon_config.yaml \
  --max-workers 8
```

Inspect any run:

```bash
cat outputs/.../runs/<run>/fast_path_status.json
cat outputs/.../runs/<run>/run_timing.json
```

## Shared Causal Feature Engine

The fast path is now organized around a generic substrate instead of one-off
strategy columns:

- `src/bt/engine/fast_path/data_session.py`
  loads each research panel once into a `MarketDataSnapshot` with integer
  symbol ids, contiguous OHLCV arrays, active membership masks, optional rich
  arrays, and a `FeatureBank`.
- `src/bt/engine/fast_path/feature_registry.py`
  defines `FeatureSpec` contracts. Every reusable feature family declares
  required inputs, warmup, causality mode, version/hash, output dtype,
  readiness columns, and candidate marker columns.
- `src/bt/engine/fast_path/candidate_events.py`
  discovers generic candidate markers such as `*_ready`,
  `*_entry_candidate`, `*_exit_candidate`, `*_continuation_required`, and
  `*_stop_check_required`. Old `htf_*_ready` and
  `l7h1_*_compiled_feature_ready` markers remain backward compatible.
- `src/bt/engine/fast_path/family_kernels.py`
  exposes `StrategyAdapterSpec`, where a family asks for feature families and
  a scheduler contract. Strategies should not hardcode precompute plumbing.
- The classic engine remains the truth layer. Sparse/compiled paths may reduce
  feature work and skip flat no-candidate timestamps, but fills, PnL, margin,
  liquidation checks, and artifacts remain classic-engine outputs.

Build registered feature families through the generic pipeline:

```bash
PYTHONPATH=src python3 -m bt.research_data.cli build-registered-features \
  --exchange binance \
  --universe stable \
  --timeframe 1m \
  --features engine_state,htf_context,l7h1_csi_displacement \
  --signal-timeframes 5m,15m,1h
```

Future strategy-family prompts should reference this architecture: declare the
feature families the strategy needs, add or update a `FeatureSpec` when a new
causal feature is required, add a `StrategyAdapterSpec`, and pass parity before
enabling sparse scheduling in production.
