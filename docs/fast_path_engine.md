# Fast Path Engine

Bulletproof_bt now has a conservative fast-path execution seam beside the
classic event-driven engine. The public workflow is unchanged: existing
orchestrator, daemon, queue, and `scripts/run_parallel_hypothesis_grid.py`
commands still call the same interfaces.

## Selection

Use either:

```bash
BULLETPROOF_EXECUTION_ENGINE=classic
BULLETPROOF_EXECUTION_ENGINE=auto
BULLETPROOF_EXECUTION_ENGINE=fast_path
```

or set:

```yaml
execution_engine: classic   # classic | auto | fast_path
```

`auto` is the safe default. Unsupported strategies fall back to the classic
engine and write `fast_path_status.json`.

## What Exists Now

- `bt.engine.fast_path.data_session.DataSession`
  loads parquet once and exposes contiguous NumPy arrays for supported future
  kernels.
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

The parity test runs a deterministic fixture once with `classic` and once with
`auto`, then compares truth artifacts.

## Operational Use

For daemon retests:

```bash
BULLETPROOF_EXECUTION_ENGINE=auto PYTHONPATH=src:. python3 orchestrator/research_daemon.py \
  --db research_db/research.sqlite \
  --config orchestrator/daemon_config.yaml \
  --max-workers 8
```

Inspect any run:

```bash
cat outputs/.../runs/<run>/fast_path_status.json
cat outputs/.../runs/<run>/run_timing.json
```
