# Parallel hypothesis grid runner (spawn-safe v2)

This repo keeps the existing CLI surface for `scripts/run_parallel_hypothesis_grid.py` unchanged, while strengthening internals for stability and observability.

## What changed

- The process pool now uses `multiprocessing.get_context("spawn")` and `ProcessPoolExecutor(..., mp_context=ctx)`.
- Work is dispatched in deterministic **waves** (chunked scheduling) instead of unbounded submission.
- Worker bootstrap now enforces native thread caps by default (without overriding user-provided values):
  - `OMP_NUM_THREADS`
  - `OPENBLAS_NUM_THREADS`
  - `MKL_NUM_THREADS`
  - `NUMEXPR_NUM_THREADS`
  - `VECLIB_MAXIMUM_THREADS`
  - `POLARS_MAX_THREADS`
- Every worker enables `faulthandler` and writes run-local diagnostics.
- Parallel runs build experiment-level shared cache metadata:
  - `summaries/shared_cache_manifest.json`
  - `summaries/precompute_registry.json`
- Workers attach read-only dataset/precompute plans via deterministic signatures and emit run context metadata.

## Shared dataset + precompute behavior

- A deterministic dataset fingerprint is generated from canonical dataset path + file metadata.
- Dataset source mode is logged as one of:
  - `opened_memory_mapped` (single-file parquet)
  - `attached_from_cache` (dataset directory streaming path)
  - `fallback_loaded_normally`
- Parquet streaming now opens through `pyarrow.memory_map(..., "r")` when pyarrow is available.
- Precompute registry uses deterministic cache keys over:
  - dataset identity
  - timeframe
  - family
  - params
  - engine version

## New per-run artifacts

Each run directory can now include:

- `worker.log` (phase checkpoints, timing, pid, memory snapshots)
- `worker_exception.txt` (traceback for normal Python exceptions)
- `faulthandler.log` (fatal crash diagnostics)
- `run_context.json` (run metadata + effective thread caps + cache attach context)

## Scheduler behavior

- Wave size defaults to `2 * max_workers`.
- Each wave is submitted, awaited, finalized, and cleaned (`gc.collect()`) before next wave.
- Parent records richer failure context in `summaries/parallel_failures.json`.

## Limits / assumptions

- This iteration focuses on robust process orchestration, shared-read dataset planning, deterministic cache signatures, and observability.
- Indicator-family materialization remains conservative to preserve causal/no-lookahead semantics; registry entries are deterministic and auditable, and can be expanded to deeper precomputed series in future work.
