# Fast Path Deprecation

## Status

The backtest fast path is deprecated and disabled for production research runs.
The classic event-driven engine is the only production source of truth.

Configs may still contain:

```yaml
execution_engine: auto
```

or:

```yaml
execution_engine: fast_path
```

but both are downgraded to classic execution. Each run writes
`fast_path_status.json` with:

- `handled: false`
- `actual_engine: classic`
- `fast_path_deprecated: true`
- `mode: classic_deprecated_fallback`

## Why It Was Deprecated

The fast-path work produced useful infrastructure, but not a universal
truth-preserving speedup across stable and volatile research:

- strategy-family kernels were not generic enough;
- volatile membership streams made static feature columns hard to keep exactly
  equivalent to classic bar-by-bar state;
- materialized volatile panels added storage and maintenance burden;
- parity gates were family/timeframe/data-profile specific;
- the speedups were not meaningful enough to justify the operational surface.

Backtesting accuracy, no-lookahead behavior, portfolio accounting, fills, risk
sizing, and rich artifacts are more important than runtime shortcuts.

## What Remains

The following modules remain in the repository as reference and testable design
work:

- `src/bt/engine/fast_path/data_session.py`
- `src/bt/engine/fast_path/feature_registry.py`
- `src/bt/engine/fast_path/candidate_events.py`
- `src/bt/engine/fast_path/family_kernels.py`
- `src/bt/engine/fast_path/l7_h1_kernel.py`

They must not be used to change production run behavior unless a future
architecture provides a universal, deterministic, parity-tested execution model.

## What Still Stays Active

This deprecation does not affect:

- research_data downloads;
- optimized raw/canonical parquet ingestion;
- stable and volatile research panel loaders;
- rich state features;
- Tier2A signal episode mode;
- Tier2B/Tier2 portfolio simulation mode;
- global daemon scheduling and resource controls;
- post-run analysis, verdict cards, state findings, and research memory.

Files named `bootstrap_research_data_fast.py` are data-ingestion accelerators,
not the deprecated backtest fast path.

## Cleanup Guidance

Safe cleanup candidates after confirming no active process needs them:

- `outputs/kernel_comparison/`
- old parity comparison outputs;
- old fast-path audit reports.

Do not delete research panels or `_volatile_active` materialized panels unless
the current loader configuration has been checked. They may still be useful as
data-layout artifacts, even though the backtest execution fast path is disabled.

