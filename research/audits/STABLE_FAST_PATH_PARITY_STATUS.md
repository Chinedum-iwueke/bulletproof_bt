# Stable Fast Path Parity Status

Generated: 2026-06-11T15:50:20Z

## What Is Implemented

- Stable research panels can now carry causal precomputed HTF context columns
  (`htf_5m_*`, `htf_15m_*`, `htf_1h_*`).
- The classic engine can consume those columns through
  `PrecomputedHTFContextStrategyAdapter` when
  `data.htf_context_source: precomputed`.
- `scripts/compare_strategy_family_kernel.py` compares classic versus fast
  paths and now emits heartbeats during long gates.
- `scripts/compare_all_stable_fast_paths.py` runs one gate per
  hypothesis/timeframe and writes a resumable matrix.
- L7-H1 keeps its family-specific compiled feature/event path. Volatile remains
  classic fallback by design.

## Verified Gates In This Pass

| Hypothesis | Timeframe | Window | Result | Notes |
| --- | --- | --- | --- | --- |
| `l1_h2_compression_mean_reversion.yaml` | `5m` | 2025-05-05 to 2025-05-07 | PASS | Metrics, trades, equity matched. Generic HTF path was slower on this sample. |
| `l1_h11b.yaml` | `15m` | 2025-05-05T00:00Z to 2025-05-05T00:45Z | PASS | Metrics, trades, equity matched. Short smoke gate after stopping overlapping comparison jobs. |

Earlier L7-H1 stable gates passed in the existing comparison artifacts and are
the only confirmed material speedup path so far.

## Not Yet Complete

The all-hypothesis two-day matrix was interrupted by overlapping comparison
processes and partial output directories. It must be rerun from a clean output
root before claiming every existing family/timeframe passed parity.

Do not mark the remaining strategy families as enabled with a family-specific
kernel until their own matrix rows pass.

## Resume Command

Use a clean output root or delete partial directories first:

```bash
PYTHONPATH=src:. python3 scripts/compare_all_stable_fast_paths.py \
  --hypotheses-dir research/hypotheses \
  --output-root outputs/kernel_comparison/stable_family_parity_all \
  --config configs/engine.yaml \
  --local-config configs/local/engine.lab.yaml \
  --data-root research_data \
  --exchange binance \
  --timeframe 1m \
  --stable-manifest research_data/manifests/stable_universe.parquet \
  --start 2025-05-05 \
  --end 2025-05-07 \
  --max-workers 2 \
  --gate-workers 1 \
  --run-timeout-seconds 2400 \
  --clean
```

For quicker smoke gates, shorten the window, but treat that as a smoke test
rather than final family enablement.
