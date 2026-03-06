# H1B Parallel Grid Runner (volfloor_donchian + volfloor_ema_pullback)

This repo now includes a **repo-safe process-level parallel runner** for H1B strategy sweeps.

## Scripts

- `scripts/build_volfloor_donchian_grid.py`
- `scripts/build_volfloor_ema_pullback_grid.py`
- `scripts/run_parallel_grid.py`
- `scripts/grid_status.py`

## What gets generated

Under `--experiment-root`, the scripts produce:

- `manifests/<strategy>_grid_36.csv`
- `overrides/<run_id>.yaml`
- `runs/<run_id>/...` (isolated run artifacts)
- `logs/<run_id>.log`
- `status/grid_status.csv`
- `status/failures.csv`

## Build the 36-run manifest + overrides

```bash
python scripts/build_volfloor_donchian_grid.py \
  --experiment-root outputs/volfloor_donchian_parallel

python scripts/build_volfloor_ema_pullback_grid.py \
  --experiment-root outputs/volfloor_ema_pullback_parallel
```

Both grids are deterministic (ordered by `vol_floor`, then `adx_min`, then `er_min`) and create run IDs like:

- `run_001__vol60_adx18_er035_n16`

## Launch in parallel

```bash
python scripts/run_parallel_grid.py \
  --experiment-root outputs/volfloor_donchian_parallel \
  --manifest outputs/volfloor_donchian_parallel/manifests/volfloor_donchian_grid_36.csv \
  --base-config configs/engine.yaml \
  --data data/crypto_5m_sample \
  --max-workers 10 \
  --skip-completed
```

### Resume modes

- `--skip-completed`: skip runs with successful `run_status.json` + required artifact set.
- `--retry-failed`: run only runs currently classified failed.
- `--dry-run`: print status/output structure without launching backtests.
- `--run-filter <substring>`: run only matching `run_id`s.

## Status summary

```bash
python scripts/grid_status.py \
  --status-csv outputs/volfloor_donchian_parallel/status/grid_status.csv
```

## Notes

- Parallelism is **across runs** only; each run remains single-process engine execution.
- The runner reuses the existing run entrypoint (`scripts/run_backtest.py`) and engine APIs.
- Recommended starting point on a 36-core / 60 GB VM: `--max-workers 10`.
