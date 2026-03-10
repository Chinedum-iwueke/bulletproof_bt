# Hypothesis Parallel Grid Workflow

This framework generalizes parallel grid execution for **any HypothesisContract**.

## Serial vs Parallel

- `src/bt/experiments/hypothesis_runner.py` owns single-run execution semantics (contract loading, tier runtime overrides, engine invocation).
- `src/bt/experiments/parallel_grid.py` orchestrates many rows from a manifest in bounded process parallelism.

## Build manifest from any hypothesis

```bash
python scripts/build_hypothesis_grid.py \
  --hypothesis research/hypotheses/l1_h1_vol_floor_trend.yaml \
  --experiment-root outputs/l1_h1_parallel_stable \
  --phase tier2
```

Outputs:

- `manifests/<hypothesis_stem>_<phase>_grid.csv`
- `contract_snapshot/<hypothesis>.yaml`
- `summaries/<hypothesis_stem>_<phase>_grid_summary.json`

## Run in parallel

```bash
python scripts/run_parallel_hypothesis_grid.py \
  --experiment-root outputs/l1_h1_parallel_stable \
  --manifest outputs/l1_h1_parallel_stable/manifests/l1_h1_vol_floor_trend_tier2_grid.csv \
  --config configs/engine.yaml \
  --local-config configs/local/engine.lab.yaml \
  --data /home/omenka/research_data/bt/curated/stable_data_1m_canonical \
  --max-workers 6 \
  --skip-completed
```

## Skip-completed semantics

A row is considered completed only when:

1. run output directory exists
2. `run_status.json` exists and contains `status=PASS`
3. all required engine artifacts are present

Partial directories are treated as `INCOMPLETE` and will rerun.

## Output structure

- `runs/row_00001__<variant>__tier2/...`
- `summaries/manifest_status.csv`
- `summaries/failures.csv`
- `summaries/phase_rollup.csv`
- `summaries/phase_segment_rollups.csv` (aggregated per-run metadata segment metrics)

## L1-H1 usage

The same workflow supports Tier2, Tier3, and validate phases without strategy hardcoding because parameters/timeframes are sourced from the L1-H1 hypothesis contract and executed through the shared hypothesis runner path.

## Practical worker count

Start with `--max-workers 4..8` depending on CPU and data I/O throughput.

## Reusable metadata segment rollups

Each completed run now emits:

- `segment_rollups.csv`
- `segment_rollups.jsonl`

These are computed from entry metadata persisted in `fills.jsonl` and joined to closed trades in `trades.csv` by `(symbol, entry_ts, side)`. Segment rollups are generic (not hypothesis-hardcoded) and can group by one or more entry metadata keys (for example `gate_pass`, `q_comp`, `rvhat_pct_t`, `entry_reason`).

Default first-use keys are strategy-aware for immediate usefulness:

- L1-H2: `entry_reason`, `q_comp`
- L1-H1: `gate_pass`
- L1-H3: `rvhat_pct_t`

For L1-H2 specifically, `comp_gate_t` is typically constant at entry due to gate filtering, so `entry_reason` and `q_comp` are the first informative production segments.
