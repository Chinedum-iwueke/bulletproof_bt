# Experiment Dataset Builder

## Purpose

`extract_experiment_dataset.py` is the canonical post-experiment extraction step that converts a completed experiment root into compact, ML/research-ready datasets under `research_data/`.

This is designed for reusable hypothesis families (H1/H2/H3/H4/H5 and future families), not a one-off export script.

## Expected experiment-root structure

Typical root:

- `contract_snapshot/`
- `manifests/`
- `runs/`
- `summaries/`

Per-run artifacts used:

- `performance.json`
- `trades.csv`
- `config_used.yaml`
- `cost_breakdown.json` (optional enrichment)
- `run_status.json` (optional completeness/integrity signals)
- `performance_by_bucket.csv` (optional enrichment)

Experiment-level artifacts used:

- `summaries/run_summary.csv` (preferred run-level metric source when present)
- `manifests/*.csv` (manifest provenance and params)
- `contract_snapshot/*.yaml` (hypothesis provenance)

The extractor does **not** require giant log artifacts (`decisions.jsonl`, `fills.jsonl`) for v1.

## CLI usage

```bash
python scripts/extract_experiment_dataset.py \
  --experiment-root outputs/l1_h5a_parallel_vol \
  --runs-glob "runs/*" \
  --skip-existing
```

Arguments:

- `--experiment-root` (required)
- `--runs-glob` (default `runs/*`)
- `--out-dir` (default `<experiment_root>/research_data`)
- `--skip-existing`
- `--overwrite`
- `--verbose`
- `--min-trades-per-run` (default `1`)
- `--top-run-count-for-labels` (default `5`, reserved for label strategy tuning)

## Output files

Written under `<experiment_root>/research_data/`:

1. `trades_dataset.parquet`
2. `runs_dataset.parquet`
3. `dataset_manifest.json`
4. `feature_dictionary.json`
5. `experiment_summary.json`
6. `extraction_log.json`
7. `dropped_runs.csv`

## Schema contract versioning

`runs_dataset.parquet` and `trades_dataset.parquet` are emitted with a strict **v1** contract. The extraction metadata in `dataset_manifest.json` includes `schema_version: "v1"` so downstream consumers can enforce compatibility checks.

## Dataset contracts

### `trades_dataset.parquet`

One row per completed trade, with:

- experiment/run provenance (`experiment_id`, `hypothesis_id`, `dataset_tag`, `run_id`, `manifest_row_index`, `variant_id`, `parameter_set_id`, `params_json`)
- flattened params (`param_*` columns)
- trade identity/time/price fields
- trade outcomes (`pnl`, `pnl_pct`, `pnl_r`, `gross_pnl`, `net_pnl`, costs, MFE/MAE, duration, exit_reason, win_flag)
- optional context columns included only when present in `trades.csv`
- cross-run label enrichment (`run_net_pnl`, `run_sharpe`, `run_max_drawdown`, `run_trade_count`, `run_rank_by_net_pnl`, `run_is_top_decile`, `run_passes_min_trade_count`)

### `runs_dataset.parquet`

One row per run directory, with:

- provenance (`experiment_root`, `experiment_id`, `hypothesis_id`, `dataset_tag`, `run_id`, manifest linkage, params, config/snapshot paths)
- flattened `param_*` features
- run performance metrics (summary-first precedence)
- diagnostics/cost rollups and exit-reason distribution JSON
- integrity/status flags (`run_complete_flag`, `required_artifacts_present`, `parse_success_flag`, `dropped_reason`)
- ranking fields (`run_rank_by_net_pnl`, `run_rank_by_sharpe`, `run_is_top_decile`, `run_is_bottom_decile`)

Nullability is best-effort where source artifacts do not expose a field.

## Precedence rules

### Run-level metrics

1. `summaries/run_summary.csv` (if available)
2. `performance.json`
3. derived from `trades.csv` if needed

### Trade-level metrics

1. `trades.csv`
2. optional enrichment from lightweight artifacts (`cost_breakdown.json`, `performance_by_bucket.csv`)

### Parameter handling

- preserve `params_json` as original serialized JSON when available
- also flatten top-level params to `param_*`
- prevent collisions by namespacing under `param_`

## Failure and dropped-run behavior

- extraction fails loudly when no valid run-level rows can be produced
- extraction fails loudly when no trade rows can be produced
- dropped/excluded runs are explicitly written to `dropped_runs.csv`
- additional diagnostics (missing artifacts, fallback derivations, row counts, timing) are written to `extraction_log.json`

## Recommended workflow

1. Run grid execution.
2. Run `post_run_analysis.py`.
3. Run `extract_experiment_dataset.py`.
4. Verify `research_data/` outputs.
5. Optionally prune raw run artifacts.

## Retention guidance after verification

Keep permanently:

- `contract_snapshot/`
- `manifests/`
- `summaries/`
- `research_data/`

Keep selectively:

- top 3-5 run folders
- one median run
- one poor run
- anomaly/debug runs

Delete from most runs (after verification):

- `decisions.jsonl`
- `fills.jsonl`
- optionally full run folders once extracted datasets are validated and reference runs are retained
