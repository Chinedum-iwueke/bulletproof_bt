# Cleanup Experiment Runs

## Purpose

`cleanup_experiment_runs.py` is the canonical final step after grid execution, post-run analysis, and dataset extraction.

It enforces dataset readiness, ranks runs from `research_data/runs_dataset.parquet`, retains a curated reference subset, prunes heavy logs, optionally removes non-retained run folders, and writes full cleanup audit artifacts.

## CLI usage

```bash
python scripts/cleanup_experiment_runs.py \
  --experiment-root outputs/l1_h5a_parallel_vol \
  --runs-glob "runs/*" \
  --retain-top-n 5 \
  --retain-median 1 \
  --retain-worst 1 \
  --delete-logs \
  --delete-nonretained-runs
```

Arguments:

- `--experiment-root` (required)
- `--runs-glob` (default `runs/*`)
- `--out-dir` (default `<experiment_root>/research_data`)
- `--retain-top-n` (default `5`)
- `--retain-median` (default `1`)
- `--retain-worst` (default `1`)
- `--ranking-metric` (default `net_pnl`)
- `--delete-logs`
- `--delete-nonretained-runs`
- `--keep-equity-for-retained`
- `--skip-existing-extraction`
- `--overwrite-extraction`
- `--dry-run`
- `--verbose`

## Retention policy

Default retention set is built from ranking order:

1. top `N` runs (`--retain-top-n`)
2. median run (`--retain-median`)
3. worst run (`--retain-worst`)

Duplicates are naturally de-duplicated. Ranking defaults to:

- `net_pnl` descending
- `sharpe` descending (tie-break)
- `max_drawdown` ascending (tie-break)

Runs missing the ranking metric are excluded from primary ranking with explicit warnings. If no runs have a valid ranking metric, cleanup falls back to sharpe/max-drawdown ordering.

## Deletion rules

No deletion is attempted unless extraction outputs validate successfully.

### Retained runs

When `--delete-logs` is set:

- delete `decisions.jsonl`
- delete `fills.jsonl`
- delete `equity.csv` unless `--keep-equity-for-retained` is set

Keep run-level reference artifacts such as `trades.csv`, `performance.json`, `config_used.yaml`, and `cost_breakdown.json` (if present).

### Non-retained runs

- If `--delete-nonretained-runs`: delete full run folder.
- Else if `--delete-logs`: delete `decisions.jsonl` and `fills.jsonl` only.

## Audit artifacts

Written under `<experiment_root>/research_data/`:

1. `retention_plan.json`
2. `retained_runs.csv`
3. `deleted_runs.csv`
4. `cleanup_log.json`

`retention_plan.json` captures policy settings and retained/deleted run IDs. `deleted_runs.csv` captures per-action outcomes. `cleanup_log.json` captures deleted files/folders, warnings, skipped deletions, bytes reclaimed, and duration.

## Dry-run behavior

`--dry-run` writes the same planning and audit artifacts, computes what would be deleted, and performs no filesystem deletion.

## Recommended workflow

1. Run grid execution.
2. Run `post_run_analysis.py`.
3. Run `extract_experiment_dataset.py` (or let cleanup ensure extraction outputs).
4. Run `cleanup_experiment_runs.py --dry-run` first.
5. Inspect `research_data/retention_plan.json`.
6. Re-run cleanup without `--dry-run`.

> `decisions.jsonl` and `fills.jsonl` are normally deleted even for retained runs when `--delete-logs` is enabled.
