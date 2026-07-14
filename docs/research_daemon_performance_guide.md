# Research Daemon Performance Guide

This daemon is designed for long-running hypothesis research without weakening backtest truth. Speed settings should never bypass no-lookahead joins, strict HTF completeness, missing-bar behavior, execution costs, or required artifacts.

## Recommended Settings

For this VM class, roughly 60 GB RAM and 36 cores:

- Start with `--max-workers 6`.
- Use `--max-workers 8` once RSS is stable across a full stable+volatile pipeline.
- Use `--max-workers 4` when Ollama or other CPU-heavy processes are active.
- Use `--max-workers 12` only after benchmark runs show enough RAM headroom and no swap pressure.

The daemon config currently defaults to 6 workers with:

```yaml
runner_reserve_ram_gb: 8
runner_min_free_ram_gb: 6
runner_fail_fast: false
runner_resume_strict: true
```

For automatic worker sizing:

```bash
PYTHONPATH=src python3 scripts/run_parallel_hypothesis_grid.py ... \
  --max-workers 8 \
  --max-workers-auto \
  --reserve-ram-gb 8 \
  --min-free-ram-gb 6
```

## Monitoring

Useful commands:

```bash
tmux list-sessions
tail -f logs/research_daemon.log
cat logs/research_daemon_heartbeat.json
ps -eo pid,ppid,stat,%cpu,%mem,rss,cmd --sort=-rss | head -30
free -h
```

Watch for sustained swap usage, workers above expected RSS, repeated `BrokenProcessPool`, or heartbeat stages stuck much longer than normal.

## State Feature Profiles

Default:

```yaml
state_features:
  enabled: true
  profile: full
```

`full` preserves rich entry-state fields including mark/index/funding/OI/liquidation features. `minimal` keeps causal OHLCV state only: ATR/ATR%, volatility percentile, spread proxy percentile, TR/ATR displacement, CSI proxy, trend state, and time/day context.

Use `minimal` only when throughput matters more than perp feature enrichment for that batch.

## Decision Logging Profiles

Long volatile grids can spend most of their wall time and disk bandwidth writing
diagnostic state-only decisions. The engine supports three profiles:

- `full`: write every emitted decision record. Use for short parity/debug runs.
- `research_sparse`: write approved orders and real risk rejections, but skip
  `state_log_only` diagnostic signals. The run writes
  `decision_logging_summary.json` with deterministic written/skipped counts.
- `parity_debug`: full behavior except optional deterministic sampling of
  state-only negatives via `decision_negative_sample_every`.

This does not change strategy evaluation, risk sizing, execution, fills, trades,
equity, or performance metrics. It only changes how much diagnostic non-action
JSONL is serialized. Truth artifacts remain `fills.jsonl`, `trades.csv`,
`equity.csv`, `performance.json`, and the run configuration.

For long daemon research use:

```yaml
outputs:
  decision_logging_profile: research_sparse
  decision_negative_sample_every: 0
```

For byte-for-byte decision-log parity tests use:

```yaml
outputs:
  decision_logging_profile: full
```

## Safe Overnight Runs

Use the daemon, not a manually looped shell:

```bash
tmux new-session -d -s research_daemon_24_7 \
  'cd /home/omenka/Projects/bulletproof_bt && PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 orchestrator/research_daemon.py --db research_db/research.sqlite --config orchestrator/daemon_config.yaml >> logs/research_daemon_tmux.log 2>&1'
```

Keep `runner_resume_strict: true`. A stale `run_status.json` must not hide missing artifacts.

## Global Capacity Scheduler

For mixed long-running queues, prefer the global capacity scheduler over
starting several independent daemon sessions by hand. The scheduler launches
one-shot daemon jobs up to a global worker budget, pauses whole process groups
when free RAM falls below the pressure threshold, refreshes their queue locks
while paused, and resumes them when RAM recovers.

It does not change a running grid's math, strategy path, data, fills, PnL, or
artifacts. It only controls wall-clock scheduling around whole daemon jobs.
Existing multiprocessing pools cannot be safely resized mid-run, so dynamic
"more workers" means launching additional queued jobs when there is headroom.

Recommended start command:

```bash
tmux new-session -d -s research_capacity_scheduler \
  'cd /home/omenka/Projects/bulletproof_bt && PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 orchestrator/global_capacity_scheduler.py --db research_db/research.sqlite --config orchestrator/daemon_config.yaml >> logs/research_capacity_scheduler_tmux.log 2>&1'
```

Useful live checks:

```bash
cat logs/research_capacity_scheduler_state.json
tail -f logs/research_capacity_scheduler.log
tmux list-sessions
ps -eo pid,ppid,pgid,stat,%cpu,%mem,rss,cmd --sort=-rss | head -40
```

Default scheduler config:

```yaml
capacity_scheduler:
  target_workers: 28
  max_workers_per_job: 12
  max_concurrent_jobs: 4
  min_free_ram_gb: 8
  pause_free_ram_gb: 6
  resume_free_ram_gb: 12
```

Use the hysteresis gap between `pause_free_ram_gb` and `resume_free_ram_gb` so
jobs do not rapidly stop/start around one threshold. If Proxmox reports high
memory while Linux `MemAvailable` is healthy, the scheduler still follows the
guest's available-memory signal because that is the correct pressure indicator
inside the VM.

## Recovery

If a run fails:

1. Stop the daemon cleanly.
2. Inspect `logs/research_daemon.log` and the experiment command logs under `outputs/<phase>/*_daemon_command_logs/`.
3. Fix the root cause.
4. Requeue or unlock the affected queue item.
5. Restart the daemon.

Do not delete run directories before extraction unless the experiment is intentionally discarded.

## Cleanup

Cleanup may delete large logs only after these exist:

- `summaries/run_summary.csv`
- `research_data/runs_dataset.parquet`
- `research_data/trades_dataset.parquet`
- retained run selection artifacts

Do not delete `config_used.yaml`, `trades.csv`, `equity.csv`, or `performance.json` for runs you may need to resume or audit.

## Benchmarking

Run:

```bash
PYTHONPATH=src python3 scripts/benchmark_runner_performance.py \
  --hypothesis research/hypotheses/<hypothesis>.yaml \
  --data-root research_data \
  --data-kind research_panel \
  --exchange binance \
  --universe stable \
  --phase tier2 \
  --runs 4 \
  --max-workers 2
```

Outputs are written to:

- `research/audits/runner_benchmark_<timestamp>.json`
- `research/audits/runner_benchmark_<timestamp>.md`

## What Not To Optimize Away

Do not add interpolation, synthetic bars, future-aware funding/OI joins, always-active volatile universes, stale artifact reuse, or shared mutable market-data caches across incompatible configs.
