# Research Daemon (Phase 3)

This daemon runs approved backtest jobs continuously from the SQLite `queues` table.

## 1) Initialize database

```bash
python orchestrator/init_research_db.py --db research_db/research.sqlite
```

## 2) Queue a hypothesis

```bash
python orchestrator/queue_hypothesis.py \
  --db research_db/research.sqlite \
  --hypothesis research/hypotheses/<hypothesis>.yaml \
  --name <name> \
  --priority 80 \
  --max-workers 6
```

## 3) Run daemon once (smoke test)

```bash
python orchestrator/research_daemon.py \
  --db research_db/research.sqlite \
  --config orchestrator/daemon_config.yaml \
  --once
```

## 4) Run daemon 24/7 (tmux)

```bash
tmux new -s research-daemon

python orchestrator/research_daemon.py \
  --db research_db/research.sqlite \
  --config orchestrator/daemon_config.yaml
```

For a production queue with multiple hypothesis pipelines, prefer the global
capacity scheduler. It launches one-shot daemon jobs up to a shared worker
budget, pauses whole jobs under RAM pressure, and resumes them when memory
recovers without changing backtest semantics:

```bash
tmux new-session -d -s research_capacity_scheduler \
  'cd /home/omenka/Projects/bulletproof_bt && PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 orchestrator/global_capacity_scheduler.py --db research_db/research.sqlite --config orchestrator/daemon_config.yaml >> logs/research_capacity_scheduler_tmux.log 2>&1'
```

Monitor it with:

```bash
cat logs/research_capacity_scheduler_state.json
tail -f logs/research_capacity_scheduler.log
```

## 5) Optional systemd service

```ini
[Unit]
Description=Research Daemon
After=network.target

[Service]
Type=simple
WorkingDirectory=/home/omenka/Projects/bulletproof_bt
ExecStart=/home/omenka/Projects/.venv/bin/python orchestrator/research_daemon.py --db research_db/research.sqlite --config orchestrator/daemon_config.yaml
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```

## Notes

- Queue name defaults to `approved_backtests`.
- Logs are written to `logs/research_daemon.log`.
- Heartbeat is written to `logs/research_daemon_heartbeat.json`.
- Use `--dry-run` to preview the next command without executing a job.
- Research Memory runs automatically after a successful pipeline by default: `run_research_memory_after_pipeline: true`.
- Data mode defaults to canonical Binance `research_data/` panels:
  `data_mode: research_panel`, `data_root: research_data`, `data_kind: research_panel`,
  `exchange: binance`, `timeframe: 1m`.
- Stable grids use `--data-root research_data --data-kind research_panel --exchange binance --universe stable`.
- Volatile grids use `--data-root research_data --data-kind research_panel --exchange binance --universe volatile --membership-path research_data/manifests/volatile_universe_membership.parquet`.
- Legacy curated folders are ignored by default, even if stale queue payloads contain `stable_data` or `vol_data`. To intentionally use old curated folders, queue with `--data-mode legacy_curated --stable-data ... --vol-data ...`.

## Research Memory

The daemon builds or refreshes deterministic research memory after each successful pipeline. To run it manually after a batch:

```bash
python orchestrator/research_memory.py \
  --db research_db/research.sqlite \
  --outputs-root outputs \
  --verdicts-dir research/verdicts \
  --state-findings-dir research/state_findings \
  --alpha-zoo-dir research/alpha_zoo \
  --output-dir research/memory \
  --write-db
```

Query a live or proposed state snapshot:

```bash
python orchestrator/research_memory.py \
  --db research_db/research.sqlite \
  --query similar_state \
  --state-json current_state.json
```

This layer writes evidence and proposed recommendations only. It does not run backtests, queue hypotheses, approve gates, deploy strategies, or trade live.

## Interpretation (Phase 4): local Ollama default

### 1) Install Ollama (Ubuntu)

```bash
curl -fsSL https://ollama.com/install.sh | sh
```

### 2) Start Ollama

```bash
ollama serve
```

Or if installed as system service:

```bash
sudo systemctl enable ollama
sudo systemctl start ollama
sudo systemctl status ollama
```

### 3) Pull model

```bash
ollama pull qwen2.5:14b
```

### 4) Test model

```bash
ollama run qwen2.5:14b "Return JSON only: {\"ok\": true}"
```

### 5) Test API

```bash
curl http://127.0.0.1:11434/api/generate \
  -d '{
    "model": "qwen2.5:14b",
    "prompt": "Return JSON only: {\"ok\": true}",
    "stream": false
  }'
```

### 6) Run interpretation manually

```bash
python orchestrator/interpret_experiment_results.py \
  --db research_db/research.sqlite \
  --name <name> \
  --hypothesis research/hypotheses/<hypothesis>.yaml \
  --stable-root outputs/<phase>/<name>_parallel_stable \
  --vol-root outputs/<phase>/<name>_parallel_vol \
  --llm-provider ollama \
  --model qwen2.5:14b
```

### 7) Run without LLM

```bash
python orchestrator/interpret_experiment_results.py \
  --db research_db/research.sqlite \
  --name <name> \
  --hypothesis research/hypotheses/<hypothesis>.yaml \
  --stable-root outputs/<phase>/<name>_parallel_stable \
  --vol-root outputs/<phase>/<name>_parallel_vol \
  --llm-provider none
```

Expected outputs:

- `research/verdicts/<phase>/<name>_verdict.json`
- `research/verdicts/<phase>/<name>_verdict.md`
- `research/verdicts/<phase>/<name>_llm_packet.json`
- `research/verdicts/<phase>/<name>_llm_prompt.txt`
