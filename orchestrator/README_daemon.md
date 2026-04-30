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
  --stable-root outputs/<name>_parallel_stable \
  --vol-root outputs/<name>_parallel_vol \
  --llm-provider ollama \
  --model qwen2.5:14b
```

### 7) Run without LLM

```bash
python orchestrator/interpret_experiment_results.py \
  --db research_db/research.sqlite \
  --name <name> \
  --hypothesis research/hypotheses/<hypothesis>.yaml \
  --stable-root outputs/<name>_parallel_stable \
  --vol-root outputs/<name>_parallel_vol \
  --llm-provider none
```

Expected outputs:

- `research/verdicts/<name>_verdict.json`
- `research/verdicts/<name>_verdict.md`
- `research/verdicts/<name>_llm_packet.json`
- `research/verdicts/<name>_llm_prompt.txt`
