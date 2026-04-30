# Research Dashboard

Install dependencies:

```bash
pip install fastapi uvicorn jinja2 python-multipart
```

Run:

```bash
python orchestrator/run_dashboard.py --db research_db/research.sqlite --host 127.0.0.1 --port 8765
```

Open: http://127.0.0.1:8765

SSH tunnel:

```bash
ssh -L 8765:127.0.0.1:8765 omenka@<vm-ip>
```

Enable actions:

```bash
python orchestrator/run_dashboard.py --db research_db/research.sqlite --host 127.0.0.1 --port 8765 --enable-actions
```
