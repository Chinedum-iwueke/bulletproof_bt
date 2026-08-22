# BT-001 full-suite and contract-drift closure

BT-001 preserves the existing Bulletproof engine and closes drift around it. It does not add strategy logic or a parallel execution path.

## Supported environment

- Python: 3.11
- Runtime bounds: NumPy 2.2, pandas 2.2, PyArrow 23
- Reproducible development environment: `requirements/dev-py311.lock`

Install with:

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements/dev-py311.lock
.venv/bin/python -m pip install --no-deps -e .
```

## Declared matrix

```bash
.venv/bin/python scripts/run_bt001_matrix.py collection
.venv/bin/python scripts/run_bt001_matrix.py all
.venv/bin/python -m ruff check scripts/run_bt001_matrix.py
.venv/bin/python -m mypy --ignore-missing-imports src/bt/exec/observability/channels.py src/bt/research_data/jobs/daily_validation.py src/bt/research_data/live/liquidation_collector.py src/bt/risk/reject_codes.py scripts/run_bt001_matrix.py
```

The deterministic shards are `exec`, `hypotheses`, `auxiliary`, and `root-1` through `root-4`. Root test modules are sorted by path and distributed round-robin, so membership is stable across runs.

## Closed drift

- Pytest collection is scoped to source tests and no longer traverses research outputs.
- The Python 3.11 environment is dependency bounded; pandas 3 is outside the supported contract.
- Existing H10A, H10B, H2B, and H3C strategy contracts deleted during repository cleanup are restored from Git history.
- Assertions follow the current preregistered grids and explicit signal-timeframe semantics.
- Research-data validation reads both legacy and market-namespaced layouts.
- Operational run identifiers are excluded from deterministic outcome comparisons.
- Alert files flush each record durably.

The milestone evidence is the clean declared matrix, clean collection, and scoped lint/type results recorded in the Invariance Research Bible evidence ledger. Repository-wide Ruff currently reports 114 inherited findings and `risk_engine.py` has 18 inherited mypy findings; BT-001 records those baselines rather than mixing broad cleanup into contract closure.
