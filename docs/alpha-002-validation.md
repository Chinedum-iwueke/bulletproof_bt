# ALPHA-002 Native Scientific Executor

## Ownership

Bulletproof owns every quantitative operation in ALPHA-002. Hermes supplies an
immutable no-capital assignment; this repository verifies the assignment, reads the
admitted panel, resolves an exact registered hypothesis, runs the classic engine and
emits the native evidence. Hermes does not recreate returns, costs, trades or
statistics.

## Resolution and unsupported work

Only an explicit `hypothesis_id:`/`strategy:` marker or a full exact registered title
can select a hypothesis contract. Similarity and nearest-strategy fallback are
forbidden. If no exact implementation exists, the executor writes a cited
`strategy-engineering-requirement.json`, reports `strategy_generation`, and records
zero trials. It never pretends that a contract-only artifact is a backtest.

## Native execution

The first execution version prospectively selects one value from each already
registered parameter dimension and records a one-trial exhaustive search plan. It
binds the actual panel SHA-256, full repository commit, point-in-time 60/20/20 split,
classic Tier2 market model and representation/search/code digests. The classic engine
is the sole execution and accounting authority.

Experiment truth must pass before the run bundle is finalized. The final 20% temporal
window is evaluated independently from the earlier observations. Candidate support
requires at least 50 held-out trades, positive held-out mean net R and positive mean
net R after subtracting the observed cost drag once more. All failures remain in the
receipt.

The finalized bundle is copied atomically to a content-addressed durable directory;
an existing destination must carry the same manifest digest. A native SQLite memory
precommit binds the bundle, campaign, question and dataset digests. Neither receipt
contains order, capital, promotion or self-approval authority.

## Tests

```bash
.venv/bin/python -m pytest -q tests/test_alpha_research_assignment.py
.venv/bin/ruff check \
  scripts/run_alpha_research_assignment.py \
  tests/test_alpha_research_assignment.py
```

The production qualification receipt is retained in the company Bible. A successful
backtest is not a live-trading authorization.
