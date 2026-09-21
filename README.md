## Copyright

Copyright (c) 2026 Chinedum Iwueke.

# Bulletproof BT

Bulletproof BT is the quantitative engine and market-data laboratory for
Invariance Research. It performs deterministic, event-driven research,
backtesting, portfolio and risk computation, shadow replay, and tightly gated
venue execution.

Its central invariant is:

> Same admitted data + same reviewed code + same configuration = identical
> evidence. No lookahead, no interpolation, and no silent assumptions.

Bulletproof is not the company control plane. Hermes Swarm owns agents,
approvals, task lifecycles, evidence registration, and operational visibility.
Bulletproof owns the numerical work and emits immutable producer receipts that
Hermes can validate and retain without reimplementing the calculation.

## Intended Research Loop

Together with Hermes, this repository is intended to support a continuous loop:

1. inventory real market data and construct point-in-time eligible universes;
2. transform admitted 1-minute data into the causal representation required by
   a predictive question;
3. compile a falsifiable hypothesis into a native strategy contract;
4. execute a preregistered, bounded search through the classic engine;
5. evaluate walk-forward behavior, costs, drawdowns, leakage, selection bias,
   trade support, and reproducibility;
6. retain positive, negative, invalid, and failed results with full artifacts;
7. publish terminal evidence to Bulletproof memory and Hermes;
8. monitor admitted candidates prospectively before any live capital decision.

The system is designed to discover and falsify micro-alpha around the clock. It
does not promise alpha, tune indefinitely, or convert a backtest into trading
authority.

## Cross-Repository Ownership

| Concern | Bulletproof | Hermes Swarm |
| --- | --- | --- |
| Market-data lake and panels | Authoritative producer | Catalogs admitted digests and availability |
| Data representation and resampling | Deterministic implementation | Selects and approves bounded intent |
| Strategy code and hypothesis YAML | Native implementation | Engineering task, approval, provenance |
| Backtests, grids, ML/RL evaluation | Authoritative computation | Scheduling, independent review, lifecycle |
| Portfolio, risk, OMS and venue replay | Authoritative computation | Registry, visibility, approval and audit |
| Research memory | Native run/result memory | Institutional evidence and cross-agent context |
| Live orders and capital | Fail-closed runtime only | Separate founder/risk authority and supervision |

Control-plane fixtures are never quantitative evidence. Bulletproof receipts do
not grant themselves promotion, shadow, order, or capital authority.

## Current Capability

### Deterministic research engine

- event-driven, bar-by-bar execution with closed-bar strategy input;
- strict no-lookahead and causal source-timestamp validation;
- crypto, FX, equity, and basic futures instrument abstractions;
- tiered fees, spread, slippage, delay, margin, liquidation, and stop models;
- risk-normalized sizing and canonical R-multiple accounting;
- walk-forward and out-of-sample evaluation, cost stress, benchmarks, and
  selection-bias evidence;
- schema-versioned run bundles, decisions, fills, trades, metrics, manifests,
  status, and failure artifacts.

### Research-data lake

- Binance, Bybit, and OKX perpetual futures adapters;
- Binance and Bybit spot data support;
- OHLCV, mark, index, funding, open-interest, and liquidation collection where
  the venue exposes the source;
- canonical research panels with backward-as-of joins and preserved source
  timestamps;
- stable, volatile, and point-in-time custom universe construction;
- inventory, coverage, quality, entitlement, lineage, and immutable manifest
  receipts;
- no filling or interpolation of missing market bars.

The lake's named stable and volatile universes are useful inputs, not permanent
research boxes. A reviewed experiment may select any point-in-time eligible
asset or basket across admitted data, provided the selection rule is frozen
before outcomes are observed.

### Representation and search

- strict whole-minute resampling from a UTC 1-minute base feed;
- arbitrary positive `m`, `h`, and `d` durations such as `7m`, `12m`, `2h`, and
  `2d`;
- rollover-only emission of complete higher-timeframe buckets;
- no seconds, fractional durations, calendar months, or strategy-visible
  incomplete buckets;
- bounded hypothesis grids and parallel execution with deterministic worker
  plans, memory limits, heartbeats, diagnostics, and restart-safe queue state;
- the governed alpha path limits one hypothesis to at most eight preregistered
  variants; additional compute may run separate approved hypotheses in parallel.

### Institutional quantitative producers

Bulletproof owns the computational producers for DATA, discovery, ML, offline
RL, portfolio, risk, execution, shadow, demo, and venue-telemetry milestones.
These modules emit digest-bound no-authority receipts for Hermes rather than
moving analytics into the control plane.

ML and RL components are evaluation tools inside the governed research process.
They do not autonomously train, deploy, or fund a policy without their own data,
evaluation, calibration, admission, and authority receipts.

### Execution path

- canonical market, order, fill, position, balance, and clock events;
- idempotent OMS lifecycle and restart reconciliation;
- simulated, Bybit, and Binance adapter surfaces;
- paper, shadow, Bybit demo, and live runtime entry points;
- execution-quality calibration and degradation feedback;
- deterministic real-time risk, freeze, kill, recovery, and canary controls;
- canonical venue replay and Mission Control publication through Hermes.

Authenticated Bybit demo drills and canonical venue-replay publication have
been exercised in the wider system. This is operational evidence, not evidence
of profitability. Live code remains fail-closed behind candidate admission,
environment-specific certification, trade-only credentials, deterministic risk,
short-lived founder approval, and a serialized micro-live canary.

## Current Qualification Boundary

- There is no blanket claim that the strategy catalog contains profitable
  alpha.
- Commissioning slices can verify wiring and artifacts but cannot satisfy full
  scientific qualification.
- A full candidate must survive its frozen historical window, out-of-sample and
  cost gates, independent review, and prospective shadow monitoring.
- Negative and invalid outcomes are first-class retained research results.
- Fast paths require parity with the classic engine before their evidence can be
  used.
- `LIVE-001` is `blocked_before_capital` until its external operational and
  candidate prerequisites are genuinely current.
- Venue credentials and research datasets are local operational state and must
  never enter Git or producer receipts.

## Install

Bulletproof uses Python 3.11+ and PEP 621 packaging.

```bash
git clone https://github.com/Chinedum-iwueke/bulletproof_bt.git
cd bulletproof_bt
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest -q
```

The distribution name is `bulletproof_bt`; the import module and primary CLI
are `bt`.

```bash
bt --help
python scripts/run_backtest.py --help
```

## Common Workflows

Run one classic backtest:

```bash
python scripts/run_backtest.py \
  --data <PATH> \
  --config configs/engine.yaml
```

Run an experiment grid:

```bash
python scripts/run_experiment_grid.py \
  --config configs/engine.yaml \
  --experiment configs/experiments/h1_volfloor_donchian.yaml \
  --data <PATH> \
  --out <OUT_DIR>
```

Inspect and validate the native lake:

```bash
python -m bt.research_data.cli refresh-instruments --exchange all
python -m bt.research_data.cli fetch-status
python -m bt.research_data.cli validate --all
```

Backfill and build a causal Binance panel:

```bash
python -m bt.research_data.cli fetch-backfill \
  --market perp \
  --exchange binance \
  --dataset ohlcv \
  --symbol BTCUSDT \
  --timeframe 1m \
  --start 2021-01-01 \
  --end now

python -m bt.research_data.cli build-panel \
  --exchange binance \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT \
  --timeframe 1m
```

Run a parallel grid from canonical panels:

```bash
python scripts/run_parallel_hypothesis_grid.py \
  --experiment-root <OUTPUT_ROOT> \
  --manifest <GRID_MANIFEST> \
  --config configs/engine.yaml \
  --local-config configs/local/engine.lab.yaml \
  --data-root research_data \
  --data-kind research_panel \
  --exchange binance \
  --universe stable \
  --timeframe 1m \
  --max-workers 8 \
  --skip-completed
```

For governed Hermes work, use
[`scripts/run_alpha_research_assignment.py`](scripts/run_alpha_research_assignment.py)
and the capacity queue scripts rather than bypassing the approved assignment.

## Artifact Contract

A normal classic run emits a schema-versioned directory containing, at minimum:

```text
run_<id>/
  config_used.yaml
  performance.json
  equity.csv
  trades.csv
  fills.jsonl
  decisions.jsonl
  performance_by_bucket.csv
  cost_breakdown.json
  summary.txt
  run_manifest.json
  run_status.json
```

Governed research adds the frozen hypothesis and representation contracts,
truth and leakage reports, search ledger, selection-bias evidence, independent
review, atomic bundle digest, publication envelope, and memory receipt. Failures
must preserve enough evidence to diagnose the stage and must not be rewritten as
successful runs.

## Project Structure

```text
src/bt/core/                    Engine and configuration resolution
src/bt/data/                    Validation and strict resampling
src/bt/research_data/           Exchange adapters, lake, panels, universes
src/bt/strategy/                Native strategies and strategy contract
src/bt/evaluation/              Alpha and held-out evaluation
src/bt/governance/              Qualification and Hermes research bridge
src/bt/institutional/           Digest-bound quantitative producers
src/bt/exec/                    OMS, adapters, runtime, replay, safety
src/bt/risk/                    Sizing, stops, margin, deterministic risk
src/bt/portfolio/               Cash, positions, liquidation accounting
orchestrator/                   Research daemons and durable orchestration
research/hypotheses/            Preregistered hypothesis YAML contracts
research/audits/                Retained investigations and admission evidence
scripts/                        Operator, pilot, grid, capacity, and venue CLIs
tests/                          Determinism, causality, contract, and regression tests
```

## Documentation

Start with:

- [Core contract](docs/core_contract.md)
- [Dataset contract](docs/dataset_contract.md)
- [Research data](docs/research_data.md)
- [Timeframe resampling](docs/timeframe_resampler.md)
- [Hypothesis contract](docs/hypothesis_contract.md)
- [Strategy generation instructions](docs/hypothesis_strategy_generation_prompt_instructions.md)
- [Backtest truth certification](docs/backtest_truth_certification.md)
- [Governed research bridge](docs/governed_research_bridge.md)
- [Parallel grid runner](docs/parallel_grid_runner.md)
- [Alpha commissioning](docs/alpha_commissioning.md)
- [Execution contract](docs/exec_contract.md)
- [Bybit adapter contract](docs/bybit_adapter_contract.md)
- [Live hardening contract](docs/bybit_live_hardening_contract.md)
- [LIVE-001 validation](docs/live-001-validation.md)

Historical validation documents describe evidence at a particular commit. They
do not replace a fresh baseline, run receipt, or production health check.

## Machine-Verifiable Baseline

The read-only `implementation-baseline-v1` collector records the Git pin and
dirty state, sanitized origin, runtime versions, dependency state, tracked
schema hashes, declared acceptance commands, and controlled claim vocabulary.
It does not read credentials, ignored files, or the market-data lake.

```bash
python scripts/implementation_baseline.py collect \
  --repository . \
  --output /tmp/bulletproof-baseline.json
python scripts/implementation_baseline.py validate \
  /tmp/bulletproof-baseline.json
```

Collection fails closed on a dirty worktree. `--allow-dirty` records the dirty
paths for audit purposes but does not create a release-quality baseline.

## Security and Trading Safety

Never commit exchange keys, account identifiers, operator tokens, private data,
or environment files. Demo and live credentials must be separate, trade-only,
withdrawal-disabled, IP-restricted, root-owned, and mode `0600`. A live runtime
must start read-only or frozen unless every exact candidate, environment, risk,
reconciliation, kill, and approval prerequisite is current.
