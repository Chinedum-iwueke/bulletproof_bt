# Hypothesis Strategy Generation Prompt Instructions

Use this document when creating a new Bulletproof_bt hypothesis family from a
plain-English hypothesis card. The output must be a complete research-ready
strategy package: hypothesis YAML, strategy Python module, strategy-family
compiled feature kernel, tests, docs, and daemon-compatible run wiring.

## Non-Negotiables

Do not weaken any Bulletproof_bt truth guarantees:

- No lookahead.
- No interpolation or synthetic bars.
- Missing bars mean missing decisions.
- Strict UTC timestamps.
- HTF signals must use only fully closed HTF bars.
- Funding and OI may only be used when available at or before the decision bar.
- Execution semantics remain in the classic engine unless a compiled execution
  kernel has parity tests for fees, slippage, spread, delay, intrabar behavior,
  stop handling, risk sizing, fills, decisions, trades, and equity.
- Rich logs must remain: decisions, fills, trades, equity, performance, state
  features, structural buckets, verdict artifacts, and research memory inputs.
- Every generated strategy must be able to pass the experiment truth gate in
  `scripts/validate_experiment_truth.py` before its artifacts can feed verdicts,
  state discovery, terminal cards, or research memory.

## Required Clarifications Before Generating Code

If a hypothesis card is ambiguous, ask or make an explicit conservative choice
and document it in the hypothesis YAML and strategy doc. The generated package
must clarify:

- Which dataset it expects: OHLCV-only, enriched research panel, stable universe,
  volatile universe, or both.
- Which timestamp each signal feature is allowed to use.
- Which HTF bars are required and how closed-bar completeness is enforced.
- Whether funding/OI/mark/index/basis are signal inputs, logging-only context,
  or unavailable fallbacks.
- Exact stop contract: stop source, stop update policy, and whether unresolved
  stops reject entries.
- Exact risk contract: `risk.r_per_trade`, maximum positions, maximum entry
  notional, maximum gross notional, and whether pyramiding/flips are allowed.
- Whether the strategy is expected to be deployable under
  `risk.max_notional_pct_equity: 0.005` and `risk.max_leverage: 1.0`.
- Expected run artifact fields needed for post-run learning.
- Falsification conditions that should scrap or refine the hypothesis.

If the card implies unconstrained exposure, hidden leverage, unresolved stops,
future settlement values, or discretionary exits that cannot be represented
deterministically, reject or rewrite that part before producing strategy code.

## Required Files For A New Hypothesis

Given a hypothesis card, generate:

- `research/hypotheses/<hypothesis_id>.yaml`
- `src/bt/strategy/<strategy_name>.py`
- `src/bt/engine/fast_path/<strategy_family>_kernel.py`
- `docs/hypotheses/<hypothesis_id>.md`
- Contract and integration tests under `tests/hypotheses/` and `tests/`

Update:

- `src/bt/strategy/__init__.py` to import/register the strategy.
- `src/bt/engine/fast_path/signal_compiler.py` to advertise the compiled
  feature kernel when the strategy and data shape are supported.
- `src/bt/engine/fast_path/batch_runner.py` to report the selected safe mode.
- `src/bt/research_data/jobs/state_features.py` and
  `src/bt/research_data/cli.py` if the kernel needs materialized feature
  columns.

## Hypothesis YAML Contract

The YAML must define:

- `hypothesis_id`
- `name`
- `description`
- `hypothesis_family`
- `status`
- `strategy.name`
- `strategy.params`
- `grid` for Tier2/Tier3 parameters
- `evaluation.metrics`
- `falsification_criteria`
- `expected_failure_modes`
- logging requirements for required state fields and decision trace
- risk assumptions and deployment constraints
- data availability assumptions and OHLCV-only fallback behavior
- truth-gate expectations, including whether forced liquidations are expected
  or should invalidate the run

Use `dataset_kind: research_panel` through daemon/run configs, not legacy
external curated folders. New research runs should use:

```bash
PYTHONPATH=src python3 scripts/run_parallel_hypothesis_grid.py \
  --experiment-root outputs/tier2/<hypothesis>_parallel_stable \
  --manifest outputs/tier2/<hypothesis>_parallel_stable/manifests/<manifest>.csv \
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

Volatile runs must use:

```bash
PYTHONPATH=src python3 scripts/run_parallel_hypothesis_grid.py \
  --experiment-root outputs/tier2/<hypothesis>_parallel_vol \
  --manifest outputs/tier2/<hypothesis>_parallel_vol/manifests/<manifest>.csv \
  --config configs/engine.yaml \
  --local-config configs/local/engine.lab.yaml \
  --data-root research_data \
  --data-kind research_panel \
  --exchange binance \
  --universe volatile \
  --membership-path research_data/manifests/volatile_universe_membership.parquet \
  --timeframe 1m \
  --max-workers 8 \
  --skip-completed
```

## Strategy.py Requirements

Every strategy must:

- Register with `@register_strategy("<strategy_name>")`.
- Implement `on_bars(ts, bars_by_symbol, tradeable, ctx)`.
- Respect `tradeable`; inactive volatile symbols must not generate entries.
- Use `ctx["htf"][timeframe][symbol]` for HTF decisions.
- Emit entry signals with complete metadata for research logs.
- Emit explicit close-only exit signals with `close_only: true` and
  `is_exit: true`.
- Preserve `decision_trace` generated by `make_decision_trace`.
- Use engine-compatible explicit stops through `stop_price` or `stop_spec`.
- Never compute future values inside strategy state.
- Never override engine risk sizing, margin, fees, slippage, spread, delay, or
  intrabar semantics inside strategy code.
- Never size directly from desired notional unless the hypothesis explicitly
  documents a notional-sizing model and the engine risk layer still applies
  `max_notional_pct_equity` and `max_gross_notional_pct_equity`.

Entry metadata must include:

- `strategy`
- `strategy_id`
- `family_variant`
- `family_pattern`
- `entry_reason`
- `entry_price`
- `entry_reference_price`
- `intended_entry_price`
- `signal_timeframe`
- `execution_timeframe`
- `risk_accounting`
- `r_per_trade`
- `stop_model`
- `stop_price`
- `entry_stop_price`
- `stop_distance`
- parameter values used by the run
- `decision_trace`

## Rich State Logging Requirements

When source data exists, every trade should preserve:

- `entry_state_funding_raw`
- `entry_state_funding_pctile`
- `entry_state_funding_z`
- `entry_state_funding_regime`
- `entry_state_oi_level`
- `entry_state_oi_change`
- `entry_state_oi_change_pct`
- `entry_state_oi_accel`
- `entry_state_oi_accel_pctile`
- `entry_state_oi_z`
- `entry_state_mark_price`
- `entry_state_index_price`
- `entry_state_basis_raw`
- `entry_state_basis_pct`
- `entry_state_basis_pctile`
- `entry_state_premium_raw`
- `entry_state_premium_pctile`
- `entry_state_crowding_proxy`
- `entry_state_constraint_stress_proxy`
- `entry_state_csi_raw`
- `entry_state_csi_pctile`
- `entry_state_csi_bucket`
- `entry_state_csi_source`
- `entry_state_csi_components_json`

If a strategy has its own family-specific state, include both generic
`entry_state_*` fields and strategy-specific raw fields. Unknown enriched fields
must survive extraction if they use approved prefixes:

- `identity_`
- `entry_state_`
- `entry_decision_`
- `execution_`
- `risk_`
- `path_`
- `counterfactual_`
- `label_`

## Strategy-Family Kernel Requirements

Every new family must include a kernel module under `src/bt/engine/fast_path/`.
The first safe kernel target is a compiled causal feature kernel, not a full
execution engine replacement.

A feature kernel must:

- Accept canonical research panel columns.
- Use only rows with `ts <= decision_ts`.
- Emit features keyed by decision timestamp and symbol.
- Include a boolean `<prefix>compiled_feature_ready` column.
- Use deterministic names scoped by family and timeframe.
- Be optional: the strategy must fall back to Python feature computation if
  compiled columns are absent.
- Be parity-tested on small fixtures against the Python strategy feature path.

Only enable a full compiled execution kernel after tests prove parity for:

- entry timing
- exits
- stops/trailing stops
- fees
- slippage
- spread
- delay bars
- worst-case intrabar behavior
- fills
- decisions
- trades
- equity
- performance metrics
- rich metadata columns

## Kernel Wiring Pattern

1. Add `src/bt/engine/fast_path/<family>_kernel.py`.
2. Add `build_<family>_feature_frame(panel, params=...)`.
3. Add a research-data job/CLI command to stamp feature columns:

```bash
PYTHONPATH=src python3 -m bt.research_data.cli build-<family>-kernel-features \
  --exchange binance \
  --universe stable \
  --timeframe 1m
```

4. Make the strategy check `bar.extra["<prefix>compiled_feature_ready"]`.
5. If present, use compiled features and add `"feature_kernel": "compiled"`.
6. If absent, use original Python feature computation.
7. Update fast-path status to report `classic_with_compiled_<family>_features`.

## Documentation For The Hypothesis

The hypothesis doc must explain:

- The claim in one sentence.
- Market structure rationale.
- Exact data inputs.
- Signal rules.
- Entry and exit rules.
- Risk controls.
- Parameter grid.
- What artifacts are logged.
- Evaluation metrics.
- Falsification criteria.
- Expected failure modes.
- Which rich research panel columns are used.
- Which compiled feature kernel is provided.
- Whether the kernel is feature-only or full execution.

## Tests

Add tests that prove:

- Hypothesis grid materializes deterministically.
- Strategy emits decision trace metadata.
- Rich metadata survives trade logging.
- OHLCV-only fallback does not crash.
- Enriched research panel data populates state fields.
- Compiled feature kernel is causal.
- Compiled feature kernel output matches Python feature semantics on a small
  fixture within documented tolerance.
- Fast-path status is accurate.
- Missing compiled columns fall back to classic feature calculation.
- Volatile membership does not expose inactive symbols.

## Queue And Daemon Readiness Checklist

Before queueing a new hypothesis:

- `PYTHONPATH=src pytest -q <focused tests>` passes.
- `python scripts/build_hypothesis_grid.py ...` writes the expected manifest.
- Stable preflight passes.
- Volatile preflight passes.
- If a family kernel was added, kernel features are materialized for the
  intended universe.
- A one-day volatile smoke run completes.
- `PYTHONPATH=src python3 scripts/validate_experiment_truth.py --experiment-root <root>`
  passes on the smoke output.
- `trades.csv` contains the required rich fields.
- `run_timing.json` and `fast_path_status.json` are present.
- Strategy terminal cards, state findings, verdicts, and research memory can
  consume the artifacts.

## Automatic Truth Gate

The daemon pipeline runs `scripts/validate_experiment_truth.py` immediately
after stable and volatile backtests complete and before post-run analysis. This
gate fails the pipeline when completed artifacts show:

- missing required artifacts
- incomplete or non-PASS run status
- trade count, win rate, or EV mismatch between `performance.json` and
  `trades.csv`
- invalid R metrics
- `pnl_net / risk_amount` disagreement with net R
- actual filled notional above `max_notional`
- negative `free_margin_post`
- forced liquidation
- funding/OI source timestamps later than the trade decision timestamp
- missing source-of-truth trade schema columns

Strategies should be generated so this gate is expected to pass. If a
hypothesis intentionally tests forced liquidation, leverage, or margin stress,
that must be isolated in a dedicated research profile and not mixed into normal
production-deployable hypothesis grids.
