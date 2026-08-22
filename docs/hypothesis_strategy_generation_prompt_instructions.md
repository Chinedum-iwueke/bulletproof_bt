# Hypothesis Strategy Generation Prompt Instructions

Use this document when creating a new Bulletproof_bt hypothesis family from a
plain-English hypothesis card. The output must be a complete research-ready
strategy package: hypothesis YAML, strategy Python module, strategy-family
adapter/feature contract, tests, docs, and daemon-compatible run wiring.

## Non-Negotiables

Do not weaken any Bulletproof_bt truth guarantees:

- No lookahead.
- No interpolation or synthetic bars.
- Missing bars mean missing decisions.
- Strict UTC timestamps.
- HTF signals must use only fully closed HTF bars.
- Funding and OI may only be used when available at or before the decision bar.
- Execution semantics remain in the classic engine.
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
- Exact sizing and risk contract: `sizing.mode`/`risk.mode`,
  `risk.r_per_trade` for risk-at-stop sizing or
  `risk.notional_pct_equity` for fixed-notional sizing, maximum positions,
  maximum entry notional, maximum gross notional, cap policy, minimum risk
  utilization, and whether pyramiding/flips are allowed.
- Whether the strategy is expected to be deployable under the production
  guardrails: `risk.r_per_trade: 0.005`,
  `risk.max_notional_pct_equity`, `risk.max_gross_notional_pct_equity`, and
  `risk.max_leverage: 1.0`. Risk-at-stop is the default production sizing
  target; notional, aggregate exposure, margin, liquidity, and minimum-stop
  rules remain independent safety constraints.
- Whether capped risk-at-stop trades should be kept truthfully or rejected:
  `risk.cap_policy: allow_clip_with_truth` keeps capped trades while logging
  actual stop risk, while `risk.cap_policy: reject_if_clipped` rejects any
  trade that cannot use the requested risk. If kept, specify
  `risk.min_risk_utilization_pct` and keep
  `risk.report_under_risked_trades: true`.
- Expected run artifact fields needed for post-run learning.
- Falsification conditions that should scrap or refine the hypothesis.

If the card implies unconstrained exposure, hidden leverage, unresolved stops,
future settlement values, or discretionary exits that cannot be represented
deterministically, reject or rewrite that part before producing strategy code.

## Required Files For A New Hypothesis

Given a hypothesis card, generate:

- `research/hypotheses/<hypothesis_id>.yaml`
- `src/bt/strategy/<strategy_name>.py`
- `docs/hypotheses/<hypothesis_id>.md`
- Contract and integration tests under `tests/hypotheses/` and `tests/`

Update:

- `src/bt/strategy/__init__.py` to import/register the strategy.
- Do not add strategy-family fast-path kernels for new hypotheses. The fast
  path is deprecated; generated strategies must be correct on the classic
  event-driven engine.

## Deprecated Fast Path Note

Do not instruct Codex to create compiled kernels, sparse candidate adapters, or
materialized fast-path feature columns for a new strategy. The accepted
production architecture is:

- classic event-driven strategy evaluation;
- causal features from current/historical bars only;
- rich `Signal.metadata` and `Bar.extra` logging;
- no-lookahead state snapshots;
- post-run analysis, extraction, state discovery, verdict cards, and research
  memory consuming the standard artifacts.

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
- sizing intent, expressed with one of the two explicit forms below
- data availability assumptions and OHLCV-only fallback behavior
- truth-gate expectations, including whether forced liquidations are expected
  or should invalidate the run

Use `dataset_kind: research_panel` through daemon/run configs, not legacy
external curated folders. New research runs should use:

Every generated YAML must also contain this exact machine-enforced block:

```yaml
truth_contract:
  version: "1.0"
  profile: production
  no_lookahead: true
  strict_utc: true
  missing_bars: no_decision
  interpolation: forbidden
  htf_completeness: closed_only
  aux_join_direction: backward
  execution_authority: engine
  risk_authority: engine
  accounting: engine_canonical_R
  truth_gate_required: true
  fast_path_generation: forbidden
  research_memory_requires_certification: true
```

Do not reinterpret these values. A hypothesis requiring different semantics is
a separate non-production research profile and must not enter the normal SaaS
or daemon queue.

## Research Tier Selection

Use the explicit research tiers:

- `tier2a`: signal-episode evidence. Use this to learn whether the setup has
  state-conditioned edge. It is labelled `research_mode: signal_episode` and
  must not be described as deployable portfolio evidence.
- `tier2b`: portfolio-simulation evidence. Use this to test account-path,
  capital, margin, and concurrency realism. Legacy `tier2` is treated as
  `tier2b`.
- `tier3`: stricter promotion/robustness testing after Tier 2 evidence.

Generated strategies must not change behavior between Tier 2A and Tier 2B.
The tier changes the research question and artifact labels, not the signal
definition. ML extraction and research memory consume both tiers, but consumers
must filter by `research_mode` or `evidence_type`.

Every generated hypothesis should use one of these sizing blocks:

```yaml
sizing:
  mode: risk_at_stop
  r_per_trade: 0.005
  cap_policy: allow_clip_with_truth
  min_risk_utilization_pct: 0.10
  report_under_risked_trades: true
```

or, only when the hypothesis is intentionally exposure-first:

```yaml
sizing:
  mode: fixed_notional_pct_equity
  notional_pct_equity: 0.05
```

Do not describe `r_per_trade` as position size. `r_per_trade` means intended
loss at the initial stop. `notional_pct_equity` means position exposure. When a
notional, gross, or margin cap clips a risk-at-stop trade, the engine must log
`requested_risk_amount`, actual `risk_amount`, `risk_utilization_pct`, and
`under_risked_trade` so post-run analysis can learn from the trade without
pretending it used the full risk budget.

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
  --max-workers 12 \
  --max-workers-auto \
  --max-ram-per-worker-gb 4 \
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
  --max-workers 12 \
  --max-workers-auto \
  --max-ram-per-worker-gb 4 \
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
- Never size directly inside strategy code. If the hypothesis requires
  exposure-first sizing, express that through
  `sizing.mode: fixed_notional_pct_equity`; the engine remains the only
  authority for quantity, caps, margin, fees, slippage, spread, delay, and
  intrabar semantics.

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
- `sizing_mode`
- `cap_policy`
- `requested_risk_amount`
- `risk_utilization_pct` when available
- `under_risked_trade` when available
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

## Deprecated Fast Path Requirements

Do not add fast-path kernels, compiled event adapters, sparse candidate-event
shortcuts, or materialized strategy-family feature columns for new hypotheses.
The backtest fast path is deprecated. Every generated strategy must be correct
and complete on the classic event-driven engine.

Optimization is allowed only when it is semantics-neutral infrastructure such
as safer IO, bounded memory usage, streaming artifact writes, or post-run
analysis batching. It must not change which bars the strategy sees, which
orders are emitted, how stops/fills are resolved, how risk is sized, or which
truth artifacts are written.

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
- Confirmation that the classic engine is the execution source of truth.

## Tests

Add tests that prove:

- Hypothesis grid materializes deterministically.
- Strategy emits decision trace metadata.
- Rich metadata survives trade logging.
- OHLCV-only fallback does not crash.
- Enriched research panel data populates state fields.
- `fast_path_status.json` records classic execution or deprecated fallback.
- Volatile membership does not expose inactive symbols.

## Queue And Daemon Readiness Checklist

Before queueing a new hypothesis:

- Run the mandatory admission gate:

  ```bash
  PYTHONPATH=src python3 scripts/validate_hypothesis_admission.py \
    --hypothesis research/hypotheses/<hypothesis>.yaml \
    --output research/audits/<hypothesis>_strategy_admission.json
  ```

- Admission must report `PASS`. Do not queue around a failed check, suppress a
  check, or relabel a warning. The daemon runs the same gate again before it
  builds manifests.

- `PYTHONPATH=src pytest -q <focused tests>` passes.
- `python scripts/build_hypothesis_grid.py ...` writes the expected manifest.
- Stable preflight passes.
- Volatile preflight passes.
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

## Definition Of A Certified Result

A completed process is not automatically a trustworthy result. Results may be
shown as engine-certified in the SaaS, used by verdict agents, or ingested into
research memory only when all of these are true:

1. Strategy admission passed for the exact hypothesis and strategy source
   hashes.
2. Every requested manifest row has a terminal `PASS` run status.
3. Required decisions, fills, trades, equity, configuration, and performance
   artifacts existed before extraction/cleanup.
4. Stable and volatile experiment truth reports both say `PASS` with zero hard
   failures.
5. Canonical extraction completed with zero dropped runs and unique trade and
   parameter-set identities.
6. The extracted dataset passed the second truth validation.
7. Cleanup occurred only after extraction and certification.
8. Research memory ingested only rows marked `metrics_valid=true`.

The stable SaaS enforcement seam is
`bt.saas.truth_certification.require_truth_certification`. Engine-generated
results without a passing report and canonical datasets must be presented as
`UNVERIFIED` or rejected, never as a valid backtest.

## Required Generator Completion Response

When Codex generates a strategy family from this document, it must not claim
completion after merely writing code. Its final response must report:

- hypothesis admission result and report path
- focused test count and result
- classic-versus-fast parity result, or `classic_only`
- OHLCV-only and enriched-data smoke results
- stable and volatile membership smoke results
- exact risk, notional, gross-exposure, leverage, stop, and execution contract
- any unavailable rich-data fields and fallback behavior

Any failed item means the package is not queue-ready.
