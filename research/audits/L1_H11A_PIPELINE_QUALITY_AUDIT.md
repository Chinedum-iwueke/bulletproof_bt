# L1-H11A Pipeline Quality Audit

Generated: 2026-06-16 UTC

## Executive Verdict

The `l1_h11a` pipeline completed end to end and produced the expected learning artifacts:

- Stable grid: 24/24 completed runs, 152,284 extracted trades.
- Volatile grid: 24/24 completed runs, 31,538 extracted trades.
- Verdict bundle: present, final verdict `REFINE_EXIT`.
- Strategy terminal cards: 7 cards present.
- State discovery: 433 combined findings.
- Research memory: tier2 stable and volatile trades ingested with 0 invalid trades.
- Rich state data: funding, OI, mark, index, basis, and enriched CSI columns are present in extracted trades.
- No-lookahead audit: `funding_source_ts <= entry_time` and `oi_source_ts <= entry_time` had 0 violations in both stable and volatile extracted datasets.

However, the already-completed `l1_h11a` results should be treated as pre-fix research artifacts, not deployment-grade evidence. The raw retained trade logs show material hidden notional exposure relative to the configured `r_per_trade=0.005`.

## Critical Finding: Notional Exposure Policy

The old risk policy sized quantity from R-at-stop and then allowed much larger notional when stop distance was tight.

Retained raw stable trades:

- Rows audited: 25,350
- Median actual notional / equity: 34.91%
- 95th percentile actual notional / equity: 84.84%
- Max actual notional / equity: 103.48%
- Rows above 20% notional / equity: 17,599
- Rows above 100% notional / equity: 22
- Rows scaled by margin: 10,310

Retained raw volatile trades:

- Rows audited: 5,207
- Median actual notional / equity: 12.68%
- 95th percentile actual notional / equity: 29.97%
- Max actual notional / equity: 336.58%
- Rows above 20% notional / equity: 1,071
- Rows above 100% notional / equity: 5
- Rows scaled by margin: 33

This does not invalidate the arithmetic of the backtest engine, but it invalidates the deployment interpretation of the results for large-capital risk management. The fix implemented in this pass caps actual entry notional and gross book notional before orders are emitted.

## Patch Summary

Implemented:

- Added `risk.max_gross_notional_pct_equity`.
- Added gross notional accounting to `Portfolio`.
- Added same-timestamp gross-notional reservation in `BacktestEngine`.
- Added per-entry and gross exposure metadata:
  - `max_gross_notional`
  - `current_gross_notional`
  - `remaining_gross_notional`
  - `gross_cap_applied`
  - `gross_cap_reason`
- Updated default research configs:
  - `max_leverage: 1.0`
  - `max_notional_pct_equity: 0.005`
  - `max_gross_notional_pct_equity: 0.025`
- Updated dataset extraction to preserve sizing/margin fields and derived exposure ratios:
  - `actual_notional_pct_equity`
  - `requested_notional_pct_equity`
  - `margin_pct_equity`
- Scoped daemon post-pipeline research-memory ingestion to the current stable/volatile experiment roots, avoiding broad stale-artifact scans.

## Artifact Consistency

Stable and volatile extraction manifests both report:

- `runs_scanned = 24`
- `runs_parsed = 24`
- `runs_dropped = 0`

Extracted trade/run consistency:

- Trade count differences between `trades_dataset.parquet` and `runs_dataset.parquet`: 0.
- Mean R differences between extracted trades and run rows: numerical epsilon only.
- Retained raw PnL arithmetic rows with gross-fees-slippage-net mismatch: 0.

## Rich State Learning

Confirmed columns include:

- `entry_state_funding_raw`
- `entry_state_funding_pctile`
- `entry_state_oi_level`
- `entry_state_oi_accel_pctile`
- `entry_state_mark_price`
- `entry_state_index_price`
- `entry_state_basis_pctile`
- `entry_state_csi_source`

State discovery emitted rich derivative-state classes including:

- `FUNDING_EXTREME_EDGE_STATE`
- `FUNDING_EXTREME_AVOID_STATE`
- `OI_BUILDUP_EDGE_STATE`
- `OI_BUILDUP_AVOID_STATE`
- `BASIS_PREMIUM_EDGE_STATE`
- `CONSTRAINT_STRESS_TAIL_STATE`
- `COST_KILLED_DERIVATIVE_STRESS_STATE`

Research memory currently contains real tier2 l1_h11a rows plus earlier tiny kernel-comparison rows. Going forward, daemon-scoped ingestion prevents post-pipeline memory updates from broad-scanning unrelated parity artifacts.

## Result Interpretation

The backtest engine and pipeline are now much closer to production research quality, but the completed `l1_h11a` verdict should be treated as a learning signal about setup/exit behavior under the old exposure policy, not as deployable performance evidence.

The correct next production-quality action is to rerun `l1_h11a` after the notional cap patch. The rerun should be expected to produce lower turnover and materially different EV because entries will no longer receive hidden notional amplification from tight stops.

## Tests Run

```text
PYTHONPATH=src pytest -q \
  tests/test_research_memory.py \
  tests/test_research_orchestration_data_profiles.py \
  tests/test_risk_engine.py \
  tests/test_config_resolver_risk_aliases.py \
  tests/test_config_completeness_validator.py \
  tests/test_default_engine_config_contains_all_supported_keys.py \
  tests/test_dataset_extraction_preserves_enriched_fields.py \
  tests/test_risk_reject_codes_stability.py

55 passed
```

