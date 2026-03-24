# Engine Input Contract for Parsed Artifacts

## Entry point
Use one of:

- `bt.saas.service.run_analysis_from_parsed_artifact(parsed_artifact, config=None)`
- `StrategyRobustnessLabService.run_analysis_from_parsed_artifact(parsed_artifact, config=None)`

## Required normalized input model

`ParsedArtifactInput`:

- `artifact_kind`: `trade_csv | artifact_bundle | parameter_sweep`
- `richness`: `trade_only | trade_plus_metadata | trade_plus_context | research_complete`
- `trades`: `list[NormalizedTradeRecord]` (**required; must be non-empty**)
- `strategy_metadata`: free-form metadata map
- optional enrichments:
  - `equity_curve`
  - `assumptions`
  - `params`
  - `parameter_sweep`
  - `ohlcv_present`
  - `benchmark_present`
  - `parser_notes`
  - `diagnostic_eligibility`

`parameter_sweep` (primary Parameter Stability contract):

- `parameter_names: list[str]`
- `runs: list[ParameterSweepRunInput]`
  - `run_id: str`
  - `params: {param_name -> scalar value}`
  - `trades: list[NormalizedTradeRecord]` (or summary metric fallback at parser stage)
  - optional `summary` and `metadata`
- optional `assumptions`, `execution_context`

`NormalizedTradeRecord` includes canonical trade fields such as symbol/side/entry/exit/price/size/fees/pnl/mae/mfe plus optional context tags.

## Behavior

- Engine normalizes records into internal trade frame via existing normalization pipeline.
- Normalization is semantic/alias-driven (`entry_time|timestamp -> entry_ts`, `qty|size -> quantity`, `pnl|net_pnl -> pnl_net`, etc.) so parsed artifacts are not coupled to one CSV schema.
- For `parameter_sweep`, parser validation enforces consistent parameter keys per run and at least two unique parameter combinations.
- If `pnl` is absent, the engine infers net PnL when entry/exit/quantity/side are present.
- If `equity_curve` is absent or incomplete, equity is reconstructed from trade PnL.
- Metadata from parsed artifact is merged into run metadata.
- Parameter stability uses full sweep topology when `parameter_sweep` is present; otherwise it falls back to single-run proxy mode.

### Capability-driven model (parsed artifacts)

Parsed artifact analysis now executes as:

1. accepted dataset → semantic normalization
2. semantic normalization → canonical capability detection
3. capabilities → diagnostic/figure emission

Canonical semantic capabilities emitted in `capability_profile.artifact_capabilities` include:

- `has_trade_timestamps`, `has_exit_timestamps`
- `has_entry_exit_prices`, `has_quantity`
- `has_net_pnl`, `has_gross_pnl`
- `has_cost_fields`, `has_fee_fields`, `has_slippage_fields`
- `has_excursion_fields`
- `has_risk_fields`, `has_stop_distance_fields`, `has_r_multiple_fields`
- `has_equity_series`
- `has_market_context`, `has_benchmark_context`
- `has_parameter_grid`

Derived figure/diagnostic capabilities include:

- `can_build_equity_curve`
- `can_build_duration_distribution`
- `can_build_histogram_from_returns`
- `can_build_histogram_from_r_multiples`
- `can_build_mae_mfe_scatter`
- `can_build_cost_drag_summary`
- `can_build_execution_sensitivity_baseline`
- `can_build_monte_carlo_paths`
- `can_build_ruin_model`
- `can_build_regime_analysis`
- `can_build_parameter_stability`

Diagnostic emitters consume these capabilities to decide which sub-diagnostics and figures are emitted (for example MAE/MFE scatter, duration histogram, R-multiple histogram, cost-drag metadata), rather than keying only off coarse artifact kind.

## Recommended on-disk upload format (primary)

Structured bundle:

- `manifest.json`
  - `parameter_names` (required)
  - `runs[]` (required, at least 2)
    - `run_id` (required)
    - `params` (required; exact keys must equal `parameter_names`)
    - `trades_file` (recommended) or inline `trades`
    - optional `summary`, `metadata`
  - optional `assumptions`, `execution_context`, `strategy_name`
- `runs/*.csv` trade files referenced by `trades_file`

Secondary advanced mode:

- Single combined table where each row includes:
  - `run_id`
  - parameter columns (one stable value per run_id)
  - canonical trade columns (`entry_time`, `symbol`, `side`, plus pnl or inferable pricing/size fields)

## Structured output model

`EngineAnalysisResult` returns:

- `run_context`: artifact/richness and available context flags
- `capability_profile`: per-diagnostic status + reason + input requirements
- `warnings`: low sample and parser notes
- `diagnostics`: normalized blocks (`overview`, `distribution`, `monte_carlo`, `stability`, `execution`, `regimes`, `ruin`, `report`)
- `raw_payload`: underlying service payload for compatibility

### Risk of Ruin input contract

Minimum viable ruin model requires:

- normalized trade distribution (`trades`)
- explicit `account_size`
- explicit `risk_per_trade_pct` (or equivalent fixed-fractional sizing input)

Optional enrichments:

- stop policy assumptions
- compounding model overrides
- Monte Carlo-linked survivability outputs
- stress scenario grids

Behavior:

- missing `account_size` and/or `risk_per_trade_pct` => ruin diagnostic is emitted as **limited**, with explicit missing-input reasons and no fabricated `probability_of_ruin`
- when minimum inputs exist => ruin emits full survivability summary metrics, assumptions, limitations, recommendations, interpretation, and at least one chart-ready ruin/stress figure

## Degradation policy

- `diagnostic_eligibility` can disable specific diagnostics at call-time; disabled blocks are returned as `{"status": "skipped"}` and marked unavailable in capability profile.

- No trades: diagnostics marked unavailable.
- Low sample (<30 trades): trade-derived diagnostics marked limited.
- Missing params/parameter_sweep: stability marked limited (single-run proxy).
- Missing OHLCV: regimes marked limited (trade-sequence proxy).
- Missing ruin sizing inputs (`account_size`, `risk_per_trade_pct`): ruin marked limited with explicit requirements; full ruin metrics withheld until inputs are provided.
