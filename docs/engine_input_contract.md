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
- For `parameter_sweep`, parser validation enforces consistent parameter keys per run and at least two unique parameter combinations.
- If `pnl` is absent, the engine infers net PnL when entry/exit/quantity/side are present.
- If `equity_curve` is absent or incomplete, equity is reconstructed from trade PnL.
- Metadata from parsed artifact is merged into run metadata.
- Parameter stability uses full sweep topology when `parameter_sweep` is present; otherwise it falls back to single-run proxy mode.

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

## Degradation policy

- `diagnostic_eligibility` can disable specific diagnostics at call-time; disabled blocks are returned as `{"status": "skipped"}` and marked unavailable in capability profile.

- No trades: diagnostics marked unavailable.
- Low sample (<30 trades): trade-derived diagnostics marked limited.
- Missing params/parameter_sweep: stability marked limited (single-run proxy).
- Missing OHLCV: regimes marked limited (trade-sequence proxy).
