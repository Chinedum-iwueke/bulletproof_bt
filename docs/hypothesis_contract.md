# Hypothesis Contract

## Purpose
`HypothesisContract` is the pre-registered research protocol used to execute hypothesis variants deterministically, enforce execution-aware tier requirements, and emit standardized machine-readable logs.

## YAML structure
Each contract YAML defines:
- metadata (`hypothesis_id`, `title`, `research_layer`, `version`, etc.)
- `required_indicators`
- `parameter_grid` (explicit immutable value lists)
- optional `gates`, `entry`, and `exit` sections
- `evaluation.required_tiers` (defaults to `Tier2` + `Tier3`)
- `logging` schema hints
- optional `runtime_controls` such as `max_variants`

## Deterministic materialization
Grid expansion uses a stable cartesian product over sorted parameter keys. Each variant receives:
- `grid_id` (`g00000`, `g00001`, ...)
- `config_hash` (canonical SHA-256 over sorted JSON params)

No variants are dropped silently; invalid grids fail loudly.

## Tier enforcement
`HypothesisContract` defaults required tiers to `Tier2` and `Tier3` when unspecified.
`run_hypothesis_contract` checks available tiers before execution and raises `MissingRequiredTierError` if any required tier is missing.

<<<<<<< codex/implement-engine-capabilities-for-hypothesis-testing-jqv1dd
The runner supports two deterministic execution workflows:
- `all_tiers`: run every required tier for every variant.
- `sequential`: run required tiers in order (for example Tier2 screening then Tier3 confirmation) with explicit promotion gating. Any non-promoted downstream tier is logged as `status=skipped` (never silently dropped).

=======
>>>>>>> main
## Standard logging schema
Each run record includes canonical fields such as:
`run_id`, `hypothesis_id`, `grid_id`, `config_hash`, symbol/timeframe window, tier, params/indicator/gate snapshots, core metrics, status, and failure reason.

## Indicators for blueprint-style gating
The contract system supports gates using:
- `session_vwap`: session-reset intraday value anchor
- `anchored_vwap`: event/index anchored value anchor
- `bb_width`: normalized volatility regime filter
- `adx`: directional trend-strength filter
- `efficiency_ratio`: trend efficiency / noisiness filter

## Workflow
1. Author YAML contract under `research/hypotheses/`.
2. Load via `HypothesisContract.from_yaml(...)`.
3. Materialize deterministic variants (`to_run_specs`).
4. Execute each variant for all required tiers via `run_hypothesis_contract`.
5. Consume standardized rows in downstream diagnostics and scanners.
