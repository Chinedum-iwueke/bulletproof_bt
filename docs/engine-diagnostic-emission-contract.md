# Engine Diagnostic Emission Contract

This document defines the engine-native payload emitted by:

- `StrategyRobustnessLabService.run_analysis_from_parsed_artifact(...)`
- `bt.saas.service.run_analysis_from_parsed_artifact(...)`

## Global Diagnostic Envelope

Each diagnostic block (`overview`, `distribution`, `monte_carlo`, `execution`, `stability`, `regimes`, `ruin`, `report`) is emitted with a stable envelope:

- `available: bool`
- `limited: bool`
- `reason_unavailable: str | null`
- `limitations: list[str]`
- `summary_metrics: dict[str, scalar | null]`
- `figures: list[FigurePayload]`
- `interpretation: list[str] | dict[str, Any]` (diagnostic-specific; overview emits structured narrative)
- `warnings: list[str]`
- `assumptions: list[str]`
- `recommendations: list[str]`
- `metadata: dict[str, Any]`
- `payload: dict[str, Any]` (full native diagnostic payload for backward compatibility)

Skipped diagnostics are emitted as:

- `{"status": "skipped", "reason": "..."}`

## Figure Payload Types

Engine emits structured, UI-agnostic figure payloads:

- `type: "line_series"` with `x` + multi-series values
- `type: "histogram"` with explicit bins (`start`, `end`, `count`)
- `type: "scatter"` with `points`
- `type: "fan_chart"` with percentile bands (`p5`, `p25`, `p50`, `p75`, `p95`)
- `type: "heatmap"` with typed cells
- `type: "bar_groups"` for grouped bar comparisons

## Diagnostic-Specific Emission

### overview

Minimum:
- robustness and top-line posture
- key trade/equity metrics
- warnings + limitations

Trade-only:
- reconstructed equity figure if no uploaded equity curve
- truthful no-benchmark posture when absent
- rich top-line summary metrics suitable for UI selection (`robustness_score`, `trade_count`, `win_rate`, `expectancy`, `profit_factor`, `payoff_ratio`, `realized_max_drawdown_pct`, `worst_mc_drawdown_pct`)
- structured interpretation:
  - `summary: str`
  - `positives: list[str]`
  - `cautions: list[str]`
- explicit verdict block:
  - `posture` (`robust_under_current_assumptions` | `promising_but_incomplete` | `fragile_under_stress` | `inconclusive_due_to_missing_context`)
  - `confidence` (`high` | `medium` | `low`)
  - `verdict_reasons: list[str]`
- figure provenance metadata:
  - `metadata.figure_provenance.equity_curve` (`engine_emitted` or `reconstructed_from_trades`)
  - `metadata.figure_provenance.benchmark_overlay` (`reserved_not_emitted`, benchmark support pending)

Richer bundle unlocks:
- benchmark-relative equity comparison

### distribution

Minimum:
- strong trade-forensics summary bundle:
  - `trade_count`
  - `expectancy`, `win_rate`, `mean_return`, `median_return`
  - `gross_profit`, `gross_loss`, `gross_loss_abs`
  - `payoff_ratio`, `profit_factor` (when both wins/losses exist)
  - `return_std`
  - `percentile_10`, `percentile_90`
  - `skewness`, `kurtosis`
  - `mean_duration`, `median_duration` (only when durations are derivable)

Figures:
- **Primary figures (always for trade data):**
  - return histogram with explicit engine bins + metadata markers
  - win/loss grouped bars with counts and percentages
- **Secondary figures (conditional only):**
  - MAE/MFE scatter only when both excursion fields exist
  - duration histogram only when valid entry/exit timestamps exist

Interpretation:
- structured narrative object:
  - `summary`
  - `positives`
  - `cautions`
  - `shape_insights` (engine-computed, e.g., skew direction, tail concentration, asymmetry profile)

Truthful capability signaling:
- missing excursion/duration content is represented via:
  - `limitations`
  - `warnings`
  - `metadata.available_subdiagnostics`
- no fake empty MAE/MFE or duration figure blocks are emitted

Trade-only:
- this is a primary rich diagnostic and should remain strong from trade records alone:
  - robust summary metrics
  - real histogram bins
  - real win/loss distribution
  - shape interpretation
  - explicit assumptions, limitations, and recommendations

### monte_carlo

Minimum:
- summary metrics:
  - `worst_drawdown` (pct)
  - `p95_drawdown` (pct tail drawdown severity proxy)
  - `median_drawdown` (pct)
  - `p_ruin` (probability)
- plus legacy compatibility metrics:
  - `worst_simulated_drawdown_pct`
  - `median_drawdown_pct`
  - `drawdown_p95_pct`
  - `probability_of_ruin`
- drawdown distribution structure:
  - `drawdown_distribution.histogram_bins`
  - `drawdown_distribution.percentiles`
- ruin context:
  - `ruin_threshold_fraction`
  - `ruin_threshold_equity`
- explicit assumptions / limitations / recommendations arrays

Figures:
- equity fan chart (`p5`, `p25`, `p50`, `p75`, `p95` bands)
- drawdown histogram

Trade-only:
- baseline IID bootstrap simulation from realized trade sequence is first-class and expected to produce
  substantive survivability output (fan chart, drawdown distribution, ruin probability when threshold is defined)

Richer bundle unlocks:
- conditional/regime-aware simulation

### execution

Minimum:
- scenario-based fee/slippage/spread sensitivity matrix
- baseline vs stressed expectancy with edge decay metrics
- resilience score (derived from stressed edge retention)
- explicit assumptions, limitations, recommendations, and metadata

Figures:
- expectancy decay across stress scenarios (line or grouped bar)

Trade-only:
- baseline execution sensitivity is valid from trade records plus default/provided cost assumptions
- must not require OHLCV to emit baseline execution diagnostics

Richer bundle unlocks:
- OHLCV/spread proxies and richer execution metadata for enhanced realism
- venue-specific microstructure assumptions

### stability

Minimum:
- stability proxy score in single-run mode
- explicit limitation messaging

Figures:
- heatmap when grid metadata exists

Trade-only:
- typically `limited` with proxy only

Richer bundle unlocks:
- topology metrics (plateau ratio, fragility) from parameter grids

### regimes

Minimum:
- proxy regime summaries and consistency score

Figures:
- session expectancy bars
- volatility-regime bars

Trade-only:
- proxy-only with explicit assumptions

Richer bundle unlocks:
- OHLCV/labeled regime decomposition

### ruin

Minimum:
- stateful availability (`available` / `limited` / `unavailable`) with explicit reasons
- required-input framing:
  - `trades`
  - `account_size`
  - `risk_per_trade_pct`
- summary metrics (when required inputs exist):
  - `probability_of_ruin`
  - `expected_stress_drawdown`
  - `survival_probability`
  - optional sizing guardrails (`max_tolerable_risk_per_trade`, `minimum_survivable_capital` when computable)
- explicit assumptions:
  - account size
  - risk-per-trade
  - sizing model
  - compounding model
  - IID sequencing assumption
  - Monte Carlo linkage flag
- explicit limitations/recommendations tied to missing sizing inputs and model simplifications

Figures:
- at least one survivability curve when model is active:
  - ruin probability by drawdown threshold, and/or
  - survival curve, and/or
  - risk-per-trade sensitivity curve

Preferred optional:
- `risk_scenarios` sensitivity table for `0.5% / 1.0% / 2.0% / 5.0%` risk-per-trade

Trade-only:
- must be `limited` or `unavailable` when explicit sizing inputs are missing
- must not fabricate `probability_of_ruin` from trade-only artifacts alone

Richer bundle unlocks:
- policy-aware and dynamic sizing stress curves

### report

Minimum report-ready sections:
- `executive_verdict`
  - `status`: `robust` | `conditional` | `fragile` | `not_deployment_ready`
  - `headline`: short report title
  - `summary`: short decision rationale
- `confidence_level`
  - `level`: `high` | `medium` | `low`
  - `summary`: confidence rationale based on evidence richness/completeness
- `executive_summary`
  - concise what/so-what/now-what narrative suitable for client rendering
- `diagnostics_summary`
  - per-diagnostic status (`overview`, `distribution`, `monte_carlo`, `stability`, `execution`, `regimes`, `ruin`, `report`)
  - one-line takeaway + confidence impact (`supports` or `weakens`)
- `methodology`
  - engine identity + runtime seam
  - artifact richness and assumptions framing
  - Monte Carlo details (seed, simulations, drawdown thresholds)
  - parser notes when available
- `limitations`
- `deployment_guidance`
  - explicit deployment-now framing
  - where strategy may be used vs where it should not be used
  - required improvements before deployment
- `recommendations`
  - prioritized action-oriented next steps
- `key_metrics_snapshot`
  - top-line decision metrics (score/win-rate/expectancy/drawdown/ruin/edge-decay when available)
- `report_figures`
  - curated inclusion hints for export (`equity_curve`, `return_histogram`, `monte_carlo_fan`, optional ruin curve)
- `metadata`
  - report scope, artifact label, analysis date/id, export-readiness flags

Trade-only:
- meaningful report remains available with explicit caveats, deployment restrictions, and higher-priority recommendations for richer bundles

## Truthfulness Rules

- Never fabricate unavailable diagnostics.
- Use `available/limited/reason_unavailable` to communicate capability truthfully.
- Include limitations + recommendations when richer inputs are required.
- Prefer partial truthful output over empty blocks when metrics are derivable.
