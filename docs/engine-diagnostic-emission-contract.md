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
- fee/slippage/spread sensitivity
- break-even cost multiplier
- resilience score

Figures:
- cost sensitivity line series

Trade-only:
- valid if cost columns or defaults are available

Richer bundle unlocks:
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
- probability of ruin
- stress drawdown summary
- capital threshold context

Figures:
- threshold probability line

Trade-only:
- derived from Monte Carlo outputs and account assumptions

Richer bundle unlocks:
- policy-aware and dynamic sizing stress curves

### report

Minimum report-ready sections:
- `header`
- `executive_summary`
- `validation_posture`
- diagnostic availability metadata
- assumptions / limitations / recommendations
- final verdict

Trade-only:
- meaningful report remains available, including what richer bundles would unlock

## Truthfulness Rules

- Never fabricate unavailable diagnostics.
- Use `available/limited/reason_unavailable` to communicate capability truthfully.
- Include limitations + recommendations when richer inputs are required.
- Prefer partial truthful output over empty blocks when metrics are derivable.
