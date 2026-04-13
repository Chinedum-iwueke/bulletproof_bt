# L1-H8 Post-Run Diagnostics

## Why generic diagnostics are insufficient
L1-H8 is a continuation-after-pullback family. Aggregate EV and win-rate alone do not explain whether:
- pullbacks were truly shallow,
- continuation extended quickly after entry,
- runner monetization improved outcomes,
- or costs converted structurally-valid setups into net losers.

## Output layout
When `scripts/post_run_analysis.py --include-diagnostics` runs on an experiment containing `l1_h8_trend_continuation_pullback`, diagnostics are written under:

`<experiment_root>/summaries/diagnostics/l1_h8/`

Key outputs:
- `h8_trade_diagnostics.csv`
- `pullback_quality_summary.csv`
- `ev_by_pullback_depth_bucket.csv`
- `ev_by_pullback_bars.csv`
- `ev_by_reference_mode.csv`
- `continuation_strength_summary.csv`
- `continuation_strength_by_timeframe.csv`
- `continuation_strength_by_symbol.csv`
- `failure_mode_summary.csv`
- `failure_mode_by_variant.csv`
- `failure_mode_by_timeframe.csv`
- `runner_capture_summary.csv`
- `runner_capture_by_variant.csv`
- `cost_kill_summary.csv`
- `cost_kill_by_timeframe.csv`
- `cost_kill_by_symbol.csv`

## Pullback depth buckets
Deterministic ATR-normalized buckets:
- `0-0.5_atr`
- `0.5-1.0_atr`
- `1.0-1.5_atr`
- `>1.5_atr`

## Failure-mode labels
Deterministic labels used for operational triage:
- `trend_filter_weak`
- `pullback_too_deep`
- `pullback_too_long`
- `reclaim_failed`
- `continuation_stalled`
- `runner_gave_back`
- `cost_killed`
- `signal_noise`

## Canonical R discipline
All EV/expectancy-style metrics are built from engine-written trade fields (`r_multiple_gross`, `r_multiple_net`, `mfe_r`, `mae_r`).
No strategy-local alternative R model is introduced.

## Known limitations
- `time_to_tp1_bars` is approximated from available path diagnostics when explicit TP1 timestamps are absent.
- `continuation_leg_vs_pullback_ratio` depends on pullback-depth metadata being present on entry fills.
- Runner decomposition is based on trade-level endpoints and path diagnostics, not full intra-trade fill decomposition.
