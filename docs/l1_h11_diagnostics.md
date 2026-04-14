# L1-H11 Diagnostics

H11 diagnostics are emitted by `scripts/post_run_analysis.py --include-diagnostics` through family dispatch in `bt.analytics.postmortem`.

## Pullback quality
- `pullback_quality_summary.csv`
- `ev_by_pullback_depth_bucket.csv`
- `pullback_quality_by_timeframe.csv`
- `pullback_quality_by_symbol.csv`

## Impulse strength
- `impulse_strength_summary.csv`
- `ev_by_impulse_bucket.csv`
- `impulse_strength_by_timeframe.csv`

## Entry position
- `entry_position_summary.csv`
- `ev_by_entry_position_bucket.csv`

## Failure modes
- `failure_mode_summary.csv`
- `failure_mode_by_variant.csv`
- `failure_mode_by_timeframe.csv`

Deterministic labels: `weak_impulse`, `overdeep_pullback`, `late_entry`, `noise_stop`, `trend_failure`, `protection_too_tight`, `cost_killed`, `signal_noise`.

## Protection discipline
- `protection_discipline_summary.csv`
- `lock_rule_effect_summary.csv`
- `vwap_giveback_effect_summary.csv`

## Cost-kill
- `cost_kill_summary.csv`
- `cost_kill_by_timeframe.csv`
- `cost_kill_by_symbol.csv`

All EV and R metrics use canonical trade outputs (`r_multiple_gross`, `r_multiple_net`, `mfe_r`, `mae_r`) produced by the engine path.
