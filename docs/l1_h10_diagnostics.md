# L1-H10 Diagnostics

The H10 post-run layer emits machine-readable files under `summaries/diagnostics/l1_h10/`.

## Tail potential
- `tail_potential_summary.csv`
- `tail_potential_by_variant.csv`
- `tail_potential_by_timeframe.csv`
- `tail_potential_by_symbol.csv`

## Cost-kill
- `cost_kill_summary.csv`
- `cost_kill_by_timeframe.csv`
- `cost_kill_by_symbol.csv`
- `cost_kill_by_parameter_slice.csv`

## Win-rate stability
- `win_rate_stability_summary.csv`
- `win_rate_by_timeframe.csv`
- `win_rate_by_symbol.csv`
- `win_rate_by_parameter_slice.csv`

## Failure modes
- `failure_mode_summary.csv`
- `failure_mode_by_variant.csv`
- `failure_mode_by_timeframe.csv`

Outputs use engine-truth fields from `trades.csv` (`r_multiple_gross`, `r_multiple_net`, `mfe_r`, `mae_r`) with entry metadata joins from `fills.jsonl`.
