# L1-H11A Baseline Quality-Filtered Continuation

## Purpose
Baseline continuation filter study using trend strength + ATR-bounded pullback reclaim zone.

## Entry
- Trend: long if EMA20 > EMA50; short if EMA20 < EMA50.
- ADX filter: `adx_entry >= adx_min`.
- Fixed impulse requirement: `swing_distance_atr >= 1.0` (not grid-searched in H11A).
- Pullback depth gate: `pull_entry_atr_low <= pullback_depth_atr <= pull_entry_atr_high`.
- Trigger: EMA20 reclaim close in trend direction.

## Exit / management
- Frozen ATR stop (`stop_distance = stop_atr_mult * ATR_signal_tf`).
- Trend-failure exit on signal-bar EMA structure flip.
- Stop checks run on 1m monitoring clock.

## Exact grid (24)
- `signal_timeframe ∈ {15m, 1h}`
- `adx_min ∈ {20, 25}`
- `pull_entry_atr_low ∈ {0.35, 0.50, 0.65}`
- `pull_entry_atr_high ∈ {0.80, 1.00}`
