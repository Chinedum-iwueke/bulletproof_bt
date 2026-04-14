# L1-H11B Pullback Geometry / Impulse Study

## Purpose
Test whether continuation quality depends jointly on impulse strength and pullback geometry.

## Entry
Same family entry as H11A, with explicit impulse search:
- `swing_distance_atr >= configured threshold`.
- Pullback zone and EMA20 reclaim trigger retained.
- ADX uses fixed baseline threshold (non-grid dimension).

## Exit / management
- Same baseline frozen stop and trend-failure exit family as H11A.
- No special protection-discipline overlays.

## Exact grid (24)
- `signal_timeframe ∈ {15m, 1h}`
- `swing_distance_atr ∈ {1.0, 1.5, 2.0}`
- `pull_entry_atr_low ∈ {0.35, 0.50}`
- `pull_entry_atr_high ∈ {0.80, 1.00}`
