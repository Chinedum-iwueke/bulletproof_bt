# L1-H11C Protection Discipline Study

## Purpose
Evaluate whether post-entry protection controls improve continuation survivability and net EV.

## Entry
Uses the H11A signal family unchanged (trend + impulse + pullback + reclaim baseline).

## Protection semantics
- Initial stop model: structure-aware base distance with ATR padding (`stop_padding_atr`) and frozen initial risk.
- Lock rule: when open profit reaches `lock_r`, stop is advanced to breakeven.
- VWAP giveback: if enabled, after lock arming, exit on adverse close across Session VWAP.

## Exit / management
- 1m stop monitoring.
- 1m VWAP giveback monitoring when enabled.
- Trend-failure exit on signal-bar EMA structure failure.

## Exact grid (24)
- `signal_timeframe ∈ {15m, 1h}`
- `stop_padding_atr ∈ {0.25, 0.50, 0.75}`
- `lock_r ∈ {1.0, 1.5}`
- `vwap_giveback ∈ {off, on}`
