# L2-H3 Reference-Price Reversion Using Session VWAP

## Claim

Reversion-to-reference strategies built around session VWAP outperform naive mean reversion when the reference price is aligned to liquidity and execution benchmarks.

## What Is Being Tested

The strategy computes a UTC-day `SessionVWAP` and fades dislocations measured by:

```text
z = (close - session_vwap) / ATR_14
```

Entries are allowed only when both inherited regime filters pass:

- L1-H2 compression gate: past-only low realized-volatility state
- L1-H4 liquidity gate: past-only low spread-proxy uncertainty state

The grid varies only:

- `z0 ∈ {0.8, 1.2}`
- `k_atr ∈ {1.5, 2.0}`

This keeps the test focused on reference-price dislocation and ATR stop sensitivity.

## Session Anchor

`SessionVWAP` resets at UTC 00:00. Each entry logs:

- `anchor_id`
- `session_anchor_id`
- `session_vwap`
- `z`
- `session_hour`

The strategy uses only bars already observed inside the current UTC session. There is no interpolation and no future session information.

## Entry And Exit

Entry:

- if `z <= -z0`, fade long toward session VWAP;
- if `z >= z0`, fade short toward session VWAP.

Exit:

- session VWAP touch;
- frozen ATR stop;
- signal-bar time stop;
- hard UTC session-end exit.

Stops, fills, costs, margin checks, PnL, and artifacts remain under the classic engine.

## Falsification

Scrap or rebuild if the strategy has no net Tier2 EV after costs. If low thresholds trade frequently but fail after costs, the reference may still be meaningful but requires larger dislocations or a stronger regime filter.

## Expected Failure Modes

- UTC day is the wrong anchor for the dominant participants.
- Reversion catches falling knives during forced-flow cascades.
- Tier3 costs eliminate the lower `z0` variant.
- Session-hour edge is concentrated in too few observations.
