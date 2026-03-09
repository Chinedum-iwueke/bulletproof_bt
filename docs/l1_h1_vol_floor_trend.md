# L1-H1 Volatility Floor Gates Trend Continuation

## Hypothesis
Trend continuation via `EMA20-EMA50` has positive net EV only when realized volatility (ATR/close) is above a rolling 30-day percentile floor.

## Formulas
- `trend_dir_t = sign(EMA20_t - EMA50_t)`
- `rv_t = ATR14_t / close_t`
- `vol_pct_t = percentile_rank(rv_t, trailing_window_30d)` (causal, past-only state)
- Gate: `vol_pct_t >= theta_vol`

## Causality Rules
- No entries until EMA20/EMA50/ATR14 are warm.
- Rolling percentile uses only values observed up to bar `t`.
- 30-day bars mapping (24/7): 5m=8640, 15m=2880, 1h=720.

## Pre-registered grid
- `theta_vol`: 0.50, 0.60, 0.70, 0.80, 0.85
- `k_atr`: 2.0, 2.5
- `T_hold`: 24, 48
- Optional TP: `tp_enabled` in {false,true}, `m_atr=2.0`

## Logging artifacts
Decision metadata includes `rv_t`, `vol_pct_t`, `gate_pass`, `trend_dir_t`, `stop_distance`. Risk engine contributes `risk_amount` and preserves fill cost fields (`spread_cost`, `slippage_cost`, `fee_cost`).

## Validation workflow
- Required tiers remain `Tier2` and `Tier3`.
- Run phases supported: `tier2`, `tier3`, `validate`.
- Validation status is incomplete until both tiers are present.

## Falsification and failure modes
- Reject if high-vol buckets are non-positive net EV in Tier2 and Tier3.
- If only Tier1 survives, classify as fragile micro-edge.
- Watch for bull beta masquerading as edge, asset lookback mismatch, and execution drag dominance.
