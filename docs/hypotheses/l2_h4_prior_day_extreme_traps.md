# L2-H4 Prior-Day High/Low Liquidity-Level Traps

## Claim

Breakout attempts into strictly closed prior-day extremes bifurcate by regime: high-volatility states should continue, while compression states should mean-revert after rejection.

## What Is Being Tested

The strategy computes prior-day high and low from strict completed `1d` bars:

- `PDH`: prior-day high
- `PDL`: prior-day low

It then evaluates 5-minute signal bars against those levels.

High-vol regime:

- if close breaks above `PDH + delta_atr * ATR`, enter long;
- if close breaks below `PDL - delta_atr * ATR`, enter short.

Compression regime:

- if the bar wicks above PDH but closes back inside the range, fade short;
- if the bar wicks below PDL but closes back inside the range, fade long.

Entries must be within `epsilon_atr * ATR` of the relevant level. Only one attempt is allowed per UTC day per symbol.

## Causality Contract

- PDH/PDL come only from closed `1d` bars emitted by the strict resampler.
- The current day’s partial high/low is never used as a prior-day level.
- Regime gates are past-only rolling calculations on closed signal bars.
- Stops, fills, costs, margin checks, PnL, and artifacts remain under the classic engine.

## Grid

```text
epsilon_atr ∈ {0.25, 0.5}
delta_atr   ∈ {0.1, 0.2}
k_atr       ∈ {2.0, 2.5}
```

The signal timeframe is fixed at `5m` in v1 to keep branch attribution clean.

## Required Evidence

Every entry logs:

- `pdh`, `pdl`, `prior_day_anchor_id`
- distance to PDH/PDL
- selected regime
- trigger type
- high-vol and compression gate states
- attempt day id
- stop/risk metadata
- decision trace

Evaluation should emphasize conditional EV by branch and monotonicity versus volatility percentile buckets.

## Falsification

Scrap the thesis if there is no EV bifurcation between high-vol continuation and compression fade regimes. If one side works and the other fails, keep only the minimal surviving branch.
