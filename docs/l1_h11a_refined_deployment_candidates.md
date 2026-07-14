# L1-H11A Refined Deployment Candidates

This refinement keeps the H11A EMA20/EMA50 continuation entry family intact and tests only the changes supported by the existing H11A artifacts:

- side/state gating before entry,
- tighter initial stop caps,
- breakeven and profit-lock stop advancement after favorable excursion,
- a runner trail once a trade has enough MFE to justify giving it room.

The original H11A grid showed no stable-regime deployment candidate. Stable rows were uniformly negative. The refined grid is therefore a volatile-regime deployment-research grid, with stable/rerun evidence used as a rejection check.

## Four Profiles

`h11a_1h_core_protected`

- Parent evidence: volatile row 12, with row 6 as neighboring confirmation.
- Geometry: 1h, ADX 20, pullback 0.65-1.0 ATR.
- Gate: long only, liquid/moderate liquidity, avoid positive/very-positive basis, avoid negative/very-negative funding.
- Exit: break even after +1R, lock +1R after +2R, runner trail after +3R with 1.5R room, initial stop cap 0.75R.

`h11a_1h_quality_balanced`

- Parent evidence: row 12 plus broad 1h quality cuts.
- Geometry: 1h, ADX 20, pullback 0.65-1.0 ATR.
- Gate: both sides, block fragile/broken liquidity, block positive/very-positive basis, block negative/very-negative funding, require CSI low/mid.
- Exit: break even after +1R, lock +1R after +2R, runner trail after +3R with 1.5R room, initial stop cap 0.75R.

`h11a_15m_liquid_midvol_runner`

- Parent evidence: volatile row 3.
- Geometry: 15m, ADX 20, pullback 0.5-0.8 ATR.
- Gate: long only, liquid, vol_mid, mild/no impulse, CSI low.
- Exit: break even after +1R, lock +0.5R after +1.5R, runner trail after +2.5R with 1.25R room, initial stop cap 0.75R.

`h11a_15m_explosive_moderate`

- Parent evidence: volatile rows 5 and 17, treated as a high-risk stress candidate because the edge is outlier-heavy.
- Geometry: 15m, ADX 20, pullback 0.65-0.8 ATR.
- Gate: long only, vol_extreme, moderate liquidity, strong/extreme impulse, avoid positive/very-positive basis.
- Exit: break even after +1R, lock +1R after +2R, runner trail after +3R with 1.75R room, initial stop cap 0.75R.

## Exit Rationale

The old volatile H11A row 12 reached +1R on roughly half its trades, but more than a third of those still closed flat or negative. About one third reached +2R, yet around 40% of those closed below +1R. The refined exit therefore protects early R first, then switches into a favorable-extreme trail for larger winners instead of waiting only for slow trend failure.

## Deployment Standard

None of these profiles should be promoted from the old extracted stats alone. They need fresh Tier2/Tier3 reruns with:

- top-trade removed metrics,
- stable-regime rejection checks,
- daily/weekly drawdown review,
- risk-normalized portfolio behavior against H11B/H11C candidates.
