# L1-H11B Refined Deployment Candidates

This refinement keeps the H11B pullback-geometry experiment intact and narrows it using the current Tier2 artifacts:

- volatile `1h` rows carry the broad positive edge,
- stable rows remain negative and should be used as rejection checks,
- `15m` is only interesting when filtered into funding-squeeze states,
- exits need the same break-even, profit-lock, and runner-trail discipline added to H11A.

## Four Profiles

`h11b_1h_core_geometry`

- Parent evidence: volatile row 23, with rows 6/4/22 as neighboring confirmation.
- Geometry: 1h, ADX 20, pullback 0.5-1.0 ATR, swing distance 1.5 ATR.
- Gate: long only, avoid fragile/broken liquidity, require CSI low/mid, avoid basis very positive and funding very negative.
- Exit: break even after +1R, lock +1R after +2R, runner trail after +3R with 1.5R room, initial stop cap 0.75R.

`h11b_1h_mild_basis_runner`

- Parent evidence: H11B 1h mild impulse/basis-positive/funding-neutral pocket.
- Geometry: 1h, ADX 20, pullback 0.5-1.0 ATR, swing distance 1.5 ATR.
- Gate: long only, mild impulse, basis positive, funding neutral.
- Exit: break even after +1R, lock +1R after +2R, runner trail after +3R with 1.75R room, initial stop cap 0.75R.

`h11b_15m_midvol_funding_squeeze`

- Parent evidence: H11B 15m vol_mid/liquid/funding_negative squeeze pocket.
- Geometry: 15m, ADX 20, pullback 0.35-0.8 ATR, swing distance 1.0 ATR.
- Gate: long only, vol_mid, liquid, funding negative.
- Exit: break even after +1R, lock +0.5R after +1.5R, runner trail after +2.5R with 1.25R room, initial stop cap 0.75R.

`h11b_15m_liquid_mild_squeeze`

- Parent evidence: H11B 15m liquid/mild impulse/funding_negative squeeze pocket.
- Geometry: 15m, ADX 20, pullback 0.35-0.8 ATR, swing distance 1.0 ATR.
- Gate: long only, liquid, mild impulse, funding negative.
- Exit: break even after +1R, lock +0.5R after +1.5R, runner trail after +2.5R with 1.25R room, initial stop cap 0.75R.

## Deployment Standard

The 1h profiles are the main candidates. The 15m profiles are worth testing because the state pockets were strongly positive, but they are narrow and likely outlier-sensitive. Require fresh volatile/stable reruns, top-trade-removed EV, symbol concentration checks, daily/weekly drawdown checks, and portfolio interaction against H11A/H11C before promotion.
