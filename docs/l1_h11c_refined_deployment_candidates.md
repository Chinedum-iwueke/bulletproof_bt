# L1-H11C Refined Deployment Candidates

This refinement keeps the H11C protection-discipline experiment intact and lets the data decide where protection is worth testing next:

- volatile `1h` rows are the real deployment candidates,
- stable rows are mostly flat-to-negative and remain rejection checks,
- broad `15m` H11C is negative,
- only very narrow `15m` fragile/extreme/basis-positive pockets showed positive EV, so those are diagnostic rather than deployment-ready.

## Four Profiles

`h11c_1h_core_protected`

- Parent evidence: volatile rows 20/7/19/9.
- Geometry: 1h, ADX 20, pullback 0.5-1.0 ATR, impulse 1.0 ATR.
- Protection base: structure-plus-0.25 ATR padding, lock family retained, VWAP giveback on.
- Gate: long only, avoid fragile/broken liquidity, require CSI low/mid, avoid positive/very-positive basis and negative/very-negative funding.
- Exit: break even after +1R, lock +1R after +2R, runner trail after +3R with 1.5R room.

`h11c_1h_mid_moderate_runner`

- Parent evidence: strongest H11C 1h state pocket: vol_mid, moderate liquidity, funding neutral.
- Geometry: 1h, ADX 20, pullback 0.5-1.0 ATR, impulse 1.0 ATR.
- Protection base: structure-plus-0.25 ATR padding, lock family retained, VWAP giveback on.
- Gate: long only, vol_mid, moderate liquidity, funding neutral.
- Exit: break even after +1R, lock +1R after +2R, runner trail after +3R with 1.75R room.

`h11c_15m_fragile_extreme_runner`

- Parent evidence: narrow 15m pocket: long, fragile liquidity, extreme impulse, basis very positive.
- Geometry: 15m, ADX 20, pullback 0.5-1.0 ATR, impulse 1.0 ATR.
- Protection base: structure-plus-0.25 ATR padding, VWAP giveback off.
- Exit: break even after +1R, lock +0.5R after +1.5R, runner trail after +2.5R with 1.25R room.

`h11c_15m_mid_fragile_basis_runner`

- Parent evidence: broader but weaker 15m pocket: vol_mid, fragile liquidity, basis very positive.
- Geometry: 15m, ADX 20, pullback 0.5-1.0 ATR, impulse 1.0 ATR.
- Protection base: structure-plus-0.25 ATR padding, VWAP giveback off.
- Exit: break even after +1R, lock +0.5R after +1.5R, runner trail after +2.5R with 1.25R room.

## Deployment Standard

Treat H11C `1h` as the serious candidate set. Treat H11C `15m` as a rescue/diagnostic experiment until it survives top-trade removal, stable rejection, and concentration checks. Any H11C promotion should prove that the protection layer improves exit capture without choking the right tail.
