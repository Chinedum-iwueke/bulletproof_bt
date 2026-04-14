# L1-H10 Parent Family

## Name
L1-H10 High Win-Rate / Tight TP Systems with Selective Tail Capture.

## Canonical semantics
- Base data: `1m` OHLCV.
- Signal computation: completed higher-timeframe bars only.
- Exit monitoring: `1m` clock.
- Initial risk: frozen ATR stop at entry (`stop_distance = k_atr_stop * ATR_entry`).
- R accounting: engine canonical only (`ev_r_gross`, `ev_r_net`, `mfe_r`, `mae_r`, avg win/loss in R, payoff ratio).
- H10A/H10B exits for this phase: full TP or stop; no partials/runners.

## Research questions
1. Can high-win-rate/tight-TP structures survive Tier2/Tier3 costs?
2. What win-rate is required given negative payoff asymmetry?
3. Are failures dominated by cost drag or signal failure?
4. Is there sufficient unrealized tail potential to justify later H10C runner design?
