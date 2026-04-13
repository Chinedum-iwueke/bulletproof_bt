# L1-H8 Parent Family — Trend Continuation After Shallow Pullback

## Hypothesis card (parent)
- Claim: in established directional trends, shallow pullbacks that interact with continuation references and reclaim trend direction can deliver positive EV.
- Sequence: trend context -> shallow pullback -> continuation-reference interaction -> reclaim/resume trigger -> entry in trend direction -> fixed ATR-initial-stop risk management.
- Runtime semantics:
  - Base/execution data frequency: **1m**.
  - Signal timeframe: variant-specific (`15m` or `1h`).
  - Signal logic only on closed HTF bars.
  - Stop/exit monitoring on the 1m execution clock.
  - Engine-canonical R (`engine_canonical_R`) is the only R truth.
- Mandatory stop semantics:
  - ATR computed on signal timeframe.
  - Initial stop distance = `stop_atr_mult * ATR_entry`.
  - Stop frozen at entry for canonical initial risk.
  - H8A/H8B/H8C/H8D do not use dynamic ATR stop recomputation.
  - H8E may trail runner post-TP1, but does not redefine initial risk.

## Variant cards
- **L1-H8A (Baseline):** `{signal_timeframe: [15m, 1h], adx_min: [20,25], pullback_max_bars: [2,3,4], partial_at_r: [1.5,2.0]}` = 24.
- **L1-H8B (Timeframe robustness):** `{signal_timeframe: [15m, 1h], adx_min: [20,25,30], pullback_max_bars: [2,3], partial_at_r: [1.5,2.0]}` = 24.
- **L1-H8C (High selectivity):** `{signal_timeframe: [15m, 1h], adx_min: [25,30,35], pullback_max_bars: [1,2], fail_fast_bars: [4,8]}` = 24.
- **L1-H8D (Reference study):** `{signal_timeframe: [15m, 1h], pullback_reference_mode: [ema_only,vwap_only,ema_or_vwap], pullback_max_bars: [2,3], adx_min: [20,25]}` = 24.
- **L1-H8E (Management study):** `{signal_timeframe: [15m, 1h], partial_at_r: [1.5,2.0,2.5], fail_fast_bars: [4,8], trail_atr_mult: [2.5,3.0]}` = 24.

## Post-run diagnostics
See `docs/l1_h8_diagnostics.md` for H8-specific diagnostics emitted by `src/bt/analytics/h8_postmortem.py`.
