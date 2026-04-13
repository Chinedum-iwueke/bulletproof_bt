# L1-H8 Parent Family — Trend Continuation After Shallow Pullback

L1-H8 studies continuation entries in established EMA(9/21) trends after shallow pullbacks into EMA fast and/or SessionVWAP, with ADX trend-strength gating.

## Shared Design
- Canonical base data: **1m**.
- Institutional two-clock semantics: HTF signal on closed bars, 1m execution/exit monitoring.
- Canonical risk accounting: **engine_canonical_R**.
- Entry: pullback + continuation reclaim trigger + trend alignment.
- Stops: ATR-multiple frozen at entry.
- Management: partial-at-R + protected runner trail, with optional fail-fast.

## Variants
- `L1-H8A`: baseline.
- `L1-H8B`: timeframe robustness.
- `L1-H8C`: high selectivity.
- `L1-H8D`: pullback reference mode test.
- `L1-H8E`: management optimization.

## Post-run diagnostics
See `docs/l1_h8_diagnostics.md` for the reusable Family-B diagnostics suite integrated into `scripts/post_run_analysis.py`.
