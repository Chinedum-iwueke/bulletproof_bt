# Strategy Family Kernel Coverage

This file tracks fast-path kernel readiness for current hypothesis families.
Do not mark a family as enabled until its kernel is causal, tested, and wired
without changing classic execution truth.

## Enabled

| Family | Strategy | Kernel Mode | Notes |
| --- | --- | --- | --- |
| CSI-gated displacement trend | `l7_h1_csi_gated_displacement_trend` | `classic_with_compiled_l7h1_features` | Compiled feature kernel stamps causal L7-H1 HTF decision features. Classic engine remains source of truth for execution, risk, fills, logs, and metrics. |
| Generic stable HTF context | HTF strategies using `ctx["htf"]` | `classic_with_precomputed_htf_context` | Stable research panels can be stamped with causal `htf_<tf>_*` columns. The adapter reconstructs the classic emitted-HTF context while classic strategy/risk/execution remain source of truth. Enable family-by-family only after `scripts/compare_all_stable_fast_paths.py` records parity for that family/timeframe. |

## Parity Harness

Run the stable family parity matrix with:

```bash
PYTHONPATH=src:. python3 scripts/compare_all_stable_fast_paths.py \
  --hypotheses-dir research/hypotheses \
  --output-root outputs/kernel_comparison/stable_family_parity_all \
  --config configs/engine.yaml \
  --local-config configs/local/engine.lab.yaml \
  --data-root research_data \
  --exchange binance \
  --timeframe 1m \
  --stable-manifest research_data/manifests/stable_universe.parquet \
  --start 2025-05-05 \
  --end 2025-05-07 \
  --max-workers 2 \
  --gate-workers 4 \
  --run-timeout-seconds 2400 \
  --clean
```

The matrix writes `stable_fast_path_matrix.csv`; a family/timeframe is enabled
only when metrics, equity, and semantic trades match classic output.

## Requires Kernel Work

The following existing strategy families still use classic Python feature
calculation and classic execution. They must follow
`docs/hypothesis_strategy_generation_prompt_instructions.md` before being
marked fast-path enabled.

| Strategy Pattern | Current Strategy Modules |
| --- | --- |
| Vol-floor trend and pullback | `l1_h1_vol_floor_trend`, `l1_h1b_salvage`, `volfloor_ema_pullback`, `volfloor_donchian` |
| Compression / mean reversion | `l1_h2_compression_mean_reversion`, `l1_h2b_confirmed_fade`, `l1_h4a_liquidity_gate_mean_reversion`, `l1_h4b_liquidity_gate_size_adjusted_mean_reversion`, `l1_h6a_vov_gate_mean_reversion`, `l1_h10a_mean_reversion_small_tp` |
| HAR / regime switching | `l1_h3_har_rv_gate_trend`, `l1_h3b_har_rv_gate_mean_reversion`, `l1_h3c_har_regime_switch`, `l1_h5b_vol_managed_har_trend` |
| Vol-managed trend | `l1_h5a_vol_managed_trend` |
| Squeeze / expansion / pullback | `l1_h7_squeeze_expansion_pullback` |
| Trend continuation / pullback | `l1_h8_trend_continuation_pullback`, `l1_h11_quality_filtered_continuation` |
| Momentum / breakout | `l1_h9_momentum_breakout`, `l1_h10b_breakout_scalping` |

## Rule

For every new or existing family, first add a compiled feature kernel and make
the strategy consume optional precomputed feature columns. Only then consider a
full compiled execution kernel, and only with artifact parity tests against the
classic engine.
