# L1-H1 Volatility Floor Gates Trend Continuation

## Hypothesis
Trend continuation via `EMA20-EMA50` has positive net EV only when realized volatility (ATR/close) is above a rolling 30-day percentile floor.

## Two-clock execution model
- **Signal timeframe** (`5m` / `15m` / `1h`): EMA/ATR/vol gate and entry decisioning are computed on closed resampled bars.
- **Execution/risk timeframe** (`1m`): stop-loss and optional take-profit are monitored on the base minute stream for responsive exits.
- `T_hold` is counted in completed **signal** bars.

## Formulas
- `trend_dir_t = sign(EMA20_t - EMA50_t)`
- `rv_t = ATR14_t / close_t`
- `vol_pct_t = percentile_rank(rv_t, trailing_window_30d)` (causal, past-only state)
- Gate: `vol_pct_t >= theta_vol`

## Causality rules
- No entries until EMA20/EMA50/ATR14 are warm on the signal timeframe.
- Rolling percentile uses only values observed up to bar `t`.
- Signal events are formed on completed signal bars only.
- No lookahead into unfinished HTF buckets.

## Pre-registered grid
- `theta_vol`: 0.50, 0.60, 0.70, 0.80, 0.85
- `k_atr`: 2.0, 2.5
- `T_hold`: 24, 48 (signal bars)
- Optional TP: `tp_enabled` in {false,true}, `m_atr=2.0`

## Logging artifacts
Decision metadata includes `rv_t`, `vol_pct_t`, `gate_pass`, `trend_dir_t`, `stop_distance`. Risk engine contributes `risk_amount` and preserves fill cost fields (`spread_cost`, `slippage_cost`, `fee_cost`).

## Validation workflow
- Required tiers remain `Tier2` and `Tier3`.
- Run phases supported: `tier2`, `tier3`, `validate`.
- Validation status is incomplete until both tiers are present.

## Production runner examples
```bash
python -m bt.experiments.hypothesis_runner \
  --config configs/engine.yaml \
  --local-config configs/local/engine.lab.yaml \
  --data /home/omenka/research_data/bt/curated/stable_data_1m_canonical \
  --out outputs/l1_h1_tier2 \
  --hypothesis research/hypotheses/l1_h1_vol_floor_trend.yaml \
  --phase tier2
```

```bash
python -m bt.experiments.hypothesis_runner \
  --config configs/engine.yaml \
  --local-config configs/local/engine.lab.yaml \
  --data /home/omenka/research_data/bt/curated/stable_data_1m_canonical \
  --out outputs/l1_h1_tier3 \
  --hypothesis research/hypotheses/l1_h1_vol_floor_trend.yaml \
  --phase tier3
```

```bash
python -m bt.experiments.hypothesis_runner \
  --config configs/engine.yaml \
  --local-config configs/local/engine.lab.yaml \
  --data /home/omenka/research_data/bt/curated/stable_data_1m_canonical \
  --out outputs/l1_h1_validate \
  --hypothesis research/hypotheses/l1_h1_vol_floor_trend.yaml \
  --phase validate
```
