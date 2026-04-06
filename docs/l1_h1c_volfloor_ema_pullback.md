# L1-H1C Volatility-Floor EMA Pullback Continuation

## What L1-H1C is
L1-H1C is the formal HypothesisContract wrapper for `volfloor_ema_pullback`: a trend-continuation strategy that requires trend alignment, ADX confirmation, and a volatility floor before pullback-style entries into the fast EMA.

## How L1-H1C differs from baseline L1-H1
- L1-H1C uses **pullback-triggered continuation entries** to EMA fast instead of baseline continuation-only entry semantics.
- L1-H1C adds an explicit **ADX filter** and an optional **efficiency-ratio threshold** gate.
- L1-H1C supports `ema_trend_end` and `chandelier` exit families in strategy behavior, while this contract locks baseline `exit_type=ema_trend_end`.

## Base data and two-clock runtime semantics
- Canonical input data: **1m OHLCV**.
- Signal logic clock: **15m completed HTF bars** from `ctx["htf"]["15m"]`.
- Execution/risk integration clock: **1m runtime path**.
- Entry reference price: **live 1m close when available**, otherwise HTF close fallback.
- HTF discipline: process only new completed signal bars via `last_htf_ts` guard.

## Exact signal logic (preserved)
### Trend model
- Long bias: `ema_fast > ema_slow`
- Short bias: `ema_fast < ema_slow`

### Regime gates
- `adx >= adx_min`
- `vol_pct_rank >= vol_floor_pct`, where `vol_pct_rank` is percentile rank of current `natr = atr/close` against trailing `natr_history`.
- If `er_min` is set, `efficiency_ratio > er_min`; else ER gate is pass-through.

### Pullback trigger
- Long pullback: `low <= ema_fast` and `close > ema_fast`
- Short pullback: `high >= ema_fast` and `close < ema_fast`

## Volatility floor definition (locked)
- `natr_value = ATR(signal_tf) / close(signal_tf)`.
- Rank is computed on a **0-100 scale** (percent), not 0-1.
- Gate passes only when rank meets/exceeds `vol_floor_pct`.

## Efficiency ratio definition (locked)
- `ER = directional_move / path_length` over trailing closes.
- If `path_length == 0`, ER is exactly `0.0`.
- Gate rule is strict: `ER > er_min`.

## Stop model (locked)
- Stop is computed at entry from signal-timeframe ATR and entry reference price.
- Long: `stop_price = entry_ref - stop_atr_mult * atr`
- Short: `stop_price = entry_ref + stop_atr_mult * atr`
- Stop distance is frozen at entry (`stop_update_policy=frozen_at_entry`).

## Exit model (locked baseline)
- Contract baseline exit model: `ema_trend_end`.
- Strategy also supports `chandelier` semantics unchanged for fixed-mode runs.

## Position discipline
- No pyramiding.
- Single active position state per symbol.
- Exit is processed before any same-bar new entry.

## Pre-registered parameter family (L1-H1C)
The contract exposes the tested family with fixed defaults plus the pre-registered varying set:
- `vol_floor_pct ∈ {70, 80, 85}`
- `adx_min ∈ {22, 25}`
- `er_min ∈ {0.55, 0.45, 0.35}`
- `er_lookback ∈ {10, 16, 20}`

Resulting effective grid size is **54 variants** (`3 × 2 × 3 × 3`).

## Required decision logging fields
L1-H1C decision metadata includes:
- strategy/timeframe/exit mode: `strategy`, `tf`, `signal_timeframe`, `exit_monitoring_timeframe`, `exit_type`
- indicators and gates: `ema_fast`, `ema_slow`, `adx`, `efficiency_ratio`, `er_min`, `vol_pct_rank`, `vol_floor_pct`
- trigger diagnostics: `long_bias`, `short_bias`, `long_pullback`, `short_pullback`
- stop contract details: `entry_reference_price`, `stop_price`, `stop_distance`, `stop_source`, `stop_details`, `stop_model`, `stop_update_policy`
- chandelier snapshot: `chandelier`

## Execution examples
Serial:
```bash
python -m bt.experiments.hypothesis_runner \
  --config configs/engine.yaml \
  --local-config configs/local/engine.lab.yaml \
  --data /home/omenka/research_data/bt/curated/stable_data_1m_canonical \
  --out outputs/l1_h1c_tier2 \
  --hypothesis research/hypotheses/l1_h1c_volfloor_ema_pullback.yaml \
  --phase tier2
```

Parallel:
```bash
PYTHONPATH=src python scripts/build_hypothesis_grid.py \
  --hypothesis research/hypotheses/l1_h1c_volfloor_ema_pullback.yaml \
  --experiment-root outputs/l1_h1c_parallel_stable \
  --phase tier2
```

```bash
PYTHONPATH=src python scripts/run_parallel_hypothesis_grid.py \
  --experiment-root outputs/l1_h1c_parallel_stable \
  --manifest outputs/l1_h1c_parallel_stable/manifests/l1_h1c_volfloor_ema_pullback_tier2_grid.csv \
  --config configs/engine.yaml \
  --local-config configs/local/engine.lab.yaml \
  --data /home/omenka/research_data/bt/curated/stable_data_1m_canonical \
  --max-workers 6 \
  --skip-completed
```
