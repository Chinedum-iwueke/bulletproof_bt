# L1-H1 Volatility Floor Gates Trend Continuation

## Hypothesis
Trend continuation via `EMA20-EMA50` has positive net EV only when realized volatility (`ATR14/close`) is above a rolling 30-calendar-day percentile floor.

## Base data and two-clock model
- Canonical engine input is **1m OHLCV**.
- Signal indicators and entry decisions run on a configurable **signal timeframe** (`5m` / `15m` / `1h`; intended default `15m`).
- Stop/TP monitoring runs on the base **1m** stream after entry.
- `T_hold` is measured in completed **signal bars**.

This is a strict two-clock design: 1m monitoring improves exit responsiveness without changing signal semantics.

## Signal logic (on closed signal bars)
- `EMA20`, `EMA50`, `ATR14` are computed on the signal timeframe.
- `trend_dir_t = sign(EMA20_t - EMA50_t)`.
- `rv_t = ATR14_t / close_t`.
- `vol_pct_t = percentile_rank(rv_t, trailing_window_30d)` (past-only rolling state).
- Entry gate: `vol_pct_t >= theta_vol` and `trend_dir_t != 0`.

## Frozen stop and optional TP model (L1-H1 baseline)
At entry:
- `atr_entry` is sampled from the **signal-timeframe** ATR at entry.
- `stop_distance = k_atr * atr_entry`.
- `stop_price` is derived once and then frozen.
- If `tp_enabled=true`, `tp_distance = m_atr * atr_entry` and `tp_price` is also frozen.

After entry:
- L1-H1 does **not** recompute ATR for stop/TP movement.
- L1-H1 is **not** a trailing-stop, chandelier, or dynamic-volatility stop strategy.

## Time stop semantics
- Exit when `signal_bars_held >= T_hold`.
- `signal_bars_held` increments only on completed signal bars, never on raw 1m bars.

## Why this baseline is locked this way
- Preserves clean hypothesis attribution (entry logic vs execution responsiveness).
- Keeps `R` and stop-distance definition stable for downstream analytics.
- Maintains deterministic, audit-friendly behavior under refactors.

## Pre-registered grid
- `theta_vol`: 0.50, 0.60, 0.70, 0.80, 0.85
- `k_atr`: 2.0, 2.5
- `T_hold`: 24, 48 (signal bars)
- Optional TP: `tp_enabled` in {false,true}, `m_atr=2.0`

## Decision/log metadata
Entry metadata includes:
`rv_t`, `vol_pct_t`, `gate_pass`, `trend_dir_t`, `atr_entry`, `stop_distance`, `stop_price`, `tp_enabled`, `tp_price` (if enabled), `signal_timeframe`, `exit_monitoring_timeframe`, `hold_time_unit`.

Exit metadata includes:
`exit_reason` and trigger-specific frozen values (`stop_price`, `tp_price`) while downstream accounting continues to preserve `spread_cost`, `slippage_cost`, and `fee_cost`.
