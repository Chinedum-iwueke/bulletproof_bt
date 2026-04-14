# L1-H10B — Breakout Scalping

## Entry
- Signal timeframe in `{5m, 15m}`.
- `ATR_14` and `ADX_14` on signal timeframe.
- Breakout reference: **previous signal-bar close**.
- Long if `close_t >= prev_close + breakout_atr * ATR_t` and `ADX >= adx_min_fixed`.
- Short if `close_t <= prev_close - breakout_atr * ATR_t` and `ADX >= adx_min_fixed`.

## Exit
- Frozen ATR stop: `stop_distance = k_atr_stop * ATR_entry`.
- Full TP: `tp_distance = tp_r * stop_distance`.
- Stop/TP monitored on `1m` bars.

## 24-run grid
- `signal_timeframe ∈ {5m, 15m}`
- `breakout_atr ∈ {0.5, 0.75, 1.0}`
- `tp_r ∈ {0.5, 1.0}`
- `k_atr_stop ∈ {2.5, 3.5}`
