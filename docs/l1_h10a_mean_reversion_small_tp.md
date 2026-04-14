# L1-H10A — Mean Reversion Small TP

## Entry
- Signal timeframe in `{5m, 15m}`.
- `ATR_14` and SessionVWAP computed on signal timeframe.
- `z_vwap_t = (close_t - session_vwap_t) / atr_t`.
- Long if `z_vwap_t <= -z0`; short if `z_vwap_t >= z0`.

## Exit
- Frozen ATR stop: `stop_distance = k_atr_stop * ATR_entry`.
- Full TP: `tp_distance = tp_r * stop_distance`.
- Stop/TP monitored on `1m` bars.

## 24-run grid
- `signal_timeframe ∈ {5m, 15m}`
- `z0 ∈ {0.8, 1.0, 1.2}`
- `tp_r ∈ {0.5, 1.0}`
- `k_atr_stop ∈ {2.5, 3.5}`
