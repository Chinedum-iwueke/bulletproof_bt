# State Feature Layer (Phase 9)

The state layer computes causal `entry_state_*` features per symbol/timestamp using past-only rolling windows.

Highlights:
- Trend: EMA fast/slow, slopes, relationship.
- Volatility: ATR, ATR%, realized vol percentiles.
- Liquidity: spread proxy percentile, volume/dollar volume, liquidity regime.
- Displacement: true range, `tr_over_atr`, displacement regime.
- CSI proxy (no aux feeds required):
  - `csi_proxy = 0.35*vol_pctile + 0.35*tr_over_atr_pctile - 0.30*spread_proxy_pctile` clipped to `[0,1]`.
- Readiness flags: `entry_state_trend_ready`, `entry_state_vol_ready`, `entry_state_liquidity_ready`, `entry_state_csi_ready`, `entry_state_htf_ready`.

No-lookahead rule: percentiles and rolling metrics are computed using only rows at or before `ts`.
