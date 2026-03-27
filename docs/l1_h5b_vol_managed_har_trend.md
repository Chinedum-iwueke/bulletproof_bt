# L1-H5B Volatility-Managed Exposure Overlay on HAR-Gated Trend

## Scope
L1-H5B is the first production second-stage L1-H5 variant.
It preserves L1-H3A and adds only a continuous volatility-managed sizing overlay.

## Why L1-H5B follows L1-H5A
- L1-H5A validated the volatility overlay mechanism on the simpler L1-H1 baseline.
- L1-H5B applies the same sizing idea to the stronger HAR-gated trend family (L1-H3A) without changing directional logic.
- This ordering isolates the incremental effect of volatility-managed sizing on an already filtered trend regime.

## Why L1-H3A is the base family
- L1-H3A already locks HAR RV forecasting, deterministic OLS refit discipline, and HAR percentile gating.
- L1-H3A already locks frozen `close * sqrt(RV_hat)` stop distance.
- L1-H5B inherits those controls intact and layers sizing only.

## Locked two-clock runtime model
- Canonical input data: **1m** OHLCV.
- Signal timeframe: **15m** completed bars.
- Exit monitoring timeframe: **1m** bars.
- Entries are computed only on completed signal bars.
- Stop checks remain active on every 1m bar.
- Time stop `T_hold` is measured in completed signal bars.

## Preserved L1-H3A entry and exit family
- Direction: `trend_dir_t = sign(EMA20 - EMA50)` on 15m signal bars.
- Gate: HAR `RV_hat_t` percentile gate (`rvhat_pct_t >= gate_quantile`).
- Stop distance: `k * close_t * sqrt(RV_hat_t)`.
- Stop update policy: frozen at entry.
- No TP logic, no trailing/chandelier, no extra gates.
- No pyramiding.

## Volatility-managed sizing overlay (only new feature)
On completed signal bars:
- `r_t = ln(close_t / close_{t-1})`
- `sigma_t = sqrt(mean(r^2 over last W))`
- `W in {24h, 72h}`; on 15m bars this is `{96, 288}` bars.

Deterministic reference:
- `sigma_star = rolling_median(sigma_t over trailing past-only window)`
- Baseline production reference window: `72h` (288 signal bars).

Scale and size:
- `s_t = clip(sigma_star / sigma_t, s_min, s_max)`
- `s_min in {0.25, 0.5}`
- `s_max in {1.0, 1.5}`
- `qty_R = (equity * r_per_trade) / (k * close_t * sqrt(RV_hat_t))`
- `qty_final = qty_R * s_t`

## Required logging fields
At entry, decision metadata includes:
- HAR fields: `RV_hat_t`, `rvhat_pct_t`, `fit_ts_used`, `fit_window_days`
- Overlay fields: `sigma_t`, `sigma_star`, `s_t`, `qty_R`, `qty_final`, `cap_hit_lower`, `cap_hit_upper`, `vol_window_hours`
- Runtime semantics fields: `signal_timeframe`, `exit_monitoring_timeframe`, `hold_time_unit`

## Falsification logic and failure modes
Reject baseline L1-H5B if:
- tail risk metrics (p99(-R), drawdown depth, drawdown duration) do not improve enough versus L1-H3A, or
- EV retention versus L1-H3A is materially worse than achieved tail reduction.

Expected failure modes:
- downscaling before high-conviction trend acceleration,
- sigma reference lag around structural breaks,
- persistent clipping at `s_min`/`s_max` indicating poor calibration.
