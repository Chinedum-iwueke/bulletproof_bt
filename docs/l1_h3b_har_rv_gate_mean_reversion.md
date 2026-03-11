# L1-H3B HAR-gated L1-H2 Mean Reversion

## Scope (locked first production version)

L1-H3B is **HAR-gated L1-H2** only:
- signal timeframe: `5m`
- base engine data: canonical `1m`
- entry family: L1-H2 SessionVWAP mean-reversion fade (`z_vwap` thresholds)
- replacement components only:
  - ATR compression gate -> HAR `RV_hat` low-percentile gate
  - ATR stop distance -> `k * close_t * sqrt(RV_hat_t)` frozen at entry

No trend-family migration, no ML, no ADX/ER filters, no trailing/chandelier logic, no extra TP model.

## Realised-vol definitions

On signal bars:
- `rv1_t = (ln(close_t / close_{t-1}))^2`
- `RV_d = mean(rv1 over last 1 calendar day)`
- `RV_w = mean(rv1 over last 7 calendar days)`
- `RV_m = mean(rv1 over last 30 calendar days)`

For 5m bars (24/7 crypto):
- bars/day = `288`
- `RV_d` window = `288`
- `RV_w` window = `2016`
- `RV_m` window = `8640`

## HAR model and fit discipline

Forecast:
- `RV_hat = a + b*RV_d + c*RV_w + d*RV_m`

Fit discipline (locked):
- deterministic OLS only
- rolling fit windows: `{180, 365}` calendar days
- coefficients fit on past-only observations strictly prior to decision timestamp
- refit cadence: once per completed signal day
- warmup requires: `RV_m` readiness + eligible fit-window observations + gate history

## Entry and gate rules

SessionVWAP fade baseline (unchanged family):
- `z_vwap_t = (close_t - SessionVWAP_t) / ATR_14_t`
- long if `z_vwap_t <= -z0`
- short if `z_vwap_t >= z0`

HAR low-vol gate (replacement):
- compute causal rolling percentile rank `rvhat_pct_t` of `RV_hat_t`
- allow fade entry iff `rvhat_pct_t <= gate_quantile_low`
- preregistered `gate_quantile_low` grid: `{0.3, 0.7}`
  - `0.3`: strict low-forecast-vol compression gate
  - `0.7`: broader/negative-control threshold (less strict compression)

## Stop, hold, and exit semantics

At entry:
- `stop_distance = k * close_t * sqrt(RV_hat_t)`
- stop distance and stop price are frozen at entry
- no trailing/widening/tightening after entry

Time stop:
- unchanged from L1-H2 family
- `T_hold` counted in completed **signal bars** (5m), not base 1m bars

Profit exit:
- VWAP-touch only, monitored on 1m base bars
  - long exits when monitored close >= active SessionVWAP
  - short exits when monitored close <= active SessionVWAP

## Two-clock semantics

- signal indicators and entry decisions run only on completed 5m bars
- stop and VWAP-touch exits are monitored on base 1m bars

## Artifacts and audit outputs

Per run, strategy artifacts include:
- `har_coefficients.json`: fit timestamps, `a,b,c,d`, fit window days, train spans
- `har_split_manifest.json`: walk-forward split and applicability metadata

Decision metadata includes:
- `RV_hat_t`, `rvhat_pct_t`, `gate_pass`, `fit_ts_used`, `fit_window_days`
- `z_vwap_t`, `session_vwap_t`, `entry_reason`, `stop_distance`, `risk_amount`

## Falsification logic and expected failure modes

Reject L1-H3B baseline if:
- out-of-sample Tier2/Tier3 `EV_r_net` does not improve versus L1-H2
- drawdown-duration and tail behavior do not improve consistently across 180d and 365d fits

Expected failure modes:
- low-vol gate may miss profitable fades during transition regimes
- coefficient staleness after structural market breaks despite daily refit cadence
- broader `gate_quantile_low=0.7` can dilute strict compression selectivity
