# L1-H4A Liquidity Uncertainty Gate Applied to Mean Reversion

## Hypothesis
L1-H4A tests whether a deterministic liquidity-uncertainty proxy improves **net** expectancy for the L1-H2 SessionVWAP mean-reversion family by suppressing entries during high spread-uncertainty regimes.

This is a proxy-based research baseline. It does **not** claim direct observation of true bid-ask spread.

## Why L1-H2 is the baseline
L1-H2 already provides:
- SessionVWAP fade entries on the signal clock,
- frozen ATR stop,
- VWAP-touch profit exit,
- signal-bar time stop,
- and locked two-clock semantics.

L1-H4A adds only one new overlay: a liquidity gate.

## Locked semantics
- **Canonical base data:** `1m` OHLCV.
- **Signal timeframe default:** `5m`.
- **Exit monitoring timeframe:** `1m`.
- **Baseline reference:** `L1-H2`.
- **Strategy family:** mean reversion.
- **VWAP primitive:** SessionVWAP (`vwap_mode=session`).
- **Time stop unit:** completed signal bars (`hold_time_unit=signal_bars`).
- **No pyramiding:** enabled.

## Spread proxy and gate
Signal-timeframe feature:

- `spread_proxy_t = 0.5 * (high_t - low_t) / close_t`

Liquidity gate:

- `liq_gate_t = spread_proxy_t <= quantile(spread_proxy_{t-L..t-1}, q_liq)`
- `L` = 30 calendar days in signal bars (for `5m`: `8640` bars).
- Quantile is deterministic, linear interpolation, and past-only.

Warmup behavior:
- no gate decision until the full trailing window is populated,
- no entries until ATR, SessionVWAP, and gate are all ready.

## Preserved L1-H2 entry/exit family
Entry signal on completed signal bars:
- `z_vwap_t = (close_t - SessionVWAP_t) / ATR_14_t`
- long if `z_vwap_t <= -z0` and `liq_gate_t == True`
- short if `z_vwap_t >= z0` and `liq_gate_t == True`

Exit family (unchanged from L1-H2 baseline):
- frozen ATR multiple stop at entry,
- VWAP-touch profit exit monitored on 1m,
- time stop measured in completed signal bars,
- no trailing/chandelier/spread-forced exit logic in this baseline.

## Parameter grid
- `q_liq ∈ {0.6, 0.7}`
- `z0 ∈ {0.8, 1.0}`
- `k_atr ∈ {1.5, 2.0}`
- `T_hold ∈ {12, 24}`
- Future-ready (inactive baseline sizing extension):
  - `size_adjustment_enabled ∈ {false}`
  - `cap_multiplier ∈ {0.5, 0.75}`

## Required logging fields
At entry, L1-H4A logs:
- `spread_proxy_t`, `liq_gate_t`, `q_liq`, `q_threshold_t`
- `vwap_t`, `z_vwap_t`, `entry_reason`
- `atr_entry`, `stop_distance`, `stop_price`
- `signal_timeframe`, `exit_monitoring_timeframe`
- `effective_spread_assumptions` snapshot when execution metadata is available

Fill/exit artifacts continue to use standard cost fields (`spread_cost`, `slippage_cost`, `fee_cost`) and existing MFE/MAE outputs.

## Falsification and failure modes
Falsification:
- if spread proxy does not track realized slippage/EV degradation, the proxy is insufficient and should be replaced or augmented by quote/L2 data.

Expected failure modes:
- proxy conflates volatility and spread,
- gate degrades into a volatility proxy rather than a liquidity proxy.
