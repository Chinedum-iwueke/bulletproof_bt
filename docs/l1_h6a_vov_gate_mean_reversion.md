# L1-H6A Volatility-of-Volatility Gate Mean Reversion

## Scope
L1-H6A is the first production version of the L1-H6 line. It is intentionally narrow:
- baseline family: **L1-H2 mean reversion**
- new overlay: **volatility-of-volatility instability gate**
- no trailing stop redesign
- no trend-family integration
- no active dynamic size scaling

## What L1-H6A tests
Claim: when volatility itself becomes unstable, short-horizon execution quality deteriorates and cost-adjusted EV degrades. L1-H6A tests that claim as a causal gate layered on L1-H2.

## Locked runtime semantics
- canonical input data: **1m**
- signal timeframe: **5m**
- exit monitoring timeframe: **1m**
- strategy family: **mean_reversion**
- baseline reference: **L1-H2**
- entry family: SessionVWAP fade with ATR-normalized deviation
- stop/exit family: inherited L1-H2 (frozen ATR stop, VWAP-touch, signal-bar time stop)
- time stop unit: **completed signal bars**
- no pyramiding

## Feature definitions
On completed signal bars:
- `z_vwap_t = (close_t - SessionVWAP_t) / ATR_14_t`
- `rv_t = ATR_14_t / close_t`
- `vov_t = rolling_std(rv_t over Wvov bars)`

Gate rule:
- `vov_gate_t = (vov_t <= trailing_quantile(vov_history, q_vov))`

All calculations are past-only and deterministic.

## Window mapping
Default signal timeframe is 5m. With 24/7 assumptions:
- `Wvov_hours=24` → `288` bars
- `Wvov_hours=72` → `864` bars

Quantile reference history uses a trailing 30 calendar day lookback on the signal timeframe:
- `30d @ 5m = 8640` bars

## Entry and exit behavior
Entry preconditions:
1. indicators ready (`SessionVWAP`, `ATR_14`, `vov_t`, quantile gate)
2. `vov_gate_t` is `True`
3. mean-reversion trigger:
   - long if `z_vwap_t <= -z0`
   - short if `z_vwap_t >= z0`

Exits are unchanged from L1-H2:
- frozen ATR stop from entry
- VWAP-touch profit exit on 1m monitoring
- time stop in signal bars

## Initial 24-run constrained grid
Overlay combinations (4):
- `Wvov_hours ∈ {24,72}`
- `q_vov ∈ {0.6,0.7}`

Pre-registered base combinations (6):
1. `z0=0.8, k_atr=1.5, T_hold=12`
2. `z0=0.8, k_atr=1.5, T_hold=24`
3. `z0=0.8, k_atr=2.0, T_hold=12`
4. `z0=1.0, k_atr=1.5, T_hold=12`
5. `z0=1.0, k_atr=2.0, T_hold=12`
6. `z0=1.0, k_atr=2.0, T_hold=24`

Total first-pass budget: **24 runs**.

## Required logging fields
At entry, L1-H6A records:
- `rv_t`, `vov_t`, `Wvov_hours`, `q_vov`, `q_threshold_t`, `vov_gate_t`
- `session_vwap_t`, `z_vwap_t`, `entry_reason`
- `signal_timeframe`, `exit_monitoring_timeframe`

## Falsification and interpretation discipline
Reject L1-H6A if vov buckets fail to separate realized slippage or net EV in a consistent way. L1-H6A is a volatility-instability proxy test, not a direct jump model.
