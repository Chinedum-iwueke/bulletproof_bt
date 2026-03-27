# L1-H5A Volatility-Managed Exposure Overlay on L1-H1

## Scope
L1-H5A is the first production L1-H5 variant.
It preserves the L1-H1 trend continuation family and adds only a continuous inverse-volatility sizing overlay.

## Why L1-H1 base and 15m signal clock
- Base family is L1-H1 EMA20/EMA50 continuation, because this family is known and already production-locked.
- Signal clock remains 15m to preserve L1-H1 semantics and avoid introducing a new timing degree of freedom.

## Locked two-clock runtime model
- Canonical input data: **1m** OHLCV.
- Signal timeframe: **15m** completed bars.
- Exit monitoring timeframe: **1m** bars.
- Entries are computed only when a new completed 15m bar is available.
- Stop checks remain active on each 1m bar.
- `T_hold` is counted in completed signal bars.

## Preserved L1-H1 entry/exit family
- Entry direction: `trend_dir_t = sign(EMA20 - EMA50)` on 15m signal bars.
- No new gates.
- Stop model: fixed ATR multiple, frozen at entry.
- Time stop model: unchanged from L1-H1, measured in signal bars.
- No pyramiding.

## Volatility-managed sizing overlay (only new feature)
On completed signal bars:
- `r_t = ln(close_t / close_{t-1})`
- `sigma_t = sqrt(mean(r^2 over last W))`
- `W ∈ {24h, 72h}` where on 15m bars this is `{96, 288}` bars.

Deterministic target-vol reference:
- `sigma_star = rolling_median(sigma_t over trailing past-only window)`
- First production reference window is deterministic and causal.

Scale factor:
- `s_t = clip(sigma_star / sigma_t, s_min, s_max)`
- `s_min ∈ {0.25, 0.5}`
- `s_max ∈ {1.0, 1.5}`

Position size:
- `qty_R = (equity * r_per_trade) / stop_distance`
- `qty_final = qty_R * s_t`

## Required logging fields
At entry, metadata includes:
- `sigma_t`, `sigma_star`, `size_factor_t`
- `vol_window_hours`
- `qty_R`, `qty_final`
- `cap_hit_lower`, `cap_hit_upper`
- `signal_timeframe`, `exit_monitoring_timeframe`
- baseline stop/entry fields inherited from L1-H1

## Falsification discipline
Reject for a component if:
- EV retention is materially worse than crash-tail reduction, or
- p99(-R), max drawdown, and drawdown duration do not improve enough to justify EV loss.

Expected failure modes:
- downscaling before large trend bursts,
- lagged sigma estimates during jump regimes.
