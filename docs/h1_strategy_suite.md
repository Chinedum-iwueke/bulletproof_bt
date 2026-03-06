# H1 Strategy Suite: Exit Type Configuration

## Variant B: `volfloor_donchian`

Supports `strategy.exit_type`:
- `donchian_reversal` (default): Exit on 10-bar Donchian reversal.
- `chandelier`: Exit on Chandelier ATR trail cross.
- `partial_donchian`: Take one partial at `+partial_take_profit_r` (default `1.0R`) for `partial_fraction` (default `0.5`), then remainder exits via Donchian reversal.
- `partial_chandelier`: Same partial rule, then remainder exits via Chandelier.

Parameters:
- `chandelier_lookback` (default: `22`)
- `chandelier_mult` (default: `2.5`)
- `partial_fraction` (default: `0.5`)
- `partial_take_profit_r` (default: `1.0`)
- `er_lookback` (default: `10`)
- `er_min` (default: disabled / `None`)

## Variant A: `volfloor_ema_pullback`

Entry logic:
- Long bias: `EMA20 > EMA50`, `ADX >= adx_min`, `vol_pct > vol_floor_pct`, `ER > er_min`
- Short bias: `EMA20 < EMA50`, `ADX >= adx_min`, `vol_pct > vol_floor_pct`, `ER > er_min`
- Long entry: `Low <= EMA20` and `Close > EMA20`
- Short entry: `High >= EMA20` and `Close < EMA20`

Supports `strategy.exit_type`:
- `ema_trend_end` (default): Exit long when `EMA20 < EMA50`; short when `EMA20 > EMA50`
- `chandelier`: Exit on Chandelier ATR trail cross

Parameters:
- `chandelier_lookback` (default: `22`)
- `chandelier_mult` (default: `2.5`)
- `stop_atr_mult` (default: `2.0`)
- `er_lookback` (default: `10`)
- `er_min` (default: disabled / `None`)

All strategy metadata includes the selected `exit_type` for deterministic run artifacts (`config_used.yaml`, decisions, and signal metadata).
