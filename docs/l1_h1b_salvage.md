# L1-H1B salvage fork notes

L1-H1B keeps the L1-H1 volatility-floor + EMA continuation entry, but changes exit architecture:

- initial stop is frozen ATR multiple at entry (`k_atr_entry_stop`),
- fixed take-profit is disabled,
- optional chandelier trailing stop activates by either `trail_activate_after_bars` or `trail_activate_after_profit_r`,
- effective stop after trail activation is the tighter of initial and chandelier stops.

Pathwise diagnostics are emitted to `trades.csv` for closed trades, including `mfe_r`, `mae_r`, hold/time-to-MFE metrics, and trail activation diagnostics.
