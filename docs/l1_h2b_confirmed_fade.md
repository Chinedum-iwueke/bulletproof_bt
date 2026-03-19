# L1-H2B mechanism notes

Hypothesis statement:

> In volatility-compression regimes, SessionVWAP deviations are tradable only after failed expansion is confirmed. The expected edge comes from trapped continuation participants unwinding after price extends away from VWAP, fails to sustain the move, and re-enters inward toward session fair value.

Entry is a state machine:
1. compression gate must be true,
2. extension away from SessionVWAP arms setup (`z_vwap` beyond `z_ext`),
3. inward re-entry confirmation triggers fade entry (`z_vwap` back inside `z_reentry`),
4. optional reversal-close confirmation can be required.

Exits remain mean-reversion identity: frozen ATR stop, SessionVWAP touch, short signal-bar time stop.
