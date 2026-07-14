# L2-H2 Entry-Timeframe Boundary Gate

## Claim

Restricting entries to deterministic higher-timeframe boundaries increases net EV by reducing mid-bar micro-noise entries while preserving 1m exit monitoring.

## What Is Being Tested

This hypothesis tests an execution-control layer, not a new alpha formula. The underlying control signal is the existing `l1_h1_vol_floor_trend` strategy running on a 1m signal clock. The experiment varies only `entry_timeframe`:

- `none`: ungated control
- `5m`: entries only on 5-minute UTC boundaries
- `15m`: entries only on 15-minute UTC boundaries
- `1h`: entries only on hourly UTC boundaries

The engine applies `EntryTimeframeGate` after the strategy emits candidate signals. Entry signals are filtered by `is_timeframe_boundary(ts, entry_timeframe)`. Exit signals remain eligible on every 1m bar.

## Why This Matters

Crypto trades continuously, which means weak lower-timeframe signals can appear inside noisy microstructure bursts. Boundary gating is a cheap deterministic control: it asks whether waiting for a higher-timeframe timestamp boundary removes low-quality turnover without deleting the real edge.

## Causality Contract

- Boundary checks use only the current UTC timestamp.
- No future OHLCV, funding, OI, mark, or index data is used.
- Exits are not boundary-gated.
- Missing bars remain missing decisions.
- The classic engine remains authoritative for fills, stops, PnL, liquidation checks, and artifacts.

## Required Evidence

The run should compare each gated variant with the `none` control on:

- trade count reduction
- turnover reduction
- net EV change
- gross EV change
- cost drag
- drawdown duration
- under-risked trade rate

The gate adapter writes `entry_timeframe_gate.json` with allowed, blocked, and preserved-exit counts. Allowed trade rows include `allow_entries`, `entry_timeframe_boundary`, and `entry_timeframe_gate_applied` metadata. Blocked entry candidates are intentionally not converted into trades.

## Falsification

Scrap or redesign the gate if:

- net EV drops materially without drawdown or cost benefits,
- 1h gating only looks good because sample size collapses,
- the ungated control has better net EV and acceptable turnover,
- improvements fail to survive Tier3 execution assumptions.

## Expected Failure Mode

The gate may miss the first minute of a fast move. If that happens, the cost savings from reduced churn may not compensate for lower capture of impulsive winners.
