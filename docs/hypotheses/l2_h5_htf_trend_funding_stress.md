# L2-H5 HTF Trend x Funding Stress Gate

## Claim

Trend pullback entries are higher quality when closed-bar HTF trend aligns with low funding stress. High positive funding stress indicates crowded positioning and higher reversal or forced-deleveraging risk.

## What Is Being Tested

L2-H5 extends the L2-H1 HTF trend pullback setup:

- closed `1h` EMA50/EMA200 defines trend direction;
- `5m` EMA20 pullback/recovery defines entry timing;
- funding z-score blocks entries when stress is high.

Funding stress is:

```text
fund_z = zscore(funding_rate over prior funding events)
stress = fund_z > funding_z_threshold
```

The grid tests:

```text
funding_z_threshold ∈ {1.0, 1.5}
funding_lookback_days ∈ {14, 30}
```

The strategy samples funding once per unique `funding_source_ts`, not once per repeated 1m as-of row.

## Causality Contract

- Funding is accepted only when `funding_source_ts <= decision_ts`.
- Future funding timestamps are rejected and never used for entries.
- Funding z-score is based on previously observed funding events.
- HTF trend uses strictly closed 1h bars.
- The classic engine remains authoritative for fills, costs, PnL, stops, liquidation checks, and artifacts.

## Entry And Exit

Entry:

- same LTF pullback/recovery logic as L2-H1;
- HTF trend must align;
- `fund_z <= funding_z_threshold`.

Exit:

- ATR stop;
- time stop;
- funding flip;
- funding unwind back inside `funding_unwind_band`.

## Required Evidence

Every entry logs:

- `funding_rate`
- `fund_z`
- `funding_source_ts`
- `funding_source_valid`
- `funding_provenance_hash`
- `funding_history_count`
- HTF trend state
- pullback state
- stop/risk metadata
- decision trace

## Falsification

Remove the funding gate if stressed and normal regimes do not differ in EV, tail risk, or forced liquidation frequency. Reject any apparent edge that depends on future funding timestamps or exchange-specific funding quirks.
