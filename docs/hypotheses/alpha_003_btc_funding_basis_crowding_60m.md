# BTC funding-basis crowding and subsequent 60m return

This Tier2B research-only hypothesis asks exactly: **Does point-in-time positive BTCUSDT funding stress combined with a positive completed-bar mark-to-index basis predict a lower BTCUSDT close-to-close return over the next 60m than matched controls?**

At a 5m rollover, only the five closed 1m intervals in the preceding bucket are used. Funding is a latest-source-time backward join with `funding_source_ts <= decision_ts`; the rollover bar is excluded. Funding stress is measured against a past-only fixed 90-day (25,920 decision) distribution. The current completed close is included in trailing 60m return and trailing 6h realized volatility. The 60m target requires twelve complete, contiguous 5m buckets.

Controls are selected deterministically, one-to-one without reuse, by nearest past-60m return, past-6h volatility, and UTC funding-cycle position. Every decision remains in the evaluation artifact as treated, control, invalid, or failed-unmatched. Threshold selection uses validation matched-control evidence—not engine PnL—and each variant receives only its own evaluation.

The strategy is an engine adapter for short entries and explicit close-only 60m exits. The classic engine alone owns fills, costs, delay, sizing, stops, margin and accounting. This package grants no capital, order, promotion, or self-approval authority.
