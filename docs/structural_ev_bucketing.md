# Structural EV Bucketing (Phase 9)

`E[R]` on all trades is not enough. We need `E[R | structure]`.

Implemented outputs include per-bucket EV for:
- CSI
- Volatility
- Liquidity
- Displacement
- Setup class
- Joint buckets (CSI×Vol, CSI×Liquidity, Vol×Liquidity)

If required fields are missing, we emit `summaries/ev_by_bucket_missing_fields.json` and skip only affected analyses (never silently collapse to only `all`, except baseline `overall_all_trades`).
