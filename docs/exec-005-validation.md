# EXEC-005 validation

EXEC-005 calibrates existing Bulletproof execution models against point-in-time
observations. It does not submit orders or create another execution engine.

## Contract

- Each observation binds an immutable identity, source-event digest, venue, listing,
  order type, side, size bucket, regime, sample role and causal availability time.
- Calibration and holdout identities are disjoint. Observations unavailable at the
  declared `known_at` are excluded rather than leaked into the fit.
- Fill probability, partial fills, censoring, acknowledgement/fill latency,
  implementation shortfall, model error and adverse selection retain uncertainty.
- Results are stratified by venue, order type, size bucket and regime. Sparse,
  censored, unavailable or shifted strata select an explicit pessimistic fallback.
- Pessimistic execution cost cannot improve on the empirical upper bound or the
  founder-declared prior bound.
- The resulting BT-005 model bundle binds empirical provenance, calibration window,
  dataset digest, fit diagnostics, holdout diagnostics, applicability and fallback.
- The producer receipt binds EXEC-001, EXEC-002, EXEC-004, SHADOW-001, the prior
  BT-005 bundle, dataset, configuration and all output digests.

## Acceptance evidence

Focused tests cover point-in-time exclusion, duplicate and conflicting identities,
partial and censored fills, sparse samples, holdout shift, monotone pessimistic cost,
invalid clocks, incomplete fills, deterministic replay, dependency drift and bundle
tampering. The deterministic pilot contains no venue credentials and has no capital,
order, routing, allocation or promotion authority.

The pilot fixture proves the calibration and fallback contract. It does not claim that
its fixture values are calibrated estimates for Bybit, Binance or a production order
path. Those claims require retained prospective observations from the certified
shadow/demo/live adapters.
