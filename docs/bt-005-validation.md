# BT-005 validation

BT-005 qualifies the existing classic execution mechanics; it does not introduce a second backtest engine or claim empirical calibration that does not exist.

## Contract

- Every model card declares its kind, implementation, applicability, timestamp semantics, parameters, uncertainty, stress range, provenance, fallback and incompatibilities.
- Empirical model cards require a digest-bound calibration sample plus fit and holdout diagnostics.
- The declared classic bundle records fee, spread, fixed-slippage/impact, bar-delay and market-fill behavior already implemented by Bulletproof.
- Rebates, funding cashflows, borrow availability/cost and capacity-constrained fills are explicitly unsupported. Runs requiring them fail closed instead of treating absent costs as zero.
- Atomic run finalization requires `market_model_bundle.json`; the document, its cards and the lineage `market_model_bundle_digest` are all verified.
- Pessimistic cost stress is monotone: a worse assumption may not reduce modeled execution costs.

## Retained model evidence

The classic Tier 1/2/3 values remain founder-declared policy profiles, not observationally calibrated venue estimates. Their model cards therefore use `source=declared-policy`, contain no invented calibration dataset, and expose conservative stress intervals. Future empirical replacements must use a new version and immutable calibration digest; prior run bundles retain their original digest.

## Acceptance evidence

Focused tests cover deterministic registry serialization, duplicate identity rejection, empirical provenance validation, unsupported funding/borrow/capacity, monotone stress, required bundle artifacts, lineage mismatch, tampering, run-bundle replay, spread ordering, fee/slippage reconciliation and existing stress behavior. Full pinned-suite and CI receipts are recorded in the merged PR.

Rollback pins the prior market-model bundle version and marks dependent evidence superseded when assumptions change. Removing BT-005 admission does not alter the classic engine's trading or accounting code.
