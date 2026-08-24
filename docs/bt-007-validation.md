# BT-007 Representation Contract Validation

BT-007 certifies the representation boundary above Bulletproof's existing immutable
dataset snapshots, canonical research panels, classic feature builders and strategy
implementations. It does not replace those implementations or introduce an alternate
backtest path.

## Contract

`representation-contract-v1.0.0` binds:

- dataset snapshot identity and digest;
- repository commit and representation code digest;
- decision and entity clocks;
- point-in-time universe membership columns;
- each representation, feature and label's sources, transform identity, implementation
  digest, observation and availability clocks, warm-up, missingness and completeness;
- stateless or train-only fit policy and fitted-state digest;
- label horizon; and
- train, validation, test, fit, purge and embargo boundaries.

The emitted contract and leakage report are canonical, digest-bound JSON. A BT-006
search plan binds the representation digest before trials exist. A BT-003 run bundle
cannot finalize unless it contains the matching contract and a certified, tamper-evident
leakage report.

## Fail-closed checks

The causal replay rejects future observations or joins, revisions unavailable at the
decision clock, premature labels, incomplete higher-timeframe values, warm-up output,
undeclared missing values, duplicate decision identities, point-in-time universe
leakage, expired membership, evaluation rows outside the registered split, overlapping
splits, insufficient purge or embargo and fit state learned outside training.

## Retained validation

- Focused representation, dataset, feature, search-plan, runner and run-bundle slice:
  57 tests pass.
- The fresh-process CLI reconstructs and certifies a deterministic fixture from JSON
  plus Parquet.
- The JSON Schema validates the emitted contract and digest tampering is rejected.
- Scoped Ruff passes.
- The pinned Python 3.11 suite passes 1,258 tests with 27 explicit skips and two
  pre-existing pandas warnings. The exact GitHub CI matrix is the merge gate.

Rollback stops new certification and pins an earlier representation digest. Existing
contracts, leakage reports, failed audits and dependent run bundles remain immutable;
descendants of a superseded representation must be marked stale by the institutional
registry.
