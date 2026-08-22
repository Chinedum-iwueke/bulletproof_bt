# BT-002 Validation

**Date:** 2026-08-22
**Scope:** bounded OHLCV immutable snapshot and point-in-time availability contract

## Automated acceptance

The focused suite covers deterministic identity, JSON Schema validation, partition
digest replay, duplicate rejection, explicit gap accounting, UTC clock semantics,
future-availability rejection, prospective membership, correction lineage, and
source-lake non-mutation.

```text
Focused and adjacent data/identity suites: 36 passed
Full BT-001 matrix: 1,241 collected; 1,214 passed; 27 existing explicit skips
Scoped Ruff: clean
```

## Read-only lake smoke

One existing canonical Binance `PRLUSDT` 1-minute OHLCV partition was inspected
read-only. No raw values or protected absolute paths are committed.

```json
{
  "status": "valid",
  "partition_count": 1,
  "row_count": 65254,
  "snapshot_id": "039e05ff-c94e-517e-a16d-58c189411089",
  "dataset_digest": "0d0c5ebe5142fbdb265e5714009772c14e92ce0e8c2038f615c2774ee20426f5",
  "manifest_digest": "6a301840222500fc03c00ce693976fabd355db828f73b2030beb58884f36927c",
  "source_replayed": true
}
```

The source file size and modification timestamp were unchanged after manifest
construction. The derived manifest was written to a temporary directory outside
the lake and removed with that temporary workspace.

## Registration boundary

This milestone produces the digest-bound validation report required for RI
registration. Publishing it to Hermes is deliberately deferred to BT-003/BT-008;
BT-002 does not introduce a second evidence registry or a network write path.
