# Immutable Dataset and Availability Contract

BT-002 adds a derived, read-only snapshot contract around the existing research
data lake. It does not replace the lake layout, loaders, coverage catalog, universe
builder, or canonical Parquet data.

## Contract

Each snapshot binds:

- the SHA-256, byte size, schema, row count, event range, availability range,
  duplicate count, and gap-interval count of every selected partition;
- the canonical BASE-002 identity and a deterministic snapshot UUID;
- UTC event-clock semantics, including whether `ts` means bar open or bar close;
- recorded `available_at` values or a declared deterministic close-plus-lag rule;
- a knowledge cutoff that excludes observations not yet available;
- point-in-time universe membership, including when membership became known;
- an append-only correction ledger that identifies replaced content without
  mutating the earlier snapshot;
- provenance, source-root alias, and access classification.

Missing bars remain missing. The builder does not forward-fill them. Corrections
produce successor snapshots; they never rewrite an existing snapshot identity.

## Bounded build

The command requires explicit partitions and writes the derived manifest outside
source partitions:

```bash
python scripts/build_dataset_snapshot.py build \
  --source-root /protected/research_data \
  --partition /protected/research_data/canonical/perp/bybit/BTCUSDT/timeframe=1m/ohlcv.parquet \
  --membership /derived/contracts/btc-membership.json \
  --output /derived/contracts/btc-ohlcv-snapshot.json \
  --source bybit-public-api \
  --market perp \
  --exchange bybit \
  --timeframe 1m \
  --timestamp-semantics bar_open \
  --knowledge-cutoff 2026-08-22T00:00:00Z
```

Replay source integrity before a run:

```bash
python scripts/build_dataset_snapshot.py validate \
  --manifest /derived/contracts/btc-ohlcv-snapshot.json \
  --source-root /protected/research_data
```

The manifest and validation report are safe inputs to the future RI registration
adapter. Raw rows and absolute protected-data paths are not embedded.

## Scope boundary

BT-002 certifies only explicitly selected partitions. It does not certify the
entire lake, infer vendor corrections, manufacture unavailable membership history,
or grant data entitlements. Wider catalog promotion belongs to DATA-001 through
DATA-003.
