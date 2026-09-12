# EXEC-011 Venue Telemetry and Replay

Bulletproof owns canonical venue-event normalization, point-in-time replay,
reconciliation, and trade-episode reconstruction. Hermes registers the immutable
producer receipt and a bounded projection; it does not recalculate execution state.

Every event binds the venue, shadow/demo/live environment, account pseudonym,
canonical instrument, exchange/source/receive clocks, stream sequence and cursor,
raw-reference digest, normalization version, and correction or reconciliation
lineage. Raw private payloads and credentials are forbidden.

`replay_venue_telemetry` is deterministic under input reordering, suppresses exact
duplicates, rejects identity collisions, applies immutable corrections, reports
sequence gaps, and fails the projection to `degraded` on REST/stream disagreement.
Its projection covers orders, fills, positions, cash, margin, fees, funding,
incidents, and completed trade episodes.

Run the no-capital fixture:

```bash
PYTHONPATH=src python scripts/exec011_pilot.py --output /tmp/exec011-native.json
```

Passing this fixture establishes source and replay behavior only. It does not certify
a private venue stream, demo account, live account, or order-submission path.
